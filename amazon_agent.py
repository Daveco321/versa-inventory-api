"""
amazon_agent.py: the Amazon agent. On weekday mornings it reads Amazon and Transcal, works out
what Amazon needs, sets up an UNCONFIRMED Amazon draft and emails David the plan with the Amazon
SKU list and the Transcal pick ticket attached. It never ships on its own (David, Nov 23 2026):
only an approver's reply does anything.

    agent = Agent(deps)
    agent.run(now=None, dry_run=False)                 -> dict saying what happened
    agent.handle_reply(sender, subject, text, now=None) -> dict
    agent.status()                                      -> dict for GET /api/amazon/status
    agent.update_settings(changes)                      -> settings (no Amazon data in them)

Replies reach it through the amazon@ mailbox (email_agent.py checks the sender is who they claim):
    YES ...                          confirm at Amazon, email Transcal the ticket and box labels,
                                     then tell David it is done
    NO / cancel                      cancel the draft; nothing goes anywhere
    drop <style>                     a new draft without that style and a new email
    only N cartons of <style>        the same, with that style cut to N cartons
    anything else                    a short note on what it can do

deps (a dict, so the server and the tests wire different things):
    client()             SpApiClient, or None while Amazon has not approved the app
    store_get(name)      / store_put(name, obj): encrypted state under inventory/amazon/
    stock_rows()         [amazon_planner.StockRow]: AM rows at Transcal with what is held for others
    prepack_resolver()   -> function(base) -> carton ratio (one rules snapshot per run)
    meta_for(base)       -> {'brand', 'color', 'fit', 'fabrication'}
    send_email(to, subject, html, attachments, cc=None) -> (ok, detail)
    pull(client)         optional, default amazon_sync.pull
    inbound              optional, default the amazon_inbound module (tests pass a fake)

No email address, account id or Amazon number is written in this file: settings and state live
encrypted, and this repository is public.
"""
import base64
import re
import threading
from datetime import datetime, timedelta, timezone

import amazon_inbound as _inbound_module
import amazon_pick_ticket as T
import amazon_planner as P
import amazon_proposal as M
import amazon_skus as K
import amazon_sync as Y

STATE = 'state'
SETTINGS = 'settings'
SNAPSHOT = 'snapshot'
LOG_MAX = 300

DEFAULT_SETTINGS = {
    'david': '',                 # who gets every proposal
    'approvers': [],             # whose reply can ship (lower case)
    'transcal_to': [],           # Transcal's pick-ticket inbox
    'transcal_cc': [],           # copied on the Transcal email
    'source_address': None,      # Amazon AddressInput for Transcal (name, addressLine1, city, ..., phoneNumber)
    'box_spec': {'length': 24, 'width': 18, 'height': 9, 'weight_lb': 25},
    'box_spec_confirmed': False, # until Transcal's real carton is known, the email says what was used
    'expire_days': 3,            # an unanswered proposal is cancelled after this many days
    'planner': {},               # overrides for amazon_planner.DEFAULTS
}

_YES = re.compile(r'^\s*(yes|yep|yeah|y|ok|okay|approved?|go ahead|good to go|go|send it|send|confirm(ed)?|'
                  r'do it|ship it|sure|looks good|sounds good|perfect|great)\b', re.I)
_NO = re.compile(r'^\s*(no|nope|cancel|stop|hold|don\'?t|do not|kill it)\b', re.I)
# a style token always holds a digit, so "no thanks" can never read as a style
_STYLE = r'([A-Z]*\d[A-Z0-9]*)'
_DROP = re.compile(r'\b(?:drop|remove|skip|take\s+out|leave\s+out)\s+' + _STYLE + r'\b', re.I)
_CAP = re.compile(r'\b(?:only\s+)?(\d{1,4})\s*(?:cartons?|ctns?|boxes|box)\s+(?:of|for)\s+' + _STYLE + r'\b', re.I)
_REF = re.compile(r'\[ref\s+([A-Z0-9-]{4,20})\]', re.I)


def _now(now=None):
    return now or datetime.now(timezone.utc)


def _iso(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


def _new_text(text):
    """The reply itself, without the quoted thread below it."""
    out = []
    for line in str(text or '').splitlines():
        s = line.strip()
        if s.startswith('>'):
            break
        if re.match(r'^on .+ wrote:$', s, re.I) or re.match(r'^-{2,}\s*original message', s, re.I):
            break
        if re.match(r'^(from|sent|to|subject):\s', s, re.I) and out:
            break
        out.append(line)
    return '\n'.join(out).strip()


class Agent:
    def __init__(self, deps):
        self.d = dict(deps)
        self._lock = threading.RLock()
        self.inbound = self.d.get('inbound') or _inbound_module
        self.pull = self.d.get('pull') or Y.pull

    # ── state ──
    def settings(self):
        s = dict(DEFAULT_SETTINGS)
        s.update(self.d['store_get'](SETTINGS) or {})
        s['approvers'] = [str(a).strip().lower() for a in (s.get('approvers') or []) if str(a).strip()]
        return s

    def update_settings(self, changes):
        with self._lock:
            cur = dict(self.d['store_get'](SETTINGS) or {})
            for k, v in dict(changes or {}).items():
                if k in DEFAULT_SETTINGS:
                    cur[k] = v
            self.d['store_put'](SETTINGS, cur)
            return self.settings()

    def _state(self):
        st = self.d['store_get'](STATE) or {}
        st.setdefault('proposals', {})
        st.setdefault('current', None)
        st.setdefault('log', [])
        return st

    def _save(self, st):
        st['log'] = st['log'][-LOG_MAX:]
        self.d['store_put'](STATE, st)

    @staticmethod
    def _log(st, now, event, detail=''):
        st['log'].append({'at': _iso(now), 'event': event, 'detail': str(detail)[:500]})

    def status(self):
        st = self._state()
        cur = st['proposals'].get(st['current']) if st['current'] else None
        s = self.settings()
        return {'connected_to_amazon': self.d['client']() is not None,
                'ready': self._missing(s) == [],
                'missing_settings': self._missing(s),
                'current': None if not cur else {k: cur.get(k) for k in ('id', 'status', 'created', 'totals', 'error')},
                'last_run': st.get('last_run'),
                'recent': st['log'][-15:]}

    @staticmethod
    def _missing(s):
        miss = []
        if not s.get('david'):
            miss.append('david')
        if not s.get('approvers'):
            miss.append('approvers')
        if not s.get('transcal_to'):
            miss.append('transcal_to')
        if not s.get('source_address'):
            miss.append('source_address')
        return miss

    # ── the daily run ──
    def run(self, now=None, dry_run=False):
        now = _now(now)
        with self._lock:
            st = self._state()
            s = self.settings()
            client = self.d['client']()
            if client is None:
                return self._finish(st, now, 'waiting', 'Amazon has not approved the connection yet', dry_run)
            cur = st['proposals'].get(st['current']) if st['current'] else None
            if cur and cur['status'] == 'awaiting_ok' and not dry_run:
                age = now - datetime.strptime(cur['created'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
                if age < timedelta(days=int(s['expire_days'])):
                    return self._finish(st, now, 'waiting_for_david', f"proposal {cur['id']} is waiting for a reply")
                self._cancel_quietly(client, cur, st, now, 'expired', 'no reply in time')

            try:
                snap = self.pull(client)
            except Exception as e:                  # e.g. the Amazon keys were refused
                return self._finish(st, now, 'amazon_error', f'could not read Amazon: {e}', dry_run)
            if not dry_run:
                self.d['store_put'](SNAPSHOT, snap)
            plan = P.plan_shipment(Y.listings_from(snap), self.d['stock_rows'](), self.d['prepack_resolver'](),
                                   s.get('planner') or None)
            if snap.get('problems'):
                self._log(st, now, 'amazon_problems', '; '.join(snap['problems']))
            if not plan['cartons']:
                return self._finish(st, now, 'nothing_to_ship', f"{len(plan['skipped'])} styles checked, none need a shipment",
                                    dry_run, plan=plan)
            if dry_run:
                return self._finish(st, now, 'dry_run', 'plan only, nothing set up or sent', True, plan=plan)
            missing = self._missing(s)
            if missing:
                return self._finish(st, now, 'not_configured', 'settings missing: ' + ', '.join(missing), plan=plan)
            return self._propose(client, st, s, plan, now)

    def _finish(self, st, now, status, detail, dry_run=False, plan=None):
        out = {'status': status, 'detail': detail}
        if plan is not None:
            out['totals'] = plan['totals']
            out['styles'] = [{k: x[k] for k in ('base', 'cartons', 'units', 'why')} for x in plan['styles']]
        if not dry_run:
            st['last_run'] = {'at': _iso(now), 'status': status, 'detail': detail}
            self._log(st, now, 'run', f'{status}: {detail}')
            self._save(st)
        return out

    def _ref(self, st, now):
        stem = f'A{now.month:02d}{now.day:02d}'
        n = sum(1 for p in st['proposals'] if p.startswith(stem))
        return f'{stem}{chr(ord("a") + n % 26)}'

    def _propose(self, client, st, s, plan, now, note=None, replaces=None):
        ref = self._ref(st, now)
        prop = {'id': ref, 'status': 'awaiting_ok', 'created': _iso(now), 'totals': plan['totals'],
                'plan': _compact(plan), 'draft': None, 'error': None, 'replies': [], 'replaces': replaces}
        st['proposals'][ref] = prop
        st['current'] = ref
        try:
            draft = self.inbound.create_draft(client, f'Versa agent {ref}', s['source_address'], plan['cartons'],
                                              s['box_spec'], self.inbound.ready_window_start(now))
        except Exception as e:                      # Amazon would not set it up: tell David, keep nothing open
            prop.update(status='failed', error=str(e)[:500])
            st['current'] = None
            subject = f"Amazon shipment could not be set up ({now.month}/{now.day})"
            text = (f"The agent planned {plan['totals']['cartons']} cartons for Amazon, but Amazon would not set up "
                    f"the shipment: {e}. The SKU list is attached. Nothing was sent. It will try again on the next run.")
            self._email(s['david'], subject, f'<p>{_esc(text)}</p>',
                        [_attach(M.sku_list_filename(now), M.sku_list_xlsx(plan, self.d['meta_for']))])
            self._log(st, now, 'draft_failed', str(e))
            self._save(st)
            return {'status': 'draft_failed', 'detail': str(e)[:300], 'ref': ref}
        prop['draft'] = draft
        ticket = T.build_ticket(self.inbound.ticket_shipments(draft))
        fcs = [(f"PENDING-{x['fc']}", x['fc'], len(x['cartons']),
                sum(sum(c.values()) for _w, c in x['cartons'])) for x in draft['shipments']]
        extra = []
        if note:
            extra.append(note)
        extra.append(f"Amazon placement fee: ${draft.get('placement_fee', 0):,.2f}. "
                     f"UPS (Amazon partnered) shipping: ${draft.get('shipping_cost', 0):,.2f}.")
        if not s.get('box_spec_confirmed'):
            b = s['box_spec']
            extra.append(f"Carton size and weight sent to Amazon: {b['length']}x{b['width']}x{b['height']} in, "
                         f"{b['weight_lb']} lb. Tell me if Transcal's cartons differ.")
        if any(x.get('address_source') == 'missing' for x in draft['shipments']):
            extra.append('One Amazon warehouse is new to me. Its street address will come off the box labels.')
        for w in draft.get('warnings') or []:
            extra.append(f'Amazon note: {w}')
        subject, html, _text = M.proposal_email(plan, now, fcs=fcs, ref=ref, extra=extra)
        files = [_attach(M.sku_list_filename(now), M.sku_list_xlsx(plan, self.d['meta_for'])),
                 _attach(T.ticket_filename(now, f'Amazon draft {ref}'), ticket)]
        ok, detail = self._email(s['david'], subject, html, files)
        prop['email'] = {'ok': ok, 'detail': str(detail)[:200]}
        if not ok:                                  # David never saw it: do not leave a draft waiting on him
            self._cancel_quietly(client, prop, st, now, 'failed', f'email to David failed: {detail}')
            self._save(st)
            return {'status': 'email_failed', 'ref': ref, 'detail': str(detail)[:300]}
        self._log(st, now, 'proposed', f"{ref}: {plan['totals']['cartons']} cartons, {plan['totals']['units']} units")
        self._save(st)
        return {'status': 'proposed', 'ref': ref, 'totals': plan['totals'], 'email': ok}

    def _email(self, to, subject, html, attachments=None, cc=None):
        if not to:
            return False, 'no recipient set'
        return self.d['send_email'](to, subject, html, attachments or [], cc=cc)

    def _cancel_quietly(self, client, prop, st, now, status, why):
        try:
            if prop.get('draft') and client is not None:
                self.inbound.cancel_draft(client, prop['draft']['inbound_plan_id'])
        except Exception as e:
            why += f' (cancel at Amazon failed: {e})'
        prop['status'] = status
        if st.get('current') == prop['id']:
            st['current'] = None
        self._log(st, now, status, f"{prop['id']}: {why}")

    # ── David's reply ──
    def handle_reply(self, sender, subject, text, now=None):
        now = _now(now)
        with self._lock:
            st = self._state()
            s = self.settings()
            who = str(sender or '').strip().lower()
            if who not in s['approvers']:
                self._log(st, now, 'reply_ignored', f'{who} is not an approver')
                self._save(st)
                return {'status': 'ignored', 'detail': 'sender is not an approver'}
            m = _REF.search(subject or '')
            ref = m.group(1) if m else st.get('current')
            prop = st['proposals'].get(ref) if ref else None
            reply = _new_text(text)
            if not prop:
                self._email(who, f'Re: {subject}', '<p>There is no Amazon shipment waiting for an answer right now.</p>')
                return {'status': 'no_proposal'}
            prop['replies'].append({'at': _iso(now), 'from': who, 'text': reply[:500]})
            if prop['status'] != 'awaiting_ok':
                self._save(st)
                self._email(who, f'Re: {subject}', f"<p>Shipment {prop['id']} is already {prop['status'].replace('_', ' ')}. "
                                                    'Nothing else was done.</p>')
                return {'status': 'not_waiting', 'ref': prop['id']}
            client = self.d['client']()
            first = reply.splitlines()[0] if reply else ''
            drops = {K.norm_sku(x) for x in _DROP.findall(reply)}
            caps = {K.norm_sku(b): int(n) for n, b in _CAP.findall(reply)}
            if drops or caps:
                return self._change(client, st, s, prop, drops, caps, now, subject)
            if _YES.match(first):
                return self._confirm(client, st, s, prop, now, subject)
            if _NO.match(first):
                self._cancel_quietly(client, prop, st, now, 'cancelled', f'{who} said no')
                self._save(st)
                self._email(who, f'Re: {subject}', '<p>Cancelled. Nothing was sent to Amazon or Transcal.</p>')
                return {'status': 'cancelled', 'ref': prop['id']}
            self._save(st)
            self._email(who, f'Re: {subject}', '<p>I did not catch that. Reply YES to send it as it is, NO to cancel, '
                                               'or tell me what to change, for example "drop AMNASU207SLS" or '
                                               '"only 5 cartons of AMUSCD179RFB".</p>')
            return {'status': 'not_understood', 'ref': prop['id']}

    def _confirm(self, client, st, s, prop, now, subject):
        if client is None:
            return {'status': 'error', 'detail': 'not connected to Amazon'}
        draft = prop['draft']
        try:
            done = self.inbound.confirm_draft(client, draft)
        except Exception as e:
            prop.update(status='failed', error=str(e)[:500])
            st['current'] = None
            self._log(st, now, 'confirm_failed', f"{prop['id']}: {e}")
            self._save(st)
            self._email(s['david'], f'Re: {subject}', f'<p>Amazon would not confirm the shipment: {_esc(str(e))}</p>'
                                                      '<p>Nothing was sent to Transcal. Please check Send to Amazon in Seller Central.</p>')
            return {'status': 'confirm_failed', 'detail': str(e)[:300]}
        prop['status'] = 'confirmed'
        prop['confirmed'] = {'shipments': [{k: x[k] for k in ('shipment_id', 'confirmation_id', 'fc', 'boxes', 'address_source')}
                                           for x in done['shipments']], 'at': _iso(now)}
        ticket = T.build_ticket(self.inbound.ticket_shipments(draft, done))
        units = prop['totals']['units']
        cartons = prop['totals']['cartons']
        ids = ', '.join(f"{x['confirmation_id']} ({x['fc']})" for x in done['shipments'])
        files = [_attach(T.ticket_filename(now, f"Amazon ({len(done['shipments'])} FBA)"), ticket)]
        files += [_attach(f"{x['confirmation_id']} box labels.pdf", x['labels_pdf']) for x in done['shipments']]
        body = (f'<p>Please pick and ship the attached Amazon order: {cartons} cartons, {units:,} pieces, '
                f"in {len(done['shipments'])} shipments: {_esc(ids)}.</p>"
                '<p>The pick ticket and the Amazon box labels are attached, one label per carton. '
                'Ship by UPS on the Amazon labels.</p><p>Thank you,<br>Versa</p>')
        ok, detail = self._email(s['transcal_to'], f'Original Brands - Amazon Shipment {units} pieces', body, files,
                                 cc=s.get('transcal_cc') or None)
        prop['status'] = 'sent' if ok else 'confirmed'
        st['current'] = None
        self._log(st, now, 'sent_to_transcal' if ok else 'transcal_email_failed', f"{prop['id']}: {ids} {detail}")
        self._save(st)
        note = ('<p>Done. Amazon confirmed the shipment and Transcal has the pick ticket and box labels.</p>' if ok else
                f'<p>Amazon confirmed the shipment, but the email to Transcal failed ({_esc(str(detail))}). '
                'The ticket and labels are attached so you can forward them.</p>')
        self._email(s['david'], f'Re: {subject}', note + f'<p>FBA shipments: {_esc(ids)}.</p>', files if not ok else files[:1])
        return {'status': prop['status'], 'ref': prop['id'], 'shipments': ids}

    def _change(self, client, st, s, prop, drops, caps, now, subject):
        plan = _expand(prop['plan'])
        bases = {x['base'] for x in plan['styles']}

        def resolve(token):
            """Exact style, or the one style on this shipment that ends with what David typed."""
            if token in bases:
                return token
            hits = [b for b in bases if b.endswith(token)]
            return hits[0] if len(hits) == 1 else token
        drops = {resolve(t) for t in drops}
        caps = {resolve(t): n for t, n in caps.items()}
        unknown = sorted((drops | set(caps)) - bases)
        if unknown:
            self._save(st)
            self._email(s['david'], f'Re: {subject}', f"<p>{_esc(', '.join(unknown))} is not on this shipment. "
                                                      'Nothing was changed.</p>')
            return {'status': 'not_on_shipment', 'unknown': unknown}
        new_plan = _trim(plan, drops, caps)
        self._cancel_quietly(client, prop, st, now, 'replaced', 'David asked for changes')
        if not new_plan['cartons']:
            self._save(st)
            self._email(s['david'], f'Re: {subject}', '<p>That leaves nothing to ship, so the shipment is cancelled.</p>')
            return {'status': 'cancelled', 'ref': prop['id']}
        changes = [f'dropped {b}' for b in sorted(drops)] + [f'{b} cut to {n} cartons' for b, n in sorted(caps.items())]
        return self._propose(client, st, s, new_plan, now, note='Updated as you asked: ' + ', '.join(changes) + '.',
                             replaces=prop['id'])


# ── helpers ──────────────────────────────────────────────────────────────────
def _esc(text):
    import html
    return html.escape(str(text))


def _attach(filename, data):
    return {'filename': filename, 'content': base64.b64encode(data).decode('ascii')}


def _compact(plan):
    """A plan that survives JSON: cartons as [warehouse SKU, contents]."""
    return {'styles': plan['styles'], 'totals': plan['totals'],
            'cartons': [[c.warehouse_sku, c.contents] for c in plan['cartons']]}


def _expand(compact):
    return {'styles': compact['styles'], 'totals': compact['totals'],
            'cartons': [T.Carton(w, c) for w, c in compact['cartons']], 'skipped': []}


def _trim(plan, drops, caps):
    """The plan without dropped styles and with capped styles cut to N cartons (prepacks kept first)."""
    keep, count = [], {}
    by_base = {}
    for c in plan['cartons']:
        base = K.split_sku(c.warehouse_sku).base
        by_base.setdefault(base, []).append(c)
    for base, cs in by_base.items():
        if base in drops:
            continue
        cs = sorted(cs, key=lambda c: (not c.is_prepack, c.warehouse_sku))
        if base in caps:
            cs = cs[:max(0, caps[base])]
        keep += cs
        count[base] = cs
    styles = []
    for st in plan['styles']:
        cs = count.get(st['base'])
        if not cs:
            continue
        info = {(ln['warehouse_sku'], ln['amazon_sku']): ln for ln in st['lines']}
        lines = {}
        for c in cs:
            for m, q in c.contents.items():
                key = (c.warehouse_sku, m)
                ln = lines.setdefault(key, dict(info.get(key) or {'warehouse_sku': c.warehouse_sku, 'amazon_sku': m,
                                                                   'size': K.split_sku(m).size,
                                                                   'kind': 'prepack' if c.is_prepack else 'single'},
                                                 units=0, cartons=0))
                ln['units'] += q
        for c in cs:                                  # a prepack carton counts once per line it feeds
            for m in c.contents:
                lines[(c.warehouse_sku, m)]['cartons'] += 1
        new = dict(st)
        new['lines'] = list(lines.values())
        new['cartons'] = len(cs)
        new['prepack_cartons'] = sum(1 for c in cs if c.is_prepack)
        new['single_cartons'] = new['cartons'] - new['prepack_cartons']
        new['units'] = sum(c.units for c in cs)
        styles.append(new)
    return {'styles': styles, 'cartons': keep, 'skipped': [],
            'totals': {'styles': len(styles), 'cartons': len(keep), 'units': sum(c.units for c in keep)}}
