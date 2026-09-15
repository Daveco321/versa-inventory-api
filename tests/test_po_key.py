"""Canonical PO key (INV-01). Run: python -B -m unittest discover -s tests -t .

The vectors are synthetic. A local parity run against a corpus of real PO
strings is possible by pointing PO_KEY_CORPUS at a JSON list of
[raw, key, kind] triples; that corpus must never be committed."""
import json
import os
import unittest

from tests._support import load_app

HERE = os.path.dirname(os.path.abspath(__file__))


class PoKeyVectors(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app()
        with open(os.path.join(HERE, 'po_key_vectors.json'), encoding='utf-8') as f:
            cls.vectors = json.load(f)

    def test_all_vectors(self):
        cases = self.vectors['cases']
        self.assertEqual(len(cases), 37)
        bad = []
        for c in cases:
            got = self.app._po_key(c['raw'])
            if got != (c['key'], c['kind']):
                bad.append((c, got))
        self.assertEqual(bad, [], f'{len(cases) - len(bad)} of {len(cases)} vectors pass')

    def test_join_key_scoping(self):
        jk = self.app._po_join_key
        # Leading zeros on all-digit POs never split one PO in two.
        self.assertEqual(jk('ZALPHA', '0077001001'), jk('zalpha ', '77001001'))
        # Code families are never merged for matching.
        self.assertNotEqual(jk('ZBETA', '12345'), jk('ZBETA1', '12345'))
        # Punctuation is kept: a dashed PO never meets the bare digits.
        self.assertNotEqual(jk('ZALPHA', '10-424242'), jk('ZALPHA', '10424242'))
        # Codes keep their leading zeros.
        self.assertNotEqual(jk('ZALPHA', '01-234567'), jk('ZALPHA', '1-234567'))
        # Blank, Excel-damaged and name-only POs never join.
        self.assertIsNone(jk('ZALPHA', ''))
        self.assertIsNone(jk('ZALPHA', None))
        self.assertIsNone(jk('ZALPHA', '6.00E+11'))
        self.assertIsNone(jk('ZALPHA', 'SAMPLE PROGRAM'))
        self.assertEqual(jk('ZALPHA', 55), 'ZALPHA|55')

    def test_lookup_spellings(self):
        sp = self.app._po_lookup_spellings
        self.assertEqual(sp('0077001001'), ['0077001001', '77001001'])
        self.assertEqual(sp('77001001'), ['77001001'])
        self.assertEqual(sp(' 0045 '), ['0045', '45'])
        self.assertEqual(sp('66-000001'), ['66-000001'])
        self.assertEqual(sp(''), [])

    @unittest.skipUnless(os.environ.get('PO_KEY_CORPUS'), 'local-only corpus parity (PO_KEY_CORPUS unset)')
    def test_local_corpus_parity(self):
        with open(os.environ['PO_KEY_CORPUS'], encoding='utf-8') as f:
            corpus = json.load(f)
        bad = [(raw, key, kind) for raw, key, kind in corpus if self.app._po_key(raw) != (key, kind)]
        self.assertEqual(len(bad), 0, f'{len(bad)} of {len(corpus)} corpus strings differ; first: {bad[:3]}')


if __name__ == '__main__':
    unittest.main()
