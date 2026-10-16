"""The AI agent's SDK client must not verify TLS through truststore (Oct 16 2026).

Under gunicorn's gevent workers, truststore's SSLContext recurses forever in
gevent's patched ssl, so every SDK call failed as 'Connection error.': the ATS
mailbox, the platform chat and the image audit's SDK path were all down. The
client now gets a plain ssl.SSLContext. These tests use a stand-in anthropic
module, so they need neither the SDK nor the network.
"""
import os
import ssl
import sys
import types
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _support import load_app  # noqa: E402


def _fake_anthropic(with_default_client=True):
    mod = types.ModuleType('anthropic')
    mod.made = []

    class Anthropic:
        def __init__(self, **kw):
            mod.made.append(kw)

    mod.Anthropic = Anthropic
    if with_default_client:
        class DefaultHttpxClient:
            def __init__(self, **kw):
                self.kw = kw
        mod.DefaultHttpxClient = DefaultHttpxClient
    return mod


class AgentClientSSL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def setUp(self):
        self._saved_mod = sys.modules.get('anthropic')
        self._saved_client = self.app._ai_agent_sdk_client
        self._saved_env = os.environ.get('ANTHROPIC_API_KEY')
        os.environ['ANTHROPIC_API_KEY'] = 'sk-test-not-a-key'
        self.app._ai_agent_sdk_client = None

    def tearDown(self):
        if self._saved_mod is None:
            sys.modules.pop('anthropic', None)
        else:
            sys.modules['anthropic'] = self._saved_mod
        self.app._ai_agent_sdk_client = self._saved_client
        if self._saved_env is None:
            os.environ.pop('ANTHROPIC_API_KEY', None)
        else:
            os.environ['ANTHROPIC_API_KEY'] = self._saved_env

    def test_client_gets_a_plain_ssl_context(self):
        fake = _fake_anthropic()
        sys.modules['anthropic'] = fake
        self.app._ai_agent_client()
        self.assertEqual(len(fake.made), 1)
        http_client = fake.made[0]['http_client']
        ctx = http_client.kw['verify']
        self.assertIsInstance(ctx, ssl.SSLContext)
        # ssl's own context (gevent's patched one under the workers), never truststore's subclass
        self.assertNotIn('truststore', type(ctx).__module__)
        self.assertIn(type(ctx).__module__, ('ssl', 'gevent.ssl', 'gevent._ssl3'))
        self.assertEqual(ctx.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(ctx.check_hostname)

    def test_client_is_built_once(self):
        fake = _fake_anthropic()
        sys.modules['anthropic'] = fake
        a = self.app._ai_agent_client()
        b = self.app._ai_agent_client()
        self.assertIs(a, b)
        self.assertEqual(len(fake.made), 1)

    def test_an_sdk_without_default_client_still_builds(self):
        fake = _fake_anthropic(with_default_client=False)
        sys.modules['anthropic'] = fake
        self.app._ai_agent_client()
        self.assertEqual(fake.made, [{'api_key': 'sk-test-not-a-key'}])


if __name__ == '__main__':
    unittest.main()
