"""The override photo move job never writes a base copy for a cut shorter than 8 characters.

Older dashed keys cut at their first dash (NAU-008-WHITE -> 'NAU', AC-KN-042 -> 'AC'). A copy
saved as NAU.jpg became the photo of every NAU-* key on the phone, which asks /image/<cut>
for rows without their own photo. Modern size rows still get their base copy.
Synthetic keys; no test touches the network (tests._support blocks sockets).

Run: python -B -m unittest tests.test_move_base_guard
"""
import base64
import io
import unittest

from tests._support import load_app


def _jpeg(color):
    from PIL import Image
    buf = io.BytesIO()
    Image.new('RGB', (8, 8), color).save(buf, 'JPEG')
    return 'data:image/jpeg;base64,' + base64.b64encode(buf.getvalue()).decode()


class MoveBaseGuard(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def plan(self, overrides, folder=None):
        plan, _data = self.app._ovr_move_plan(overrides, folder or {})
        return {w['file']: w for w in plan['writes']}

    def test_short_cut_gets_no_base_copy(self):
        writes = self.plan({'NAU-008-WHITE': {'image': _jpeg('white')},
                            'ZZ-KN-042': {'image': _jpeg('red')}})
        self.assertIn('NAU-008-WHITE.jpg', writes)       # the key's own file still moves
        self.assertIn('ZZ-KN-042.jpg', writes)
        self.assertNotIn('NAU.jpg', writes)
        self.assertNotIn('ZZ.jpg', writes)

    def test_modern_size_row_still_gets_its_base_copy(self):
        writes = self.plan({'ZZNASU901SLP-L': {'image': _jpeg('blue')}})
        self.assertIn('ZZNASU901SLP-L.jpg', writes)
        self.assertEqual(writes['ZZNASU901SLP.jpg']['action'], 'base copy')


if __name__ == '__main__':
    unittest.main()
