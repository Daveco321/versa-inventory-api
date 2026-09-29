"""Colour names that are solids although they carry no "Solid" (David, Sep 29 2026:
"Tony Blue" is a blue solid; the DKNY 022 colour-map row reads just "TONY BLUE")."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _support import load_app


class NamedSolidColorTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app = load_app()

    def c(self, name, brand='DKNY'):
        return self.app._apo_classify_color(name, brand)

    def test_tony_blue_is_a_blue_solid(self):
        for name in ('TONY BLUE', 'Tony Blue', '  tony   blue '):
            self.assertEqual('navy', self.c(name), name)

    def test_tony_blue_solid_was_already_blue(self):
        self.assertEqual('navy', self.c('Tony Blue Solid'))

    def test_patterns_that_mention_tony_blue_stay_fancies(self):
        self.assertEqual('fancies', self.c('WHITE GRND TONY BLUE MED STRIPE', 'VINCE'))

    def test_first_half_of_a_two_part_colour(self):
        self.assertEqual('navy', self.c('Tony Blue / White Contrast'))


if __name__ == '__main__':
    unittest.main()
