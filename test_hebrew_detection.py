import unittest
import sys
import os

# Add parent directory to path to find OSM_streets
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from OSM_streets import is_hebrew

class TestHebrewDetection(unittest.TestCase):

    def test_hebrew_street_name(self):
        self.assertTrue(is_hebrew("שדרות רוטשילד"), "Failed on Hebrew street name")

    def test_arabic_english_name(self):
        self.assertFalse(is_hebrew("Abu Ghosh"), "Failed on Arabic/English name")

    def test_arabic_name(self):
        self.assertFalse(is_hebrew("شارع الرئيسي"), "Failed on Arabic name")

    def test_english_name(self):
        self.assertFalse(is_hebrew("Main Street"), "Failed on English name")

    def test_hebrew_with_abbreviation(self):
        self.assertTrue(is_hebrew("רח' הרצל"), "Failed on Hebrew with abbreviation")

    def test_empty_string(self):
        self.assertFalse(is_hebrew(""), "Failed on empty string")

    def test_none_value(self):
        self.assertFalse(is_hebrew(None), "Failed on None value")

    def test_numbers_only(self):
        self.assertFalse(is_hebrew("123"), "Failed on numbers only")

    def test_hebrew_with_numbers(self):
        self.assertTrue(is_hebrew("רחוב 123"), "Failed on Hebrew with numbers")

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)