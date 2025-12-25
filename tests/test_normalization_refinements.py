import unittest
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from normalization import normalize_street_name

class TestNormalizationRefinements(unittest.TestCase):

    def test_doctor_abbreviation(self):
        """
        Tests that 'ד"ר' is correctly expanded to 'דוקטור'.
        This test should initially fail and pass after updating normalization.py.
        """
        self.assertEqual(normalize_street_name('ד"ר לוי'), 'דוקטור לוי')

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
