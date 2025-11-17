# tests/test_normalization.py
import unittest
from normalization import normalize_street_name

class TestNormalization(unittest.TestCase):

    def test_normalize_street_name(self):
        """
        Tests the normalize_street_name function with a variety of inputs.
        """
        test_cases = {
            "שד' רוטשילד": "שדרות רוטשילד",
            "רח' דיזנגוף": "רחוב דיזנגוף",
            "כיכר רבין": "כיכר רבין",
            "  רחוב  אבן גבירול  ": "רחוב אבן גבירול",
            "רחוב-הארבעה": "רחוב הארבעה",
            "רחוב.הארבעה": "רחוב הארבעה",
            "שד. בן גוריון": "שדרות בן גוריון",
            "King George St.": "King George St",
            None: None,
            "": ""
        }

        for input_name, expected_output in test_cases.items():
            with self.subTest(input=input_name):
                self.assertEqual(normalize_street_name(input_name), expected_output)

if __name__ == '__main__':
    unittest.main()
