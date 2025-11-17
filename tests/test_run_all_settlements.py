# tests/test_run_all_settlements.py
import unittest
from settlement_name_variations import get_settlement_name_variations

class TestRunAllSettlements(unittest.TestCase):

    def test_get_settlement_name_variations(self):
        """
        Tests that the get_settlement_name_variations function correctly
        generates a list of name variations.
        """
        # Test case 1: Name with hyphen and apostrophe
        name1 = "Test-Settlement'"
        expected1 = ["Test-Settlement'", "Test Settlement'", "Test-Settlement"]
        self.assertCountEqual(get_settlement_name_variations(name1), expected1)

        # Test case 2: Name with no special characters
        name2 = "Tel Aviv"
        expected2 = ["Tel Aviv"]
        self.assertCountEqual(get_settlement_name_variations(name2), expected2)

if __name__ == '__main__':
    unittest.main()
