import unittest
from normalization import is_arabic

class TestArabicDetection(unittest.TestCase):
    def test_is_arabic_true(self):
        """Should return True for strings containing Arabic characters."""
        arabic_names = [
            "شارע",            # Shari'a (Street)
            "السلطان",          # Al-Sultan
            "شارע السلطان",     # Shari'a Al-Sultan
            "ابو غوش",         # Abu Ghosh (Arabic)
            "المتنבי",          # Al-Mutanabbi (Arabic with some Hebrew-like phonetics)
        ]
        for name in arabic_names:
            with self.subTest(name=name):
                self.assertTrue(is_arabic(name), f"Failed to detect Arabic in: {name}")

    def test_is_arabic_false(self):
        """Should return False for strings without Arabic characters."""
        hebrew_names = [
            "רחוב",
            "הסולטן",
            "אבו גוש",
            "הנשיא",
            "123",
            "",
            None
        ]
        for name in hebrew_names:
            with self.subTest(name=name):
                self.assertFalse(is_arabic(name), f"Incorrectly detected Arabic in: {name}")

    def test_is_arabic_mixed(self):
        """Should return True for strings containing both Hebrew/English and Arabic."""
        mixed_names = [
            "רחוב شارע",
            "Street شارע",
        ]
        for name in mixed_names:
            with self.subTest(name=name):
                self.assertTrue(is_arabic(name), f"Failed to detect Arabic in mixed string: {name}")

if __name__ == '__main__':
    unittest.main()
