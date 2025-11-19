#!/usr/bin/env python3
"""
Test script to verify Hebrew name prioritization logic
"""
import pandas as pd
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from OSM_streets import is_hebrew

# Test cases
test_cases = [
    ("שדרות רוטשילד", True, "Hebrew street name"),
    ("Abu Ghosh", False, "Arabic/English name"),
    ("شارع الرئيسي", False, "Arabic name"),
    ("Main Street", False, "English name"),
    ("רח' הרצל", True, "Hebrew with abbreviation"),
    ("", False, "Empty string"),
    (None, False, "None value"),
    ("123", False, "Numbers only"),
    ("רחוב 123", True, "Hebrew with numbers"),
]

print("Testing is_hebrew() function:")
print("=" * 60)

all_passed = True
for text, expected, description in test_cases:
    result = is_hebrew(text)
    status = "✓ PASS" if result == expected else "✗ FAIL"
    if result != expected:
        all_passed = False
    print(f"{status}: {description}")
    print(f"  Input: {repr(text)}")
    print(f"  Expected: {expected}, Got: {result}")
    print()

print("=" * 60)
if all_passed:
    print("All tests passed! ✓")
    sys.exit(0)
else:
    print("Some tests failed! ✗")
    sys.exit(1)
