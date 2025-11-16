# settlement_name_variations.py

def get_settlement_name_variations(name):
    """Generates variations of a settlement name to check against OSM."""
    variations = {name}
    # Variation 1: Replace hyphens with spaces
    variations.add(name.replace('-', ' '))
    # Variation 2: Remove apostrophes
    variations.add(name.replace("'", ""))
    return list(variations)
