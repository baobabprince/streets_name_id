"""
Settlement Matcher Module

This module handles matching settlement names from LAMAS to OSM places using Nominatim,
with geographic validation to ensure results are within Israel/Palestine.
"""

import requests
import json
import os
import time
import re
import difflib
import threading
from fuzzywuzzy import fuzz
from typing import Optional, Dict, Any, Tuple, List
from dataclasses import dataclass

# Geographic bounds for Israel/Palestine (min_lat, min_lon, max_lat, max_lon)
ISRAEL_BOUNDS = (29.0, 34.0, 33.5, 36.0)

# Acceptable place types from Nominatim
VALID_PLACE_TYPES = {
    'city', 'town', 'village', 'municipality', 'administrative',
    'hamlet', 'suburb', 'neighbourhood', 'locality'
}

# Nominatim API settings
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_RATE_LIMIT = 1.0  # seconds between requests (Nominatim policy)
USER_AGENT = "StreetsNameID/1.0 (Israeli Street Mapping Project)"

# Cache settings
CACHE_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(CACHE_DIR, exist_ok=True)
NOMINATIM_CACHE_FILE = os.path.join(CACHE_DIR, "nominatim_cache.json")


@dataclass
class SettlementMatch:
    """Represents a matched settlement from Nominatim"""
    settlement_name: str
    osm_id: str
    display_name: str
    lat: float
    lon: float
    boundingbox: Tuple[float, float, float, float]
    place_type: str
    importance: float
    is_valid: bool
    validation_message: str


class NominatimCache:
    """Simple JSON-based cache for Nominatim results"""
    
    def __init__(self, cache_file: str = NOMINATIM_CACHE_FILE):
        self.cache_file = cache_file
        self.cache = self._load_cache()
    
    def _load_cache(self) -> Dict:
        """Load cache from file"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Warning: Failed to load cache: {e}")
                return {}
        return {}
    
    def _save_cache(self):
        """Save cache to file"""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Warning: Failed to save cache: {e}")
    
    def get(self, key: str) -> Optional[Dict]:
        """Get cached result"""
        return self.cache.get(key)
    
    def set(self, key: str, value: Dict):
        """Set cached result and save"""
        self.cache[key] = value
        self._save_cache()


class SettlementMatcher:
    """Handles settlement name matching using Nominatim with validation"""
    # NOTE: Previously we used a hard‑coded SPECIAL_CASES dict to rewrite
    # problematic settlement names (e.g. "תל אביב" → "תל‑אביב‑יפו").
    # This approach is brittle and does not scale.  We now rely on a generic
    # fallback that appends a country qualifier (", Israel") when the initial
    # Nominatim query yields no valid result.
    # The SPECIAL_CASES dict is removed.
    
    def __init__(self):
        self.cache = NominatimCache()
        self.last_request_time = 0
        self._lock = threading.Lock()
    
    def normalize_settlement_name(self, name: str) -> str:
        """
        Normalize settlement name for better Nominatim matching.
        
        Args:
            name: Original settlement name from LAMAS
            
        Returns:
            Normalized settlement name
        """
        if not name:
            return ""
        
        # 1️⃣ Strip whitespace and generic punctuation
        normalized = name.strip()
        
        # 2️⃣ Remove parenthetical content (e.g., "תל אביב (יפו)" → "תל אביב")
        normalized = re.sub(r'\([^)]*\)', '', normalized).strip()
        
        # 3️⃣ Normalise dashes and collapse multiple spaces
        normalized = re.sub(r'[־\-–—]', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        # 4️⃣ Strip common administrative prefixes and suffixes
        prefixes_to_remove = ['עיריית', 'מועצה מקומית', 'מועצה אזורית']
        suffixes_to_remove = ['מושב', 'קיבוץ', 'יישוב', 'עיר']
        for prefix in prefixes_to_remove:
            if normalized.startswith(prefix):
                normalized = normalized[len(prefix):].strip()
        for suffix in suffixes_to_remove:
            if normalized.endswith(suffix):
                normalized = normalized[: -len(suffix)].strip()
        
        # 5️⃣ No special‑case overrides – rely on generic fallback later.
        return normalized
    
    def _rate_limit(self):
        """Enforce Nominatim rate limiting across all threads"""
        with self._lock:
            elapsed = time.time() - self.last_request_time
            if elapsed < NOMINATIM_RATE_LIMIT:
                time.sleep(NOMINATIM_RATE_LIMIT - elapsed)
            self.last_request_time = time.time()
    
    def _is_within_israel(self, lat: float, lon: float) -> bool:
        """Check if coordinates are within Israel/Palestine bounds"""
        min_lat, min_lon, max_lat, max_lon = ISRAEL_BOUNDS
        return min_lat <= lat <= max_lat and min_lon <= lon <= max_lon
    
    def _validate_result(self, result: Dict, original_name: str) -> Tuple[bool, str]:
        """
        Validate that a Nominatim result is reasonable.
        """
        try:
            lat = float(result.get('lat', 0))
            lon = float(result.get('lon', 0))
            display_name = result.get('display_name', '')
            place_type = result.get('type', '')
            address = result.get('address', {})
            
            # 1. Geographic bounds
            if not self._is_within_israel(lat, lon):
                return False, f"Outside Israel/Palestine bounds: {display_name}"
            
            # 2. Strict Country Check (Israel or Palestinian Territories)
            country = address.get('country', '')
            country_code = address.get('country_code', '').lower()
            valid_countries = {'ישראל', 'Israel', 'Palestinian Territory', 'Palestine', 'הרשות הפלסטינית'}
            if country not in valid_countries and country_code not in {'il', 'ps'}:
                return False, f"Country mismatch ({country}): {display_name}"

            # 3. Reject non-settlement types
            REJECTED_TYPES = {
                'bus_stop', 'highway', 'road', 'building', 'house', 'apartments',
                'street_lamp', 'parking', 'memorial', 'monument', 'archaeological_site'
            }
            if place_type in REJECTED_TYPES:
                return False, f"Rejected place type '{place_type}': {display_name}"

            # 4. Distinctive Word Matching (Anti-Hijacking)
            # Ensure the distinctive part of the original name is actually in the result
            # e.g., if searching for "כפר רות", "רות" MUST be in the display name.
            
            # Identify distinctive tokens (ignore common prefixes/suffixes)
            COMMON_TOKENS = {'כפר', 'קיבוץ', 'מושב', 'יישוב', 'עיר', 'מועצה', 'אזורית', 'מקומית'}
            
            # Get tokens from the part OUTSIDE parentheses
            main_part = re.sub(r'\(.*\)', '', original_name).strip()
            normalized_main = self.normalize_settlement_name(main_part)
            orig_tokens = [t for t in normalized_main.split() if t not in COMMON_TOKENS] if normalized_main else []
            
            # Handle parenthetical alternatives (e.g. "Kfar Rosenwald (Zarit)")
            alt_tokens = []
            paren_match = re.search(r'\(([^)]+)\)', original_name)
            if paren_match:
                alt_text = self.normalize_settlement_name(paren_match.group(1))
                if alt_text:
                    alt_tokens = [t for t in alt_text.split() if t not in COMMON_TOKENS]

            # Logic: All tokens from EITHER the main part OR the alternative part must be present.
            if orig_tokens or alt_tokens:
                display_name_normalized = self.normalize_settlement_name(display_name)
                
                def all_tokens_in_text(tokens, text):
                    for token in tokens:
                        if token in text:
                            continue
                        token_match = False
                        for d_token in text.split():
                            if fuzz.ratio(token, d_token) > 85:
                                token_match = True
                                break
                        if not token_match:
                            return False
                    return True

                main_match = all_tokens_in_text(orig_tokens, display_name_normalized) if orig_tokens else False
                alt_match = all_tokens_in_text(alt_tokens, display_name_normalized) if alt_tokens else False
                
                if not main_match and not alt_match:
                    tokens_str = ", ".join(orig_tokens + alt_tokens)
                    return False, f"None of the distinctive tokens ({tokens_str}) found in result: {display_name}"

            # 5. Bounding box should be reasonable (not too large)
            bbox = result.get('boundingbox', [])
            if len(bbox) == 4:
                bbox_lat_range = float(bbox[1]) - float(bbox[0])
                bbox_lon_range = float(bbox[3]) - float(bbox[2])
                
                # Allow a larger bbox for city‑level place types
                if place_type in {'city', 'town', 'municipality', 'administrative'}:
                    max_range = 3.0
                else:
                    max_range = 2.0
                
                if bbox_lat_range > max_range or bbox_lon_range > max_range:
                    return False, f"Bounding box too large ({bbox_lat_range:.2f}°, {bbox_lon_range:.2f}°): {display_name}"
            
            return True, "Valid result"
            
        except Exception as e:
            return False, f"Validation error: {e}"
    
    def search_settlement(self, settlement_name: str, max_retries: int = 3) -> Optional[SettlementMatch]:
        """
        Search for a settlement using Nominatim with validation and AI resolution.
        """
        normalized_name = self.normalize_settlement_name(settlement_name)
        
        if not normalized_name:
            print(f"  ⚠ Empty settlement name after normalization: '{settlement_name}'")
            return None
        
        # Collect ALL valid candidates across all variants
        all_valid_candidates = {} # osm_id -> SettlementMatch
        
        query_variants = []
        # 1. Full normalized name
        query_variants.append(normalized_name)
        # 2. Normalized name with country
        query_variants.append(f"{normalized_name}, Israel")
        
        # 3. If there's parenthetical content, try extracting it as a variant (e.g. "Kfar Rosenwald (Zarit)" -> "Zarit")
        # Note: self.normalize_settlement_name strips parentheses, so we check original_name
        paren_match = re.search(r'\(([^)]+)\)', settlement_name)
        if paren_match:
            variant = paren_match.group(1).strip()
            query_variants.append(variant)
            query_variants.append(f"{variant}, Israel")

        # 4. Try splitting on dash/hyphen and searching each part
        parts = re.split(r'[\s\-]+', normalized_name)
        for part in parts:
            if part and part != normalized_name and len(part) > 2:
                query_variants.append(part)
                query_variants.append(f"{part}, Israel")

        # Use a set to maintain order but avoid duplicates
        seen_queries = set()
        unique_variants = []
        for q in query_variants:
            if q not in seen_queries:
                unique_variants.append(q)
                seen_queries.add(q)

        for q in unique_variants:
            print(f"  → Attempting Nominatim query variant: '{q}'")
            variants_results = self._perform_nominatim_query_all(settlement_name, q, max_retries)
            for match in variants_results:
                if match.osm_id not in all_valid_candidates:
                    all_valid_candidates[match.osm_id] = match
        
        if not all_valid_candidates:
            print(f"  ✗ No valid candidates found for '{settlement_name}'")
            return None
            
        candidates = list(all_valid_candidates.values())
        
        # If we have only one, return it
        if len(candidates) == 1:
            return candidates[0]
            
        # If we have multiple, use AI to resolve
        print(f"  ⇄ Multiple candidates ({len(candidates)}) found for '{settlement_name}'. Resolving with AI...")
        return self._resolve_with_ai(settlement_name, candidates)

    def _perform_nominatim_query_all(self, original_name: str, query_name: str, max_retries: int) -> List[SettlementMatch]:
        """
        Helper to perform a Nominatim query and return ALL valid matches.
        """
        cache_key = f"all_{query_name}"
        
        cached_results = self.cache.get(cache_key)
        if cached_results:
            if cached_results.get('error'):
                return []
            return [self._dict_to_match(r, original_name) for r in cached_results.get('matches', [])]

        params = {
            'q': query_name,
            'format': 'json',
            'addressdetails': 1,
            'limit': 10,
            'accept-language': 'he,en',
        }
        
        valid_matches = []
        for attempt in range(max_retries):
            try:
                self._rate_limit()
                response = requests.get(NOMINATIM_URL, params=params, headers={'User-Agent': USER_AGENT}, timeout=10)
                response.raise_for_status()
                results = response.json()
                
                if not results:
                    self.cache.set(cache_key, {'error': 'no_results'})
                    return []
                
                for result in results:
                    is_valid, validation_msg = self._validate_result(result, original_name)
                    if is_valid:
                        match = SettlementMatch(
                            settlement_name=original_name,
                            osm_id=str(result.get('osm_id', '')),
                            display_name=result.get('display_name', ''),
                            lat=float(result.get('lat', 0)),
                            lon=float(result.get('lon', 0)),
                            boundingbox=tuple(map(float, result.get('boundingbox', [0, 0, 0, 0]))),
                            place_type=result.get('type', ''),
                            importance=float(result.get('importance', 0)),
                            is_valid=True,
                            validation_message=validation_msg
                        )
                        valid_matches.append(match)
                
                # Cache all found valid matches
                self.cache.set(cache_key, {'matches': [self._match_to_dict(m) for m in valid_matches]})
                return valid_matches
                
            except Exception as e:
                print(f"  ⚠ Request error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return []
        return []

    def _resolve_with_ai(self, original_name: str, candidates: List[SettlementMatch]) -> Optional[SettlementMatch]:
        """Use AI to pick the best settlement match from a list of candidates."""
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            print("  ⚠ GEMINI_API_KEY not set. Falling back to importance-based selection.")
            return max(candidates, key=lambda x: x.importance)

        # Prepare prompt
        candidates_info = "\n".join([
            f"- ID: {c.osm_id}, Name: {c.display_name}, Type: {c.place_type}, Importance: {c.importance}"
            for c in candidates
        ])
        
        prompt = f"""Given the Israeli settlement name '{original_name}', pick the single most correct match from the following OpenStreetMap candidates.\nOnly provide the OSM ID of the best match, or 'None' if none are correct.\n\nGuidelines:\n1. Prefer the specific settlement over administrative regions or nearby places.\n2. Ensure the distinctive part of the name (e.g., 'Rut' in 'Kfar Rut') is the main subject.\n3. Ignore entries that are just generic names or hijacked by larger cities.\n\nCandidates:\n{candidates_info}\n\nOSM ID:"""

        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent?key={api_key}"
        payload = {
            "contents": [{"parts": [{"text": prompt}]}]
        }

        try:
            response = requests.post(url, json=payload, timeout=15)
            response.raise_for_status()
            result = response.json()
            text = result.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '').strip()
            
            clean_id = ''.join(filter(str.isdigit, text))
            if not clean_id:
                return None
                
            for c in candidates:
                if c.osm_id == clean_id:
                    print(f"  ✓ AI resolved '{original_name}' to: {c.display_name}")
                    return c
            return None
        except Exception as e:
            print(f"  ⚠ AI resolution failed: {e}. Falling back to importance.")
            return max(candidates, key=lambda x: x.importance)
    
    def _match_to_dict(self, match: SettlementMatch) -> Dict:
        """Convert SettlementMatch to dictionary for caching"""
        return {
            'settlement_name': match.settlement_name,
            'osm_id': match.osm_id,
            'display_name': match.display_name,
            'lat': match.lat,
            'lon': match.lon,
            'boundingbox': list(match.boundingbox),
            'place_type': match.place_type,
            'importance': match.importance,
            'is_valid': match.is_valid,
            'validation_message': match.validation_message
        }
    
    def _dict_to_match(self, data: Dict, settlement_name: str) -> SettlementMatch:
        """Convert dictionary to SettlementMatch"""
        return SettlementMatch(
            settlement_name=settlement_name,
            osm_id=data.get('osm_id', ''),
            display_name=data.get('display_name', ''),
            lat=data.get('lat', 0),
            lon=data.get('lon', 0),
            boundingbox=tuple(data.get('boundingbox', [0, 0, 0, 0])),
            place_type=data.get('place_type', ''),
            importance=data.get('importance', 0),
            is_valid=data.get('is_valid', True),
            validation_message=data.get('validation_message', '')
        )


def test_settlement_matcher():
    """Test the settlement matcher with known cases"""
    matcher = SettlementMatcher()
    
    test_cases = [
        "אודם",  # Known problematic case
        "תל אביב-יפו",
        "ירושלים",
        "חיפה",
        "באר שבע",
        "נצרת"
    ]
    
    print("=" * 60)
    print("Testing Settlement Matcher")
    print("=" * 60)
    
    for settlement in test_cases:
        print(f"\nTesting: {settlement}")
        print("-" * 60)
        match = matcher.search_settlement(settlement)
        if match:
            print(f"✓ SUCCESS")
            print(f"  Display Name: {match.display_name}")
            print(f"  Coordinates: ({match.lat:.4f}, {match.lon:.4f})")
            print(f"  Type: {match.place_type}")
            print(f"  Validation: {match.validation_message}")
        else:
            print(f"✗ FAILED - No valid match found")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--test":
            # Test mode
            if len(sys.argv) > 2:
                # Test specific settlement
                matcher = SettlementMatcher()
                settlement = sys.argv[2]
                print(f"Testing settlement: {settlement}")
                match = matcher.search_settlement(settlement)
                if match:
                    print(f"\n✓ Match found:")
                    print(f"  Display: {match.display_name}")
                    print(f"  Coords: ({match.lat:.4f}, {match.lon:.4f})")
                    print(f"  Type: {match.place_type}")
                else:
                    print(f"\n✗ No valid match found")
            else:
                # Run all tests
                test_settlement_matcher()
        else:
            print("Usage: python settlement_matcher.py --test [settlement_name]")
    else:
        print("Settlement Matcher Module")
        print("Usage: python settlement_matcher.py --test [settlement_name]")
