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
from typing import Optional, Dict, Any, Tuple
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
    
    def __init__(self):
        self.cache = NominatimCache()
        self.last_request_time = 0
    
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
        
        # Remove common prefixes/suffixes
        normalized = name.strip()
        
        # Remove parenthetical content (e.g., "תל אביב (יפו)" -> "תל אביב")
        normalized = re.sub(r'\([^)]*\)', '', normalized).strip()
        
        # Normalize dashes and spaces
        normalized = re.sub(r'[־\-–—]', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        # Remove common administrative prefixes
        prefixes_to_remove = ['עיריית', 'מועצה מקומית', 'מועצה אזורית']
        for prefix in prefixes_to_remove:
            if normalized.startswith(prefix):
                normalized = normalized[len(prefix):].strip()
        
        return normalized
    
    def _rate_limit(self):
        """Enforce Nominatim rate limiting"""
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
        
        Args:
            result: Nominatim result dictionary
            original_name: Original settlement name for context
            
        Returns:
            Tuple of (is_valid, validation_message)
        """
        try:
            lat = float(result.get('lat', 0))
            lon = float(result.get('lon', 0))
            display_name = result.get('display_name', '')
            place_type = result.get('type', '')
            
            # Check 1: Geographic bounds
            if not self._is_within_israel(lat, lon):
                return False, f"Outside Israel/Palestine bounds: {display_name}"
            
            # Check 2: Display name should contain Israel or Palestinian territories
            valid_regions = ['Israel', 'ישראל', 'Palestinian', 'فلسطين', 'West Bank', 'Gaza']
            if not any(region in display_name for region in valid_regions):
                return False, f"Display name doesn't contain expected region: {display_name}"
            
            # Check 3: Place type should be appropriate
            if place_type and place_type not in VALID_PLACE_TYPES:
                return False, f"Invalid place type '{place_type}': {display_name}"
            
            # Check 4: Bounding box should be reasonable (not too large)
            bbox = result.get('boundingbox', [])
            if len(bbox) == 4:
                bbox_lat_range = float(bbox[1]) - float(bbox[0])
                bbox_lon_range = float(bbox[3]) - float(bbox[2])
                # If bounding box is larger than 2 degrees, it's probably too large
                if bbox_lat_range > 2.0 or bbox_lon_range > 2.0:
                    return False, f"Bounding box too large ({bbox_lat_range:.2f}°, {bbox_lon_range:.2f}°): {display_name}"
            
            return True, "Valid result"
            
        except Exception as e:
            return False, f"Validation error: {e}"
    
    def search_settlement(self, settlement_name: str, max_retries: int = 3) -> Optional[SettlementMatch]:
        """
        Search for a settlement using Nominatim with validation.
        
        Args:
            settlement_name: Name of the settlement to search
            max_retries: Maximum number of retry attempts
            
        Returns:
            SettlementMatch object if found and valid, None otherwise
        """
        normalized_name = self.normalize_settlement_name(settlement_name)
        
        if not normalized_name:
            print(f"  ⚠ Empty settlement name after normalization: '{settlement_name}'")
            return None
        
        # Check cache first
        cache_key = f"{normalized_name}::Israel"
        cached_result = self.cache.get(cache_key)
        if cached_result:
            print(f"  ✓ Cache hit for '{settlement_name}'")
            if cached_result.get('error'):
                return None
            return self._dict_to_match(cached_result, settlement_name)
        
        # Query Nominatim
        print(f"  → Searching Nominatim for '{normalized_name}'...")
        
        params = {
            'q': f"{normalized_name}, Israel",
            'format': 'json',
            'addressdetails': 1,
            'limit': 5,  # Get top 5 results to find best match
            'accept-language': 'he,en'
        }
        
        for attempt in range(max_retries):
            try:
                self._rate_limit()
                
                response = requests.get(
                    NOMINATIM_URL,
                    params=params,
                    headers={'User-Agent': USER_AGENT},
                    timeout=10
                )
                response.raise_for_status()
                
                results = response.json()
                
                if not results:
                    print(f"  ✗ No results found for '{settlement_name}'")
                    self.cache.set(cache_key, {'error': 'no_results'})
                    return None
                
                # Try to find the first valid result
                for result in results:
                    is_valid, validation_msg = self._validate_result(result, settlement_name)
                    
                    if is_valid:
                        match = SettlementMatch(
                            settlement_name=settlement_name,
                            osm_id=result.get('osm_id', ''),
                            display_name=result.get('display_name', ''),
                            lat=float(result.get('lat', 0)),
                            lon=float(result.get('lon', 0)),
                            boundingbox=tuple(map(float, result.get('boundingbox', [0, 0, 0, 0]))),
                            place_type=result.get('type', ''),
                            importance=float(result.get('importance', 0)),
                            is_valid=True,
                            validation_message=validation_msg
                        )
                        
                        print(f"  ✓ Valid match: {match.display_name}")
                        
                        # Cache the result
                        self.cache.set(cache_key, self._match_to_dict(match))
                        
                        return match
                    else:
                        print(f"  ✗ Invalid result: {validation_msg}")
                
                # No valid results found
                print(f"  ✗ No valid results for '{settlement_name}' (all failed validation)")
                self.cache.set(cache_key, {'error': 'no_valid_results'})
                return None
                
            except requests.exceptions.RequestException as e:
                print(f"  ⚠ Request error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    self.cache.set(cache_key, {'error': f'request_failed: {e}'})
                    return None
        
        return None
    
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
