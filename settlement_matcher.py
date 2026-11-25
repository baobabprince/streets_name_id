"""
Settlement Matcher Module

This module handles matching settlement names from LAMAS to OSM places using Nominatim,
with geographic validation to ensure results are within Israel/Palestine.

The batch pipeline resolves settlements offline first (see ``settlement_index.py``)
and calls Nominatim only for the names the local OSM index cannot explain.  The
cache keeps both positive and negative results, and can defer disk writes so a
whole batch is persisted with a single atomic write.
"""

import datetime
import requests
import json
import os
import time
import re
import unicodedata
from typing import Optional, Dict, Any, Tuple
from dataclasses import dataclass

from rapidfuzz.fuzz import ratio

from settlement_names import (
    NON_SETTLEMENT_ADMIN_LEVELS,
    NON_SETTLEMENT_PLACE_TYPES,
    admin_level,
    canonical_key,
    has_geresh_conflict,
    has_qualifier_conflict,
    is_regional_boundary_name,
    load_aliases,
    load_osm_id_overrides,
    lookup_by_settlement_key,
)

# Geographic bounds for Israel/Palestine (min_lat, min_lon, max_lat, max_lon)
ISRAEL_BOUNDS = (29.0, 34.0, 33.5, 36.0)

# Acceptable place types from Nominatim.  Suburbs/neighbourhoods are deliberately
# excluded: a quarter inside a city must never "steal" a LAMAS settlement's
# identity (see settlement_index.NON_SETTLEMENT_PLACE_TYPES).
VALID_PLACE_TYPES = {
    'city', 'town', 'village', 'municipality', 'administrative',
    'hamlet', 'locality'
}
VALID_COUNTRY_CODES = {'il', 'ps'}
# Nominatim address fields that name an enclosing *settlement*.  A result whose
# address says it is inside a different city/town is a neighbourhood, not an
# independent LAMAS settlement.
ADDRESS_SETTLEMENT_KEYS = ('city', 'town', 'village', 'municipality')
ADDRESS_SUBDIVISION_KEYS = (
    'suburb', 'neighbourhood', 'neighborhood', 'quarter', 'city_district',
    'district', 'borough',
)
VALID_PLACE_CLASSES = {'place', 'boundary'}
SETTLEMENT_ALIASES = {
    'אבטין': ('איבטין', 'Ibtin'),
    'אביה': ('חוות אביה',),
    'אבורובייעה': ('أبو ربيعة', 'أبو ربية'),
}
# v6: allow regional-council address fields, pinned institutions, and geresh/qualifier guards.
MATCH_CACHE_VERSION = 'v6'
MIN_NAME_MATCH_SCORE = 92
# Negative results are cached too, but for a shorter period: OSM is updated often.
UNRESOLVED_CACHE_TTL_DAYS = 7
MATCHED_CACHE_TTL_DAYS = 180

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
    osm_type: str = ""
    category: str = ""
    country_code: str = ""
    match_source: str = "nominatim"
    name_score: float = 0.0
    center_distance_m: Optional[float] = None
    osm_ids: Tuple[str, ...] = ()

    def to_place_dict(self) -> Dict[str, Any]:
        """Dict consumed by ``run_pipeline`` / ``fetch_pbf_street_data``."""
        payload: Dict[str, Any] = {
            "settlement_name": self.settlement_name,
            "display_name": self.display_name,
            "lat": self.lat,
            "lon": self.lon,
            "boundingbox": tuple(self.boundingbox),
            "place_type": self.place_type,
            "osm_id": self.osm_id,
            "osm_type": self.osm_type,
            "category": self.category,
            "country_code": self.country_code,
            "match_source": self.match_source,
            "name_score": self.name_score,
            "center_distance_m": self.center_distance_m,
            "validation_message": self.validation_message,
        }
        if self.osm_ids:
            payload["osm_ids"] = list(self.osm_ids)
        return payload


class NominatimCache:
    """JSON cache for Nominatim results (positive *and* negative results).

    ``auto_save=False`` lets a batch resolve hundreds of names in memory and
    write the cache exactly once with :meth:`save`.  Writes are atomic so a
    crashed run cannot corrupt the cache.
    """

    def __init__(
        self,
        cache_file: str = NOMINATIM_CACHE_FILE,
        auto_save: bool = True,
        unresolved_ttl_days: int = UNRESOLVED_CACHE_TTL_DAYS,
    ):
        self.cache_file = cache_file
        self.auto_save = auto_save
        self.unresolved_ttl_days = unresolved_ttl_days
        self.cache = self._load_cache()
        self.dirty = False

    @staticmethod
    def _is_fresh(entry: Dict, days: int) -> bool:
        checked_at = entry.get("checked_at") if isinstance(entry, dict) else None
        if not checked_at:
            # Legacy entries have no timestamp: treat them as usable matches.
            return "status" not in (entry or {})
        try:
            age = datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromisoformat(
                checked_at
            )
        except (TypeError, ValueError):
            return False
        return age.days <= days

    def _load_cache(self) -> Dict:
        """Load cache from file (supports the legacy flat format)."""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    payload = json.load(f)
                if isinstance(payload, dict) and isinstance(payload.get("entries"), dict):
                    return payload["entries"]
                if isinstance(payload, dict):
                    return payload
            except Exception as e:
                print(f"Warning: Failed to load cache: {e}")
        return {}

    def save(self):
        """Persist the whole cache in one atomic write."""
        try:
            os.makedirs(os.path.dirname(self.cache_file) or ".", exist_ok=True)
            temp_file = f"{self.cache_file}.tmp"
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(
                    {"version": MATCH_CACHE_VERSION, "entries": self.cache},
                    f,
                    ensure_ascii=False,
                    indent=2,
                )
            os.replace(temp_file, self.cache_file)
            self.dirty = False
        except Exception as e:
            print(f"Warning: Failed to save cache: {e}")

    # Backwards-compatible alias used by older callers.
    _save_cache = save

    def flush(self):
        if self.dirty:
            self.save()

    def get(self, key: str) -> Optional[Dict]:
        """Get cached result"""
        return self.cache.get(key)

    def cached_match(self, key: str) -> Optional[Dict]:
        """Return a usable cached match dict, or ``None``."""
        entry = self.cache.get(key)
        if not isinstance(entry, dict):
            return None
        if entry.get("status") == "unresolved":
            return None
        if entry.get("status") == "matched" and not self._is_fresh(
            entry, MATCHED_CACHE_TTL_DAYS
        ):
            return None
        payload = entry.get("match", entry)
        if not isinstance(payload, dict) or payload.get("error"):
            return None
        return payload

    def is_fresh_unresolved(self, key: str) -> bool:
        entry = self.cache.get(key)
        return (
            isinstance(entry, dict)
            and entry.get("status") == "unresolved"
            and self._is_fresh(entry, self.unresolved_ttl_days)
        )

    def _store(self, key: str, entry: Dict) -> None:
        self.cache[key] = entry
        self.dirty = True
        if self.auto_save:
            self.save()

    def set(self, key: str, value: Dict):
        """Set a successful result and save (unless auto_save is disabled)."""
        self._store(key, {
            "status": "matched",
            "checked_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "match": value,
        })

    def set_unresolved(self, key: str, reason: str = "no valid result"):
        """Remember a failed lookup so the next run does not repeat the request."""
        self._store(key, {
            "status": "unresolved",
            "checked_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "reason": reason,
        })


class SettlementMatcher:
    """Handles settlement name matching using Nominatim with validation"""
    # NOTE: Previously we used a hard‑coded SPECIAL_CASES dict to rewrite
    # problematic settlement names (e.g. "תל אביב" → "תל‑אביב‑יפו").
    # This approach is brittle and does not scale.  We now rely on a generic
    # fallback that appends a country qualifier (", Israel") when the initial
    # Nominatim query yields no valid result.
    # The SPECIAL_CASES dict is removed.
    
    def __init__(self, cache: Optional[NominatimCache] = None):
        self.cache = cache if cache is not None else NominatimCache()
        self.last_request_time = 0
        # Exposed to the batch resolver so the audit CSV can say *why* a
        # Nominatim lookup was rejected, not just "no valid result".
        self.last_rejection_reason = ""
    
    @staticmethod
    def normalize_settlement_name(name: str) -> str:
        """
        Normalize settlement name for better Nominatim matching.
        
        Args:
            name: Original settlement name from LAMAS
            
        Returns:
            Normalized settlement name
        """
        if not name:
            return ""
        
        normalized = unicodedata.normalize('NFKC', name).strip()
        normalized = re.sub(r'\([^)]*\)|\)[^()]*\(', ' ', normalized)
        normalized = re.sub(r'[־\-–—]', ' ', normalized)
        normalized = re.sub(r'\s+(שבט|יישוב)\s*$', '', normalized)
        prefixes_to_remove = ['עיריית', 'מועצה מקומית', 'מועצה אזורית']
        suffixes_to_remove = ['מושב', 'קיבוץ', 'יישוב', 'עיר']
        for prefix in prefixes_to_remove:
            if normalized.startswith(prefix):
                normalized = normalized[len(prefix):].strip()
        for suffix in suffixes_to_remove:
            if normalized.endswith(suffix):
                normalized = normalized[: -len(suffix)].strip()
        return re.sub(r'\s+', ' ', normalized).strip()

    @classmethod
    def _normalized_match_name(cls, name: str) -> str:
        """Canonical comparison key shared with the offline settlement index."""
        return canonical_key(name)

    @classmethod
    def _query_variants(cls, settlement_name: str) -> list[str]:
        """Cleaned name first, then curated aliases (bounded query count)."""
        normalized = cls.normalize_settlement_name(settlement_name)
        key = canonical_key(normalized)
        aliases = list(SETTLEMENT_ALIASES.get(normalized, ()))
        aliases.extend(SETTLEMENT_ALIASES.get(key, ()))
        curated = lookup_by_settlement_key(load_aliases(), settlement_name)
        if curated:
            aliases.extend(curated)
        variants = [normalized]
        seen = {key}
        for alias in aliases:
            alias_key = canonical_key(alias)
            if alias_key and alias_key not in seen:
                seen.add(alias_key)
                variants.append(alias)
        return variants[:6]

    @staticmethod
    def _pinned_id_matches(result: Dict[str, Any], original_name: str) -> bool:
        """A verified config pin may override the suburb/class guard for one id."""
        pinned = lookup_by_settlement_key(load_osm_id_overrides(), original_name)
        if not pinned:
            return False
        pin_list = pinned if isinstance(pinned, (list, tuple)) else [pinned]
        result_id = str(result.get("osm_id") or "")
        result_type = str(result.get("osm_type") or "").strip().lower()
        for item in pin_list:
            value = str(item).strip().lower()
            prefix = value[0] if value[:1] in {"n", "w", "r"} else ""
            raw = value[1:] if prefix else value
            expected_type = {"n": "node", "w": "way", "r": "relation"}.get(prefix)
            if result_id == raw and (expected_type is None or result_type == expected_type):
                return True
        return False

    @classmethod
    def _other_settlement_in_address(
        cls, result: Dict[str, Any], query_names: list[str]
    ) -> Optional[Tuple[str, str]]:
        """Detect a feature nested inside a differently named settlement.

        Returns the offending ``(address key, value)``.  Fuzzy comparison is
        used so legitimate spelling variants (כאוכב/כווכב) are not rejected.
        Regional councils (``מועצה אזורית ...``) are enclosing administrative
        jurisdictions, not competing cities, so they are ignored here.
        """
        address = result.get('address', {})
        if not isinstance(address, dict):
            return None
        query_keys = {
            key for name in query_names if name and (key := canonical_key(name))
        }
        # A city's own address may use an English/alt name; those are still the
        # same settlement, not an enclosing different one.
        query_keys.update(
            key
            for name in cls._candidate_names(result)
            if name and (key := canonical_key(name))
        )
        if not query_keys:
            return None
        for key in ADDRESS_SETTLEMENT_KEYS:
            value = address.get(key)
            if not value:
                continue
            if is_regional_boundary_name(value):
                continue
            parent_key = canonical_key(value)
            if not parent_key or parent_key in query_keys:
                continue
            if max(ratio(parent_key, q) for q in query_keys) >= MIN_NAME_MATCH_SCORE:
                continue
            return key, str(value)
        result_name = str(result.get('name') or '').strip()
        if result_name:
            result_key = canonical_key(result_name)
            for key in ADDRESS_SUBDIVISION_KEYS:
                value = address.get(key)
                if value and canonical_key(value) == result_key:
                    return key, str(value)
        return None

    @classmethod
    def _candidate_names(cls, result: Dict[str, Any]) -> list[str]:
        names = []
        if result.get('name'):
            names.append(result['name'])
        namedetails = result.get('namedetails', {})
        if isinstance(namedetails, dict):
            for key, value in namedetails.items():
                if key.startswith(('name:', 'alt_name', 'official_name', 'short_name')):
                    names.extend(str(value).split(';'))
        display_name = result.get('display_name', '')
        if display_name:
            names.append(display_name.split(',')[0])
        return names

    @classmethod
    def _name_match_score(cls, result: Dict[str, Any], query_names: list[str]) -> float:
        accepted = {cls._normalized_match_name(name) for name in query_names}
        candidates = {
            cls._normalized_match_name(name)
            for name in cls._candidate_names(result)
            if name
        }
        if not accepted or not candidates:
            return 0
        return max(ratio(query, candidate) for query in accepted for candidate in candidates)
    
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
    
    def _validate_result(
        self,
        result: Dict,
        original_name: str,
        accepted_names: Optional[list[str]] = None,
    ) -> Tuple[bool, str]:
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
            category = result.get('category', result.get('class', ''))
            address = result.get('address', {})
            country_code = (
                address.get('country_code', '').casefold()
                if isinstance(address, dict)
                else ''
            )

            if country_code not in VALID_COUNTRY_CODES:
                return False, f"Country is not Israel/Palestine: {display_name}"

            # A neighbourhood/quarter with the same name is not a settlement.
            extratags = result.get('extratags', {})
            level = admin_level(
                extratags.get('admin_level', '') if isinstance(extratags, dict) else ''
            )
            pinned = self._pinned_id_matches(result, original_name)
            if (
                not pinned
                and (
                    str(place_type).casefold() in NON_SETTLEMENT_PLACE_TYPES
                    or level in NON_SETTLEMENT_ADMIN_LEVELS
                )
            ):
                return False, (
                    f"Non-settlement OSM feature ({category}/{place_type}"
                    f"{f', admin_level={level}' if level else ''}): "
                    f"{display_name}"
                )

            query_names = accepted_names or [original_name]
            if not pinned:
                candidate_names = self._candidate_names(result)
                if has_geresh_conflict(original_name, candidate_names):
                    return False, (
                        f"Geresh mismatch for '{original_name}': {display_name}"
                    )
                if has_qualifier_conflict(original_name, candidate_names):
                    return False, (
                        f"Settlement qualifier mismatch for '{original_name}': {display_name}"
                    )
                parent = self._other_settlement_in_address(result, query_names)
                if parent:
                    return False, (
                        f"Feature is inside another settlement "
                        f"(address {parent[0]}={parent[1]}): {display_name}"
                    )
            name_score = self._name_match_score(result, query_names)
            if not pinned and name_score < MIN_NAME_MATCH_SCORE:
                return False, f"OSM name does not match '{original_name}' (score {name_score:.1f})"

            exact_name = name_score == 100
            valid_place_class = category in VALID_PLACE_CLASSES
            valid_residential_alias = (
                category == 'landuse' and place_type == 'residential' and exact_name
            )
            if not pinned and not (valid_place_class or valid_residential_alias):
                return False, f"Not a settlement feature ({category}/{place_type}): {display_name}"

            if not result.get('osm_id') or result.get('osm_type') not in {'node', 'way', 'relation'}:
                return False, f"Missing usable OSM element ID: {display_name}"
            
            # Check 1: Geographic bounds
            if not self._is_within_israel(lat, lon):
                return False, f"Outside Israel/Palestine bounds: {display_name}"
            
            # Check 2: Previously we required the display name to contain specific region keywords.
            # This was too restrictive for settlements that are listed without explicit region tags.
            # We now only log a warning but allow the result to pass validation.
            # If the region is clearly outside Israel/Palestine we still reject it via the geographic check.
            # (No region keyword validation)
            
            # Check 3: Place type should be appropriate
            if (
                not pinned
                and place_type not in VALID_PLACE_TYPES
                and not valid_residential_alias
            ):
                return False, f"Invalid place type '{place_type}': {display_name}"
            
            # Check 4: Bounding box should be reasonable (not too large)
            bbox = result.get('boundingbox', [])
            if len(bbox) == 4:
                bbox_lat_range = float(bbox[1]) - float(bbox[0])
                bbox_lon_range = float(bbox[3]) - float(bbox[2])
                
                # Allow a larger bbox for city‑level place types (Fix 2)
                if result.get('type') in {'city', 'town', 'municipality', 'administrative'}:
                    max_range = 3.0   # up to ~300 km – enough for a metro area
                else:
                    max_range = 2.0   # original strict limit for villages, etc.
                
                if bbox_lat_range > max_range or bbox_lon_range > max_range:
                    return False, f"Bounding box too large ({bbox_lat_range:.2f}°, {bbox_lon_range:.2f}°): {display_name}"
            
            if pinned:
                return True, f"Pinned OSM id override (name score {name_score:.1f})"
            return True, f"Exact settlement name match ({name_score:.1f})"
            
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
        
        query_variants = self._query_variants(settlement_name)

        match = None
        for q in query_variants:
            if match:
                break
            print(f"  → Attempting Nominatim query variant: '{q}'")
            match = self._perform_nominatim_query(
                settlement_name, q, max_retries, query_variants
            )
            if not match:
                print(f"    ✗ Variant '{q}' yielded no valid result.")
        return match

    def _perform_nominatim_query(
        self,
        original_name: str,
        query_name: str,
        max_retries: int,
        accepted_names: list[str],
    ) -> Optional[SettlementMatch]:
        """
        Helper to perform a Nominatim query and handle caching/validation.
        The query_name is already a fully‑formed query (may include country qualifier).
        """
        cache_key = f"{MATCH_CACHE_VERSION}:{query_name}"
        self.last_rejection_reason = ""

        if self.cache.is_fresh_unresolved(cache_key):
            entry = self.cache.get(cache_key) or {}
            self.last_rejection_reason = str(
                entry.get("reason", "cached unresolved Nominatim result")
            )
            print(f"  ✓ Cache hit (known unresolved) for '{query_name}'")
            return None

        cached_result = self.cache.cached_match(cache_key)
        if cached_result:
            print(f"  ✓ Cache hit for query '{query_name}'")
            if not cached_result.get('error'):
                cached_match = self._dict_to_match(cached_result, original_name)
                cached_candidate = {
                    'name': cached_match.display_name.split(',')[0],
                    'display_name': cached_match.display_name,
                }
                cached_candidate.update({
                    'category': cached_match.category,
                    'type': cached_match.place_type,
                    'address': {'country_code': cached_match.country_code},
                    'lat': cached_match.lat,
                    'lon': cached_match.lon,
                    'osm_id': cached_match.osm_id,
                    'osm_type': cached_match.osm_type,
                })
                is_valid, validation_msg = self._validate_result(
                    cached_candidate, original_name, accepted_names
                )
                if is_valid:
                    cached_match.validation_message = validation_msg
                    if validation_msg.startswith("Pinned OSM id override"):
                        cached_match.match_source = "pinned"
                    return cached_match

        print(f"  → Searching Nominatim for '{query_name}'...")
        
        params = {
            'q': query_name,
            'format': 'jsonv2',
            'addressdetails': 1,
            'namedetails': 1,
            'limit': 10,
            'accept-language': 'he,en',
            'countrycodes': 'il,ps',
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
                    print(f"  ✗ No results found for '{original_name}'")
                    self.last_rejection_reason = "No Nominatim results"
                    self.cache.set_unresolved(cache_key, self.last_rejection_reason)
                    return None
                
                valid_results = []
                rejected_reasons = []
                for result in results:
                    is_valid, validation_msg = self._validate_result(
                        result, original_name, accepted_names
                    )
                    if is_valid:
                        valid_results.append((result, validation_msg))
                    else:
                        rejected_reasons.append(validation_msg)

                if valid_results:
                    valid_results.sort(
                        key=lambda item: (
                            self._name_match_score(item[0], accepted_names),
                            float(item[0].get('importance', 0)),
                        ),
                        reverse=True,
                    )
                    best_result, validation_msg = valid_results[0]
                    match = SettlementMatch(
                        settlement_name=original_name,
                        osm_id=str(best_result.get('osm_id', '')),
                        display_name=best_result.get('display_name', ''),
                        lat=float(best_result.get('lat', 0)),
                        lon=float(best_result.get('lon', 0)),
                        boundingbox=tuple(map(float, best_result.get('boundingbox', [0, 0, 0, 0]))),
                        place_type=best_result.get('type', ''),
                        importance=float(best_result.get('importance', 0)),
                        is_valid=True,
                        validation_message=validation_msg,
                        osm_type=best_result.get('osm_type', ''),
                        category=best_result.get('category', ''),
                        country_code=best_result.get('address', {}).get('country_code', ''),
                        name_score=self._name_match_score(best_result, accepted_names),
                        match_source=(
                            "pinned"
                            if validation_msg.startswith("Pinned OSM id override")
                            else "nominatim"
                        ),
                    )
                    print(f"  ✓ Valid match: {match.display_name}")
                    self.cache.set(cache_key, self._match_to_dict(match))
                    return match
                print(f"  ✗ No valid results for '{original_name}' (all failed validation)")
                self.last_rejection_reason = "; ".join(
                    dict.fromkeys(rejected_reasons)
                )[:500] or "all results failed validation"
                self.cache.set_unresolved(cache_key, self.last_rejection_reason)
                return None
                
            except (requests.exceptions.RequestException, ValueError) as e:
                self.last_rejection_reason = f"Nominatim request/validation error: {e}"
                print(f"  ⚠ Request error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                if isinstance(e, requests.exceptions.RequestException):
                    return None
                raise
        
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
            'validation_message': match.validation_message,
            'osm_type': match.osm_type,
            'category': match.category,
            'country_code': match.country_code,
            'match_source': match.match_source,
            'name_score': match.name_score,
            'center_distance_m': match.center_distance_m,
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
            validation_message=data.get('validation_message', ''),
            osm_type=data.get('osm_type', ''),
            category=data.get('category', ''),
            country_code=data.get('country_code', ''),
            match_source=data.get('match_source', 'nominatim'),
            name_score=float(data.get('name_score', 0.0) or 0.0),
            center_distance_m=data.get('center_distance_m'),
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
            print("✓ SUCCESS")
            print(f"  Display Name: {match.display_name}")
            print(f"  Coordinates: ({match.lat:.4f}, {match.lon:.4f})")
            print(f"  Type: {match.place_type}")
            print(f"  Validation: {match.validation_message}")
        else:
            print("✗ FAILED - No valid match found")
    
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
                    print("\n✓ Match found:")
                    print(f"  Display: {match.display_name}")
                    print(f"  Coords: ({match.lat:.4f}, {match.lon:.4f})")
                    print(f"  Type: {match.place_type}")
                else:
                    print("\n✗ No valid match found")
            else:
                # Run all tests
                test_settlement_matcher()
        else:
            print("Usage: python settlement_matcher.py --test [settlement_name]")
    else:
        print("Settlement Matcher Module")
        print("Usage: python settlement_matcher.py --test [settlement_name]")
