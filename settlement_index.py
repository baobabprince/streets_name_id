"""Offline-first matching of LAMAS settlements to OSM places.

Historically every settlement was geocoded with Nominatim, one rate-limited
request per name, and the result was written to a JSON file on every request.
This module replaces that with a single resolution pass:

1. a national OSM name index (administrative boundaries + ``place=*`` features
   extracted once from the local PBF),
2. canonical/fuzzy name matching shared with :mod:`settlement_names`,
3. geographic validation against the LAMAS settlement centres
   (``config/settlement_centers.json``, ITM -> WGS84),
4. a Nominatim fallback *only* for names the local index cannot explain,
5. one consolidated, atomically written cache with positive **and** negative
   results, plus a CSV audit report.

The output of :meth:`SettlementResolver.resolve` is a mapping of LAMAS
settlement name -> place dict, directly consumable by ``run_pipeline``.
"""

from __future__ import annotations

import csv
import datetime
import json
import logging
import math
import os
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

from rapidfuzz import fuzz, process

from settlement_matcher import NominatimCache, SettlementMatch, SettlementMatcher
from settlement_names import (
    ALIASES_FILE as ALIASES_PATH,
    CITY_ADMIN_LEVEL,
    MAX_SETTLEMENT_BBOX_SPAN_DEG as MAX_SPAN_DEG,
    MIN_SETTLEMENT_HALF_HEIGHT_LAT as MIN_PLACE_HALF_HEIGHT_LAT,
    MIN_SETTLEMENT_HALF_WIDTH_LON as MIN_PLACE_HALF_WIDTH_LON,
    NON_SETTLEMENT_ADMIN_LEVELS,
    admin_level as _admin_level,
    canonical_key,
    fuzzy_form,
    has_geresh_conflict,
    has_qualifier_conflict,
    is_regional_boundary_name,
    ktiv_haser_key,
    load_aliases,
    load_osm_id_overrides,
    lookup_by_settlement_key,
    name_variants,
    place_is_settlement,
    qualifier_match_bonus,
    settlement_identity_key,
)

logger = logging.getLogger(__name__)

ROOT = os.path.dirname(__file__)
DATA_DIR = os.path.join(ROOT, "data")
CENTERS_FILE = os.path.join(ROOT, "config", "settlement_centers.json")
STORE_PATH = os.path.join(DATA_DIR, "settlement_matches.json")
REPORT_PATH = os.path.join(ROOT, "batch_reports", "settlement_match_report.csv")

# v5: identity-keyed store/centres, geresh & qualifier separation (גת vs ג'ת,
# איחוד vs מאוחד, מושב vs קיבוץ), ktiv-haser tier, non-substring fuzzy matching,
# multi-OSM-id pins, and strict duplicate-OSM-id dropping.
STORE_VERSION = 5
MATCHED_TTL_DAYS = 90
UNRESOLVED_TTL_DAYS = 7
SCORE_CUTOFF = 88.0
MAX_CENTER_DISTANCE_M = 25_000.0
NAME_COLUMNS = (
    "name:he", "name", "name:en", "name:ar", "alt_name",
    "official_name", "old_name", "short_name",
)
# Valid Israel/Palestine bounding envelope for settlement centre coordinates.
_VALID_CENTER_BOUNDS = (29.4, 34.2, 33.4, 35.9)  # min_lat, min_lon, max_lat, max_lon


def _is_valid_center(lon: float, lat: float) -> bool:
    """Reject placeholder (31.0, 35.0) and out-of-Israel centre coordinates."""
    min_lat, min_lon, max_lat, max_lon = _VALID_CENTER_BOUNDS
    if not (min_lat <= lat <= max_lat and min_lon <= lon <= max_lon):
        return False
    # cities_geo.txt uses (31.0, 35.0) as a placeholder for ungeocoded settlements.
    if abs(lat - 31.0) < 0.005 and abs(lon - 35.0) < 0.005:
        return False
    return True


def _utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def haversine_m(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Great-circle distance in metres."""
    radius = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * radius * math.asin(min(1.0, math.sqrt(a)))


@lru_cache(maxsize=2)
def load_settlement_centers(path: str = CENTERS_FILE) -> Dict[str, Tuple[float, float]]:
    """Return ``{settlement identity key: (lon, lat)}`` from the ITM dataset."""
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        places = payload.get("places", payload)
        from pyproj import Transformer

        transformer = Transformer.from_crs("EPSG:2039", "EPSG:4326", always_xy=True)
        centers: Dict[str, Tuple[float, float]] = {}
        for name, coords in places.items():
            if not isinstance(coords, (list, tuple)) or len(coords) < 2:
                continue
            key = settlement_identity_key(name)
            if not key:
                continue
            lon, lat = transformer.transform(float(coords[0]), float(coords[1]))
            if _is_valid_center(lon, lat):
                centers[key] = (lon, lat)
        # Centres for renamed or mis-geocoded settlements in settlement_centers.json.
        alias_payload = {}
        if os.path.exists(ALIASES_PATH):
            with open(ALIASES_PATH, "r", encoding="utf-8") as handle:
                alias_payload = json.load(handle)
        for name, coords in (
            alias_payload.get("center_overrides", {})
            if isinstance(alias_payload, dict) else {}
        ).items():
            if str(name).startswith("_"):
                continue
            key = settlement_identity_key(name)
            if not key:
                continue
            if coords is None:
                centers.pop(key, None)
                continue
            if not isinstance(coords, (list, tuple)) or len(coords) < 2:
                continue
            lon, lat = transformer.transform(float(coords[0]), float(coords[1]))
            if _is_valid_center(lon, lat):
                centers[key] = (lon, lat)
        # Renamed settlements (נצרת עילית -> נוף הגליל) are in the centre file
        # under their old name; reuse that centre for the current LAMAS name.
        for name_key, aliases in load_aliases().items():
            if name_key in centers:
                continue
            for alias in aliases:
                alias_center = lookup_by_settlement_key(centers, alias)
                if alias_center is not None:
                    centers[name_key] = alias_center
                    break
        return centers
    except Exception as exc:  # config is optional; never block a batch on it
        logger.warning("Could not load settlement centres from %s: %s", path, exc)
        return {}


OSM_TYPE_PREFIXES = {"n": "node", "w": "way", "r": "relation"}


def _split_osm_id(raw: Any) -> Tuple[str, str]:
    value = str(raw or "").strip()
    prefix = value[:1].lower()
    if prefix in OSM_TYPE_PREFIXES and value[1:].isdigit():
        return value[1:], OSM_TYPE_PREFIXES[prefix]
    return value, ""


def _infer_osm_type(row: Any) -> str:
    """Recover node/way/relation when osmium exported a bare numeric id."""
    for column in ("osm_type", "@type", "element_type"):
        value = row.get(column) if hasattr(row, "get") else None
        normalized = str(value or "").strip().lower()
        if normalized in {"node", "way", "relation"}:
            return normalized
        if normalized in OSM_TYPE_PREFIXES:
            return OSM_TYPE_PREFIXES[normalized]
    return ""


def _row_names(row: Any) -> List[str]:
    names: List[str] = []
    for column in NAME_COLUMNS:
        value = row.get(column) if hasattr(row, "get") else None
        if value is None:
            continue
        text = str(value).strip()
        if not text or text.lower() in {"nan", "none", "<na>", "nat", "null"}:
            continue
        for part in text.replace("|", ";").split(";"):
            part = part.strip()
            if part:
                names.append(part)
    return list(dict.fromkeys(names))


@dataclass
class OSMPlace:
    """One OSM feature that can represent a settlement."""

    osm_id: str
    osm_type: str
    names: List[str]
    lat: float
    lon: float
    bbox: Tuple[float, float, float, float]  # Nominatim order: min_lat, max_lat, min_lon, max_lon
    place_type: str
    category: str
    admin_level: str = ""
    source: str = "pbf"
    is_settlement: bool = True
    inside_city: bool = False
    span_deg: float = 0.0
    has_polygon: bool = False
    osm_ids: List[str] = field(default_factory=list)

    @property
    def id_key(self) -> str:
        return f"{self.osm_type}:{self.osm_id}"


def _city_polygons(boundaries: Any):
    """(geometry, canonical name keys) of city-level boundaries (admin_level 8)."""
    if boundaries is None or getattr(boundaries, "empty", True):
        return []
    polygons = []
    for _, row in boundaries.iterrows():
        if _admin_level(row.get("admin_level")) != CITY_ADMIN_LEVEL:
            continue
        names = _row_names(row)
        if any(is_regional_boundary_name(n) for n in names):
            continue
        geometry = row.get("geometry")
        if geometry is None or getattr(geometry, "is_empty", True):
            continue
        keys = {canonical_key(name) for name in names}
        polygons.append((geometry, keys))
    return polygons


def _is_inside_other_city(point, name_keys: set, city_polygons) -> bool:
    """True when a feature sits inside a *differently named* city boundary."""
    for geometry, city_keys in city_polygons:
        if city_keys & name_keys:
            continue  # the city boundary itself, e.g. "יפו" inside תל אביב-יפו
        try:
            if geometry.contains(point):
                return True
        except Exception:
            continue
    return False


def build_osm_places(
    boundaries: Any = None, places: Any = None
) -> List[OSMPlace]:
    """Build a deduplicated OSM settlement index from PBF-derived GeoDataFrames.

    Features that are parts of a settlement (``place=suburb`` /
    ``neighbourhood`` / admin_level 9+) are kept for diagnostics but marked
    ``is_settlement=False`` so they can never win a LAMAS settlement match.
    """
    records: Dict[str, OSMPlace] = {}
    city_polygons = _city_polygons(boundaries)
    for frame, default_type, category in (
        (boundaries, "administrative", "boundary"),
        (places, "village", "place"),
    ):
        if frame is None or getattr(frame, "empty", True):
            continue
        for _, row in frame.iterrows():
            names = _row_names(row)
            geometry = row.get("geometry")
            if not names or geometry is None or getattr(geometry, "is_empty", True):
                continue
            try:
                point = geometry.representative_point()
                minx, miny, maxx, maxy = geometry.bounds
                has_polygon = getattr(geometry, "geom_type", "") in {
                    "Polygon",
                    "MultiPolygon",
                }
                if category == "place" and not has_polygon:
                    # Point features would produce a zero-area bbox downstream;
                    # pad only points, never exact settlement polygons.
                    center_x, center_y = (minx + maxx) / 2, (miny + maxy) / 2
                    minx = min(minx, center_x - MIN_PLACE_HALF_WIDTH_LON)
                    maxx = max(maxx, center_x + MIN_PLACE_HALF_WIDTH_LON)
                    miny = min(miny, center_y - MIN_PLACE_HALF_HEIGHT_LAT)
                    maxy = max(maxy, center_y + MIN_PLACE_HALF_HEIGHT_LAT)
            except Exception:
                continue
            osm_id, osm_type = _split_osm_id(
                row.get("osm_id") or row.get("@id") or row.get("id")
            )
            if not osm_type:
                osm_type = _infer_osm_type(row)
            if not osm_id:
                continue
            key = f"{osm_type}:{osm_id}"
            place_type = str(row.get("place") or default_type)
            admin_level = _admin_level(row.get("admin_level"))
            is_settlement = place_is_settlement(place_type, admin_level, category)
            span_deg = max(maxy - miny, maxx - minx)
            if category == "boundary":
                if span_deg > MAX_SPAN_DEG or any(
                    is_regional_boundary_name(n) for n in names
                ):
                    is_settlement = False  # district / regional council, not a town
            name_keys = {canonical_key(name) for name in names}
            inside_city = (
                category == "place"
                and not is_settlement
                and _is_inside_other_city(point, name_keys, city_polygons)
            )
            record = OSMPlace(
                osm_id=osm_id,
                osm_type=osm_type,
                names=names,
                lat=float(point.y),
                lon=float(point.x),
                bbox=(miny, maxy, minx, maxx),
                place_type=place_type,
                category=category,
                admin_level=admin_level,
                is_settlement=is_settlement,
                inside_city=inside_city,
                span_deg=span_deg,
                has_polygon=has_polygon,
            )
            existing = records.get(key)
            if existing is None:
                records[key] = record
            else:
                existing.names = list(dict.fromkeys([*existing.names, *names]))
                existing.is_settlement = existing.is_settlement or is_settlement
                existing.inside_city = existing.inside_city and inside_city
                existing.has_polygon = existing.has_polygon or has_polygon
                if category == "place":
                    existing.place_type = place_type
    return list(records.values())


@dataclass
class MatchDecision:
    """Outcome of one settlement lookup, including why it was rejected."""

    place: Optional[OSMPlace] = None
    score: float = 0.0
    distance_m: Optional[float] = None
    matched_name: str = ""
    reason: str = ""
    demoted: List[str] = field(default_factory=list)
    rejected_types: List[str] = field(default_factory=list)
    pinned: bool = False


class SettlementNameIndex:
    """Canonical + ktiv-haser + fuzzy index that only returns whole-settlement features."""

    def __init__(self, places: Sequence[OSMPlace]):
        self.places = list(places)
        self.exact: Dict[str, List[OSMPlace]] = {}
        self.ktiv_haser: Dict[str, List[OSMPlace]] = {}
        self.fuzzy: Dict[str, List[OSMPlace]] = {}
        self.settlement_fuzzy: Dict[str, List[OSMPlace]] = {}
        self.non_settlement_fuzzy: Dict[str, List[OSMPlace]] = {}
        self.display_by_form: Dict[str, str] = {}
        self.by_id: Dict[str, OSMPlace] = {}
        self.by_raw_id: Dict[str, List[OSMPlace]] = {}
        for place in self.places:
            self.by_id[place.id_key] = place
            self.by_raw_id.setdefault(str(place.osm_id), []).append(place)
            for name in place.names:
                key = canonical_key(name)
                if key:
                    self.exact.setdefault(key, []).append(place)
                kh = ktiv_haser_key(name)
                if kh:
                    self.ktiv_haser.setdefault(kh, []).append(place)
                form = fuzzy_form(name)
                # Ignore 1-character names (e.g. Be'er Sheva neighbourhoods "א'",
                # "ב'") so they never pollute fuzzy search or diagnostics.
                if form and len(form.replace(" ", "")) >= 2:
                    self.fuzzy.setdefault(form, []).append(place)
                    self.display_by_form.setdefault(form, name)
                    if place.is_settlement:
                        self.settlement_fuzzy.setdefault(form, []).append(place)
                    else:
                        self.non_settlement_fuzzy.setdefault(form, []).append(place)
        self.fuzzy_choices = list(self.fuzzy)
        self.settlement_fuzzy_choices = list(self.settlement_fuzzy)
        self.non_settlement_fuzzy_choices = list(self.non_settlement_fuzzy)

    @staticmethod
    def _rank(place: OSMPlace) -> tuple:
        poly_rank = 0 if place.has_polygon else 1
        # Real populated settlements (city/town/village/hamlet/administrative)
        # always outrank generic localities (e.g. Kibbutz Gat village vs. Tel Zafit locality).
        locality_rank = 1 if place.place_type == "locality" else 0
        type_rank = 0 if place.category == "boundary" else 1
        level_rank = {"8": 0, "7": 1}.get(place.admin_level, 2)
        return (poly_rank, locality_rank, type_rank, level_rank, place.id_key)

    @staticmethod
    def _describe(place: OSMPlace) -> str:
        label = f"{place.category}/{place.place_type}"
        if place.inside_city:
            label += ",inside another city"
        if place.admin_level in NON_SETTLEMENT_ADMIN_LEVELS:
            label += f",admin_level={place.admin_level}"
        return f"{place.id_key}({label})"

    @staticmethod
    def _best_display_name(place: OSMPlace, fallback: str) -> str:
        """Pick an un-folded human-readable OSM name matching ``fallback``."""
        if not place.names:
            return fallback
        fb_key = canonical_key(fallback)
        for name in place.names:
            if canonical_key(name) == fb_key:
                return name
        return place.names[0]

    def _lookup_pinned_places(self, raw_pin: str) -> List[OSMPlace]:
        value = str(raw_pin).strip().lower()
        prefix = value[0] if value[:1] in {"n", "w", "r"} else ""
        raw = value[1:] if prefix else value
        if not raw.isdigit():
            return []
        if prefix:
            osm_type = {"n": "node", "w": "way", "r": "relation"}[prefix]
            key = f"{osm_type}:{raw}"
            if key in self.by_id:
                return [self.by_id[key]]
            # Local osmium exports often omit the n/w/r prefix (osm_type == "");
            # allow matching an untyped entry whose geometry matches the prefix.
            untyped = self.by_id.get(f":{raw}")
            if untyped is not None:
                if prefix == "n" and not untyped.has_polygon:
                    return [untyped]
                if prefix in {"w", "r"} and untyped.has_polygon:
                    return [untyped]
            return []
        return list(self.by_raw_id.get(raw, []))

    def decide(
        self,
        variants: Sequence[str],
        center: Optional[Tuple[float, float]],
        score_cutoff: float = SCORE_CUTOFF,
        max_distance_m: float = MAX_CENTER_DISTANCE_M,
        pinned_id: Optional[Union[str, Sequence[str]]] = None,
        original_name: Optional[str] = None,
    ) -> MatchDecision:
        """Choose a settlement feature; neighbourhoods can never win."""
        guard_name = original_name or (variants[0] if variants else "")

        def acceptable(place: OSMPlace) -> bool:
            dist = self._distance(place, center)
            return dist is None or dist <= max_distance_m

        def compatible(place: OSMPlace) -> bool:
            if not guard_name:
                return True
            if has_geresh_conflict(guard_name, place.names):
                return False
            if has_qualifier_conflict(guard_name, place.names):
                return False
            return True

        if pinned_id:
            # Accept a single pin ("n123"/"w123"/"r123"/bare numeric) or a list
            # of pins (for merged settlements like שער שומרון = שערי תקווה + עץ אפרים).
            if isinstance(pinned_id, (list, tuple)):
                resolved: List[OSMPlace] = []
                pin_tokens: List[str] = []
                for token in pinned_id:
                    cands = [p for p in self._lookup_pinned_places(str(token)) if acceptable(p)]
                    if cands:
                        resolved.append(cands[0])
                        pin_tokens.append(str(token).strip())
                if not resolved:
                    return MatchDecision(
                        reason=f"Pinned OSM ids {pinned_id!r} are not in the local OSM index"
                    )
                if len(resolved) == 1:
                    place = resolved[0]
                    return MatchDecision(
                        place=place, score=100.0,
                        distance_m=self._distance(place, center),
                        matched_name=guard_name or (place.names[0] if place.names else ""),
                        pinned=True,
                    )
                min_lat = min(p.bbox[0] for p in resolved)
                max_lat = max(p.bbox[1] for p in resolved)
                min_lon = min(p.bbox[2] for p in resolved)
                max_lon = max(p.bbox[3] for p in resolved)
                avg_lat = sum(p.lat for p in resolved) / len(resolved)
                avg_lon = sum(p.lon for p in resolved) / len(resolved)
                composite = OSMPlace(
                    osm_id="+".join(p.osm_id for p in resolved),
                    osm_type="composite",
                    names=[guard_name] if guard_name else [p.names[0] for p in resolved if p.names],
                    lat=avg_lat,
                    lon=avg_lon,
                    bbox=(min_lat, max_lat, min_lon, max_lon),
                    place_type=resolved[0].place_type,
                    category=resolved[0].category,
                    is_settlement=True,
                    has_polygon=any(p.has_polygon for p in resolved),
                    osm_ids=pin_tokens,
                )
                return MatchDecision(
                    place=composite, score=100.0,
                    distance_m=self._distance(composite, center),
                    matched_name=guard_name or composite.names[0],
                    pinned=True,
                )

            candidates = self._lookup_pinned_places(str(pinned_id))
            if not candidates:
                return MatchDecision(
                    reason=f"Pinned OSM id {pinned_id!r} is not in the local OSM index"
                )
            for place in candidates:
                if acceptable(place):
                    return MatchDecision(
                        place=place, score=100.0,
                        distance_m=self._distance(place, center),
                        matched_name=place.names[0] if place.names else "",
                        pinned=True,
                    )
            return MatchDecision(
                reason=f"Pinned OSM id {pinned_id!r} is too far from the LAMAS centre"
            )

        demoted: List[str] = []
        demoted_ids: set[str] = set()
        rejected_types: List[str] = []

        def note_demoted(place: OSMPlace) -> None:
            if place.id_key in demoted_ids:
                return
            demoted_ids.add(place.id_key)
            demoted.append(self._describe(place))
            rejected_types.append(place.place_type)

        def exact_rank(decision: MatchDecision) -> tuple:
            place = decision.place
            assert place is not None
            dist_m = decision.distance_m or 0.0
            near_bucket = 0 if dist_m <= 3000.0 else 1
            qual_bonus = qualifier_match_bonus(guard_name, place.names)
            return (near_bucket, -qual_bonus, self._rank(place), dist_m)

        # 1. Exact canonical match.  A settlement feature beats any
        #    suburb/neighbourhood; opposing qualifiers and ג'/ג conflicts are
        #    rejected so twin settlements (גת vs ג'ת, איחוד vs מאוחד) never collide.
        best_exact: Optional[MatchDecision] = None
        for variant in variants:
            key = canonical_key(variant)
            for place in self.exact.get(key, []):
                if not acceptable(place):
                    continue
                if not place.is_settlement:
                    if compatible(place):
                        note_demoted(place)
                    continue
                if not compatible(place):
                    continue
                decision = MatchDecision(
                    place=place, score=100.0,
                    distance_m=self._distance(place, center),
                    matched_name=self._best_display_name(place, variant),
                )
                if best_exact is None or exact_rank(decision) < exact_rank(best_exact):
                    best_exact = decision

        # 1.5. Hebrew ktiv-haser (plene/defective) match (e.g. עופרה <-> עפרה,
        #      יוקנעם <-> יקנעם, סוואעד <-> סואעד, קידר <-> קדר, עינב <-> ענב).
        best_ktiv: Optional[MatchDecision] = None
        if best_exact is None:
            for variant in variants:
                kh = ktiv_haser_key(variant)
                if not kh:
                    continue
                for place in self.ktiv_haser.get(kh, []):
                    if not acceptable(place):
                        continue
                    if not place.is_settlement:
                        if compatible(place):
                            note_demoted(place)
                        continue
                    if not compatible(place):
                        continue
                    decision = MatchDecision(
                        place=place, score=99.0,
                        distance_m=self._distance(place, center),
                        matched_name=self._best_display_name(place, variant),
                    )
                    if best_ktiv is None or exact_rank(decision) < exact_rank(best_ktiv):
                        best_ktiv = decision

        # 2. Bounded whole-name fuzzy match (fuzz.ratio — never partial/WRatio,
        #    so short substrings like לח"י/קלחים or 1-letter neighbourhoods cannot match).
        query_forms = list(
            dict.fromkeys(
                f for v in variants
                if (f := fuzzy_form(v)) and len(f.replace(" ", "")) >= 2
            )
        )
        best: Optional[MatchDecision] = None
        best_effective = -1.0
        if best_exact is None and best_ktiv is None:
            for query in query_forms:
                q_compact_len = len(query.replace(" ", ""))
                for choice, score, _ in process.extract(
                    query, self.settlement_fuzzy_choices, scorer=fuzz.ratio,
                    score_cutoff=score_cutoff, limit=6,
                ):
                    c_compact_len = len(choice.replace(" ", ""))
                    if min(q_compact_len, c_compact_len) / max(q_compact_len, c_compact_len) < 0.70:
                        continue
                    for place in self.settlement_fuzzy.get(choice, []):
                        if not acceptable(place) or not compatible(place):
                            continue
                        dist = self._distance(place, center)
                        poly_bonus = 2.0 if place.has_polygon else 0.0
                        qual_bonus = 3.0 * qualifier_match_bonus(guard_name, place.names)
                        locality_penalty = 2.0 if place.place_type == "locality" else 0.0
                        effective = (
                            score + poly_bonus + qual_bonus - locality_penalty
                            - min((dist or 0.0) / 1000.0, 25.0) * 0.4
                        )
                        if effective > best_effective:
                            best_effective = effective
                            best = MatchDecision(
                                place=place, score=float(score), distance_m=dist,
                                matched_name=self.display_by_form.get(choice, choice),
                            )

        # Record non-settlement fuzzy candidates for audit diagnostics only
        # (never allowed to crowd out real settlements).
        for query in query_forms:
            q_compact_len = len(query.replace(" ", ""))
            for choice, _score, _ in process.extract(
                query, self.non_settlement_fuzzy_choices, scorer=fuzz.ratio,
                score_cutoff=score_cutoff, limit=4,
            ):
                c_compact_len = len(choice.replace(" ", ""))
                if min(q_compact_len, c_compact_len) / max(q_compact_len, c_compact_len) < 0.70:
                    continue
                for place in self.non_settlement_fuzzy.get(choice, []):
                    if acceptable(place) and compatible(place):
                        note_demoted(place)

        winner = best_exact or best_ktiv or best
        if winner is not None:
            winner.demoted = list(demoted)
            winner.rejected_types = list(rejected_types)
            return winner

        if demoted:
            kinds = ", ".join(sorted({t for t in rejected_types if t}))
            return MatchDecision(
                reason=(
                    "Only non-settlement OSM features matched "
                    f"({kinds or 'suburb/neighbourhood'}): "
                    + ", ".join(dict.fromkeys(demoted))[:300]
                )
            )
        return MatchDecision(reason="No OSM settlement name candidate found")

    @staticmethod
    def _distance(
        place: OSMPlace, center: Optional[Tuple[float, float]]
    ) -> Optional[float]:
        if center is None:
            return None
        return haversine_m(center[0], center[1], place.lon, place.lat)

    def best_for(
        self,
        variants: Sequence[str],
        center: Optional[Tuple[float, float]],
        score_cutoff: float = SCORE_CUTOFF,
        max_distance_m: float = MAX_CENTER_DISTANCE_M,
    ) -> Optional[Tuple[OSMPlace, float, Optional[float], str]]:
        """Backwards-compatible tuple form of :meth:`decide`."""
        decision = self.decide(variants, center, score_cutoff, max_distance_m)
        if decision.place is None:
            return None
        return (
            decision.place,
            decision.score,
            decision.distance_m,
            decision.matched_name,
        )


def match_settlements_offline(
    settlements: Iterable[str],
    index: SettlementNameIndex,
    centers: Optional[Dict[str, Tuple[float, float]]] = None,
    aliases: Optional[Dict[str, List[str]]] = None,
    score_cutoff: float = SCORE_CUTOFF,
    max_distance_m: float = MAX_CENTER_DISTANCE_M,
    rejections: Optional[Dict[str, str]] = None,
    pinned: Optional[Dict[str, str]] = None,
) -> Dict[str, SettlementMatch]:
    """Resolve LAMAS names against the local OSM index.

    ``rejections`` (optional) collects the reason for every name that could not
    be resolved — notably names that only match a suburb/neighbourhood.
    ``pinned`` maps a canonical LAMAS name to an explicit OSM id for the rare
    settlement that is genuinely mapped as a non-settlement feature.
    """
    centers = load_settlement_centers() if centers is None else centers
    aliases = load_aliases() if aliases is None else aliases
    pinned = load_osm_id_overrides() if pinned is None else pinned
    matches: Dict[str, SettlementMatch] = {}
    for settlement in settlements:
        center = lookup_by_settlement_key(centers, settlement)
        decision = index.decide(
            name_variants(settlement, aliases),
            center,
            score_cutoff=score_cutoff,
            max_distance_m=max_distance_m,
            pinned_id=lookup_by_settlement_key(pinned, settlement),
            original_name=settlement,
        )
        if decision.place is None:
            if rejections is not None:
                rejections[settlement] = decision.reason
            continue
        place = decision.place
        note = " (pinned override)" if decision.pinned else ""
        if decision.demoted:
            demoted_count = len(set(decision.demoted))
            kinds = ", ".join(
                sorted({t for t in decision.rejected_types if t})
            )
            note += (
                f"; ignored {demoted_count} non-settlement OSM candidates "
                f"({kinds or 'suburb/neighbourhood'})"
            )
        matches[settlement] = SettlementMatch(
            settlement_name=settlement,
            osm_id=place.osm_id,
            osm_type=place.osm_type,
            display_name=(
                f"{decision.matched_name}, Israel"
                if decision.matched_name else settlement
            ),
            lat=place.lat,
            lon=place.lon,
            boundingbox=place.bbox,
            place_type=place.place_type,
            importance=0.0,
            is_valid=True,
            validation_message=(
                f"Local OSM {place.category}/{place.place_type} match on "
                f"'{decision.matched_name}' (score {decision.score:.1f}){note}"
            ),
            category=place.category,
            country_code="il",
            match_source="pinned" if decision.pinned else "pbf",
            name_score=decision.score,
            center_distance_m=decision.distance_m,
            osm_ids=tuple(place.osm_ids),
        )
    return matches


class SettlementMatchStore:
    """One JSON file holding every settlement resolution (positive or negative)."""

    def __init__(
        self,
        path: str = STORE_PATH,
        matched_ttl_days: int = MATCHED_TTL_DAYS,
        unresolved_ttl_days: int = UNRESOLVED_TTL_DAYS,
    ):
        self.path = path
        self.matched_ttl_days = matched_ttl_days
        self.unresolved_ttl_days = unresolved_ttl_days
        self.entries: Dict[str, Dict[str, Any]] = {}
        self._load()

    @staticmethod
    def key(settlement: str) -> str:
        return settlement_identity_key(settlement)

    def _load(self) -> None:
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            if isinstance(payload, dict) and payload.get("version") == STORE_VERSION:
                self.entries = payload.get("entries", {})
        except (OSError, ValueError) as exc:
            logger.warning("Could not read settlement match store %s: %s", self.path, exc)

    @staticmethod
    def _fresh(entry: Dict[str, Any], ttl_days: int) -> bool:
        try:
            checked = datetime.datetime.fromisoformat(entry["checked_at"])
            if checked.tzinfo is None:
                checked = checked.replace(tzinfo=datetime.timezone.utc)
            age = datetime.datetime.now(datetime.timezone.utc) - checked
            return age.days <= ttl_days
        except (KeyError, TypeError, ValueError):
            return False

    def get(self, settlement: str) -> Optional[Dict[str, Any]]:
        entry = self.entries.get(self.key(settlement))
        if not isinstance(entry, dict):
            return None
        ttl = (
            self.unresolved_ttl_days
            if entry.get("status") == "unresolved"
            else self.matched_ttl_days
        )
        return entry if self._fresh(entry, ttl) else None

    def set_many(self, entries: Iterable[Dict[str, Any]]) -> None:
        for entry in entries:
            settlement = entry["settlement"]
            self.entries[self.key(settlement)] = {
                **entry,
                "checked_at": entry.get("checked_at") or _utc_now(),
            }

    def save(self) -> None:
        """Persist the whole store with a single atomic write."""
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        temp_path = f"{self.path}.tmp"
        with open(temp_path, "w", encoding="utf-8") as handle:
            json.dump(
                {
                    "version": STORE_VERSION,
                    "generated_at": _utc_now(),
                    "entries": self.entries,
                },
                handle,
                ensure_ascii=False,
                indent=2,
            )
        os.replace(temp_path, self.path)
        logger.info("Saved %d settlement resolutions to %s", len(self.entries), self.path)

    def stats(self, settlements: Optional[Sequence[str]] = None) -> Dict[str, int]:
        entries = (
            [self.entries[self.key(name)] for name in settlements if self.key(name) in self.entries]
            if settlements is not None
            else list(self.entries.values())
        )
        stats: Dict[str, int] = {
            "matched": 0, "unresolved": 0, "pbf": 0, "nominatim": 0, "pinned": 0
        }
        for entry in entries:
            if entry.get("status") == "matched":
                stats["matched"] += 1
                source = entry.get("source", "nominatim")
                stats[source if source in ("pbf", "nominatim", "pinned") else "nominatim"] += 1
            else:
                stats["unresolved"] += 1
        return stats


def write_match_report(
    settlements: Sequence[str],
    matches: Dict[str, Dict[str, Any]],
    unresolved: Dict[str, str],
    path: str = REPORT_PATH,
) -> str:
    """Write one CSV audit trail of the whole settlement matching pass."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            "settlement", "status", "source", "name_score", "center_distance_m",
            "osm_id", "osm_type", "display_name", "reason",
            "validation_message",
        ])
        for settlement in settlements:
            place = matches.get(settlement)
            if place:
                writer.writerow([
                    settlement, "matched", place.get("match_source", ""),
                    place.get("name_score", ""), place.get("center_distance_m", ""),
                    place.get("osm_id", ""), place.get("osm_type", ""),
                    place.get("display_name", ""), "",
                    place.get("validation_message", ""),
                ])
            else:
                writer.writerow([
                    settlement, "unresolved", "", "", "", "", "", "",
                    unresolved.get(settlement, "not checked"), "",
                ])
    return path


@dataclass
class ResolveResult:
    matches: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    unresolved: Dict[str, str] = field(default_factory=dict)
    stats: Dict[str, Any] = field(default_factory=dict)


class SettlementResolver:
    """Resolve a whole LAMAS settlement list once, offline first."""

    def __init__(
        self,
        store: Optional[SettlementMatchStore] = None,
        matcher: Optional[SettlementMatcher] = None,
        nominatim_cache: Optional[NominatimCache] = None,
        use_pbf: bool = True,
        allow_nominatim: bool = True,
        offline_index: Optional[SettlementNameIndex] = None,
        boundaries: Any = None,
        places: Any = None,
        centers: Optional[Dict[str, Tuple[float, float]]] = None,
        aliases: Optional[Dict[str, List[str]]] = None,
        score_cutoff: float = SCORE_CUTOFF,
        max_center_distance_m: float = MAX_CENTER_DISTANCE_M,
    ):
        self.store = store or SettlementMatchStore()
        self.matcher = matcher
        self.nominatim_cache = nominatim_cache
        self.use_pbf = use_pbf
        self.allow_nominatim = allow_nominatim
        self.offline_index = offline_index
        self._boundaries = boundaries
        self._places = places
        self.centers = load_settlement_centers() if centers is None else centers
        self.aliases = load_aliases() if aliases is None else aliases
        self.score_cutoff = score_cutoff
        self.max_center_distance_m = max_center_distance_m

    def _get_offline_index(self) -> Optional[SettlementNameIndex]:
        if self.offline_index is not None:
            return self.offline_index
        if not self.use_pbf:
            return None
        try:
            from osm_pbf_loader import load_national_boundaries, load_national_places

            boundaries, places = self._boundaries, self._places
            if boundaries is None:
                try:
                    boundaries = load_national_boundaries()
                except Exception as exc:
                    logger.warning("OSM boundary cache unavailable: %s", exc)
                    boundaries = None
            if places is None:
                try:
                    places = load_national_places()
                except Exception as exc:
                    logger.warning("OSM place cache unavailable: %s", exc)
                    places = None
            osm_places = build_osm_places(boundaries, places)
            if not osm_places:
                return None
            self.offline_index = SettlementNameIndex(osm_places)
            logger.info(
                "Built offline OSM settlement index with %d features / %d names",
                len(osm_places),
                len(self.offline_index.fuzzy_choices),
            )
            return self.offline_index
        except Exception as exc:
            logger.warning("Offline settlement index unavailable: %s", exc)
            return None

    def _nominatim_matches(
        self, settlements: Sequence[str]
    ) -> Tuple[Dict[str, SettlementMatch], Dict[str, str]]:
        matches: Dict[str, SettlementMatch] = {}
        unresolved: Dict[str, str] = {}
        cache = self.nominatim_cache or NominatimCache(auto_save=False)
        matcher = self.matcher or SettlementMatcher(cache=cache)
        try:
            for settlement in settlements:
                match = matcher.search_settlement(settlement)
                if match is None:
                    last_reason = getattr(matcher, "last_rejection_reason", "")
                    unresolved[settlement] = (
                        last_reason
                        if isinstance(last_reason, str) and last_reason.strip()
                        else "No valid Nominatim result"
                    )
                    continue
                center = lookup_by_settlement_key(self.centers, settlement)
                if center:
                    distance = haversine_m(center[0], center[1], match.lon, match.lat)
                    match.center_distance_m = distance
                    if distance > self.max_center_distance_m:
                        unresolved[settlement] = (
                            f"Nominatim result {distance / 1000:.1f} km from LAMAS centre"
                        )
                        continue
                match.name_score = match.name_score or 100.0
                matches[settlement] = match
        finally:
            cache.flush()  # one write for the whole fallback pass
        return matches, unresolved

    def _drop_duplicate_osm_ids(
        self,
        matches: Dict[str, SettlementMatch],
        unresolved: Dict[str, str],
        place_dicts: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> None:
        """Two LAMAS names pointing at one OSM feature is a matching error."""
        by_id: Dict[str, List[str]] = {}
        for name, match in matches.items():
            by_id.setdefault(f"{match.osm_type}:{match.osm_id}", []).append(name)
        if place_dicts:
            for name, place in list(place_dicts.items()):
                if name in matches:
                    continue
                by_id.setdefault(f"{place.get('osm_type', '')}:{place.get('osm_id', '')}", []).append(name)
        for names in by_id.values():
            if len(names) < 2:
                continue

            def _rank_claim(name: str) -> tuple:
                if name in matches:
                    score = float(matches[name].name_score or 0.0)
                    dist = matches[name].center_distance_m
                    pinned = matches[name].match_source == "pinned"
                elif place_dicts:
                    place = place_dicts.get(name, {})
                    score = float(place.get("name_score") or 0.0)
                    dist = place.get("center_distance_m")
                    pinned = place.get("match_source") == "pinned"
                else:
                    score, dist, pinned = 0.0, None, False
                has_dist = dist is not None
                return (
                    0 if pinned else 1,
                    -score,
                    0 if has_dist else 1,
                    dist if has_dist else float("inf"),
                    name,
                )

            names.sort(key=_rank_claim)
            winner = names[0]
            for loser in names[1:]:
                kept_id = matches[loser].osm_id if loser in matches else (
                    place_dicts.get(loser, {}).get("osm_id") if place_dicts else None
                )
                unresolved[loser] = (
                    f"Duplicate OSM id {kept_id} (kept for {winner})"
                )
                matches.pop(loser, None)
                if place_dicts:
                    place_dicts.pop(loser, None)

    def resolve(
        self, settlements: Sequence[str], refresh: bool = False
    ) -> ResolveResult:
        """Resolve ``settlements`` in one pass and persist the store once."""
        matches_objs: Dict[str, SettlementMatch] = {}
        matches: Dict[str, Dict[str, Any]] = {}
        unresolved: Dict[str, str] = {}
        pending: List[str] = []
        new_entries: List[Dict[str, Any]] = []
        reused_names: set[str] = set()
        reused = 0

        # 1. Reuse fresh, previously resolved names (including negative results).
        for settlement in settlements:
            entry = None if refresh else self.store.get(settlement)
            if entry:
                reused += 1
                reused_names.add(settlement)
                if entry.get("status") == "matched" and entry.get("match"):
                    matches[settlement] = dict(entry["match"])
                else:
                    unresolved[settlement] = entry.get("reason", "Previously unresolved")
                continue
            pending.append(settlement)

        # 2. Offline index for everything not covered by the store.
        offline_count = 0
        nominatim_count = 0
        offline_rejections: Dict[str, str] = {}
        if pending:
            index = self._get_offline_index()
            if index is not None:
                offline = match_settlements_offline(
                    pending,
                    index,
                    centers=self.centers,
                    aliases=self.aliases,
                    score_cutoff=self.score_cutoff,
                    max_distance_m=self.max_center_distance_m,
                    rejections=offline_rejections,
                    pinned=load_osm_id_overrides(),
                )
                matches_objs.update(offline)
                offline_count = len(offline)
                pending = [name for name in pending if name not in offline]

        # 3. Nominatim only for the leftovers.  Offline rejection reasons are
        #    kept in the audit trail even when the fallback also fails.
        if pending and self.allow_nominatim:
            nominatim_matches, nominatim_unresolved = self._nominatim_matches(pending)
            matches_objs.update(nominatim_matches)
            for name, reason in nominatim_unresolved.items():
                offline_reason = offline_rejections.get(name)
                unresolved[name] = (
                    f"{offline_reason} | {reason}" if offline_reason else reason
                )
            nominatim_count = len(nominatim_matches)
        elif pending:
            for settlement in pending:
                unresolved[settlement] = offline_rejections.get(
                    settlement, "Not found in local OSM index"
                )
        for name, reason in offline_rejections.items():
            if name not in matches_objs and name not in matches:
                unresolved.setdefault(name, reason)

        self._drop_duplicate_osm_ids(matches_objs, unresolved, matches)

        # 4. Persist only names that were actually (re)checked, once.
        for settlement in settlements:
            if settlement in reused_names and not refresh:
                stored = self.store.get(settlement)
                stored_matched = bool(stored and stored.get("status") == "matched")
                if stored_matched == (settlement in matches):
                    continue  # unchanged; no need to rewrite this entry
            match_obj = matches_objs.get(settlement)
            if match_obj:
                place = match_obj.to_place_dict()
                matches[settlement] = place
                new_entries.append({
                    "settlement": settlement,
                    "status": "matched",
                    "source": match_obj.match_source,
                    "name_score": match_obj.name_score,
                    "center_distance_m": match_obj.center_distance_m,
                    "validation_message": place.get("validation_message", ""),
                    "match": place,
                })
            else:
                unresolved.setdefault(settlement, "No valid settlement match")
                new_entries.append({
                    "settlement": settlement,
                    "status": "unresolved",
                    "source": "",
                    "reason": unresolved[settlement],
                })

        self.store.set_many(new_entries)
        try:
            self.store.save()  # the single consolidated write for this run
        except OSError as exc:
            logger.warning("Could not persist settlement match store: %s", exc)

        stats = {
            "requested": len(settlements),
            "reused_from_store": reused,
            "matched_offline": offline_count,
            "matched_nominatim": nominatim_count,
            "matched": len(matches),
            "unresolved": len(unresolved),
            "rejected_non_settlement": sum(
                1
                for reason in offline_rejections.values()
                if "non-settlement" in reason.lower()
            ),
            "matched_pinned": sum(
                1 for match in matches_objs.values()
                if match.match_source == "pinned"
            ),
        }
        logger.info("Settlement resolution: %s", stats)
        return ResolveResult(matches=matches, unresolved=unresolved, stats=stats)
