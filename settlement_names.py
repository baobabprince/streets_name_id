"""Shared settlement-name normalization for LAMAS and OSM records.

Both datasets spell the same settlement in different ways (dashes vs. spaces,
parenthesised annotations such as ``)יישוב(``, ``קרית`` vs. ``קריית``, Arabic
spelling variants, ``עיריית`` prefixes, ...).  Keeping one canonicalizer here
lets the settlement index, the Nominatim matcher and the street pipeline agree
on what "the same settlement" means.

The module is intentionally dependency-free (stdlib only) so it can be imported
by every stage of the pipeline without pulling in pandas/geopandas.
"""

from __future__ import annotations

import json
import os
import re
import unicodedata
from functools import lru_cache
from typing import Dict, Iterable, List

CONFIG_DIR = os.path.join(os.path.dirname(__file__), "config")
ALIASES_FILE = os.path.join(CONFIG_DIR, "settlement_aliases.json")

# --- Which OSM features may represent a whole settlement --------------------
# A neighbourhood/quarter that happens to share a name with a town must never
# "steal" that town's identity.  These sets are shared by the offline index,
# the Nominatim fallback and single-settlement PBF lookups.
SETTLEMENT_PLACE_TYPES = {
    "city", "town", "village", "hamlet", "municipality", "administrative",
    "locality",
}
NON_SETTLEMENT_PLACE_TYPES = {
    "suburb", "neighbourhood", "neighborhood", "quarter", "district",
    "borough", "isolated_dwelling", "farm", "farmyard", "island", "islet",
    "plot", "square", "region", "state", "county", "city_block", "allotments",
}
NON_SETTLEMENT_ADMIN_LEVELS = {"2", "3", "4", "5", "6", "9", "10", "11", "12"}
CITY_ADMIN_LEVEL = "8"
# No Israeli settlement spans more than ~60 km; larger polygons are districts
# or regional councils, not the settlement itself.
MAX_SETTLEMENT_BBOX_SPAN_DEG = 0.6
# Minimum half-size for a truly node-only settlement search area
# (~2.0 km x 2.2 km max in open terrain; adjacent settlement polygons are
# subtracted automatically in osm_pbf_loader).
MIN_SETTLEMENT_HALF_WIDTH_LON = 0.011
MIN_SETTLEMENT_HALF_HEIGHT_LAT = 0.0105

_REGIONAL_BOUNDARY_RE = re.compile(
    r"(?:^\s*(?:מועצה\s+אזורית|נפת|נפה|מחוז|מ\.א\.|مجلس\s+إقليمي|محافظة|قضاء|ناحية)\b|\bregional\s+council\b)",
    re.IGNORECASE,
)

# Hyphens / maqaf / dashes are used interchangeably in Israeli place names.
_HYPHENS = "\u05be\u2010\u2011\u2012\u2013\u2014\u2015-"
_ANNOTATION_RE = re.compile(r"\([^)]*\)|\)[^()]*\(")
_PREFIXES = ("עיריית", "מועצה מקומית", "מועצה אזורית")
_SUFFIXES = ("מושב", "קיבוץ", "יישוב", "עיר")
_WHITESPACE_RE = re.compile(r"\s+")

# Hebrew final letters fold to their regular form (סנאן / סנאנ, ...).
_FINAL_LETTERS = str.maketrans({"ך": "כ", "ם": "מ", "ן": "נ", "ף": "פ", "ץ": "צ"})
_ARABIC_LETTERS = str.maketrans({
    "أ": "ا", "إ": "ا", "آ": "ا", "ٱ": "ا",
    "ى": "ي", "ؤ": "و", "ئ": "ي", "ة": "ه",
    "\u0640": "",  # tatweel
})
_KNOWN_SPELLINGS = (
    (re.compile(r"\bקרית\b"), "קריית"),
    (re.compile(r"\bקרית"), "קריית"),
)


def clean_display_name(name: object, *, keep_quotes: bool = True) -> str:
    """Return a human-readable, comparable settlement name (spaces kept).

    ``"אבו ג'ווייעד )שבט("`` -> ``"אבו ג'ווייעד"``;
    ``"תל אביב - יפו"`` -> ``"תל אביב יפו"``.
    Quotes/geresh are kept by default because they are part of the name sent to
    Nominatim; comparison helpers below drop them.
    """
    if name is None:
        return ""
    text = unicodedata.normalize("NFKC", str(name)).strip()
    if not text:
        return ""

    text = _ANNOTATION_RE.sub(" ", text)
    text = re.sub(f"[{re.escape(_HYPHENS)}]", " ", text)
    text = text.translate(_ARABIC_LETTERS)
    text = re.sub(r"[()\[\]{}]", " ", text)

    changed = True
    while changed:
        changed = False
        for prefix in _PREFIXES:
            if text.startswith(prefix):
                text = text[len(prefix):].strip()
                changed = True
        for suffix in _SUFFIXES:
            if text.endswith(suffix) and len(text) > len(suffix) + 1:
                text = text[: -len(suffix)].strip()
                changed = True

    # Remove combining marks (niqqud, Arabic harakat).
    text = "".join(
        char
        for char in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(char)
    )
    text = text.translate(_FINAL_LETTERS)
    text = re.sub(r"[.,;:!?]", "", text)
    if not keep_quotes:
        text = re.sub(r"['\u05f3\u2018\u2019`\u02bc\"\u05f4]", "", text)
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text


def fuzzy_form(name: object) -> str:
    """Normalized form used for fuzzy scoring (quotes removed, lower-cased)."""
    text = clean_display_name(name, keep_quotes=False).casefold()
    for pattern, replacement in _KNOWN_SPELLINGS:
        text = pattern.sub(replacement, text)
    return _WHITESPACE_RE.sub(" ", text).strip()


def canonical_key(name: object) -> str:
    """Compact exact-match key: punctuation, spaces and spelling noise removed."""
    # \W removes punctuation and whitespace; underscore is added explicitly.
    return re.sub(r"[\W_]+", "", fuzzy_form(name), flags=re.UNICODE).casefold()


@lru_cache(maxsize=8)
def _load_config_cached(path: str) -> dict:
    """Parsed alias/override config (missing or broken file -> empty config)."""
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return payload if isinstance(payload, dict) else {}
    except (OSError, ValueError, TypeError):
        return {}


@lru_cache(maxsize=8)
def _load_aliases_cached(path: str) -> Dict[str, List[str]]:
    payload = _load_config_cached(path)
    raw = payload.get("aliases", payload)
    if not isinstance(raw, dict):
        return {}
    return {
        canonical_key(name): [str(alias) for alias in aliases]
        for name, aliases in raw.items()
        if isinstance(aliases, (list, tuple))
    }


def load_aliases(path: str = ALIASES_FILE) -> Dict[str, List[str]]:
    """Load curated ``{canonical LAMAS name: [OSM/LAMAS aliases]}`` mappings.

    Results are cached per file path (a batch resolves hundreds of names), and
    a missing or malformed file yields an empty mapping so a fresh checkout
    works without extra setup.  Callers get a fresh outer dict so they can add
    entries without affecting other callers.
    """
    return {key: list(value) for key, value in _load_aliases_cached(path).items()}


def load_osm_id_overrides(path: str = ALIASES_FILE) -> Dict[str, str]:
    """``{canonical LAMAS name: explicit OSM id}`` escape hatch.

    Use this when a real settlement is mapped in OSM only as a suburb or
    neighbourhood: pinning its OSM id bypasses the place-type guard for that
    name alone (and is recorded as such in the audit report).
    """
    payload = _load_config_cached(path)
    raw = payload.get("osm_id_overrides", {})
    if not isinstance(raw, dict):
        return {}
    return {
        canonical_key(name): str(osm_id).strip()
        for name, osm_id in raw.items()
        if not name.startswith("_") and isinstance(osm_id, (str, int))
    }


def name_variants(
    name: object, aliases: Dict[str, List[str]] | None = None, limit: int = 6
) -> List[str]:
    """Ordered, de-duplicated query/display variants for one settlement name.

    The first variant is always the cleaned original name; curated aliases
    follow.  Keeping the list short bounds the number of Nominatim requests.
    """
    cleaned = clean_display_name(name)
    if not cleaned:
        return []
    if aliases is None:
        aliases = load_aliases()
    # Curated aliases are kept verbatim (Arabic/Hebrew spellings matter to
    # Nominatim); only the primary LAMAS name is display-normalized.
    variants = [cleaned, *aliases.get(canonical_key(name), [])]
    result: List[str] = []
    seen = set()
    for variant in variants:
        display = str(variant).strip()
        key = canonical_key(display)
        if key and key not in seen:
            seen.add(key)
            result.append(display)
        if len(result) >= limit:
            break
    return result


def all_name_keys(names: Iterable[object]) -> set[str]:
    """Canonical keys for a collection of OSM/LAMAS names (blank keys removed)."""
    return {key for name in names if (key := canonical_key(name))}


def admin_level(value: object) -> str:
    """Normalize OSM ``admin_level`` tags ("8", "8.0", None, ...)."""
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "nat", "<na>", ""}:
        return ""
    try:
        return str(int(float(text)))
    except (TypeError, ValueError):
        return text


def is_regional_boundary_name(name: object) -> bool:
    """True when an OSM boundary name denotes a regional council, subdistrict, or district."""
    if name is None:
        return False
    text = str(name).strip()
    return bool(text and _REGIONAL_BOUNDARY_RE.search(text))


def place_is_settlement(place_type: object, level: object, category: str) -> bool:
    """True only for OSM features that may represent an entire settlement."""
    normalized = str(place_type or "").strip().lower()
    if normalized in NON_SETTLEMENT_PLACE_TYPES:
        return False
    if category == "boundary" and admin_level(level) in NON_SETTLEMENT_ADMIN_LEVELS:
        return False
    if category == "place" and normalized not in SETTLEMENT_PLACE_TYPES:
        # An exact-name building, bus stop, house or other non-place feature
        # must not become a settlement just because it is missing from the denylist.
        return False
    return True
