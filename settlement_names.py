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
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union

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
_GERESH_CHARS = "'\u05f3\u2018\u2019`\u02bc"
_GERESH_RE = re.compile(f"[{re.escape(_GERESH_CHARS)}]")
_HEBREW_GERESH_CONSONANT_RE = re.compile(r"([גזצ])'")
_HEBREW_CHAR_RE = re.compile(r"[\u0590-\u05ff]")
_ANNOTATION_RE = re.compile(r"\(([^()]*)\)|\)([^()]*)\(")
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
# Canonicalize common spelling variants inside LAMAS parenthetical qualifiers.
_QUALIFIER_CANON = {
    "ישוב": "יישוב",
    "קבוצ": "קיבוצ",
    "קבוץ": "קיבוצ",
    "אחוד": "איחוד",
    "כפר הנער": "כפר נוער",
    "כפר הנוער": "כפר נוער",
}
# Generic LAMAS annotations that describe the entity type without splitting a
# twin settlement pair (unlike איחוד/מאוחד, מושב/קיבוץ, בעמק, יזרעאל, ...).
_GENERIC_QUALIFIERS = {"שבט", "יישוב", "ישוב"}
# Qualifiers that represent a settlement-type/split suffix or prefix in OSM
# (e.g. "גבעת חיים (איחוד)" / "קיבוץ מרחביה" / "יוקנעם המושבה").
_SPLIT_QUALIFIERS = {
    "איחוד", "מאוחד", "מושב", "מושבה", "קיבוצ", "קבוצה",
    "כפר נוער", "מוסד", "ישיבה", "בעמק", "יזרעאל",
}
# Mutually exclusive qualifier groups used to prevent twin settlements from
# stealing each other's OSM features.
_QUALIFIER_GROUPS: Dict[str, tuple[str, ...]] = {
    "ihud": ("איחוד", "אחוד", "ihud", "ichood", "ichud"),
    "meuhad": ("מאוחד", "meuhad", "meuchad"),
    "kibbutz": ("קיבוצ", "קבוצ", "קבוצה", "קבוצת", "kibbutz", "kibbuts", "kvutsa", "kvutzat"),
    "moshav": ("מושב", "מושבה", "המושבה", "moshav", "moshava"),
    "youth_village": ("כפר נוער", "כפר הנוער", "כפר הנער", "מוסד", "ישיבה", "youth village"),
}
_OPPOSING_QUALIFIER_GROUPS: tuple[tuple[str, str], ...] = (
    ("ihud", "meuhad"),
    ("kibbutz", "moshav"),
    ("youth_village", "moshav"),
    ("youth_village", "kibbutz"),
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


def extract_qualifiers(name: object) -> List[str]:
    """Return cleaned parenthetical annotations (normal or reversed LAMAS parens)."""
    if name is None:
        return []
    text = unicodedata.normalize("NFKC", str(name)).strip()
    if not text:
        return []
    qualifiers: List[str] = []
    for match in _ANNOTATION_RE.finditer(text):
        raw = (match.group(1) if match.group(1) is not None else match.group(2)) or ""
        cleaned = clean_display_name(raw, keep_quotes=False)
        if not cleaned:
            continue
        cleaned = _QUALIFIER_CANON.get(cleaned, cleaned)
        qualifiers.append(cleaned)
    return qualifiers


def _base_key_preserving_geresh(name: object) -> str:
    """Canonical base key that preserves Hebrew geresh on ``ג'`` / ``ז'`` / ``צ'`` / ``ח'``."""
    text = clean_display_name(name, keep_quotes=True).casefold()
    if not text:
        return ""
    for pattern, replacement in _KNOWN_SPELLINGS:
        text = pattern.sub(replacement, text)
    # Normalize all apostrophe/geresh variants to ASCII "'" and preserve them
    # only after Hebrew consonants that change sound with geresh (ג'/ז'/צ'/ח').
    text = _GERESH_RE.sub("'", text)
    text = re.sub(r"(?<![גזצח])'", "", text)
    return re.sub(r"[^\w']+|_", "", text, flags=re.UNICODE).casefold()


def settlement_identity_key(name: object) -> str:
    """Unique identity key that keeps distinct LAMAS settlements apart.

    Unlike :func:`canonical_key`, this preserves:
    - Hebrew geresh on ``ג'`` / ``ז'`` / ``צ'`` / ``ח'`` (separating ``ג'ת`` from ``גת``),
    - parenthetical qualifiers (separating ``גת )קיבוץ(``, ``טמרה )יזרעאל(``,
      ``גבעת חיים )איחוד(`` vs. ``)מאוחד(``, ``בן שמן )מושב(`` vs. ``)כפר נוער(``).

    For plain settlement names without parentheticals or geresh consonants
    (e.g. ``חיפה``, ``תל אביב - יפו``), this equals :func:`canonical_key`.
    """
    base = _base_key_preserving_geresh(name)
    if not base:
        return ""
    qualifiers = [
        re.sub(r"[\W_]+", "", q, flags=re.UNICODE).casefold()
        for q in extract_qualifiers(name)
    ]
    qualifiers = [q for q in qualifiers if q]
    if not qualifiers:
        return base
    return "|".join([base, *qualifiers])


def ktiv_haser_key(name: object) -> str:
    """Hebrew plene/defective (ktiv male/haser) comparison key.

    Unifies common LAMAS/OSM spelling differences such as ``עופרה``/``עפרה``,
    ``יוקנעם``/``יקנעם``, ``סוואעד``/``סואעד``, ``קידר``/``קדר``,
    ``עינב``/``ענב``, ``כוחלה``/``כחלה`` while keeping word boundaries intact
    so first/last letters are never dropped.
    """
    text = fuzzy_form(name)
    if not text or not _HEBREW_CHAR_RE.search(text):
        return ""
    tokens: List[str] = []
    for token in text.split():
        cleaned = re.sub(r"[\W_]+", "", token, flags=re.UNICODE)
        if not cleaned:
            continue
        if _HEBREW_CHAR_RE.search(cleaned):
            cleaned = cleaned.replace("וו", "ו").replace("יי", "י")
            if len(cleaned) >= 4:
                cleaned = cleaned[0] + re.sub(r"[וי]", "", cleaned[1:-1]) + cleaned[-1]
        tokens.append(cleaned)
    key = "".join(tokens).casefold()
    return key if len(key) >= 3 else ""


def _hebrew_geresh_signature(name: object) -> tuple[bool, bool, int]:
    """Return ``(has_hebrew, has_geresh_consonant, canonical_len)``."""
    text = clean_display_name(name, keep_quotes=True)
    if not text or not _HEBREW_CHAR_RE.search(text):
        return (False, False, 0)
    norm = _GERESH_RE.sub("'", text)
    has_g = bool(_HEBREW_GERESH_CONSONANT_RE.search(norm))
    return (True, has_g, len(canonical_key(text)))


def has_geresh_conflict(query: object, candidate_names: Iterable[object]) -> bool:
    """True when ``ג'``/``ז'``/``צ'`` geresh distinguishes two different places.

    Examples:
    - ``גת )קיבוץ(`` (plain Hebrew ``גת``, no geresh) must NEVER match ``ג'ת`` (Jatt).
    - ``ג'ת`` (short 2-letter Arabic name with ``ג'``) must NEVER match ``גת`` (Gat).
    - Longer Arabic names where OSM dropped the geresh (``ג'דיידה-מכר`` vs.
      ``גדיידה מכר``) are still allowed to match.
    """
    q_he, q_geresh, q_len = _hebrew_geresh_signature(query)
    if not q_he:
        return False
    he_candidates = []
    for cand in candidate_names:
        c_he, c_geresh, c_len = _hebrew_geresh_signature(cand)
        if c_he:
            he_candidates.append((c_geresh, c_len))
    if not he_candidates:
        return False
    if not q_geresh and all(c_geresh for c_geresh, _ in he_candidates):
        # Plain Hebrew settlement name (e.g. גת, גבע) cannot match a feature
        # whose Hebrew name(s) all carry ג'/ז'/צ' (e.g. ג'ת, ג'בע).
        return True
    if q_geresh and q_len <= 3 and not any(c_geresh for c_geresh, _ in he_candidates):
        # Very short geresh name (e.g. ג'ת, ג'בע) cannot match plain גת/גבע.
        return True
    return False


def _qualifier_group_set(names: Iterable[object]) -> set[str]:
    """Extract normalized qualifier groups present in a name or name list."""
    found: set[str] = set()
    for raw in names:
        if raw is None:
            continue
        text = unicodedata.normalize("NFKC", str(raw))
        if not text.strip():
            continue
        # Inspect both the raw tokens (including inside parens) and folded Hebrew.
        folded = "".join(
            ch for ch in unicodedata.normalize("NFKD", text)
            if not unicodedata.combining(ch)
        ).translate(_FINAL_LETTERS)
        folded = re.sub(f"[{re.escape(_HYPHENS)}()\\[\\]{{}},.;:!?'\u05f3\u2018\u2019\"\u05f4]", " ", folded)
        folded = _WHITESPACE_RE.sub(" ", folded).strip().casefold()
        padded = f" {folded} "
        for group, markers in _QUALIFIER_GROUPS.items():
            for marker in markers:
                if f" {marker} " in padded:
                    found.add(group)
                    break
    return found


def has_qualifier_conflict(query: object, candidate_names: Iterable[object]) -> bool:
    """True when query and candidate name opposing halves of a twin settlement.

    E.g. ``גבעת חיים )מאוחד(`` vs. ``גבעת חיים (איחוד)``, ``מרחביה )קיבוץ(`` vs.
    ``מושב מרחביה``, ``בן שמן )מושב(`` vs. ``כפר הנוער בן שמן``.
    """
    q_groups = _qualifier_group_set([query])
    if not q_groups:
        return False
    c_groups = _qualifier_group_set(candidate_names)
    if not c_groups:
        return False
    for left, right in _OPPOSING_QUALIFIER_GROUPS:
        if left in q_groups and right in c_groups and left not in c_groups:
            return True
        if right in q_groups and left in c_groups and right not in c_groups:
            return True
    return False


def qualifier_match_bonus(query: object, candidate_names: Iterable[object]) -> int:
    """Return 1 when the candidate explicitly carries the query's qualifier, else 0."""
    q_groups = _qualifier_group_set([query])
    if not q_groups:
        return 0
    c_groups = _qualifier_group_set(candidate_names)
    return 1 if (q_groups & c_groups) else 0


def lookup_by_settlement_key(mapping: Dict[str, Any], name: object) -> Any:
    """Look up ``name`` in a settlement-keyed dict without cross-settlement leaks.

    Checks :func:`settlement_identity_key` first. Falls back to
    :func:`canonical_key` only when ``name`` has no disambiguating qualifier and
    no ``ג'``/``ז'``/``צ'`` geresh difference, preserving compatibility with
    callers/tests that build maps with ``canonical_key``.
    """
    if not mapping or name is None:
        return None
    id_key = settlement_identity_key(name)
    if id_key and id_key in mapping:
        return mapping[id_key]
    qualifiers = extract_qualifiers(name)
    has_distinguishing_qualifier = any(q not in _GENERIC_QUALIFIERS for q in qualifiers)
    if has_distinguishing_qualifier:
        return None
    base_geresh = _base_key_preserving_geresh(name)
    if base_geresh and base_geresh in mapping:
        return mapping[base_geresh]
    _, has_geresh, canon_len = _hebrew_geresh_signature(name)
    if has_geresh and canon_len <= 3:
        return None
    canon = canonical_key(name)
    if canon and canon in mapping:
        return mapping[canon]
    return None


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
        settlement_identity_key(name): [str(alias) for alias in aliases]
        for name, aliases in raw.items()
        if not name.startswith("_")
        and isinstance(aliases, (list, tuple))
        and settlement_identity_key(name)
    }


def load_aliases(path: str = ALIASES_FILE) -> Dict[str, List[str]]:
    """Load curated ``{settlement identity key: [OSM/LAMAS aliases]}`` mappings.

    Results are cached per file path (a batch resolves hundreds of names), and
    a missing or malformed file yields an empty mapping so a fresh checkout
    works without extra setup.  Callers get a fresh outer dict so they can add
    entries without affecting other callers.
    """
    return {key: list(value) for key, value in _load_aliases_cached(path).items()}


def load_osm_id_overrides(
    path: str = ALIASES_FILE,
) -> Dict[str, Union[str, List[str]]]:
    """``{settlement identity key: explicit OSM id or list of ids}`` escape hatch.

    Use this when a real settlement is mapped in OSM only as a suburb,
    neighbourhood, institution, or multiple village polygons (e.g. a merged
    council like ``שער שומרון``): pinning its OSM id(s) bypasses the place-type
    guard for that settlement alone (and is recorded as such in the audit report).
    """
    payload = _load_config_cached(path)
    raw = payload.get("osm_id_overrides", {})
    if not isinstance(raw, dict):
        return {}
    overrides: Dict[str, Union[str, List[str]]] = {}
    for name, osm_id in raw.items():
        if name.startswith("_"):
            continue
        key = settlement_identity_key(name)
        if not key:
            continue
        if isinstance(osm_id, (str, int)):
            val = str(osm_id).strip()
            if val:
                overrides[key] = val
        elif isinstance(osm_id, (list, tuple)):
            vals = [str(item).strip() for item in osm_id if isinstance(item, (str, int)) and str(item).strip()]
            if vals:
                overrides[key] = vals[0] if len(vals) == 1 else vals
    return overrides


def name_variants(
    name: object, aliases: Dict[str, List[str]] | None = None, limit: int = 8
) -> List[str]:
    """Ordered, de-duplicated query/display variants for one settlement name.

    The first variant is the cleaned original name (with qualified twin forms
    such as ``גבעת חיים איחוד`` / ``קיבוץ מרחביה`` generated alongside it);
    curated aliases follow.  Keeping the list short bounds Nominatim requests.
    """
    cleaned = clean_display_name(name)
    if not cleaned:
        return []
    if aliases is None:
        aliases = load_aliases()
    curated = lookup_by_settlement_key(aliases, name) or []
    qualifiers = extract_qualifiers(name)

    extra_qualified: List[str] = []
    for qual in qualifiers:
        if qual in _GENERIC_QUALIFIERS:
            continue
        if qual in _SPLIT_QUALIFIERS:
            # Prefer qualified form matching OSM conventions (e.g. "גבעת חיים איחוד",
            # "קיבוץ מרחביה", "מושב מרחביה", "יוקנעם המושבה").
            extra_qualified.append(f"{cleaned} {qual}")
            if qual in {"קיבוצ", "מושב", "קבוצה"}:
                display_qual = "קיבוץ" if qual == "קיבוצ" else qual
                extra_qualified.append(f"{display_qual} {cleaned}")
        elif len(qual) >= 3:
            # Non-generic parenthetical alternate name (e.g. "כפר רוזנואלד )זרעית(" ->
            # "זרעית", "סואעד )חמירה(" -> "סואעד חמירה", "סואעד )כמאנה( )שבט(" -> "כמאנה").
            extra_qualified.append(f"{cleaned} {qual}")
            extra_qualified.append(qual)

    # Keep the primary cleaned name first when there are no split qualifiers
    # (preserves Nominatim query order for e.g. "אבו ג'ווייעד )שבט("), and place
    # curated aliases right after the primary forms.
    variants = [cleaned, *extra_qualified, *curated]
    result: List[str] = []
    seen: set[str] = set()
    for variant in variants:
        display = str(variant).strip()
        # Use identity key (preserving qualifiers/geresh) so qualified variants
        # like "גבעת חיים איחוד" are not dropped as duplicates of "גבעת חיים".
        key = _base_key_preserving_geresh(display)
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
