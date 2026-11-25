"""Cross-script Arabic <-> Hebrew street name matching utilities.

LAMAS publishes street names across Israel (including Arab cities and villages)
in Hebrew transliteration, whereas OpenStreetMap frequently stores them in
Arabic script (``شارع صلاح الدين``), Latin-transliterated Arabic
(``Abu Bakr al-Siddiq``), or Hebrew with Arabic definite-article variations
(``אלגליל`` vs. ``הגליל``, ``א-סלאם`` vs. ``אלסלאם``).

This module is intentionally stdlib-only so it can be shared cleanly by
``normalization.py``, ``pipeline.py``, and ``laya_ai_resolver.py``.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Optional, Sequence

ARABIC_CHAR_RE = re.compile(r"[\u0600-\u06FF]")
HEBREW_CHAR_RE = re.compile(r"[\u0590-\u05FF]")
LATIN_CHAR_RE = re.compile(r"[A-Za-z]")
DIGIT_RE = re.compile(r"\d+")

_FINAL_HEBREW_FOLD = str.maketrans({"ך": "כ", "ם": "מ", "ן": "נ", "ף": "פ", "ץ": "צ"})
_ARABIC_INDIC_DIGITS = str.maketrans(
    {chr(0x0660 + i): str(i) for i in range(10)}
    | {chr(0x06F0 + i): str(i) for i in range(10)}
)

# Leading generic Arabic street-type words (normalized: ة -> ه, أ/إ/آ -> ا).
_ARABIC_PREFIX_WORDS = frozenset({
    "شارع", "الشارع",
    "طريق", "الطريق",
    "دوار", "الدوار",
    "ساحه", "الساحه",
    "ميدان", "الميدان",
    "ممر", "الممر",
    "طلعه", "الطلعه",
    "نزله", "النزله",
    "حاره", "الحاره",
    "حي", "الحي",
    "جاده", "الجاده",
    "زقاق", "الزقاق",
    "دخله", "الدخله",
    "درج", "الدرج",
    "مفرق", "المفرق",
    "جسر", "الجسر",
    "عقبه", "العقبه",
    "امتداد", "مدخل",
})
_ARABIC_TWO_WORD_PREFIXES = frozenset({"شارع رقم", "طريق رقم"})

# Standard Semitic Arabic -> Hebrew transliteration table matching LAMAS style.
_ARABIC_TO_HEBREW = {
    "ا": "א", "أ": "א", "إ": "א", "آ": "א", "ٱ": "א", "ء": "א",
    "ب": "ב", "ت": "ת", "ث": "ת", "ج": "ג", "ح": "ח", "خ": "ח",
    "د": "ד", "ذ": "ד", "ر": "ר", "ز": "ז", "س": "ס", "ش": "ש",
    "ص": "ס", "ض": "ד", "ط": "ט", "ظ": "ד", "ع": "ע", "غ": "ע",
    "ف": "פ", "ق": "ק", "ك": "כ", "ل": "ל", "م": "מ", "ن": "נ",
    "ه": "ה", "ة": "ה", "و": "ו", "ؤ": "ו", "ي": "י", "ئ": "י", "ى": "א",
}

# Hebrew-script Arabic definite article / Arabic-origin street markers.
_ARAB_LAMAS_MARKER_RE = re.compile(
    r"^(?:אל|א-|א\s|אבו\b|אבן\b|ואדי\b|וואדי\b|ח'?לת\b|דרך\s+אל|דרך\s+א-)"
)
_SPACED_ARABIC_ARTICLE_RE = re.compile(r"(?:^|\s)(?:אל|א)(?:[\s-]|$)")
_HIGHWAY_ROUTE_RE = re.compile(r"^\s*(?:כביש|route|highway|דרך\s+מהירה)\b", re.IGNORECASE)

# Tokens that strongly indicate a Latin-transliterated Arabic street name
# (prevents random English labels like "Under Pass" from being transliterated).
_LATIN_ARABIC_MARKERS = frozenset({
    "al", "el", "abu", "ibn", "bin", "wadi", "wady", "ein", "ain",
    "kafr", "sheikh", "salah", "omar", "khalid", "ahmad", "ahmed",
    "mohammad", "muhammad", "hassan", "hussein", "sharia", "tariq", "duwar",
})
_LATIN_ARTICLE_TOKENS = frozenset({
    "al", "el", "ar", "as", "ash", "ad", "an", "at", "az",
})
_LATIN_SINGLE_TO_HEBREW = {
    "b": "ב", "c": "כ", "d": "ד", "f": "פ", "g": "ג", "h": "ה",
    "j": "ג", "k": "כ", "l": "ל", "m": "מ", "n": "נ", "p": "פ",
    "q": "ק", "r": "ר", "s": "ס", "t": "ת", "v": "ו", "w": "ו",
    "x": "כס", "y": "י", "z": "ז",
}


def has_arabic(text: object) -> bool:
    """Return True if ``text`` contains at least one Arabic-script character."""
    return bool(text) and bool(ARABIC_CHAR_RE.search(str(text)))


def has_hebrew(text: object) -> bool:
    """Return True if ``text`` contains at least one Hebrew-script character."""
    return bool(text) and bool(HEBREW_CHAR_RE.search(str(text)))


def is_latin_only(text: object) -> bool:
    """Return True if ``text`` has Latin letters and neither Hebrew nor Arabic."""
    if not text:
        return False
    s = str(text)
    return bool(LATIN_CHAR_RE.search(s)) and not HEBREW_CHAR_RE.search(s) and not ARABIC_CHAR_RE.search(s)


def _fold_arabic_word_for_prefix(word: str) -> str:
    return (
        word.replace("ة", "ه")
        .replace("أ", "ا")
        .replace("إ", "ا")
        .replace("آ", "ا")
        .replace("ٱ", "ا")
    )


def strip_arabic_street_prefix(text: str) -> str:
    """Strip leading generic Arabic street-type words while keeping the core."""
    if not text or not ARABIC_CHAR_RE.search(text):
        return (text or "").strip()
    words = text.strip().split()
    while len(words) >= 2:
        w0 = _fold_arabic_word_for_prefix(words[0])
        if len(words) >= 3:
            w01 = f"{w0} {_fold_arabic_word_for_prefix(words[1])}"
            if w01 in _ARABIC_TWO_WORD_PREFIXES:
                words = words[2:]
                continue
        if w0 in _ARABIC_PREFIX_WORDS:
            words = words[1:]
            continue
        break
    return " ".join(words)


def transliterate_arabic_to_hebrew(text: str, *, strip_prefix: bool = True) -> str:
    """Transliterate an Arabic-script street name into LAMAS-style Hebrew.

    Handles Arabic harakat/tatweel removal, Arabic-Indic digit folding,
    optional Arabic street-prefix stripping (``شارع``, ``طريق``, ``دوار``, ...),
    and construct-state (*idafa*) ta marbuta (``خلة بلوط`` -> ``חלת בלוט``).
    """
    if not text:
        return ""
    cleaned = "".join(
        ch for ch in unicodedata.normalize("NFKD", str(text))
        if not unicodedata.combining(ch)
    )
    cleaned = cleaned.replace("\u0640", "").translate(_ARABIC_INDIC_DIGITS)
    cleaned = re.sub(r"[\u05be\u2010-\u2015-]", " ", cleaned)
    cleaned = re.sub(r"[.,;:!?'\"`\u05f3\u05f4\u2018\u2019\u201c\u201d\u02bc()\[\]{}]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return ""

    if strip_prefix:
        cleaned = strip_arabic_street_prefix(cleaned)

    words = cleaned.split()
    out_words: list[str] = []
    for idx, word in enumerate(words):
        chars: list[str] = []
        for pos, ch in enumerate(word):
            # Construct-state ta marbuta before a following word is pronounced -at (ת).
            if ch == "ة" and pos == len(word) - 1 and idx + 1 < len(words):
                chars.append("ת")
            else:
                chars.append(_ARABIC_TO_HEBREW.get(ch, ch))
        out_words.append("".join(chars))
    return " ".join(out_words).strip()


def arabic_hebrew_skeleton(hebrew_core_text: str) -> str:
    """Symmetric Semitic consonantal skeleton for Arabic <-> Hebrew street matching.

    Expects a normalized, prefix-stripped Hebrew string (or the output of
    :func:`transliterate_arabic_to_hebrew`).  Unifies definite-article variants
    (``אל``, ``אל-``, ``א-``, ``ה``), ``אבן``/``בן``, ``צ``/``ס``, final ``א``/``ה``,
    doubled matres lectionis, and interior short vowels ``ו``/``י``.
    """
    if not hebrew_core_text:
        return ""
    text = str(hebrew_core_text).translate(_FINAL_HEBREW_FOLD)
    text = re.sub(r"[\u05be\u2010-\u2015-]", " ", text)
    text = re.sub(r"[.,;:!?'\"`\u05f3\u05f4\u2018\u2019\u201c\u201d\u02bc()\[\]{}]", "", text)
    tokens = text.split()

    # Merge standalone Arabic definite articles ("אל", "א") with the next token.
    merged: list[str] = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok in ("אל", "א") and i + 1 < len(tokens):
            merged.append("אל" + tokens[i + 1])
            i += 2
        else:
            merged.append(tok)
            i += 1

    cleaned: list[str] = []
    for tok in merged:
        if tok.startswith("אל") and len(tok) >= 4:
            tok = tok[2:]
        elif tok.startswith("ה") and len(tok) >= 4:
            tok = tok[1:]
        if tok in ("אבנ", "איבנ", "בנ"):
            tok = "בנ"
        tok = tok.replace("צ", "ס")
        tok = tok.replace("וו", "ו").replace("יי", "י").replace("אא", "א")
        if len(tok) >= 3 and tok.endswith("א"):
            tok = tok[:-1] + "ה"
        if len(tok) >= 4:
            tok = tok[0] + re.sub(r"[וי]", "", tok[1:-1]) + tok[-1]
        if tok:
            cleaned.append(tok)
    return " ".join(cleaned).strip()


def arabic_long_vowels_compatible(arabic_translit: str, lamas_hebrew: str) -> bool:
    """Semitic phonology guard against short single-word vowel-swap collisions.

    Arabic script omits short vowels (so Hebrew LAMAS often *adds* ``ו``/``י``
    that were absent in Arabic: ``رشد`` -> ``רושד``, ``المتنبي`` -> ``אלמותנבי``).
    However, an Arabic long ``و`` (``ו``) does not turn into a Hebrew ``י``
    without ``ו`` (e.g. ``نومن`` ``נומנ`` must NOT match ``ניימן`` ``ניימנ``).
    """
    ar_tokens = arabic_translit.translate(_FINAL_HEBREW_FOLD).split()
    he_tokens = lamas_hebrew.translate(_FINAL_HEBREW_FOLD).split()
    if not ar_tokens or not he_tokens:
        return True
    ar_skel = arabic_hebrew_skeleton(arabic_translit)
    # Multi-word or >= 4-consonant skeletons are already highly constrained.
    if " " in ar_skel or len(ar_skel) >= 4:
        return True
    ar_body = ar_tokens[-1]
    if ar_body.startswith("אל") and len(ar_body) >= 4:
        ar_body = ar_body[2:]
    he_body = he_tokens[-1]
    if he_body.startswith("אל") and len(he_body) >= 4:
        he_body = he_body[2:]
    elif he_body.startswith("ה") and len(he_body) >= 4:
        he_body = he_body[1:]
    ar_mid = ar_body[1:-1] if len(ar_body) >= 3 else ""
    he_mid = he_body[1:-1] if len(he_body) >= 3 else ""
    if "ו" in ar_mid and "ו" not in he_mid and "י" in he_mid:
        return False
    if "י" in ar_mid and "י" not in he_mid and "ו" in he_mid:
        return False
    return True


def is_arab_style_lamas_city(lamas_names: Sequence[str]) -> bool:
    """True when a meaningful share of a city's LAMAS streets use Arabic articles/names.

    Empirically, Arab towns sit at 55%-72% while mixed cities like Jerusalem sit
    near 16% and Jewish cities below 4%; 0.35 cleanly separates Arab-majority LAMAS
    tables from mixed/Jewish cities.
    """
    if not lamas_names:
        return False
    hits = sum(
        1 for name in lamas_names
        if name and _ARAB_LAMAS_MARKER_RE.match(str(name).strip())
    )
    return (hits / len(lamas_names)) >= 0.35


def has_arabic_article_in_hebrew(text: str, *, allow_attached_al: bool = False) -> bool:
    """True if a Hebrew-script street carries an Arabic definite article (``אל-``/``א-``)."""
    if not text:
        return False
    if _SPACED_ARABIC_ARTICLE_RE.search(text):
        return True
    if allow_attached_al:
        for tok in text.split():
            if tok.startswith("אל") and len(tok) >= 5:
                return True
    return False


def hebrew_arabic_article_form(
    hebrew_core_text: str,
    *,
    strip_attached_al: bool = False,
    strip_he_prefix: bool = False,
) -> str:
    """Strict Hebrew form normalizing Arabic definite articles without dropping vowels.

    Normalizes ``אל `` / ``א `` / attached ``אל`` (in Arab-style cities),
    final ``א``/``ה``, final Hebrew letters, and doubled ``וו``/``יי``/``אא``.
    Hebrew ``ה`` is stripped only when ``strip_he_prefix=True`` (used when the
    opposing side carries an Arabic definite article, e.g. ``הגליל`` <-> ``אלגליל``).
    """
    if not hebrew_core_text:
        return ""
    text = str(hebrew_core_text).translate(_FINAL_HEBREW_FOLD)
    tokens = text.split()
    out: list[str] = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok in ("אל", "א") and i + 1 < len(tokens):
            tok = tokens[i + 1]
            i += 2
        else:
            i += 1
            if strip_attached_al and tok.startswith("אל") and len(tok) >= 5:
                tok = tok[2:]
            elif strip_he_prefix and tok.startswith("ה") and len(tok) >= 4:
                tok = tok[1:]
        tok = tok.replace("וו", "ו").replace("יי", "י").replace("אא", "א")
        if len(tok) >= 3 and tok.endswith("א"):
            tok = tok[:-1] + "ה"
        if tok:
            out.append(tok)
    return " ".join(out).strip()


def latin_arabic_to_hebrew_skeleton(text: str) -> str:
    """Transliterate Latin-script Arabic street names into :func:`arabic_hebrew_skeleton` space.

    Returns ``""`` unless the name contains recognizable Arabic transliteration
    markers (``al-``, ``Abu``, ``Ibn``, ``Wadi``, ...), preventing accidental
    matching of unrelated English road descriptions.
    """
    if not is_latin_only(text):
        return ""
    cleaned = str(text).lower().strip()
    cleaned = re.sub(r"[^a-z0-9'\s-]", " ", cleaned)
    cleaned = re.sub(
        r"^(?:street|st|road|rd|sharia|shari|tariq|duwar|sahat|harah|haret)\s+",
        "",
        cleaned,
    )
    raw_tokens = [tok for tok in re.split(r"[\s-]+", cleaned) if tok]
    if not raw_tokens or not (set(raw_tokens) & _LATIN_ARABIC_MARKERS):
        return ""

    out_tokens: list[str] = []
    for tok in raw_tokens:
        if tok in _LATIN_ARTICLE_TOKENS:
            continue
        if tok in ("ibn", "bin", "ben"):
            out_tokens.append("בנ")
            continue
        if tok == "abu":
            out_tokens.append("אבו")
            continue
        if tok in ("abd", "abed", "abdul"):
            out_tokens.append("עבד")
            continue
        if tok in ("wadi", "wady"):
            out_tokens.append("ואדי")
            continue
        bare = tok.replace("'", "")
        chars: list[str] = []
        i = 0
        while i < len(bare):
            two = bare[i : i + 2]
            if two == "sh":
                chars.append("ש"); i += 2; continue
            if two in ("kh", "ch"):
                chars.append("ח"); i += 2; continue
            if two == "gh":
                chars.append("ע"); i += 2; continue
            if two == "th":
                chars.append("ת"); i += 2; continue
            if two == "dh":
                chars.append("ד"); i += 2; continue
            if two == "ph":
                chars.append("פ"); i += 2; continue
            if two in ("oo", "ou", "uu"):
                chars.append("ו"); i += 2; continue
            if two in ("ee", "ii", "ey", "ay"):
                chars.append("י"); i += 2; continue
            ch = bare[i]
            if ch in "ae":
                chars.append("א" if i == 0 else ("ה" if i == len(bare) - 1 else ""))
            elif ch == "i":
                chars.append("א" if i == 0 else ("י" if i == len(bare) - 1 else ""))
            elif ch in "ou":
                chars.append("א" if i == 0 else ("ו" if i == len(bare) - 1 else ""))
            else:
                chars.append(_LATIN_SINGLE_TO_HEBREW.get(ch, ch))
            i += 1
        mapped = re.sub(r"(.)\1+", r"\1", "".join(chars))
        if mapped:
            out_tokens.append(mapped)
    return " ".join(out_tokens).strip()


def extract_embedded_street_number(text: str) -> Optional[str]:
    """Return a unique 3–4 digit street number (e.g. ``المدينة القديمة - 6171`` -> ``"6171"``).

    Returns ``None`` for highway route names (``כביש 443``, ``Route 65``),
    multi-number strings, or 1–2 digit numbers.
    """
    if not text or _HIGHWAY_ROUTE_RE.match(str(text)):
        return None
    normalized = str(text).translate(_ARABIC_INDIC_DIGITS)
    digits = DIGIT_RE.findall(normalized)
    if len(digits) == 1 and re.fullmatch(r"\d{3,4}", digits[0]):
        return digits[0]
    return None
