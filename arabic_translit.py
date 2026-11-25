"""Arabic → Hebrew transliteration for cross-script street-name matching.

OSM streets in Arab localities are frequently named in Arabic script
(``ابن سينا``, ``شارع القدس``, ``ممر السوسن``) while the LAMAS street table
lists the same streets as Hebrew-script transliterations (``אבן סינא``,
``אל-קודס``, ``משעול אלסוסן``).  A fuzzy score between the two scripts is
always ~0, so before this module every Arabic-script OSM street was
guaranteed to end up unmatched or AI-only.

This module converts one Arabic-script name into a small, bounded set of
Hebrew-script transliteration variants that follow the conventions actually
observed in the LAMAS synonym table:

* the definite article ``ال`` appears as ``אל`` attached (``אלקודס``),
  ``אל`` spaced (``אל סנאבל``) and, for the fourteen "sun letters", also
  assimilated with a dropped ל (``א-נסר``, ``א-רייעף``-style ``א נסר``);
  LAMAS uses all three styles inconsistently, so all are generated;
* LAMAS sometimes drops a mid-word hamza/alef carrier (``الرأيف`` →
  ``א-רייעף``-style ``א רייעף`` rather than ``א ראיעף``), so a variant with
  mid-word alefs removed is generated as well;
* some letters have more than one observed LAMAS spelling after
  geresh-stripping: خ → ח/כ (``אלח'לה`` / ``אלכנדק``), ص → צ/ס
  (``אלצדיק`` / ``סלאח א-דין``), ض → צ/ד, ظ → ט/ז, غ → ע/ג and ؤ → ו/וא
  (``רואוס אל-ג'יבאל``); a second letter map covers the alternates;
* generic Arabic street-type words map to their Hebrew counterparts
  (``شارع``→רחוב, ``دوار``→כיכר, ``طريق``→דרך, ``ممر``→משעול, ``جسر``→גשר)
  so the existing Hebrew prefix-stripping keeps working on both sides;
* word-final ا and ي have a second observed spelling (``אבן סינא`` /
  ``אבן סינה``), generated as extra variants.

The output is plain Hebrew letters and spaces (no geresh): matching always
happens on :func:`normalization.normalize_street_name` output, where ׳/״
are already stripped, so emitting ``ג'``/``ח'`` would collapse to the same
letters anyway.

The module is intentionally dependency-free (stdlib only), mirroring
``settlement_names.py``.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Dict, List

# Any Arabic-block character (incl. Arabic Supplement and presentation forms).
_ARABIC_CHAR_RE = re.compile(r"[\u0600-\u06FF\u0750-\u077F\uFB50-\uFDFF\uFE70-\uFEFF]")

# Invisible bidi controls sometimes smuggled into OSM names.
_BIDI_RE = re.compile(r"[\u200e\u200f\u202a-\u202e\u2066-\u2069]")

_DEF_ARTICLE = "ال"

# Letters after which the ل of ال assimilates in pronunciation; LAMAS
# sometimes writes that pronunciation (``א-נסר`` for النصر).
_SUN_LETTERS = set("تثدذرزسشصضطظلن")

# Generic Arabic street-type words → Hebrew street-type words (these are the
# prefixes ``normalization.STREET_PREFIX_RE`` already knows how to strip).
_STREET_TYPE_WORDS: Dict[str, str] = {
    "شارع": "רחוב",
    "طريق": "דרך",
    "دوار": "כיכר",
    "ساحة": "כיכר",
    "ميدان": "כיכר",
    "ممر": "משעול",
    "جسر": "גשר",
    "كوبري": "גשר",
}

# Primary letter map: the Academy-style transliteration after geresh
# stripping (ג'/ח'/ד' collapse to ג/ח/ד in normalized LAMAS names too).
_PRIMARY_LETTERS: Dict[str, str] = {
    "ا": "א", "أ": "א", "إ": "א", "آ": "א", "ٱ": "א",
    "ب": "ב", "ت": "ת", "ث": "ת",
    "ج": "ג", "ح": "ח", "خ": "ח",
    "د": "ד", "ذ": "ד", "ر": "ר", "ز": "ז",
    "س": "ס", "ش": "ש",
    "ص": "צ", "ض": "צ", "ط": "ט", "ظ": "ט",
    "ع": "ע", "غ": "ע",
    "ف": "פ", "ق": "ק", "ك": "כ",
    "ل": "ל", "م": "מ", "ن": "נ",
    "ه": "ה", "و": "ו", "ي": "י",
    "ة": "ה", "ى": "א",
    "ء": "", "ؤ": "ו", "ئ": "י",
    # Persian/extended letters occasionally found in OSM extracts.
    "پ": "פ", "چ": "ג", "ژ": "ז", "گ": "ג", "ک": "כ", "ی": "י", "ې": "י", "ۀ": "ה",
}

# Second observed LAMAS spellings (per-letter alternates, see module docstring).
_ALTERNATE_LETTERS: Dict[str, str] = {
    "خ": "כ",   # אלכנדק (אום אל-פחם) next to אלח'זאן (רהט)
    "ص": "ס",   # סלאח א-דין, כרם א-סאחב next to אבו בכר אלצדיק
    "ض": "ד",
    "ظ": "ז",
    "غ": "ג",   # מג'אר/מגאר-style spellings next to ע'אפר (רהט)
    "ؤ": "וא",  # רואוס אל-ג'יבאל (נצרת)
}

# LAMAS also spells word-final ا as ה and word-final ى/ي as א (last word only).
_FINAL_ALEF_RE = re.compile(r"א(?=\s|$)")
_FINAL_YOD_RE = re.compile(r"יח?(?=\s|$)")

# Common personal names whose LAMAS transliteration inserts a vowel the
# letter map cannot produce (evidence: עומר בן אל-ח'טאב / עומר בן אל חטאב,
# חסן נימר חוסין, מחמד-style spellings).  Values replace the whole word.
_WORD_EXCEPTIONS: Dict[str, str] = {
    "عمر": "עומר",
    "عثمان": "עותמאן",
    "حسين": "חוסין",
    "محمد": "מוחמד",
}


def _apply_word_exceptions(text: str) -> str:
    """Replace whole-word given names with their observed LAMAS spelling."""
    return " ".join(_WORD_EXCEPTIONS.get(word, word) for word in text.split(" "))


def contains_arabic(text: object) -> bool:
    """True when ``text`` contains at least one Arabic-block character."""
    if text is None:
        return False
    return bool(_ARABIC_CHAR_RE.search(str(text)))


def _clean(text: str) -> str:
    """Strip bidi controls, harakat/niqqud-style marks and tatweel."""
    text = _BIDI_RE.sub("", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("\u0640", "")
    return re.sub(r"\s+", " ", text).strip()


def _map_street_type_words(text: str) -> str:
    """Replace whole-word Arabic street-type words with Hebrew equivalents."""
    return " ".join(_STREET_TYPE_WORDS.get(word, word) for word in text.split(" "))


def _transliterate_word(word: str, letter_map: Dict[str, str]) -> str:
    return "".join(letter_map.get(ch, ch) for ch in word)


def _transliterate(text: str, letter_map: Dict[str, str]) -> str:
    return " ".join(_transliterate_word(word, letter_map) for word in text.split(" "))


def _drop_mid_word_alef(text: str) -> str:
    """Remove alef/hamza-carrier letters that sit inside a word.

    The word-initial article alef and word-final alefs are kept.  LAMAS
    spells الرأيف as ``א-רייעף``-style ``רייעף`` — the hamza carrier is
    dropped — while it keeps long-vowel alefs elsewhere (``קאסם``), so this
    is generated as an extra variant rather than replacing the primary form.
    """
    out_words = []
    for word in text.split(" "):
        chars = list(word)
        kept = [
            ch
            for i, ch in enumerate(chars)
            if not (ch in ("ا", "أ") and 0 < i < len(chars) - 1)
        ]
        out_words.append("".join(kept))
    return " ".join(out_words)


def _styled_forms(text: str, letter_map: Dict[str, str]) -> List[str]:
    """Transliterate ``text`` in the article styles LAMAS actually uses.

    ``القدس`` → ``אלקודס`` (attached), ``אל קודס`` (spaced); ``النصر`` (ن is
    a sun letter) also gets ``א נצר`` (assimilated).  Mid-name sun-letter
    articles get one extra fully-assimilated form because LAMAS writes both
    ``אבו בכר אלצדיק`` and ``אבו בכר א-ס'דיק``.
    """
    words = [word for word in text.split(" ") if word]
    if not words:
        return []
    forms = [_transliterate(text, letter_map)]

    first = words[0]
    if first.startswith(_DEF_ARTICLE) and len(first) > len(_DEF_ARTICLE):
        stem = _transliterate_word(first[len(_DEF_ARTICLE):], letter_map)
        rest = " ".join(_transliterate_word(word, letter_map) for word in words[1:])
        rest = rest.strip()
        if stem:
            tail = f" {rest}" if rest else ""
            forms.append(f"אל {stem}{tail}")
            if first[len(_DEF_ARTICLE)] in _SUN_LETTERS:
                forms.append(f"א {stem}{tail}")

    if any(
        word.startswith(_DEF_ARTICLE)
        and len(word) > len(_DEF_ARTICLE)
        and word[len(_DEF_ARTICLE)] in _SUN_LETTERS
        for word in words
    ):
        parts = []
        for word in words:
            if (
                word.startswith(_DEF_ARTICLE)
                and len(word) > len(_DEF_ARTICLE)
                and word[len(_DEF_ARTICLE)] in _SUN_LETTERS
            ):
                parts.append("א " + _transliterate_word(word[len(_DEF_ARTICLE):], letter_map))
            else:
                parts.append(_transliterate_word(word, letter_map))
        forms.append(" ".join(part for part in parts if part))

    return forms


def _final_letter_variants(form: str) -> List[str]:
    """Alternate spellings for a name-final ا (→ה) or ى/ي (→א).

    Only the last word changes: word-final ى/ي inside multi-word names
    ("أبي" → אבי, as in סעד בן אבי וקאס) is not spelled differently in
    LAMAS, while name-final "סינא"/"סינה" and "עלי"/"עלא" variants exist.
    """
    words = form.split(" ")
    if not words or not words[-1]:
        return []
    last = words[-1]
    if last.endswith("א") and len(last) > 1:
        alternates = [last[:-1] + "ה"]
    elif last.endswith("י") and len(last) > 1:
        alternates = [last[:-1] + "א"]
    else:
        return []
    return [" ".join(words[:-1] + [alternate]) for alternate in alternates]


def transliteration_variants(name: object, max_variants: int = 16) -> List[str]:
    """Bounded, de-duplicated Hebrew transliterations of an Arabic name.

    Returns ``[]`` for names without Arabic characters.  The first variant is
    the primary (Academy-style) transliteration; the rest cover the LAMAS
    spellings documented in the module docstring.  Callers compare each
    variant against the Hebrew LAMAS names with the usual fuzzy blend.
    """
    if not contains_arabic(name):
        return []
    base = _clean(str(name))
    if not base:
        return []
    base = _map_street_type_words(base)
    if not contains_arabic(base):
        # Only mapped Hebrew street-type words/numbers remain (e.g.
        # "شارع 140" → "רחוב 140"): the mapping IS the transliteration.
        return [base] if base else []

    forms: List[str] = []
    seen = set()

    def add(form: str) -> None:
        normalized = re.sub(r"\s+", " ", form).strip()
        if normalized and normalized not in seen:
            seen.add(normalized)
            forms.append(normalized)

    texts = [base]
    with_exceptions = _apply_word_exceptions(base)
    if with_exceptions != base:
        texts.append(with_exceptions)

    all_texts: List[str] = []
    for text in texts:
        all_texts.append(text)
        dropped = _drop_mid_word_alef(text)
        if dropped != text:
            all_texts.append(dropped)

    for text in all_texts:
        for form in _styled_forms(text, _PRIMARY_LETTERS):
            add(form)
        # Name-final letter alternates, from the primary attached form only.
        for form in _final_letter_variants(_transliterate(text, _PRIMARY_LETTERS)):
            add(form)

    if any(ch in base for ch in _ALTERNATE_LETTERS):
        alternate_map = {**_PRIMARY_LETTERS, **_ALTERNATE_LETTERS}
        for text in texts:
            for form in _styled_forms(text, alternate_map):
                add(form)

    return forms[:max_variants]


def transliterate(name: object) -> str:
    """Primary Hebrew transliteration of an Arabic-script name ('' if none)."""
    variants = transliteration_variants(name)
    return variants[0] if variants else ""
