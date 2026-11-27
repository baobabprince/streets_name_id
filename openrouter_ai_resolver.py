"""OpenRouter Perplexity Decider API resolver with local Laya fallback."""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import threading
import time
from collections import deque
from typing import Any, Callable, Iterable, Sequence

import requests
from rapidfuzz import fuzz, process

from arabic_matching import (
    arabic_hebrew_skeleton,
    arabic_phonetic_skeleton,
    arabic_skeleton_to_hebrew_nouns,
    bare_hebrew_street_noun,
    cross_script_comparison_skeleton,
    has_arabic,
    has_arabic_article_in_hebrew,
    hebrew_arabic_article_form,
    hebrew_noun_to_arabic_skeletons,
    is_latin_only,
    latin_arabic_to_hebrew_skeleton,
    latin_comparison_skeleton,
    latin_to_hebrew_skeleton,
    transliterate_arabic_to_hebrew,
)
from normalization import (
    NON_STREET_FEATURE_PREFIX_RE,
    normalize_street_name,
    strip_street_prefix,
)

logger = logging.getLogger(__name__)

OPENROUTER_DECISIONS_URL = "https://openrouter.ai/api/alpha/decisions"
DEFAULT_OPENROUTER_MODEL = "perplexity/pplx-decider-v1.3:free"
DEFAULT_SHORTLIST_SIZE = 25
MAX_SHORTLIST_SIZE = 254  # Plus "no_match" -> max 255 choice options.
DEFAULT_QUESTIONS_PER_REQUEST = 100
MAX_QUESTIONS_PER_REQUEST = 400
DEFAULT_TIMEOUT_SECONDS = 90
DEFAULT_MAX_RPM = 18
DEFAULT_CACHE_PATH = os.path.join("cache", "openrouter_decisions_cache.json")
PROMPT_VERSION = "v1"

_DIGIT_RE = re.compile(r"\d+")
_FEATURE_SUFFIX_RE = re.compile(
    r"\s+(?:כיכר|ככר|גשר|פארק|גן|טיילת|חניון|צומת|מחלף)$"
)
_GENERIC_STOPWORDS = frozenset({
    "רחוב", "רח", "דרך", "דר", "שדרות", "שדרת", "שד", "סמטת", "סמטה", "סמ",
    "משעול", "שביל", "מעלה", "מעלות", "רחבת", "נתיב", "מדרגות", "מעבר",
    "שכונת", "מבוא", "כיכר", "ככר", "גשר", "פארק", "גן", "טיילת", "חניון",
    "צומת", "מחלף", "הרב", "רבי", "פרופ", "פרופסור", "ראל", "אל", "בן",
    "בר", "של", "את", "על",
})

_NO_MATCH_LABEL = "אף אחד מהרחובות אינו אותו רחוב (שם שונה או רחוב שלא קיים בלמ״ס)"
_SHARED_STATE_PROMPT = (
    "משימה: התאמת שמות רחובות מ-OpenStreetMap לרשימת רחובות רשמית של הלמ״ס בישראל. "
    "התחשב בכתיב מלא וחסר, כינויים, תארים (הרב, ד״ר, פרופ׳), סדר מילים שונה, קיצורים "
    "(רח׳, שד׳, סמ׳, ככר/כיכר), ותרגום או תעתיק בין עברית, ערבית ואנגלית. "
    "בחר no_match אם הרחוב מתייחס לאדם או מקום אחר לגמרי או אם אינו קיים באפשרויות."
)


class OpenRouterDecisionError(RuntimeError):
    """Raised when the OpenRouter Decisions API request fails or exhausts quota."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        permanent: bool = False,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.permanent = permanent


def _shortlist_variants(text: str) -> list[str]:
    """Return normalized comparison forms including feature-prefix stripped core."""
    norm = normalize_street_name(text) or str(text or "").strip()
    core = strip_street_prefix(norm)
    if not core:
        return []
    variants = [core]
    stripped = NON_STREET_FEATURE_PREFIX_RE.sub("", core).strip()
    stripped = _FEATURE_SUFFIX_RE.sub("", stripped).strip()
    if stripped and stripped != core:
        variants.append(stripped)
    return variants


def _meaningful_tokens(text: str) -> set[str]:
    tokens: set[str] = set()
    for variant in _shortlist_variants(text):
        for token in variant.split():
            if len(token) >= 2 and token not in _GENERIC_STOPWORDS:
                tokens.add(token)
    return tokens


def select_candidate_shortlist(
    osm_streets: Sequence[str],
    lamas_streets: dict[str, list[str]],
    shortlist_size: int = DEFAULT_SHORTLIST_SIZE,
    preferred_ids: dict[str, str] | None = None,
) -> dict[str, list[str]]:
    """Select the top-K plausible LAMAS IDs per OSM street using hybrid signals."""
    k = max(1, min(int(shortlist_size), MAX_SHORTLIST_SIZE))
    lids = sorted(str(lid) for lid in lamas_streets)
    if not osm_streets or not lids:
        return {str(s): [] for s in osm_streets}
    if len(lids) <= k:
        return {str(s): list(lids) for s in osm_streets}

    alias_records: list[tuple[str, str, str, str]] = []
    lid_tokens: dict[str, set[str]] = {lid: set() for lid in lids}
    dig_index: dict[str, set[str]] = {}
    phonetic_index: dict[str, set[str]] = {}
    bare_noun_index: dict[str, set[str]] = {}

    for lid in lids:
        for raw_name in lamas_streets.get(lid, []):
            name = str(raw_name or "").strip()
            if not name:
                continue
            for dig in _DIGIT_RE.findall(name):
                dig_index.setdefault(dig, set()).add(lid)
            tokens = _meaningful_tokens(name)
            lid_tokens[lid].update(tokens)
            for variant in _shortlist_variants(name):
                sk = arabic_hebrew_skeleton(variant)
                af = hebrew_arabic_article_form(
                    variant, strip_attached_al=True, strip_he_prefix=True
                )
                alias_records.append((lid, variant, sk, af))
                ph = arabic_phonetic_skeleton(variant)
                if len(ph.replace(" ", "")) >= 3:
                    phonetic_index.setdefault(ph, set()).add(lid)
                bare = bare_hebrew_street_noun(variant)
                if bare:
                    bare_noun_index.setdefault(bare, set()).add(lid)

    if not alias_records:
        return {str(s): lids[:k] for s in osm_streets}

    cores = [rec[1] for rec in alias_records]
    skeletons = [rec[2] for rec in alias_records]
    article_forms = [rec[3] for rec in alias_records]

    tok_index: dict[str, set[str]] = {}
    for lid, tokens in lid_tokens.items():
        for token in tokens:
            tok_index.setdefault(token, set()).add(lid)

    num_lids = len(lids)
    max_idf = math.log((num_lids + 1) / 2.0) or 1.0
    tok_idf = {
        token: math.log((num_lids + 1) / (len(holders) + 1)) / max_idf
        for token, holders in tok_index.items()
    }

    preferred = {str(s): str(lid) for s, lid in (preferred_ids or {}).items() if lid}
    valid_lid_set = set(lids)
    result: dict[str, list[str]] = {}

    for raw_osm in osm_streets:
        osm = str(raw_osm)
        variants = _shortlist_variants(osm)
        if not variants:
            result[osm] = lids[:k]
            continue

        scores: dict[str, float] = {}

        def _bump(lid: str, value: float) -> None:
            if value > scores.get(lid, 0.0):
                scores[lid] = value

        for variant in variants:
            for _, sc, idx in process.extract(
                variant, cores, scorer=fuzz.WRatio, score_cutoff=1.0, limit=k * 2
            ):
                _bump(alias_records[idx][0], float(sc))
            for _, sc, idx in process.extract(
                variant, cores, scorer=fuzz.token_sort_ratio, score_cutoff=1.0, limit=k
            ):
                _bump(alias_records[idx][0], float(sc))
            if len(variant) >= 4:
                for _, sc, idx in process.extract(
                    variant, cores, scorer=fuzz.partial_ratio, score_cutoff=1.0, limit=k
                ):
                    _bump(alias_records[idx][0], float(sc) * 0.90)

        # Rare-token IDF signal catches reversed/partial multi-word matches in huge cities.
        osm_tokens = _meaningful_tokens(osm)
        if osm_tokens:
            token_weight_by_lid: dict[str, float] = {}
            osm_total_idf = sum(tok_idf.get(t, 0.5) for t in osm_tokens) or 1.0
            for token in osm_tokens:
                holders = tok_index.get(token)
                if not holders:
                    continue
                weight = tok_idf.get(token, 0.0)
                for lid in holders:
                    token_weight_by_lid[lid] = token_weight_by_lid.get(lid, 0.0) + weight
            for lid, matched_idf in token_weight_by_lid.items():
                coverage = matched_idf / osm_total_idf
                _bump(lid, 65.0 + 30.0 * min(1.0, coverage))

        # Exact digit-set overlap helps numbered alleys and military units.
        osm_digits = set(_DIGIT_RE.findall(osm))
        if osm_digits:
            for dig in osm_digits:
                for lid in dig_index.get(dig, ()):
                    _bump(lid, 82.0)

        # Cross-script Arabic, Latin-Arabic, and Hebrew-article Arabic signals.
        if has_arabic(osm):
            translit = transliterate_arabic_to_hebrew(osm, strip_prefix=True)
            if translit:
                for _, sc, idx in process.extract(
                    translit, cores, scorer=fuzz.WRatio, score_cutoff=1.0, limit=k * 2
                ):
                    _bump(alias_records[idx][0], float(sc))
                sk_trans = arabic_hebrew_skeleton(translit)
                if len(sk_trans.replace(" ", "")) >= 3:
                    for _, sc, idx in process.extract(
                        sk_trans, skeletons, scorer=fuzz.ratio, score_cutoff=1.0, limit=k
                    ):
                        _bump(alias_records[idx][0], float(sc))
                ph_trans = arabic_phonetic_skeleton(translit)
                for lid in phonetic_index.get(ph_trans, ()):
                    _bump(lid, 96.0)
                for noun in arabic_skeleton_to_hebrew_nouns(ph_trans):
                    for lid in bare_noun_index.get(noun, ()):
                        _bump(lid, 95.0)
                    for lid in tok_index.get(noun, ()):
                        _bump(lid, 95.0)
        elif is_latin_only(osm):
            sk_lat = latin_arabic_to_hebrew_skeleton(osm)
            if sk_lat and len(sk_lat.replace(" ", "")) >= 3:
                for _, sc, idx in process.extract(
                    sk_lat, skeletons, scorer=fuzz.ratio, score_cutoff=1.0, limit=k * 2
                ):
                    _bump(alias_records[idx][0], float(sc))
                lsk_osm = latin_comparison_skeleton(sk_lat)
                if len(lsk_osm.replace(" ", "")) >= 3:
                    for idx, (_, _, sk_lam, _) in enumerate(alias_records):
                        if not sk_lam:
                            continue
                        ratio = float(fuzz.ratio(lsk_osm, latin_comparison_skeleton(sk_lam)))
                        if ratio >= 70.0:
                            _bump(alias_records[idx][0], ratio)
            else:
                sk_gen = latin_to_hebrew_skeleton(osm)
                if sk_gen and len(sk_gen.replace(" ", "")) >= 2:
                    for _, sc, idx in process.extract(
                        sk_gen, skeletons, scorer=fuzz.ratio, score_cutoff=1.0, limit=k * 2
                    ):
                        _bump(alias_records[idx][0], float(sc))
                    csk_osm = cross_script_comparison_skeleton(sk_gen)
                    if len(csk_osm.replace(" ", "")) >= 2:
                        for idx, (_, _, sk_lam, _) in enumerate(alias_records):
                            if not sk_lam:
                                continue
                            ratio = float(
                                fuzz.ratio(csk_osm, cross_script_comparison_skeleton(sk_lam))
                            )
                            if ratio >= 65.0:
                                _bump(alias_records[idx][0], ratio)
        else:
            for variant in variants:
                if has_arabic_article_in_hebrew(variant, allow_attached_al=True):
                    af_osm = hebrew_arabic_article_form(
                        variant, strip_attached_al=True, strip_he_prefix=True
                    )
                    if len(af_osm.replace(" ", "")) >= 3:
                        for _, sc, idx in process.extract(
                            af_osm, article_forms, scorer=fuzz.WRatio, score_cutoff=1.0, limit=k
                        ):
                            _bump(alias_records[idx][0], float(sc))
                for ph_target in hebrew_noun_to_arabic_skeletons(variant):
                    for lid in phonetic_index.get(ph_target, ()):
                        _bump(lid, 95.0)

        ranked = [
            lid
            for lid, _ in sorted(scores.items(), key=lambda item: (-item[1], item[0]))[:k]
        ]
        if len(ranked) < k:
            seen = set(ranked)
            for lid in lids:
                if lid not in seen:
                    ranked.append(lid)
                    if len(ranked) >= k:
                        break

        pref_id = preferred.get(osm)
        if pref_id and pref_id in valid_lid_set and pref_id not in ranked:
            if len(ranked) >= k:
                ranked[-1] = pref_id
            else:
                ranked.append(pref_id)

        result[osm] = ranked or lids[:k]

    return result


class DecisionCache:
    """Thread-safe atomic JSON cache for validated OpenRouter street decisions."""

    def __init__(self, cache_path: str | None = DEFAULT_CACHE_PATH) -> None:
        self.cache_path = cache_path or None
        self._lock = threading.Lock()
        self._entries: dict[str, dict[str, Any]] = {}
        self._dirty = False
        if self.cache_path and os.path.exists(self.cache_path):
            self._load()

    def _load(self) -> None:
        try:
            with open(self.cache_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            entries = payload.get("entries") if isinstance(payload, dict) else None
            if isinstance(entries, dict):
                self._entries = entries
        except Exception as exc:
            logger.warning(
                "Could not load OpenRouter cache '%s': %s; starting empty cache.",
                self.cache_path,
                exc,
            )
            self._entries = {}

    @staticmethod
    def build_key(
        *,
        model: str,
        city_name: str,
        osm_street: str,
        candidates: Sequence[tuple[str, list[str]]],
    ) -> str:
        payload = [
            model,
            PROMPT_VERSION,
            str(city_name),
            str(osm_street),
            [[str(lid), [str(n)[:120] for n in names[:3]]] for lid, names in candidates],
        ]
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
        return hashlib.sha256(encoded).hexdigest()

    def get(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            item = self._entries.get(key)
            return dict(item) if isinstance(item, dict) else None

    def put(self, key: str, decision: dict[str, Any]) -> None:
        method = str(decision.get("method") or "")
        if not method.startswith("openrouter:") or method.endswith(("_error", "_refusal")):
            return
        with self._lock:
            self._entries[key] = {
                "lamas_id": decision.get("lamas_id"),
                "confidence": float(decision.get("confidence", 0.0)),
                "reasoning": str(decision.get("reasoning", "")),
                "method": method,
            }
            self._dirty = True

    def flush(self) -> None:
        if not self.cache_path:
            return
        with self._lock:
            if not self._dirty:
                return
            parent = os.path.dirname(self.cache_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            tmp_path = f"{self.cache_path}.{os.getpid()}.tmp"
            payload = {"version": PROMPT_VERSION, "entries": self._entries}
            with open(tmp_path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
            os.replace(tmp_path, self.cache_path)
            self._dirty = False


def _is_daily_limit_error(status_code: int | None, body_text: str) -> bool:
    if status_code != 429:
        return False
    lower = (body_text or "").lower()
    return any(
        marker in lower
        for marker in ("per-day", "daily", "free-models-per-day", "requests per day")
    )


def _format_error_detail(response: requests.Response) -> str:
    try:
        data = response.json()
        err = data.get("error") if isinstance(data, dict) else None
        if isinstance(err, dict):
            msg = err.get("message") or str(err)
            return str(msg)[:300]
    except Exception:
        pass
    return (response.text or "")[:300]


class OpenRouterDecisionResolver:
    """Resolve unmatched OSM streets via OpenRouter's Decisions API."""

    def __init__(
        self,
        *,
        model: str | None = None,
        api_key: str | None = None,
        url: str = OPENROUTER_DECISIONS_URL,
        shortlist_size: int = DEFAULT_SHORTLIST_SIZE,
        questions_per_request: int = DEFAULT_QUESTIONS_PER_REQUEST,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        max_rpm: float = DEFAULT_MAX_RPM,
        max_retries: int = 2,
        cache_path: str | None = DEFAULT_CACHE_PATH,
        session: requests.Session | None = None,
        sleep_fn: Callable[[float], None] = time.sleep,
        monotonic_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        self.model = (
            model or os.environ.get("OPENROUTER_MODEL", "").strip() or DEFAULT_OPENROUTER_MODEL
        )
        self.api_key = (
            api_key if api_key is not None else os.environ.get("OPENROUTER_API_KEY", "")
        ).strip()
        self.url = url
        self.shortlist_size = max(2, min(int(shortlist_size), MAX_SHORTLIST_SIZE))
        self.questions_per_request = max(
            1, min(int(questions_per_request), MAX_QUESTIONS_PER_REQUEST)
        )
        self.timeout = float(timeout)
        self.max_rpm = float(max_rpm)
        self.max_retries = max(0, int(max_retries))
        self._session = session or requests.Session()
        self._sleep = sleep_fn
        self._monotonic = monotonic_fn
        self._rate_lock = threading.Lock()
        self._request_timestamps: deque[float] = deque()
        self._disabled_reason: str | None = None
        self.cache = DecisionCache(cache_path)
        self.stats: dict[str, Any] = {
            "api_requests": 0,
            "cache_hits": 0,
            "streets_resolved": 0,
            "refusals_or_invalid": 0,
            "total_input_tokens": 0,
            "total_output_tokens": 0,
            "total_cost": 0.0,
            "total_api_seconds": 0.0,
        }

    def is_available(self) -> bool:
        return bool(self.api_key) and self._disabled_reason is None

    def _wait_for_rate_limit(self) -> None:
        if self.max_rpm <= 0:
            return
        min_spacing = 60.0 / self.max_rpm
        with self._rate_lock:
            now = self._monotonic()
            while self._request_timestamps and (now - self._request_timestamps[0]) >= 60.0:
                self._request_timestamps.popleft()
            wait_for = 0.0
            if self._request_timestamps:
                since_last = now - self._request_timestamps[-1]
                if since_last < min_spacing:
                    wait_for = max(wait_for, min_spacing - since_last)
            if len(self._request_timestamps) >= int(self.max_rpm):
                oldest = self._request_timestamps[0]
                wait_for = max(wait_for, 60.0 - (now - oldest) + 0.05)
            if wait_for > 0:
                self._sleep(wait_for)
                now = self._monotonic()
                while (
                    self._request_timestamps
                    and (now - self._request_timestamps[0]) >= 60.0
                ):
                    self._request_timestamps.popleft()
            self._request_timestamps.append(now)

    @staticmethod
    def _build_question(
        city_name: str,
        osm_street: str,
        options: list[tuple[str, list[str]]],
    ) -> tuple[dict[str, str], dict[str, Any]]:
        option_ids: dict[str, str] = {}
        criteria: dict[str, str] = {}
        for index, (lamas_id, names) in enumerate(options, start=1):
            key = f"s_{index}"
            option_ids[key] = str(lamas_id)
            label = " | ".join(str(n)[:120] for n in names[:3] if n)
            criteria[key] = label or f"ID {lamas_id}"
        criteria["no_match"] = _NO_MATCH_LABEL

        translit_hint = ""
        if has_arabic(osm_street):
            tr = transliterate_arabic_to_hebrew(str(osm_street), strip_prefix=True)
            if tr:
                translit_hint = f" (תעתיק עברי: {tr})"

        question = {
            "type": "choice",
            "instructions": (
                f"ביישוב '{city_name}', איזה רחוב מרשימת הלמ״ס הוא אותו רחוב כמו "
                f"רחוב OSM '{osm_street}'{translit_hint}? בחר no_match אם אין התאמה אמיתית."
            ),
            "criteria": criteria,
        }
        return option_ids, question

    def _parse_single_answer(
        self,
        answer: Any,
        option_ids: dict[str, str],
    ) -> dict[str, Any] | None:
        if not isinstance(answer, dict):
            return None
        if answer.get("type") != "choice":
            return None
        selected = answer.get("choice")
        if not isinstance(selected, str):
            return None

        probabilities = answer.get("probabilities")
        prob_value: Any = None
        if isinstance(probabilities, dict):
            prob_value = probabilities.get(selected)
        if prob_value is None:
            prob_value = answer.get("confidence", 0.0)
        try:
            confidence = float(prob_value)
        except (TypeError, ValueError):
            confidence = 0.0

        if selected == "no_match":
            return {
                "lamas_id": None,
                "confidence": confidence,
                "reasoning": f"OpenRouter ({self.model}) selected no_match",
                "method": f"openrouter:{self.model}",
            }
        if selected in option_ids:
            return {
                "lamas_id": option_ids[selected],
                "confidence": confidence,
                "reasoning": f"OpenRouter ({self.model}) selected {selected}",
                "method": f"openrouter:{self.model}",
            }
        return None

    def _post_decisions(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.api_key:
            raise OpenRouterDecisionError(
                "OPENROUTER_API_KEY is not configured", permanent=True
            )
        if self._disabled_reason is not None:
            raise OpenRouterDecisionError(self._disabled_reason, permanent=True)

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "X-Title": "streets_name_id",
        }
        backoffs = (2.0, 5.0, 10.0)
        last_exc: OpenRouterDecisionError | None = None

        for attempt in range(self.max_retries + 1):
            self._wait_for_rate_limit()
            started = self._monotonic()
            try:
                response = self._session.post(
                    self.url, headers=headers, json=payload, timeout=self.timeout
                )
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_exc = OpenRouterDecisionError(
                    f"OpenRouter transport error: {exc}"
                )
                if attempt < self.max_retries:
                    delay = backoffs[min(attempt, len(backoffs) - 1)]
                    logger.warning(
                        "OpenRouter request error (attempt %d/%d): %s; retrying in %.1fs",
                        attempt + 1,
                        self.max_retries + 1,
                        exc,
                        delay,
                    )
                    self._sleep(delay)
                    continue
                raise last_exc from exc
            except requests.RequestException as exc:
                raise OpenRouterDecisionError(f"OpenRouter request failed: {exc}") from exc

            elapsed = max(0.0, self._monotonic() - started)
            status = int(response.status_code)

            if status == 200:
                try:
                    data = response.json()
                except ValueError as exc:
                    raise OpenRouterDecisionError(
                        f"OpenRouter returned invalid JSON: {exc}", status_code=200
                    ) from exc
                if not isinstance(data, dict) or not isinstance(data.get("answers"), dict):
                    raise OpenRouterDecisionError(
                        "OpenRouter response missing 'answers' object", status_code=200
                    )
                self.stats["api_requests"] += 1
                self.stats["total_api_seconds"] += elapsed
                usage = data.get("usage") if isinstance(data.get("usage"), dict) else {}
                self.stats["total_input_tokens"] += int(usage.get("input_tokens") or 0)
                self.stats["total_output_tokens"] += int(usage.get("output_tokens") or 0)
                try:
                    self.stats["total_cost"] += float(usage.get("cost") or 0.0)
                except (TypeError, ValueError):
                    pass
                return data

            detail = _format_error_detail(response)
            daily_exhausted = _is_daily_limit_error(status, detail)
            if status in (401, 402) or daily_exhausted:
                reason = (
                    f"OpenRouter HTTP {status}"
                    + (" (daily quota exhausted)" if daily_exhausted else "")
                    + (f": {detail}" if detail else "")
                )
                self._disabled_reason = reason
                raise OpenRouterDecisionError(
                    reason, status_code=status, permanent=True
                )

            if status in (429, 500, 502, 503, 504):
                last_exc = OpenRouterDecisionError(
                    f"OpenRouter HTTP {status}: {detail}", status_code=status
                )
                if attempt < self.max_retries:
                    retry_after = response.headers.get("Retry-After")
                    delay = backoffs[min(attempt, len(backoffs) - 1)]
                    if retry_after is not None:
                        try:
                            delay = min(max(0.5, float(retry_after)), 15.0)
                        except ValueError:
                            pass
                    logger.warning(
                        "OpenRouter HTTP %d (attempt %d/%d): %s; retrying in %.1fs",
                        status,
                        attempt + 1,
                        self.max_retries + 1,
                        detail or "transient error",
                        delay,
                    )
                    self._sleep(delay)
                    continue
                raise last_exc

            raise OpenRouterDecisionError(
                f"OpenRouter HTTP {status}: {detail}",
                status_code=status,
                permanent=(status == 400 and "model" in detail.lower()),
            )

        assert last_exc is not None
        raise last_exc

    def _resolve_multi_city_detailed(
        self,
        city_tasks: Sequence[tuple[str, Sequence[str], dict[str, list[str]]]],
        preferred_ids_by_city: dict[str, dict[str, str]] | None = None,
    ) -> tuple[dict[str, dict[str, dict[str, Any]]], dict[str, list[str]], list[str]]:
        """Resolve streets across one or more cities; track failed streets for fallback."""
        decisions_by_city: dict[str, dict[str, dict[str, Any]]] = {}
        failed_by_city: dict[str, list[str]] = {}
        error_notes: list[str] = []

        pending: list[tuple[str, str, str, dict[str, str], dict[str, Any]]] = []
        pref_map = preferred_ids_by_city or {}

        for raw_city, osm_streets, lamas_streets in city_tasks:
            city_name = str(raw_city)
            unique_streets = list(dict.fromkeys(str(s) for s in osm_streets if s))
            decisions_by_city.setdefault(city_name, {})
            if not unique_streets or not lamas_streets:
                continue

            city_pref = pref_map.get(city_name)
            shortlists = select_candidate_shortlist(
                unique_streets,
                lamas_streets,
                shortlist_size=self.shortlist_size,
                preferred_ids=city_pref,
            )
            for street in unique_streets:
                cand_lids = shortlists.get(street, [])
                options = [
                    (lid, list(lamas_streets.get(lid, [])))
                    for lid in cand_lids
                    if lid in lamas_streets
                ]
                if not options:
                    continue
                cache_key = DecisionCache.build_key(
                    model=self.model,
                    city_name=city_name,
                    osm_street=street,
                    candidates=options,
                )
                cached = self.cache.get(cache_key)
                if cached is not None:
                    self.stats["cache_hits"] += 1
                    self.stats["streets_resolved"] += 1
                    decisions_by_city[city_name][street] = cached
                    continue
                option_ids, question = self._build_question(city_name, street, options)
                pending.append((city_name, street, cache_key, option_ids, question))

        if not pending:
            return decisions_by_city, failed_by_city, error_notes

        for start in range(0, len(pending), self.questions_per_request):
            batch = pending[start : start + self.questions_per_request]
            questions_payload: dict[str, Any] = {}
            q_lookup: dict[str, tuple[str, str, str, dict[str, str]]] = {}
            for idx, (city_name, street, cache_key, option_ids, question) in enumerate(batch):
                q_key = f"q_{idx}"
                questions_payload[q_key] = question
                q_lookup[q_key] = (city_name, street, cache_key, option_ids)

            payload = {
                "model": self.model,
                "state": _SHARED_STATE_PROMPT,
                "questions": questions_payload,
            }

            try:
                response_data = self._post_decisions(payload)
            except OpenRouterDecisionError as exc:
                msg = str(exc)
                if msg not in error_notes:
                    error_notes.append(msg)
                for city_name, street, _, _, _ in batch:
                    failed_by_city.setdefault(city_name, []).append(street)
                    decisions_by_city[city_name][street] = {
                        "lamas_id": None,
                        "confidence": 0.0,
                        "reasoning": f"OpenRouter error: {msg}",
                        "method": "openrouter_error",
                    }
                continue

            answers = response_data.get("answers", {})
            refused_count = 0
            for q_key, (city_name, street, cache_key, option_ids) in q_lookup.items():
                parsed = self._parse_single_answer(answers.get(q_key), option_ids)
                if parsed is None:
                    refused_count += 1
                    self.stats["refusals_or_invalid"] += 1
                    failed_by_city.setdefault(city_name, []).append(street)
                    raw_ans = answers.get(q_key)
                    ans_type = (
                        raw_ans.get("type") if isinstance(raw_ans, dict) else "missing"
                    )
                    decisions_by_city[city_name][street] = {
                        "lamas_id": None,
                        "confidence": 0.0,
                        "reasoning": f"OpenRouter refusal/invalid answer ({ans_type})",
                        "method": "openrouter_refusal",
                    }
                    continue

                self.stats["streets_resolved"] += 1
                decisions_by_city[city_name][street] = parsed
                self.cache.put(cache_key, parsed)

            if refused_count:
                error_notes.append(
                    f"OpenRouter returned {refused_count} refusal/invalid answers in batch"
                )
            try:
                self.cache.flush()
            except OSError as exc:
                logger.warning("Failed to flush OpenRouter cache: %s", exc)

        return decisions_by_city, failed_by_city, error_notes

    def resolve_multi_city(
        self,
        city_tasks: Sequence[tuple[str, Sequence[str], dict[str, list[str]]]],
        preferred_ids_by_city: dict[str, dict[str, str]] | None = None,
    ) -> dict[str, dict[str, dict[str, Any]]]:
        """Resolve multiple cities together (batching questions up to request capacity)."""
        decisions_by_city, _, _ = self._resolve_multi_city_detailed(
            city_tasks, preferred_ids_by_city=preferred_ids_by_city
        )
        return decisions_by_city

    def resolve_city(
        self,
        city_name: str,
        osm_streets: Sequence[str],
        lamas_streets: dict[str, list[str]],
        preferred_ids: dict[str, str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Resolve all unmatched OSM streets for one city."""
        pref = {str(city_name): preferred_ids} if preferred_ids else None
        decisions = self.resolve_multi_city(
            [(city_name, osm_streets, lamas_streets)], preferred_ids_by_city=pref
        )
        return decisions.get(str(city_name), {})

    def resolve_street(
        self,
        city_name: str,
        osm_street: str,
        lamas_streets: dict[str, list[str]],
    ) -> dict[str, Any]:
        """Resolve one OSM street against the city's LAMAS streets."""
        decisions = self.resolve_city(city_name, [osm_street], lamas_streets)
        return decisions.get(
            str(osm_street),
            {
                "lamas_id": None,
                "confidence": 0.0,
                "reasoning": "No LAMAS streets in city",
                "method": "openrouter_error",
            },
        )


def _default_local_factory() -> Any:
    from laya_ai_resolver import LayaAIResolver

    return LayaAIResolver()


class HybridAIResolver:
    """Primary OpenRouter Decisions resolver with automatic local Laya fallback."""

    def __init__(
        self,
        *,
        primary: OpenRouterDecisionResolver | None = None,
        local_factory: Callable[[], Any] | None = None,
        model: str | None = None,
        api_key: str | None = None,
        shortlist_size: int = DEFAULT_SHORTLIST_SIZE,
        questions_per_request: int = DEFAULT_QUESTIONS_PER_REQUEST,
        max_rpm: float = DEFAULT_MAX_RPM,
        cache_path: str | None = DEFAULT_CACHE_PATH,
    ) -> None:
        self.primary = primary or OpenRouterDecisionResolver(
            model=model,
            api_key=api_key,
            shortlist_size=shortlist_size,
            questions_per_request=questions_per_request,
            max_rpm=max_rpm,
            cache_path=cache_path,
        )
        self._local_factory = local_factory or _default_local_factory
        self._local_resolver: Any = None
        self._local_init_failed = False
        self._warned_no_key = False
        self.stats: dict[str, int] = {
            "openrouter_resolved": 0,
            "local_fallback_resolved": 0,
            "fallback_events": 0,
            "errors": 0,
        }

    def is_available(self) -> bool:
        if self.primary.is_available():
            return True
        return self._get_local_resolver() is not None

    def _get_local_resolver(self) -> Any:
        if self._local_resolver is not None:
            return self._local_resolver
        if self._local_init_failed:
            return None
        try:
            resolver = self._local_factory()
        except Exception as exc:
            logger.error("Failed to initialize local Laya fallback model: %s", exc)
            self._local_init_failed = True
            return None
        if resolver is None or (
            hasattr(resolver, "is_available") and not resolver.is_available()
        ):
            logger.error("Local Laya fallback resolver is unavailable.")
            self._local_init_failed = True
            return None
        self._local_resolver = resolver
        return resolver

    def _run_local_fallback(
        self,
        city_name: str,
        streets: list[str],
        lamas_streets: dict[str, list[str]],
        reason: str,
    ) -> dict[str, dict[str, Any]]:
        self.stats["fallback_events"] += 1
        logger.warning(
            "OpenRouter fallback to local Laya for '%s' (%d streets): %s",
            city_name,
            len(streets),
            reason,
        )
        local = self._get_local_resolver()
        if local is None:
            self.stats["errors"] += len(streets)
            logger.error(
                "Both OpenRouter (%s) and local Laya fallback failed for '%s' (%d streets).",
                reason,
                city_name,
                len(streets),
            )
            return {
                s: {
                    "lamas_id": None,
                    "confidence": 0.0,
                    "reasoning": f"OpenRouter failed ({reason}); local fallback unavailable",
                    "method": "hybrid_ai_error",
                }
                for s in streets
            }

        try:
            local_decisions = local.resolve_city(city_name, streets, lamas_streets)
        except Exception as exc:
            self.stats["errors"] += len(streets)
            logger.exception(
                "Local Laya fallback crashed for '%s' (%d streets)",
                city_name,
                len(streets),
            )
            return {
                s: {
                    "lamas_id": None,
                    "confidence": 0.0,
                    "reasoning": f"OpenRouter failed ({reason}); local Laya error: {exc}",
                    "method": "hybrid_ai_error",
                }
                for s in streets
            }

        out: dict[str, dict[str, Any]] = {}
        for s in streets:
            dec = dict(local_decisions.get(s) or {})
            method = str(dec.get("method") or "local_laya")
            if method.endswith("_error"):
                self.stats["errors"] += 1
            else:
                self.stats["local_fallback_resolved"] += 1
            dec.setdefault("lamas_id", None)
            dec.setdefault("confidence", 0.0)
            prev_reason = str(dec.get("reasoning") or "").strip()
            dec["reasoning"] = f"{prev_reason} [fallback: {reason}]".strip()
            dec["method"] = method
            out[s] = dec
        return out

    def resolve_multi_city(
        self,
        city_tasks: Sequence[tuple[str, Sequence[str], dict[str, list[str]]]],
        preferred_ids_by_city: dict[str, dict[str, str]] | None = None,
    ) -> dict[str, dict[str, dict[str, Any]]]:
        """Resolve multiple cities via OpenRouter, falling back to local Laya per failure."""
        results: dict[str, dict[str, dict[str, Any]]] = {}
        if not city_tasks:
            return results

        if not self.primary.is_available():
            reason = self.primary._disabled_reason or "OPENROUTER_API_KEY not set"
            if not self._warned_no_key:
                logger.warning(
                    "OpenRouter API unavailable (%s); falling back automatically to local Laya model.",
                    reason,
                )
                self._warned_no_key = True
            for raw_city, osm_streets, lamas_streets in city_tasks:
                city_name = str(raw_city)
                unique_streets = list(dict.fromkeys(str(s) for s in osm_streets if s))
                if not unique_streets or not lamas_streets:
                    results[city_name] = {}
                    continue
                results[city_name] = self._run_local_fallback(
                    city_name, unique_streets, lamas_streets, reason
                )
            return results

        lamas_by_city = {str(city): lamas for city, _, lamas in city_tasks}
        try:
            decisions_by_city, failed_by_city, notes = (
                self.primary._resolve_multi_city_detailed(
                    city_tasks, preferred_ids_by_city=preferred_ids_by_city
                )
            )
        except Exception as exc:
            reason = f"Unexpected OpenRouter error: {exc}"
            logger.exception("OpenRouter multi-city resolution failed unexpectedly")
            for raw_city, osm_streets, lamas_streets in city_tasks:
                city_name = str(raw_city)
                unique_streets = list(dict.fromkeys(str(s) for s in osm_streets if s))
                if not unique_streets or not lamas_streets:
                    results[city_name] = {}
                    continue
                results[city_name] = self._run_local_fallback(
                    city_name, unique_streets, lamas_streets, reason
                )
            return results

        note_summary = "; ".join(notes[:2]) or "OpenRouter request/refusal failure"
        for raw_city, osm_streets, lamas_streets in city_tasks:
            city_name = str(raw_city)
            unique_streets = list(dict.fromkeys(str(s) for s in osm_streets if s))
            city_decisions = dict(decisions_by_city.get(city_name, {}))
            failed_streets = list(dict.fromkeys(failed_by_city.get(city_name, [])))

            # Catch any street missing completely from primary output too.
            missing = [
                s for s in unique_streets if s not in city_decisions and lamas_streets
            ]
            for s in missing:
                if s not in failed_streets:
                    failed_streets.append(s)

            ok_count = len(unique_streets) - len(failed_streets)
            self.stats["openrouter_resolved"] += max(0, ok_count)

            if failed_streets:
                fallback_res = self._run_local_fallback(
                    city_name, failed_streets, lamas_by_city[city_name], note_summary
                )
                city_decisions.update(fallback_res)

            results[city_name] = city_decisions

        logger.info(
            "AI resolution summary | openrouter=%d, cache_hits=%d, "
            "local_laya_fallback=%d, errors=%d, api_calls=%d, tokens=%d, cost=$%.4f",
            self.stats["openrouter_resolved"],
            self.primary.stats["cache_hits"],
            self.stats["local_fallback_resolved"],
            self.stats["errors"],
            self.primary.stats["api_requests"],
            self.primary.stats["total_input_tokens"],
            self.primary.stats["total_cost"],
        )
        return results

    def resolve_city(
        self,
        city_name: str,
        osm_streets: Sequence[str],
        lamas_streets: dict[str, list[str]],
        preferred_ids: dict[str, str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Resolve all unmatched OSM streets for one city."""
        pref = {str(city_name): preferred_ids} if preferred_ids else None
        decisions = self.resolve_multi_city(
            [(city_name, osm_streets, lamas_streets)], preferred_ids_by_city=pref
        )
        return decisions.get(str(city_name), {})

    def resolve_street(
        self,
        city_name: str,
        osm_street: str,
        lamas_streets: dict[str, list[str]],
    ) -> dict[str, Any]:
        """Resolve one OSM street with automatic OpenRouter -> local Laya fallback."""
        decisions = self.resolve_city(city_name, [osm_street], lamas_streets)
        return decisions.get(
            str(osm_street),
            {
                "lamas_id": None,
                "confidence": 0.0,
                "reasoning": "No LAMAS streets in city",
                "method": "hybrid_ai_error",
            },
        )
