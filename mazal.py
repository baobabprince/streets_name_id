"""Conservative, optional Mazal entity enrichment; never changes street IDs.

API contract: https://www.nli.org.il/en/research-and-teach/open-library/search-api/user-help
Only Identity results with an explicit NLI /authorities/<id> URL are eligible.
Unknown response schemas fail closed. The live schema must be checked in the
pilot workflow; fixtures in tests are representative JSON-LD, not live captures.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests

from normalization import normalize_street_name, strip_street_prefix

ENDPOINT = "https://api.nli.org.il/openlibrary/search"
DC = "http://purl.org/dc/elements/1.1/"
DCT = "http://purl.org/dc/terms/"
VERSION = 1
LOG = logging.getLogger(__name__)
ID_RE = re.compile(r"[0-9]{18}")
# MARC-style life dates are qualifiers, not part of the street name.
DATES = re.compile(r",\s*\d{3,4}\s*[-–]\s*\d{0,4}\s*$")


def name_key(name: str) -> str:
    text = strip_street_prefix(normalize_street_name(name) or "").casefold()
    # Token order differs between street signage and library headings.
    return " ".join(sorted(text.split()))


def _values(record: dict, field: str) -> list[str]:
    value = record.get(field, [])
    if not isinstance(value, list):
        value = [value]
    return [v if isinstance(v, str) else v["@value"] for v in value
            if isinstance(v, str) or (isinstance(v, dict) and isinstance(v.get("@value"), str))]


def parse_records(payload) -> list[dict]:
    """Parse JSON-LD only; never guess that a bibliographic ID is an authority."""
    if isinstance(payload, dict) and isinstance(payload.get("@graph"), list):
        payload = payload["@graph"]
    if not isinstance(payload, list) or any(not isinstance(r, dict) for r in payload):
        raise ValueError("Unrecognized Identity response schema")
    result = []
    for record in payload:
        url = record.get("@id", "")
        parsed = urlparse(url if isinstance(url, str) else "")
        match = re.fullmatch(r"/(?:he|en|ar)/authorities/([0-9]{18})/?", parsed.path)
        titles = _values(record, DC + "title")
        if parsed.hostname not in {"www.nli.org.il", "nli.org.il"} or not match or not titles:
            raise ValueError("Identity result lacks explicit authority URL or title")
        labels = titles + _values(record, DCT + "alternative") + _values(record, DC + "alternative")
        result.append({"mazal_id": match[1], "preferred_name": titles[0],
                       "labels": sorted(set(labels))})
    return result


def choose_entity(key: str, records: list[dict]) -> dict:
    matches = {}
    for record in records:
        if any(name_key(DATES.sub("", label)) == key for label in record["labels"]):
            matches[record["mazal_id"]] = record
    ids = sorted(matches)
    if len(ids) != 1:
        return {"status": "ambiguous" if ids else "not_found", "candidate_ids": ids}
    record = matches[ids[0]]
    return {"status": "resolved", "mazal_id": record["mazal_id"],
            "preferred_name": record["preferred_name"],
            "entity_key": "mazal:" + record["mazal_id"],
            "authority_url": "https://www.nli.org.il/he/authorities/" + record["mazal_id"],
            "method": "unique_exact_authority_label", "candidate_ids": ids}


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


class MazalClient:
    """Bounded requests, expiring cache, no secret-bearing errors or raw responses."""

    def __init__(self, api_key: str, cache_path="cache/mazal/lookups.json", *,
                 max_requests=50, delay=1.0, max_pages=3, session=None):
        if not api_key:
            raise ValueError("MAZAL_API_KEY is required")
        if max_requests < 1 or delay < 0 or max_pages < 1:
            raise ValueError("Invalid request budget, delay or page limit")
        self.api_key = api_key
        self.cache_path = Path(cache_path)
        self.max_requests, self.delay, self.max_pages = max_requests, delay, max_pages
        self.session = session or requests.Session()
        self.requests_used = 0
        self.stopped = False
        self.response_shape = None
        try:
            cache = json.loads(self.cache_path.read_text(encoding="utf-8"))
            self.cache = cache["entries"] if cache.get("version") == VERSION else {}
            if not isinstance(self.cache, dict):
                self.cache = {}
        except (OSError, ValueError, KeyError, AttributeError):
            self.cache = {}
        self.memory = {}

    def resolve(self, name: str) -> dict:
        key = name_key(name)
        if len(key.split()) < 2 or any(c.isdigit() for c in key):
            return {"status": "needs_review"}
        if key in self.memory:
            return dict(self.memory[key])
        cached = self.cache.get(key, {})
        if isinstance(cached, dict) and isinstance(cached.get("cached_at"), (int, float)):
            if 0 <= time.time() - cached["cached_at"] < 30 * 86400:
                result = {k: v for k, v in cached.items() if k != "cached_at"}
                self.memory[key] = result
                return dict(result)
        records = []
        for page in range(1, self.max_pages + 1):
            if self.stopped or self.requests_used >= self.max_requests:
                return {"status": "budget_exhausted"}
            if self.requests_used:
                time.sleep(self.delay)
            self.requests_used += 1
            try:
                response = self.session.get(
                    ENDPOINT,
                    params={"api_key": self.api_key,
                            "query": ",AND;".join("any,contains," + word for word in key.split()),
                            "material_type": "Identity", "output_format": "json",
                            "items_per_page": 50, "result_page": page},
                    headers={"Accept": "application/json", "User-Agent": "streets-name-id/mazal-pilot"},
                    timeout=(10, 45), allow_redirects=False,
                )
                if response.status_code != 200:
                    # Stop after any HTTP failure; don't burn the remaining quota.
                    self.stopped = True
                    return {"status": "http_error", "http_status": response.status_code}
                # NLI can return 200 while silently ignoring an invalid filter.
                if response.headers.get("Errors"):
                    self.stopped = True
                    return {"status": "api_error"}
                payload = response.json()
                # Diagnostic structure only (no response values, URLs or key).
                if self.response_shape is None:
                    sample = payload[0] if isinstance(payload, list) and payload else payload
                    self.response_shape = {"container": type(payload).__name__, "fields": {
                        str(k).replace(self.api_key, "REDACTED")[:160]: type(v).__name__
                        for k, v in list(sample.items())[:40]
                    } if isinstance(sample, dict) else {}}
                batch = parse_records(payload)
            except requests.RequestException:
                # Never log exception text: requests includes the URL with api_key.
                self.stopped = True
                return {"status": "network_error"}
            except (ValueError, TypeError):
                self.stopped = True
                return {"status": "schema_error"}
            records.extend(batch)
            if len(batch) < 50:
                result = choose_entity(key, records)
                self.cache[key] = {**result, "cached_at": time.time()}
                write_json(self.cache_path, {"version": VERSION, "entries": self.cache})
                self.memory[key] = result
                return dict(result)
        # A unique match on a truncated result set is NOT proven unique.
        return {"status": "truncated"}


def build_registry(names, client: MazalClient) -> dict:
    entries, groups = {}, {}
    # Stable ordering; different spellings of the same canonical key cost one lookup.
    for name in sorted(set(n.strip() for n in names if isinstance(n, str) and n.strip())):
        key = name_key(name)
        if key not in entries:
            entries[key] = client.resolve(name)
        result = entries[key]
        if result["status"] == "resolved":
            group = groups.setdefault(result["entity_key"], {
                "mazal_id": result["mazal_id"], "preferred_name": result["preferred_name"], "names": []})
            group["names"].append(name)
    return {"version": VERSION, "generated_at": datetime.now(timezone.utc).isoformat(),
            "endpoint": ENDPOINT, "requests_used": client.requests_used,
            "response_shape": client.response_shape,
            "statistics": dict(Counter(r["status"] for r in entries.values())),
            "entries": entries, "entities": groups}


def enrich_dataframe(frame, registry: dict):
    """Add separate entity metadata from an offline registry, preserving every row/ID.

    Conflicting OSM/final-LAMAS entities are withheld for review. Candidate
    best_LAMAS_name is never evidence: an approved AI override may target a
    different ID. Only an explicit final_LAMAS_name can supplement the OSM name.
    """
    result = frame.copy()
    entries = registry.get("entries", {})
    metadata = []
    for _, row in result.iterrows():
        labels = [row.get("osm_name")]
        final_id = row.get("final_LAMAS_id")
        if final_id is not None and str(final_id).strip().lower() not in {"", "nan", "none", "<na>"}:
            labels.append(row.get("final_LAMAS_name"))
        hits = [entries.get(name_key(n), {}) for n in labels if isinstance(n, str) and n.strip()]
        resolved = [r for r in hits if r.get("status") == "resolved"]
        ids = {r.get("mazal_id") for r in resolved}
        if len(ids) == 1 and all(ID_RE.fullmatch(str(i or "")) for i in ids):
            hit = resolved[0]
            mid = hit["mazal_id"]
            metadata.append((mid, hit.get("preferred_name", ""), "mazal:" + mid, "resolved"))
        else:
            metadata.append(("", "", "", "conflict" if len(ids) > 1 else "unresolved"))
    for index, column in enumerate(("mazal_id", "mazal_name", "entity_key", "mazal_status")):
        result[column] = [m[index] for m in metadata]
    return result


def enrich_from_registry(frame):
    """No API calls during mapping. With no registry, retain existing behaviour."""
    path = Path(os.environ.get("MAZAL_ENTITIES_PATH", "data/mazal_entities.json"))
    if not path.exists():
        return frame
    try:
        registry = json.loads(path.read_text(encoding="utf-8"))
        if registry.get("version") != VERSION or not isinstance(registry.get("entries"), dict):
            raise ValueError("Invalid registry")
        return enrich_dataframe(frame, registry)
    except (OSError, ValueError, AttributeError, TypeError):
        LOG.warning("Mazal registry could not be read; continuing without entity enrichment")
        return frame
