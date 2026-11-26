"""Build no_results.json: settlements that have no street page in the index.

Categories:
  unresolved       - no usable OSM settlement match (reason from match report)
  no_osm_streets   - matched, but OSM has no street data at all (no cache / empty)
  no_named_streets - matched, OSM has no named road segments in the settlement
  pending          - matched and has named OSM streets (or PBF unavailable), but not processed yet
"""

from __future__ import annotations

import json
import os
import re
from typing import Dict

import pandas as pd

from data_loader import load_or_fetch_LAMAS, osm_cache_path
from generate_index import HTML_DIR, scan_settlements
from settlement_names import canonical_key, settlement_identity_key

REASONS = {
    "unresolved": "לא נמצא יישוב מתאים ב-OSM",
    "no_osm_streets": "ביישוב אין נתוני רחובות ב-OSM",
    "no_named_streets": "אין רחובות בעלי שם ב-OSM",
    "pending": "טרם עובד",
}

MATCH_REPORT_PATH = "batch_reports/settlement_match_report.csv"
MATCH_STORE_PATH = "data/settlement_matches.json"
NO_DATA_PATH = "batch_reports/no_data_settlements.txt"
RETRY_PENDING_PATH = "batch_reports/retry_pending.txt"
LAMAS_CSV_PATH = "cache/lamas_data.csv"


def _load_lamas_street_counts(
    lamas_csv_path: str = LAMAS_CSV_PATH,
) -> Dict[str, int]:
    """Return unique street-level LAMAS_id count per settlement_identity_key.

    Excludes each settlement's own-name pseudo-record (e.g. LAMAS_id 9000) so
    villages with no official street list report 0 rather than their alias count.
    """
    if os.path.exists(lamas_csv_path):
        lam = pd.read_csv(lamas_csv_path, dtype=str)
    else:
        lam = load_or_fetch_LAMAS()
    if lam.empty or "city" not in lam.columns or "LAMAS_name" not in lam.columns:
        return {}

    lam = lam[lam["LAMAS_name"].fillna("").str.strip() != ""].copy()
    lam["_ik"] = lam["city"].map(
        lambda x: settlement_identity_key(x) if isinstance(x, str) else ""
    )
    lam["_ck"] = lam["city"].map(
        lambda x: canonical_key(x) if isinstance(x, str) else ""
    )

    if "LAMAS_id" not in lam.columns:
        return {k: int(v) for k, v in lam.groupby("_ik")["LAMAS_name"].nunique().items()}

    # Identify per-settlement own-name pseudo-record IDs (all aliases of e.g. 9000).
    own_name_mask = lam.apply(
        lambda r: bool(r["_ck"]) and canonical_key(r["LAMAS_name"]) == r["_ck"],
        axis=1,
    )
    own_pairs = set(
        zip(
            lam.loc[own_name_mask, "_ik"],
            lam.loc[own_name_mask, "LAMAS_id"].astype(str),
        )
    )
    street_mask = ~lam.apply(
        lambda r: (r["_ik"], str(r["LAMAS_id"])) in own_pairs,
        axis=1,
    )
    street_df = lam[street_mask]
    return {
        k: int(v)
        for k, v in street_df.groupby("_ik")["LAMAS_id"].nunique().items()
    }


def _load_match_rows(
    report_path: str = MATCH_REPORT_PATH,
    store_path: str = MATCH_STORE_PATH,
) -> list[dict]:
    """Load settlement match statuses, preferring the latest report + full match store."""
    by_ik: dict[str, dict] = {}

    # Seed from the persistent match store first so partial retry reports cannot
    # drop settlements or resurrect stale entries from an older no_results.json.
    if os.path.exists(store_path):
        try:
            store = json.load(open(store_path, encoding="utf-8"))
            entries = store.get("entries", {}) if isinstance(store, dict) else {}
            for entry in entries.values():
                if not isinstance(entry, dict) or not entry.get("settlement"):
                    continue
                name = str(entry["settlement"])
                ik = settlement_identity_key(name)
                by_ik[ik] = {
                    "settlement": name,
                    "status": str(entry.get("status") or ""),
                    "reason": str(
                        entry.get("reason")
                        or entry.get("validation_message")
                        or ""
                    ),
                    "match": entry.get("match") if isinstance(entry.get("match"), dict) else None,
                }
        except (OSError, ValueError):
            pass

    if os.path.exists(report_path):
        rep = pd.read_csv(report_path, encoding="utf-8-sig")
        for _, r in rep.iterrows():
            name = str(r.get("settlement") or "").strip()
            if not name:
                continue
            ik = settlement_identity_key(name)
            reason = r.get("reason")
            existing_match = by_ik.get(ik, {}).get("match")
            by_ik[ik] = {
                "settlement": name,
                "status": str(r.get("status") or ""),
                "reason": "" if pd.isna(reason) else str(reason),
                "match": existing_match,
            }

    return list(by_ik.values())


def _pbf_has_no_named_streets(match_obj: dict | None) -> bool:
    """Return True if the local national OSM PBF cache definitively has 0 named roads here."""
    if not match_obj:
        return False
    try:
        import osm_pbf_loader

        if not os.path.exists(osm_pbf_loader.DEFAULT_STREETS_PKL):
            return False
        gdf = osm_pbf_loader.fetch_pbf_street_data(match_obj)
        return (
            gdf is not None
            and isinstance(gdf, pd.DataFrame)
            and gdf.empty
            and bool(gdf.attrs.get("pbf_no_named_streets"))
        )
    except Exception:
        return False


def main(
    out: str = "no_results.json",
    *,
    html_dir: str = HTML_DIR,
    report_path: str = MATCH_REPORT_PATH,
    store_path: str = MATCH_STORE_PATH,
    lamas_csv_path: str = LAMAS_CSV_PATH,
    no_data_path: str = NO_DATA_PATH,
    retry_pending_path: str = RETRY_PENDING_PATH,
    check_pbf: bool = True,
) -> list[dict]:
    pages = {c["name"] for c in scan_settlements(html_dir)}
    page_iks = {settlement_identity_key(name) for name in pages}
    match_rows = _load_match_rows(report_path=report_path, store_path=store_path)
    if not match_rows:
        return []
    counts = _load_lamas_street_counts(lamas_csv_path=lamas_csv_path)

    nodata: set[str] = set()
    if os.path.exists(no_data_path):
        nodata = {
            line.strip()
            for line in open(no_data_path, encoding="utf-8")
            if line.strip() and not line.startswith("##")
        }

    retried: set[str] = set()
    if os.path.exists(retry_pending_path):
        retried = {
            line.strip()
            for line in open(retry_pending_path, encoding="utf-8")
            if line.strip()
        }

    items: list[dict] = []
    for r in match_rows:
        name = r["settlement"]
        ik = settlement_identity_key(name)
        if name in pages or ik in page_iks:
            continue
        if r["status"] != "matched":
            cat, detail = "unresolved", r.get("reason") or ""
        elif name in nodata or name in retried:
            has_cache = os.path.exists(osm_cache_path(name))
            cat = "no_named_streets" if (has_cache or name in retried or (check_pbf and _pbf_has_no_named_streets(r.get("match")))) else "no_osm_streets"
            detail = ""
        elif check_pbf and _pbf_has_no_named_streets(r.get("match")):
            cat, detail = "no_named_streets", ""
        else:
            cat, detail = "pending", ""
        items.append(
            {
                "name": name,
                "category": cat,
                "reason": REASONS[cat],
                "detail": re.sub(r"\s+", " ", detail)[:120],
                "lamas": int(counts.get(ik, 0)),
            }
        )

    items.sort(key=lambda i: (i["category"], -i["lamas"], i["name"]))
    with open(out, "w", encoding="utf-8") as handle:
        json.dump(items, handle, ensure_ascii=False, indent=1)
    print(len(items), pd.Series([i["category"] for i in items]).value_counts().to_dict())
    return items


if __name__ == "__main__":
    main()
