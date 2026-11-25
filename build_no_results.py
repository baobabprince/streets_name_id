"""Build no_results.json: settlements that have no street page in the index.

Categories:
  unresolved      - no usable OSM settlement match (reason from match report)
  no_osm_streets  - matched, but OSM has no street data at all (no cache / empty)
  no_named_streets- matched, OSM has street segments but none carry a name
  pending         - matched but not processed / no page generated yet
"""
import glob, json, os, re
import pandas as pd
from generate_index import scan_settlements, HTML_DIR
from settlement_names import settlement_identity_key

REASONS = {
    "unresolved": "לא נמצא יישוב מתאים ב-OSM",
    "no_osm_streets": "ביישוב אין נתוני רחובות ב-OSM",
    "no_named_streets": "ב-OSM יש כבישים אך ללא שמות",
    "pending": "טרם עובד",
}

def main(out="no_results.json"):
    pages = {c["name"] for c in scan_settlements(HTML_DIR)}
    rep = pd.read_csv("batch_reports/settlement_match_report.csv", encoding="utf-8-sig")
    lam = pd.read_csv("cache/lamas_data.csv", dtype=str)
    lam["k"] = lam["city"].map(lambda x: settlement_identity_key(x) if isinstance(x, str) else "")
    counts = lam[lam.LAMAS_name.fillna("").str.strip() != ""].groupby("k").LAMAS_name.nunique()

    nodata = set()
    path = "batch_reports/no_data_settlements.txt"
    if os.path.exists(path):
        nodata = {l.strip() for l in open(path, encoding="utf-8") if l.strip() and not l.startswith("##")}

    retried = set()
    if os.path.exists("batch_reports/retry_pending.txt"):
        retried = {l.strip() for l in open("batch_reports/retry_pending.txt", encoding="utf-8") if l.strip()}

    items = []
    for _, r in rep.iterrows():
        name = r["settlement"]
        if name in pages:
            continue
        if r["status"] != "matched":
            cat, detail = "unresolved", str(r.get("reason") or "")
        elif name in nodata or name in retried:
            # processed (or retried) and still no page: OSM has no named streets
            # inside the settlement area (nearest named street is outside it)
            cache = f"data/osm_data_{name}__Israel.pkl"
            cat, detail = ("no_named_streets" if (os.path.exists(cache) or name in retried) else "no_osm_streets"), ""
        else:
            cat, detail = "pending", ""
        items.append({"name": name, "category": cat, "reason": REASONS[cat],
                      "detail": re.sub(r"\s+", " ", detail)[:120],
                      "lamas": int(counts.get(settlement_identity_key(name), 0))})
    # A partial (retry) run overwrites the match report; keep earlier entries.
    if os.path.exists(out):
        have = {i["name"] for i in items}
        for old in json.load(open(out, encoding="utf-8")):
            if old["category"] in ("unresolved", "no_named_streets") and old["name"] not in have and old["name"] not in pages:
                items.append(old)
    items.sort(key=lambda i: (i["category"], -i["lamas"], i["name"]))
    json.dump(items, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(len(items), pd.Series([i["category"] for i in items]).value_counts().to_dict())

if __name__ == "__main__":
    main()
