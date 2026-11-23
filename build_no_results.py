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
from settlement_names import canonical_key

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
    lam["k"] = lam["city"].map(lambda x: canonical_key(x) if isinstance(x, str) else "")
    counts = lam[lam.LAMAS_name.fillna("").str.strip() != ""].groupby("k").LAMAS_name.nunique()

    nodata = set()
    path = "batch_reports/no_data_settlements.txt"
    if os.path.exists(path):
        nodata = {l.strip() for l in open(path, encoding="utf-8") if l.strip() and not l.startswith("##")}

    items = []
    for _, r in rep.iterrows():
        name = r["settlement"]
        if name in pages:
            continue
        if r["status"] != "matched":
            cat, detail = "unresolved", str(r.get("reason") or "")
        elif name in nodata:
            cache = f"data/osm_data_{name}__Israel.pkl"
            cat, detail = ("no_named_streets" if os.path.exists(cache) else "no_osm_streets"), ""
        else:
            cat, detail = "pending", ""
        items.append({"name": name, "category": cat, "reason": REASONS[cat],
                      "detail": re.sub(r"\s+", " ", detail)[:120],
                      "lamas": int(counts.get(canonical_key(name), 0))})
    items.sort(key=lambda i: (i["category"], -i["lamas"], i["name"]))
    json.dump(items, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(len(items), pd.Series([i["category"] for i in items]).value_counts().to_dict())

if __name__ == "__main__":
    main()
