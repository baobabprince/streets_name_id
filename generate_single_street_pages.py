"""Simple pages for settlements whose only LAMAS street is the settlement itself.

These settlements have no named streets in OSM to match, so instead of a street
map we publish a small page with the settlement location and its LAMAS record.
"""
import json, os
from html import escape
import pandas as pd
from data_loader import safe_place_name

OUT_DIR = "HTML"


def main():
    d = pd.read_csv("cache/lamas_data.csv", dtype=str)
    d = d[d.LAMAS_name.fillna("").str.strip() != ""]
    n = d.groupby("city").LAMAS_name.nunique()
    single = {c for c in n[n == 1].index if d[d.city == c].LAMAS_name.iloc[0].strip() == c.strip()}

    store = json.load(open("data/settlement_matches.json", encoding="utf-8"))["entries"]
    centers = {}
    for e in store.values():
        m = e.get("match")
        if m and m.get("lat") and m.get("lon"):
            centers[e["settlement"].strip()] = (float(m["lat"]), float(m["lon"]))

    no_results = json.load(open("no_results.json", encoding="utf-8"))
    todo = [i["name"] for i in no_results if i["name"].strip() in single]
    made, skipped = 0, []
    for name in todo:
        center = centers.get(name.strip())
        if not center:
            skipped.append(name)
            continue
        rec = d[d.city == name.strip()].iloc[0]
        meta = {"place": name, "single_street": True, "counts": {},
                "diagnostics": {"total_osm_streets": 0, "total_lamas_streets": 1}}
        page = f"""<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{escape(name)} — רחוב יחיד | סקר התאמת רחובות</title>
<link rel="stylesheet" href="../assets/base.css">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>body{{font-family:Heebo,system-ui,sans-serif;margin:0;padding:1.5rem;max-width:900px;margin-inline:auto}}
#m{{height:420px;border-radius:12px;margin:1rem 0}}.card{{border:1px solid #8884;border-radius:12px;padding:1rem 1.25rem}}
dt{{opacity:.65}}dd{{margin:0 0 .6rem}}</style>
</head>
<body>
<p><a href="../index.html">← חזרה לאינדקס</a></p>
<h1>{escape(name)}</h1>
<div class="card"><p>ברשימת הלמ״ס ליישוב זה רשום <b>רחוב אחד בלבד</b>, ושמו הוא שם היישוב עצמו.
ב-OpenStreetMap אין בתחומו רחובות עם שם להתאמה, ולכן אין מפת רחובות.</p>
<dl><dt>רחוב בלמ״ס</dt><dd>{escape(str(rec.LAMAS_name))}</dd>
<dt>קוד רחוב</dt><dd>{escape(str(rec.street_code))}</dd>
<dt>קוד יישוב</dt><dd>{escape(str(rec.city_code))}</dd></dl></div>
<div id="m"></div>
<script id="page-meta" type="application/json">{json.dumps(meta, ensure_ascii=False)}</script>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>var m=L.map('m').setView([{center[0]},{center[1]}],14);
L.tileLayer('https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png',{{attribution:'© OpenStreetMap'}}).addTo(m);
L.marker([{center[0]},{center[1]}]).addTo(m);</script>
</body>
</html>
"""
        path = os.path.join(OUT_DIR, f"{safe_place_name(name)}_roads.html")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(page)
        made += 1
    print(f"created {made} pages; skipped (no coordinates): {len(skipped)}", skipped[:10])


if __name__ == "__main__":
    main()
