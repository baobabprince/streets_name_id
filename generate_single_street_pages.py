"""Simple info pages for settlements whose only LAMAS street is the settlement itself.

These are settlements where the CBS (למ״ס) table contains exactly one record and
that record is the settlement's own name — i.e. there are no real streets to
match against. They are reported separately in ``no_results.json`` (category
``single_street``) and, instead of a (misleading) empty street-matching page, we
publish a small, clearly-labelled info page showing the settlement location and
its single CBS record.

The page is intentionally NOT a street-matching map: there is nothing to match.
"""
import json
import os
from html import escape

import pandas as pd
from data_loader import load_or_fetch_LAMAS, safe_place_name
from settlement_names import settlement_identity_key

OUT_DIR = "HTML"
LAMAS_CSV = "cache/lamas_data.csv"
MATCH_STORE = "data/settlement_matches.json"


def _load_lamas() -> pd.DataFrame:
    if os.path.exists(LAMAS_CSV):
        try:
            return pd.read_csv(LAMAS_CSV, dtype=str)
        except (OSError, ValueError):
            pass
    return load_or_fetch_LAMAS()


def _single_street_cities(lamas: pd.DataFrame) -> dict:
    """Return ``{identity_key: city_name}`` for settlements whose ONLY LAMAS record
    is their own name (no real streets to match against)."""
    lamas = lamas[lamas["LAMAS_name"].fillna("").str.strip() != ""]
    out: dict = {}
    for city, grp in lamas.groupby("city"):
        names = sorted({str(n).strip() for n in grp["LAMAS_name"] if str(n).strip()})
        if len(names) == 1 and names[0] == str(city).strip():
            out[settlement_identity_key(city)] = str(city).strip()
    return out


def main():
    lamas = _load_lamas()
    if lamas is None or lamas.empty or "city" not in lamas.columns:
        print("No LAMAS data available; skipping single-street info pages.")
        return

    single = _single_street_cities(lamas)
    if not single:
        print("No single-street (settlement-name-only) settlements found in LAMAS.")
        return

    # Coordinates come from the settlement match store produced by the run.
    centers: dict = {}
    if os.path.exists(MATCH_STORE):
        try:
            store = json.load(open(MATCH_STORE, encoding="utf-8"))
        except (OSError, ValueError):
            store = {}
        for entry in store.get("entries", {}).values():
            if not isinstance(entry, dict) or not entry.get("settlement"):
                continue
            match = entry.get("match")
            if isinstance(match, dict) and match.get("lat") and match.get("lon"):
                centers[settlement_identity_key(entry["settlement"])] = (
                    float(match["lat"]),
                    float(match["lon"]),
                )

    # First LAMAS record per city, for the display fields on the page.
    rec_by_city = {
        str(city).strip(): grp.iloc[0]
        for city, grp in lamas.groupby("city")
    }

    made, skipped = 0, []
    for ik, city in single.items():
        center = centers.get(ik)
        if not center:
            skipped.append(city)
            continue
        path = os.path.join(OUT_DIR, f"{safe_place_name(city)}_roads.html")
        if os.path.exists(path):
            # A page already exists for this settlement — leave it untouched.
            continue
        rec = rec_by_city.get(city)
        street_name = str(rec["LAMAS_name"]) if rec is not None else city
        street_code = str(rec.get("street_code", "")) if rec is not None else ""
        city_code = str(rec.get("city_code", "")) if rec is not None else ""
        meta = {
            "place": city,
            "single_street": True,
            "counts": {},
            "diagnostics": {
                "total_osm_streets": 0,
                "total_lamas_streets": 1,
            },
        }
        page = _PAGE_TEMPLATE.format(
            name=escape(city),
            street_name=escape(street_name),
            street_code=escape(street_code),
            city_code=escape(city_code),
            meta_json=json.dumps(meta, ensure_ascii=False),
            lat=center[0],
            lon=center[1],
        )
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(page)
        made += 1
    print(
        f"created {made} single-street info pages; "
        f"skipped (no coordinates): {len(skipped)}",
        skipped[:10],
    )


_PAGE_TEMPLATE = """<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{name} — רחוב אחד בלבד (שם היישוב) | סקר התאמת רחובות</title>
<link rel="stylesheet" href="../assets/base.css">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>body{{font-family:Heebo,system-ui,sans-serif;margin:0;padding:1.5rem;max-width:900px;margin-inline:auto}}
#m{{height:420px;border-radius:12px;margin:1rem 0}}.card{{border:1px solid #8884;border-radius:12px;padding:1rem 1.25rem}}
.note{{background:#fff7e6;border:1px solid #e0b25233;border-radius:10px;padding:.8rem 1rem;margin:.5rem 0}}
dt{{opacity:.65}}dd{{margin:0 0 .6rem}}</style>
</head>
<body>
<p><a href="../index.html">← חזרה לאינדקס</a></p>
<h1>{name}</h1>
<div class="note"><b>אין רחובות אמיתיים להתאמה.</b> ברשימת הלמ״ס ליישוב זה רשום רחוב אחד בלבד,
ותוכנו הוא שם היישוב עצמו — לא רחוב נפרד. מכיוון שאין רשימת רחובות, לא ניתן ליצור עמוד התאמת
רחובות, ולכן מוצג דף מידע זה במקום.</div>
<div class="card"><dl>
<dt>רחוב בלמ״ס</dt><dd>{street_name}</dd>
<dt>קוד רחוב</dt><dd>{street_code}</dd>
<dt>קוד יישוב</dt><dd>{city_code}</dd>
</dl></div>
<div id="m"></div>
<script id="page-meta" type="application/json">{meta_json}</script>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>var m=L.map('m').setView([{lat},{lon}],14);
L.tileLayer('https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png',{{attribution:'© OpenStreetMap'}}).addTo(m);
L.marker([{lat},{lon}]).addTo(m);</script>
</body>
</html>
"""


if __name__ == "__main__":
    main()
