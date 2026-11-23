"""Build the Petah Tikva comparison-room fixture from the existing generated page.

No network calls or matching rerun. Requires pyproj (a geopandas dependency).
SVG coordinates are recovered using the page's rounded ITM extent centre;
therefore the displayed overlay is approximate to about one metre, not a new
survey-grade geometry source. Run from any directory with:
    python scripts/build_comparison_prototype.py
"""
import json
from html.parser import HTMLParser
from pathlib import Path

from pyproj import Transformer

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "HTML/פתח_תקווה_roads.html"
OUTPUT = ROOT / "prototype/data.json"


class PageData(HTMLParser):
    def __init__(self):
        super().__init__()
        self.scripts = {}
        self.paths = []
        self.extent = None
        self.active_script = None
        self.in_missing_list = False
        self.in_heading = False
        self.heading = ""
        self.in_link = False
        self.link = ""
        self.missing = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "script" and attrs.get("id") in {"page-meta", "street-records"}:
            self.active_script = attrs["id"]
            self.scripts[self.active_script] = ""
        if tag == "svg" and attrs.get("id") == "street-map":
            self.extent = list(map(float, attrs["viewbox"].split()))
        if tag == "path" and "street-path" in attrs.get("class", "").split():
            self.paths.append((int(attrs["data-rec"]), attrs["d"]))
        if tag == "div" and attrs.get("class") == "unmatched-list":
            self.in_missing_list = True
            self.heading = ""
        if self.in_missing_list and tag == "h3":
            self.in_heading = True
        if self.in_missing_list and tag == "a":
            self.in_link = True
            self.link = ""

    def handle_data(self, data):
        if self.active_script:
            self.scripts[self.active_script] += data
        if self.in_heading:
            self.heading += data
        if self.in_link:
            self.link += data

    def handle_endtag(self, tag):
        if tag == "script":
            self.active_script = None
        if tag == "h3":
            self.in_heading = False
        if tag == "a" and self.in_link:
            if self.heading.startswith("רחובות למ״ס ללא התאמה"):
                self.missing.append(self.link)
            self.in_link = False
        if tag == "div":
            self.in_missing_list = False


def build(source=SOURCE):
    page = PageData()
    page.feed(source.read_text(encoding="utf-8"))
    meta = json.loads(page.scripts["page-meta"])
    records = json.loads(page.scripts["street-records"])
    _, _, width, height = page.extent
    cx, cy = Transformer.from_crs(2039, 3857, always_xy=True).transform(
        meta["govmap"]["x"], meta["govmap"]["y"]
    )
    to_wgs = Transformer.from_crs(3857, 4326, always_xy=True)
    geometries = [[] for _ in records]
    for rec_id, path in page.paths:
        points = []
        for pair in path.removeprefix("M ").split(" L "):
            x, y = map(float, pair.split(","))
            lon, lat = to_wgs.transform(cx - width / 2 + x, cy + height / 2 - y)
            points.append([round(lat, 7), round(lon, 7)])
        geometries[rec_id].append(points)
    approved_ids = {r[3] for r in records if r[3] and r[0] not in {"geo-review", "unmatched", "unnamed"}}
    if len(approved_ids) + len(page.missing) != meta["diagnostics"]["total_lamas_streets"]:
        raise ValueError("Official street totals do not reconcile; inspect the source before publishing")
    return {
        "meta": meta,
        "source": "../HTML/פתח_תקווה_roads.html",
        "sourceDates": None,
        "geometryNote": "שחזור מקואורדינטות העמוד הקיים ומרכז מעוגל; דיוק מיקום בקירוב של מטר.",
        "records": records,
        "geometry": geometries,
        "missingOfficial": page.missing,
        "approvedOfficialCount": len(approved_ids),
    }


if __name__ == "__main__":
    data = build()
    OUTPUT.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    print(f"Wrote {OUTPUT.relative_to(ROOT)}: {len(data['records'])} OSM records, "
          f"{len(data['missingOfficial'])} unmatched official names")
