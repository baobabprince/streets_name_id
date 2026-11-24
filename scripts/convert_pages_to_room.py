"""Re-render existing settlement pages in the comparison-room layout.

No matching is re-run and nothing is downloaded: the results already stored in
each ``HTML/*_roads.html`` page (``#page-meta``, ``#street-records``, street
geometry and the unmatched-LAMAS list) are read back and rendered with
``settlement_template.html``. Use it after changing the template or the shared
room assets, or to upgrade pages produced by the earlier SVG generator.

Supported inputs:
* earlier SVG pages: geometry is recovered from the SVG coordinates and the
  page's rounded ITM centre, so positions are approximate (about one metre);
* comparison-room pages: re-rendered from their embedded data (idempotent).

Single-street pages (``generate_single_street_pages.py``) are left untouched.

    python scripts/convert_pages_to_room.py            # all pages
    python scripts/convert_pages_to_room.py "פתח תקווה" # selected settlements
"""
from __future__ import annotations

import argparse
import json
import sys
from html.parser import HTMLParser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from settlement_page import GEOMETRY_ENCODING, decode_line, render_page  # noqa: E402

HTML_DIR = ROOT / "HTML"
SVG_NOTE = "המיקום שוחזר מקואורדינטות העמוד הקודם; דיוק בקירוב של מטר."
MISSING_HEADING = "רחובות למ״ס ללא התאמה"


class PageData(HTMLParser):
    """Collect embedded JSON blocks, SVG paths and the unmatched-LAMAS list."""

    SCRIPTS = {"page-meta", "street-records", "room-data"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.scripts: dict[str, str] = {}
        self.paths: list[tuple[int, str]] = []
        self.extent = None
        self.missing: list[str] = []
        self._script = None
        self._in_list = False
        self._heading = ""
        self._in_heading = False
        self._link = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "script" and attrs.get("id") in self.SCRIPTS:
            self._script = attrs["id"]
            self.scripts[self._script] = ""
        elif tag == "svg" and attrs.get("id") == "street-map" and attrs.get("viewbox"):
            self.extent = [float(v) for v in attrs["viewbox"].split()]
        elif tag == "path" and "street-path" in (attrs.get("class") or "").split():
            if attrs.get("data-rec") is not None and attrs.get("d"):
                self.paths.append((int(attrs["data-rec"]), attrs["d"]))
        elif tag == "div" and attrs.get("class") == "unmatched-list":
            self._in_list, self._heading = True, ""
        elif self._in_list and tag == "h3":
            self._in_heading = True
        elif self._in_list and tag == "a":
            self._link = ""

    def handle_data(self, data):
        if self._script:
            self.scripts[self._script] += data
        elif self._link is not None:
            self._link += data
        elif self._in_heading:
            self._heading += data

    def handle_endtag(self, tag):
        if tag == "script":
            self._script = None
        elif tag == "h3":
            self._in_heading = False
        elif tag == "a" and self._link is not None:
            if self._heading.strip().startswith(MISSING_HEADING):
                self.missing.append(self._link.strip())
            self._link = None
        elif tag == "div" and self._in_list and self._link is None:
            self._in_list = False


def _svg_geometry(page: PageData, meta: dict, size: int) -> list:
    from pyproj import Transformer

    geometry = [[] for _ in range(size)]
    if not page.paths or not page.extent or not meta.get("govmap"):
        return geometry
    _, _, width, height = page.extent
    cx, cy = Transformer.from_crs(2039, 3857, always_xy=True).transform(
        meta["govmap"]["x"], meta["govmap"]["y"]
    )
    left, top = cx - width / 2, cy + height / 2
    to_wgs = Transformer.from_crs(3857, 4326, always_xy=True)
    for rec_id, path in page.paths:
        xs, ys = [], []
        for pair in path.removeprefix("M ").split(" L "):
            x, y = pair.split(",")
            xs.append(left + float(x))
            ys.append(top - float(y))
        lons, lats = to_wgs.transform(xs, ys)
        if 0 <= rec_id < size:
            geometry[rec_id].append(list(zip(lats, lons)))
    return geometry


def convert(path: Path) -> str | None:
    """Return the new HTML for ``path``, or None if it is not a results page."""
    page = PageData()
    page.feed(path.read_text(encoding="utf-8"))
    if "page-meta" not in page.scripts or "street-records" not in page.scripts:
        return None
    meta = json.loads(page.scripts["page-meta"])
    if meta.get("single_street"):
        return None
    records = json.loads(page.scripts["street-records"])
    diagnostics = meta.get("diagnostics") or {}

    if "room-data" in page.scripts:
        room = json.loads(page.scripts["room-data"])
        geometry = room.get("geometry") or [[] for _ in records]
        if room.get("geometryEncoding") == GEOMETRY_ENCODING:
            geometry = [[decode_line(line) for line in lines] for lines in geometry]
        missing = room.get("missingOfficial") or []
        missing_total = room.get("missingOfficialTotal")
        note = room.get("geometryNote", "")
    else:
        geometry = _svg_geometry(page, meta, len(records))
        missing = list(page.missing)
        missing_total = diagnostics.get("unmatched_lamas_count")
        note = SVG_NOTE if any(geometry) else ""

    return render_page(
        place=meta.get("place") or path.stem.removesuffix("_roads").replace("_", " "),
        meta=meta,
        records=records,
        geometry=geometry,
        missing_official=missing,
        missing_official_total=missing_total,
        geometry_note=note,
        # Keep the recorded creation time: the results are unchanged.
        provenance=meta.get("provenance"),
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("names", nargs="*", help="settlement names (default: every page)")
    parser.add_argument("--html-dir", type=Path, default=HTML_DIR)
    args = parser.parse_args(argv)

    if args.names:
        from data_loader import safe_place_name

        paths = [args.html_dir / f"{safe_place_name(name)}_roads.html" for name in args.names]
    else:
        paths = sorted(args.html_dir.glob("*.html"))
    upgraded = refreshed = unchanged = skipped = 0
    for path in paths:
        if not path.exists():
            print(f"missing: {path}", file=sys.stderr)
            skipped += 1
            continue
        before = path.read_text(encoding="utf-8")
        html = convert(path)
        if html is None:
            skipped += 1
            continue
        if html == before:
            unchanged += 1
            continue
        path.write_text(html, encoding="utf-8")
        if 'id="room-data"' in before:
            refreshed += 1
        else:
            upgraded += 1
    print(f"Pages in {args.html_dir.resolve()}:")
    print(f"  upgraded from the earlier layout: {upgraded}")
    print(f"  already comparison-room, re-rendered with changes: {refreshed}")
    print(f"  already comparison-room, up to date (not rewritten): {unchanged}")
    print(f"  skipped (single-street / not a results page / missing): {skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
