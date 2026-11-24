"""Render one settlement results page ("חדר השוואה").

The page compares the OSM match layer (Leaflet, local vendored copy) with a
GovMap reference panel and lists every finding in a sortable/filterable table.
All data is embedded as JSON in the page, so ``HTML/*.html`` work from any
static host; the shared UI lives in ``assets/room.{js,css}`` and
``assets/score-tools.js``.

Used both by the pipeline (``generate_html.create_html_from_gdf``) and by
``scripts/convert_pages_to_room.py``, which upgrades already generated pages
without re-running matching.
"""
from __future__ import annotations

import hashlib
import json
from html import escape
from pathlib import Path
from typing import Iterable, Optional, Sequence

from provenance import page_dates_html

ROOT = Path(__file__).resolve().parent
TEMPLATE = ROOT / "settlement_template.html"
ASSET_DIR = ROOT / "assets"
# Template token -> file under assets/. Pages live in HTML/, hence "../assets".
ASSETS = {
    "@@LEAFLET_CSS@@": "vendor/leaflet/leaflet.css",
    "@@ROOM_CSS@@": "room.css",
    "@@LEAFLET_JS@@": "vendor/leaflet/leaflet.js",
    "@@SCORE_JS@@": "score-tools.js",
    "@@ROOM_JS@@": "room.js",
}
# Geometry is stored as integer degrees * 1e5 (about one metre), with every
# point after the first delta-encoded. assets/room.js decodes it.
GEOMETRY_SCALE = 100_000
GEOMETRY_ENCODING = "delta-e5"
NOT_APPROVED = {"geo-review", "unmatched", "unnamed"}


def asset_url(relative: str) -> str:
    """URL of a shared asset with a content hash, so pages never mix versions."""
    digest = hashlib.sha256((ASSET_DIR / relative).read_bytes()).hexdigest()[:16]
    return f"../assets/{relative}?v={digest}"


def encode_line(points: Iterable[Sequence[float]]) -> list[int]:
    """``[(lat, lon), ...]`` -> flat delta-encoded integers."""
    flat: list[int] = []
    prev_lat = prev_lon = 0
    for lat, lon in points:
        ilat, ilon = round(lat * GEOMETRY_SCALE), round(lon * GEOMETRY_SCALE)
        flat += [ilat - prev_lat, ilon - prev_lon]
        prev_lat, prev_lon = ilat, ilon
    return flat


def decode_line(flat: Sequence[int]) -> list[tuple[float, float]]:
    """Inverse of :func:`encode_line` (used by tests and tools)."""
    points = []
    lat = lon = 0
    for i in range(0, len(flat) - 1, 2):
        lat += flat[i]
        lon += flat[i + 1]
        points.append((lat / GEOMETRY_SCALE, lon / GEOMETRY_SCALE))
    return points


def display_name(place: str) -> str:
    """Fix LAMAS-style mirrored parentheses, e.g. "גבעת חיים )איחוד(" ."""
    text = str(place).strip()
    if text.count(")") == text.count("(") and text.find(")") < text.find("(") and ")" in text:
        text = text.replace(")", "\0").replace("(", ")").replace("\0", "(")
    return text


def approved_official_count(records: Sequence[Sequence]) -> int:
    """Distinct official codes linked by approved (final-ID) records."""
    return len({str(r[3]) for r in records if r[3] and r[0] not in NOT_APPROVED})


def _json(value) -> str:
    # "<" escaping keeps "</script>" inside names from closing the block.
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")


def mazal_entities_html(records: Sequence[Sequence]) -> str:
    """Optional authority groups; existing nine-field pages render identically."""
    groups = {}
    for record in records:
        if len(record) < 11:
            continue
        mid = str(record[9] or "")
        if len(mid) != 18 or not mid.isascii() or not mid.isdigit():
            continue
        group = groups.setdefault(mid, {"name": str(record[10] or mid), "streets": set()})
        if record[1]:
            group["streets"].add(str(record[1]))
    if not groups:
        return ""
    items = []
    for mid, group in sorted(groups.items()):
        names = escape(" / ".join(sorted(group["streets"])))
        items.append(f'<li><a href="https://www.nli.org.il/he/authorities/{mid}">'
                     f'{escape(group["name"])}</a> · <bdi>{mid}</bdi> — {names}</li>')
    return ('<section class="official-missing-block" aria-label="ישויות מז״ל">'
            '<h2>ישויות משותפות במז״ל</h2>'
            '<p>מזהי הישויות מאחדים שמות, לא רחובות פיזיים או קודי למ״ס. '
            'זוהי העשרת שמות אוטומטית, לא אימות של כוונת ההנצחה העירונית.</p><ul>'
            + "".join(items) + '</ul></section>\n      ')


def render_page(
    *,
    place: str,
    meta: dict,
    records: list,
    geometry: list,
    missing_official: Optional[list] = None,
    missing_official_total: Optional[int] = None,
    geometry_note: str = "",
    provenance: Optional[dict] = None,
) -> str:
    """Return the full HTML of a settlement page.

    ``geometry[i]`` holds the lines of ``records[i]``; each line is a list of
    ``(lat, lon)`` pairs in WGS84. ``meta`` is the ``#page-meta`` payload, also
    read by ``generate_index.py``.
    """
    if len(geometry) != len(records):
        raise ValueError("geometry must have one entry per record")
    missing = [str(name) for name in (missing_official or [])]
    total_missing = missing_official_total if missing_official_total is not None else len(missing)
    room = {
        "geometryEncoding": GEOMETRY_ENCODING,
        "geometry": [[encode_line(line) for line in lines if len(line) >= 2] for lines in geometry],
        "geometryNote": geometry_note,
        "missingOfficial": missing,
        "missingOfficialListed": len(missing),
        "missingOfficialTotal": max(int(total_missing or 0), len(missing)),
        "approvedOfficialCount": approved_official_count(records),
    }
    html = TEMPLATE.read_text(encoding="utf-8")
    for token, relative in ASSETS.items():
        html = html.replace(token, asset_url(relative))
    return (
        html.replace("@@MAZAL_ENTITIES@@", mazal_entities_html(records))
        .replace("@@PAGE_DATES@@", page_dates_html(provenance))
        .replace("@@PLACE_NAME@@", escape(display_name(place)))
        .replace("@@PAGE_META@@", _json(meta))
        .replace("@@RECORDS@@", _json(records))
        .replace("@@ROOM_DATA@@", _json(room))
    )
