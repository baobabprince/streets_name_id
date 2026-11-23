#!/usr/bin/env python3
"""
Upgrade legacy settlement pages to the current data-driven template.

Older pages embed their own CSS/JS and full-precision SVG paths (up to ~14 MB
per city). This tool extracts the street data and diagnostics from such a page,
re-renders it with ``map_template.html`` + ``assets/``, rounds coordinates to
0.1 m and writes the result in place (opt-in) or into another directory.

Usage:
    python migrate_legacy_pages.py                      # dry run on a demo set
    python migrate_legacy_pages.py --write               # migrate demo set in place
    python migrate_legacy_pages.py --all --write         # migrate every legacy page
    python migrate_legacy_pages.py HTML/כפר_סבא_roads.html --write
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
import re
from html import escape
from pathlib import Path

from provenance import page_sources_html, page_stamp_html
from generate_html import (
    COLOR_AI,
    COLOR_GEO_REVIEW,
    COLOR_PARTIAL,
    COLOR_PERFECT,
    COLOR_UNNAMED,
    COLOR_UNMATCHED,
    _build_diagnostics_html,
)

ROOT = Path(__file__).resolve().parent
TEMPLATE = ROOT / "map_template.html"
CENTERS_PATH = ROOT / "config" / "settlement_centers.json"
_CENTERS = None


def govmap_center(place: str):
    """Return the settlement's ITM centre from the bundled lookup, if known."""
    global _CENTERS
    if _CENTERS is None:
        try:
            payload = json.loads(CENTERS_PATH.read_text(encoding="utf-8"))
            _CENTERS = payload.get("places") or {}
        except (OSError, ValueError):
            _CENTERS = {}
    return _CENTERS.get(place)

DEMO_PAGES = [
    "HTML/שדה_יעקב_roads.html",
    "HTML/כפר_סבא_roads.html",
    "HTML/הרצליה_roads.html",
    "HTML/רמת_גן_roads.html",
    "HTML/תל_אביב_-_יפו_roads.html",
    "HTML/ירושלים_roads.html",
    "HTML/אור_יהודה__Israel_roads.html",
    "HTML/אזור_roads.html",
]

ATTR_RE = re.compile(r'([a-zA-Z0-9:_-]+)="([^"]*)"')
H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.S)
VIEWBOX_RE = re.compile(r'viewBox="0 0 ([\d.]+) ([\d.]+)"')
DIAG_RE = re.compile(r'<div class="label">(.*?)</div>\s*<div class="value[^"]*">(.*?)</div>', re.S)
NUMBER_RE = re.compile(r"-?\d+\.\d+")

STATUS_STYLE = {
    "perfect": (COLOR_PERFECT, 4.6, "none"),
    "ai": (COLOR_AI, 4.6, "none"),
    "geo-review": (COLOR_GEO_REVIEW, 4.6, "8,4"),
    "unmatched": (COLOR_UNMATCHED, 4.0, "none"),
    "unnamed": (COLOR_UNNAMED, 2.2, "5,5"),
}


def clean(value: str) -> str:
    return "" if value is None else str(value).strip()


def attr_escape(value: str) -> str:
    return escape(clean(value), quote=True)


def parse_tooltip(raw: str) -> dict:
    normalized = raw.replace("&#10;", "\n").replace("#10;", "\n")
    lines = [line.strip() for line in normalized.split("\n") if line.strip()]
    out = {"name": "", "lamas": "", "id": "", "score": None, "note": "",
           "ai": "", "confidence": "", "geo": "", "distance": ""}
    if not lines:
        return out
    first = lines[0]
    if " -> " in first:
        name, lamas = first.split(" -> ", 1)
        out["name"], out["lamas"] = name.strip(), lamas.strip()
    else:
        out["name"] = first
    for line in lines[1:]:
        if line.startswith("ציון:"):
            try:
                out["score"] = float(line.split(":", 1)[1].strip())
            except ValueError:
                pass
        elif line.startswith("מזהה:"):
            out["id"] = line.split(":", 1)[1].strip()
        elif line.startswith("הצעת AI"):
            out["ai"] = line.split(":", 1)[1].strip()
        elif line.startswith("ביטחון המודל:"):
            out["confidence"] = line.split(":", 1)[1].strip()
        elif line.startswith("אימות גאוגרפי:"):
            out["geo"] = line.split(":", 1)[1].strip()
        elif line.startswith("מרחק מעוגן"):
            distance = re.sub(r"[^\d.]", "", line.split(":", 1)[1])
            out["distance"] = distance
        elif line in ("התאמה מאושרת", "נדרשת בדיקה גאוגרפית", "נדרשת בדיקה גאוגרפית לפני אישור"):
            continue
        else:
            out["note"] = line
    return out


def find_div_end(text: str, start: int) -> int:
    """Return the index just past the matching </div> for a div starting at `start`."""
    depth = 0
    pos = start
    for match in re.finditer(r"</?div\b", text[start:]):
        token = match.group(0)
        pos = start + match.end()
        if token == "<div":
            depth += 1
        else:
            depth -= 1
            if depth == 0:
                # include the rest of the closing tag
                close = text.find(">", pos)
                return close + 1 if close != -1 else pos
    return len(text)


def parse_diagnostics(raw: str) -> tuple[dict, str]:
    values = {}
    for label, value in DIAG_RE.findall(raw):
        key = re.sub(r'[\s"״\'׳]', "", label)
        numbers = re.sub(r"[^\d.]", "", value)
        try:
            values[key] = int(float(numbers)) if numbers else 0
        except ValueError:
            values[key] = 0
    diag = {
        "total_osm_streets": _pick(values, "סה", "OSM"),
        "total_lamas_streets": _pick(values, "סה", "למ"),
        "confident_matches": _pick(values, "ודאיות"),
        "ai_proposed_matches": _pick(values, "AI"),
        "total_matched": _pick(values, "סה", "התאמות"),
        "unmatched_osm_streets": _pick(values, "OSM", "ללא") or _pick(values, "OSM", "להתאמה"),
        "geographic_review_matches": _pick(values, "גאוגרפית") or 0,
        "unmatched_lamas_count": _pick(values, "למ", "ללא") or _pick(values, "שלא"),
    }
    block = ""
    start = raw.find('<div class="diagnostics">')
    if start != -1:
        end = find_div_end(raw, start)
        block = raw[start:end]
        # The legacy block hard-codes z=16 (an out-of-range GovMap level).
        block = block.replace("&amp;z=16", "&amp;z=12").replace("&z=16", "&z=12")
    else:
        # Rebuild from the parsed numbers (no street lists available).
        block = _build_diagnostics_html(
            {
                **diag,
                "unmatched_lamas_percentage": "—",
                "unmatched_lamas_street_names": [],
                "unmatched_osm_street_names": [],
            },
            "",
        )
    return diag, block


def _pick(values: dict, *needles: str) -> int:
    for label, value in values.items():
        if all(needle in label for needle in needles):
            return value
    return 0


def round_path(d: str) -> str:
    return NUMBER_RE.sub(lambda m: f"{float(m.group()):.1f}", d)


def iter_path_tags(raw: str):
    """Yield complete <path …> tags, ignoring '>' inside quoted attribute values."""
    index = 0
    while True:
        start = raw.find("<path", index)
        if start == -1:
            return
        if raw[start + 5:start + 6] not in (" ", "\n", "\t", ">"):
            index = start + 5
            continue
        i = start + 5
        quote = None
        while i < len(raw):
            ch = raw[i]
            if quote:
                if ch == quote:
                    quote = None
            elif ch in ("'", '"'):
                quote = ch
            elif ch == ">":
                break
            i += 1
        yield raw[start:i + 1]
        index = i + 1


def convert_page(page_path: Path) -> tuple[str, dict]:
    raw = page_path.read_text(encoding="utf-8")
    if 'id="page-meta"' in raw and "assets/map.js" in raw:
        raise ValueError("already migrated")

    title = H1_RE.search(raw)
    name = re.sub(r"<[^>]+>", "", title.group(1)).strip() if title else page_path.stem
    name = re.sub(r"^\s*מפת\s+רחובות\s*[—–-]\s*", "", name).strip()
    name = re.sub(r",\s*Israel\s*$", "", name, flags=re.I).strip()

    viewbox = VIEWBOX_RE.search(raw)
    width, height = (viewbox.group(1), viewbox.group(2)) if viewbox else ("1000", "1000")

    counts = {"perfect": 0, "partial": 0, "ai": 0, "geo-review": 0, "unmatched": 0, "unnamed": 0}
    paths_out = []
    records = []
    record_index = {}

    for tag in iter_path_tags(raw):
        attrs = dict(ATTR_RE.findall(tag))
        d = attrs.get("d", "")
        if not d:
            continue
        tooltip_raw = attrs.get("data-tooltip", "")
        info = parse_tooltip(tooltip_raw)
        status = attrs.get("data-status", "unmatched")
        if status not in counts:
            status = "unmatched"
        osm_name = clean(attrs.get("data-osm") or info["name"])
        lamas = clean(attrs.get("data-lamas") or info["lamas"])
        lamas_id = clean(attrs.get("data-lamas-id") or info["id"])
        score = info["score"]
        try:
            if attrs.get("data-score"):
                score = float(attrs["data-score"])
        except ValueError:
            pass
        ai = clean(attrs.get("data-ai") or info.get("ai"))
        confidence = clean(attrs.get("data-confidence") or info.get("confidence"))
        geo = clean(attrs.get("data-geo") or info.get("geo"))
        distance = clean(attrs.get("data-distance") or info.get("distance"))
        if status == "ai":
            # legacy AI tooltips used the "מזהה" line for the *proposed* id
            if not ai:
                ai = lamas_id
            lamas_id = ""
            lamas = ""
        if status == "unnamed" or osm_name in ("רחוב ללא שם", "ללא שם"):
            osm_name = ""
            lamas = ""
            lamas_id = ""

        if status == "unnamed":
            color, stroke_width, dash = STATUS_STYLE["unnamed"]
        elif status == "perfect":
            color, stroke_width, dash = STATUS_STYLE["perfect"]
        elif status == "partial":
            color = COLOR_PARTIAL
            stroke_width, dash = 4.6, "none"
        else:
            color, stroke_width, dash = STATUS_STYLE[status]

        key_raw = "\u0001".join([osm_name, lamas, lamas_id, status, ai])
        key = hashlib.sha1(key_raw.encode("utf-8")).hexdigest()[:12]
        rec_id = record_index.get(key)
        if rec_id is None:
            rec_id = len(records)
            record_index[key] = rec_id
            records.append([
                status, osm_name, lamas, lamas_id,
                "" if score is None else round(score, 1),
                ai, confidence, geo, distance,
            ])
            counts[status] += 1

        safe_tooltip = re.sub(r'[<>&"]', "", tooltip_raw)
        tooltip_attr = (
            f'data-tooltip="{safe_tooltip}" '
            if status in ("ai", "geo-review")
            else ""
        )
        paths_out.append(
            f'<path d="{round_path(d)}" stroke="{color}" stroke-width="{stroke_width}" '
            f'stroke-dasharray="{dash}" vector-effect="non-scaling-stroke" '
            f'stroke-linecap="round" stroke-linejoin="round" fill="none" '
            f'class="street-path" data-status="{status}" data-rec="{rec_id}" '
            f'{tooltip_attr}>'
            f"</path>"
        )

    diag, diag_block = parse_diagnostics(raw)
    total_osm = diag.get("total_osm_streets") or 0
    total_matched = diag.get("total_matched") or 0
    match_pct = round(100.0 * total_matched / total_osm, 1) if total_osm else 0.0

    coverage_html = (
        f'<div class="coverage"><div class="coverage-bar" style="width:{match_pct}%"></div></div>'
        f'<div class="coverage-label">כיסוי התאמות מאושרות: {match_pct}% '
        f'({total_matched}/{total_osm})</div>'
    )
    meta = {
        "place": name,
        "match_pct": match_pct,
        "total_segments": len(paths_out),
        "counts": counts,
        "diagnostics": diag,
    }
    center = govmap_center(name)
    if center:
        meta["govmap"] = {"x": int(center[0]), "y": int(center[1])}
    # Keep provenance recorded by the pipeline; never invent a creation time.
    old_provenance = _existing_provenance(raw)
    if old_provenance:
        meta["provenance"] = old_provenance
    meta_json = json.dumps(meta, ensure_ascii=False).replace("<", "\\u003c")
    records_json = json.dumps(records, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")

    template = TEMPLATE.read_text(encoding="utf-8")
    html = (
        template
        .replace("@@PAGE_STAMP@@", page_stamp_html(old_provenance))
        .replace("@@PAGE_SOURCES@@", page_sources_html(old_provenance))
        .replace("@@PLACE_NAME@@", escape(name))
        .replace("@@WIDTH@@", width)
        .replace("@@HEIGHT@@", height)
        .replace("@@SVG_PATHS@@", "\n".join(paths_out))
        .replace("@@DIAGNOSTICS@@", diag_block)
        .replace("@@PAGE_META@@", meta_json)
        .replace("@@RECORDS@@", records_json)
        .replace("@@MATCH_PCT@@", str(match_pct))
        .replace("@@COVERAGE@@", coverage_html)
        .replace("@@TOTAL_MATCHED@@", str(total_matched))
        .replace("@@TOTAL_OSM@@", str(total_osm))
    )
    return html, {"name": name, "segments": len(paths_out), "coverage": match_pct,
                  "counts": counts}


def _existing_provenance(raw: str) -> dict | None:
    match = re.search(r'<script id="page-meta" type="application/json">(.*?)</script>', raw, re.S)
    if not match:
        return None
    try:
        value = json.loads(match.group(1)).get("provenance")
    except (ValueError, AttributeError):
        return None
    return value if isinstance(value, dict) and value else None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pages", nargs="*", help="page paths (default: demo set)")
    parser.add_argument("--all", action="store_true", help="migrate every legacy page under HTML/")
    parser.add_argument("--write", action="store_true", help="write output (default: dry run)")
    parser.add_argument("--out-dir", help="write converted pages here instead of in place")
    args = parser.parse_args()

    if args.all:
        targets = sorted(glob.glob(str(ROOT / "HTML" / "*.html")))
    elif args.pages:
        targets = args.pages
    else:
        targets = [str(ROOT / p) for p in DEMO_PAGES]

    converted = skipped = 0
    for target in targets:
        path = Path(target)
        if not path.exists():
            print(f"[skip] {path} (missing)")
            skipped += 1
            continue
        try:
            html, info = convert_page(path)
        except ValueError as exc:
            print(f"[skip] {path.name}: {exc}")
            skipped += 1
            continue
        before = path.stat().st_size
        after = len(html.encode("utf-8"))
        out_path = path
        if args.out_dir:
            out_path = Path(args.out_dir) / path.name
            out_path.parent.mkdir(parents=True, exist_ok=True)
        if args.write or args.out_dir:
            out_path.write_text(html, encoding="utf-8")
        print(
            f"{'[write]' if (args.write or args.out_dir) else '[dry] '} {path.name}: "
            f"{before / 1e6:.2f} MB → {after / 1e6:.2f} MB "
            f"({100 - after / before * 100:.0f}% smaller), {info['segments']} paths, "
            f"{info['coverage']}% coverage"
        )
        converted += 1

    print(f"\n{converted} converted, {skipped} skipped."
          + ("" if (args.write or args.out_dir) else " Run with --write to apply."))


if __name__ == "__main__":
    main()
