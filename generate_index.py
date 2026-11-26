#!/usr/bin/env python3
"""
Build the national dashboard (index.html) from the generated settlement pages.

The dashboard is fully data-driven: it scans ``HTML/*.html`` (both the current
data-driven pages and older self-contained pages), extracts each settlement's
coverages from its embedded summary (or its diagnostics cards), and renders a
static page with an embedded JSON payload for instant client-side search,
filtering and sorting. No server is required — it works on GitHub Pages and
from a local checkout.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import glob
import json
import os
import re
from html import escape
from pathlib import Path
from urllib.parse import quote

from provenance import (
    build_time,
    display_dates,
    format_local,
    parse_iso,
    zone_label,
)

HTML_DIR = "HTML"
OUTPUT_FILE = "index.html"
DATA_FILE = "dashboard_data.json"

TIERS = {
    "excellent": "כיסוי 90% ומעלה",
    "good": "כיסוי 70–90%",
    "fair": "כיסוי 40–70%",
    "poor": "כיסוי מתחת ל־40%",
    "missing": "ללא מאגר להשוואה",
    "none": "אין נתונים",
}

_DIAG_PAIR = re.compile(
    r'<div class="label">(.*?)</div>\s*<div class="value[^"]*">(.*?)</div>', re.S
)
_PAGE_META = re.compile(
    r'<script id="page-meta" type="application/json">(.*?)</script>', re.S
)
_RECORDS = re.compile(r'<script id="street-records"[^>]*>(.*?)</script>', re.S)
ROOT = Path(__file__).resolve().parent
_H1 = re.compile(r"<h1[^>]*>(.*?)</h1>", re.S)
_STATUS = re.compile(r'data-status="([a-z-]+)"')


def _plain_label(text: str) -> str:
    return re.sub(r'[\s"״\'׳]', "", text)


def _to_int(value) -> int:
    try:
        return int(float(str(value).replace(",", "").strip()))
    except (TypeError, ValueError):
        return 0


def _pick(diag: dict[str, int], *needles: str) -> int | None:
    """Find a diagnostics value by (normalized) label fragments."""
    for label, value in diag.items():
        if all(needle in label for needle in needles):
            return value
    return None


def parse_page(path: str) -> dict | None:
    with open(path, encoding="utf-8") as handle:
        raw = handle.read()

    meta = {}
    meta_match = _PAGE_META.search(raw)
    if meta_match:
        try:
            meta = json.loads(meta_match.group(1))
        except (ValueError, TypeError):
            meta = {}

    diag: dict[str, int] = {}
    for label, value in _DIAG_PAIR.findall(raw):
        diag[_plain_label(label)] = _to_int(value)

    total_osm = _to_int(meta.get("diagnostics", {}).get("total_osm_streets"))
    total_lamas = _to_int(meta.get("diagnostics", {}).get("total_lamas_streets"))
    matched = _to_int(meta.get("diagnostics", {}).get("total_matched"))
    ai = _to_int(meta.get("diagnostics", {}).get("ai_proposed_matches"))
    geo = _to_int(meta.get("diagnostics", {}).get("geographic_review_matches"))
    unmatched = _to_int(meta.get("diagnostics", {}).get("unmatched_osm_streets"))

    if not total_osm:
        total_osm = _pick(diag, "סה", "OSM") or 0
    if not total_lamas:
        total_lamas = _pick(diag, "סה", "למ") or 0
    if not matched:
        matched = _pick(diag, "התאמות", "ודאיות") or _pick(diag, "סה", "התאמותמאושרות") or 0
    if not ai:
        ai = _pick(diag, "AI") or 0
    if not geo:
        geo = _pick(diag, "גאוגרפית") or 0
    if not unmatched:
        unmatched = _pick(diag, "OSM", "ללא") or _pick(diag, "OSM", "להתאמה") or 0

    status_counts = {"perfect": 0, "partial": 0, "ai": 0, "geo-review": 0, "unmatched": 0, "unnamed": 0}
    for status in _STATUS.findall(raw):
        if status in status_counts:
            status_counts[status] += 1
    if meta.get("counts"):
        for key, value in meta["counts"].items():
            if key in status_counts:
                status_counts[key] = int(value)

    # Diagnostic ai_proposed_matches historically also includes approved AI.
    # Only final IDs in record metadata establish approval; do not infer it
    # from the legacy field name or double-count AI on top of matched totals.
    ai_approved = ai_pending = 0
    records_match = _RECORDS.search(raw)
    try:
        records = json.loads(records_match.group(1)) if records_match else []
    except (ValueError, TypeError):
        records = []
    for record in records if isinstance(records, list) else []:
        if not isinstance(record, list) or len(record) < 4 or record[0] != "ai":
            continue
        final_id = str(record[3] or "").strip()
        if final_id and final_id.lower() not in {"none", "nan", "<na>"}:
            ai_approved += 1
        else:
            ai_pending += 1
    ai = max(ai, ai_approved + ai_pending)
    ai_unknown = max(0, ai - ai_approved - ai_pending)

    # --- display name ---
    name = str(meta.get("place") or "").strip()
    if not name:
        h1 = _H1.search(raw)
        if h1:
            name = re.sub(r"<[^>]+>", "", h1.group(1)).strip()
            name = re.sub(r"^\s*מפת\s+רחובות\s*[—–-]\s*", "", name).strip()
    if not name:
        name = Path(path).stem.replace("_roads", "").replace("_", " ").strip()
    name = re.sub(r"\s+", " ", name).strip(" ,")

    if total_osm == 0 and matched == 0:
        # Pages built before the diagnostics block: fall back to unique names.
        names = set(re.findall(r'data-name="([^"]+)"', raw))
        if names:
            total_osm = len(names)
            matched = len(
                set(re.findall(r'data-name="([^"]+)"[^>]*data-status="(?:perfect|partial)"', raw))
            )
            if not matched:
                total_osm = max(status_counts["perfect"] + status_counts["partial"] + status_counts["unmatched"], total_osm)

    matched = min(matched, total_osm) if total_osm else matched
    unmatched = unmatched if unmatched else max(total_osm - matched, 0)
    unmatched = min(unmatched, max(total_osm - matched, 0)) if total_osm else unmatched
    coverage = (matched / total_osm * 100.0) if total_osm else 0.0
    coverage = round(max(0.0, min(100.0, coverage)), 1)

    if not total_osm:
        tier = "none"
    elif not total_lamas:
        tier = "missing"
    elif coverage >= 90:
        tier = "excellent"
    elif coverage >= 70:
        tier = "good"
    elif coverage >= 40:
        tier = "fair"
    else:
        tier = "poor"

    record = {
        "name": name,
        "file": os.path.basename(path),
        "href": f"{HTML_DIR}/{quote(os.path.basename(path), safe='')}",
        "osm": int(total_osm),
        "lamas": int(total_lamas),
        "matched": int(matched),
        "unmatched": int(unmatched),
        "ai": int(ai),
        "ai_approved": ai_approved,
        "ai_pending": ai_pending,
        "ai_unknown": ai_unknown,
        "geo": int(geo),
        "coverage": coverage,
        "counts": status_counts,
        "tier": tier,
    }
    dates = _page_dates(meta)
    if dates:  # legacy pages have no recorded creation time; never guess one
        record["dates"] = dates
    return record


def scan_settlements(html_dir: str = HTML_DIR) -> list[dict]:
    cities: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for path in sorted(glob.glob(os.path.join(html_dir, "*.html"))):
        try:
            parsed = parse_page(path)
        except Exception as exc:  # pragma: no cover - defensive
            print(f"[warn] could not parse {path}: {exc}")
            continue
        if not parsed:
            continue
        dedupe = parsed["name"]
        if dedupe in seen:
            continue
        seen.add(dedupe)
        cities.append(parsed)
    return cities


def _last_batch_run() -> str | None:
    """Timestamp of the newest batch summary, exactly as it was recorded.

    Batch summaries store naive host-local times; they are shown as recorded
    without converting or claiming a time zone.
    """
    stamps = []
    for path in glob.glob("batch_reports/batch_summary_*.json"):
        try:
            with open(path, encoding="utf-8") as handle:
                stamp = json.load(handle).get("timestamp", "")
        except Exception:
            continue
        if parse_iso(stamp):
            stamps.append(stamp)
    return max(stamps, key=lambda value: parse_iso(value).replace(tzinfo=None), default=None)


def _page_dates(meta: dict) -> dict | None:
    return display_dates(meta.get("provenance"))


def _time_html(value: str | None, missing: str = "לא תועד") -> str:
    text = format_local(value)
    if not text:
        return escape(missing)
    zone = zone_label(value)
    return (f'<time datetime="{escape(value)}">{escape(text)}</time>'
            + (f"<small> {escape(zone)}</small>" if zone else ""))


def aggregate(cities: list[dict]) -> dict:
    totals = {
        "cities": len(cities),
        "osm": sum(c["osm"] for c in cities),
        "lamas": sum(c["lamas"] for c in cities),
        "matched": sum(c["matched"] for c in cities),
        "unmatched": sum(c["unmatched"] for c in cities),
        "ai": sum(c["ai"] for c in cities),
        "geo": sum(c["geo"] for c in cities),
        "segments": {k: sum(c["counts"].get(k, 0) for c in cities) for k in
                     ("perfect", "partial", "ai", "geo-review", "unmatched", "unnamed")},
    }
    totals["coverage"] = round(totals["matched"] / totals["osm"] * 100, 1) if totals["osm"] else 0.0
    # OSM names and official codes are different units; their ratio is not
    # official-list coverage. AI approvals are already part of matched.
    totals["lamas_coverage"] = None
    for field in ("ai_approved", "ai_pending", "ai_unknown"):
        totals[field] = sum(c.get(field, 0) for c in cities)
    totals["open"] = totals["unmatched"]
    tiers = {key: 0 for key in TIERS}
    for city in cities:
        tiers[city["tier"]] += 1
    totals["tiers"] = tiers
    buckets = [0] * 10
    for city in cities:
        if city["tier"] in ("missing", "none"):
            continue
        idx = min(9, int(city["coverage"] // 10))
        buckets[idx] += 1
    totals["buckets"] = buckets
    totals["comparable"] = sum(
        1 for city in cities if city["tier"] not in ("missing", "none")
    )
    totals["missing_data"] = tiers.get("missing", 0) + tiers.get("none", 0)
    dated = [c["dates"]["generated_at"] for c in cities if c.get("dates", {}).get("generated_at")]
    totals["dated_pages"] = len(dated)
    totals["newest_page_at"] = max(dated, key=lambda v: parse_iso(v), default=None)
    totals["oldest_page_at"] = min(dated, key=lambda v: parse_iso(v), default=None)
    return totals


def _fmt(number: int | float) -> str:
    return f"{int(number):,}"


def _ring(coverage: float) -> tuple[str, str, str]:
    radius = 64
    circumference = 2 * 3.141592653589793 * radius
    offset = circumference * (1 - coverage / 100.0)
    return f"{circumference:.2f}", f"{offset:.2f}", f"{coverage:.1f}"


def _donut_style(totals: dict) -> tuple[str, int]:
    parts = [
        ("non_ai_matched", "var(--perfect)"),
        ("ai_approved", "var(--ai)"),
        ("open", "var(--surface-3)"),
    ]
    totals = {**totals, "non_ai_matched": max(totals["matched"] - totals.get("ai_approved", 0), 0)}
    total = totals["osm"] or 1
    stops = []
    acc = 0.0
    for key, color in parts:
        value = totals[key]
        if not value:
            continue
        share = value / total * 100
        stops.append(f"{color} {acc:.2f}% {acc + share:.2f}%")
        acc += share
    if not stops:
        stops = ["var(--surface-3) 0 100%"]
    return "conic-gradient(" + ",".join(stops) + ")", round(totals["matched"] / total * 100)


def _attention(cities: list[dict], limit: int = 8) -> list[dict]:
    pool = [c for c in cities if c["osm"] >= 20 and c["lamas"] > 0]
    pool.sort(key=lambda c: (c["coverage"], -c["osm"]))
    return pool[:limit]


NO_RESULTS_FILE = "no_results.json"


def _no_results_section() -> str:
    """Collapsible list of settlements that have no street page at all.

    Items are split into two logically distinct buckets: settlements that DO have
    a CBS street list (real coverage gaps worth investigating) and settlements
    that have no CBS streets at all (so a street page can never exist). Within
    each bucket the existing per-reason categories are preserved as sub-groups.
    """
    try:
        with open(NO_RESULTS_FILE, encoding="utf-8") as handle:
            items = json.load(handle)
    except (OSError, ValueError):
        return ""
    if not items:
        return ""
    order = [
        "unresolved",
        "no_osm_streets",
        "no_named_streets",
        "no_lamas_streets",
        "single_street",
        "pending",
    ]
    # True  -> has a CBS street list but no page (actionable gaps)
    # False -> no CBS streets at all (a page can never be produced)
    groups_order = [True, False]
    group_labels = {
        True: "עם רשימת רחובות בלמ״ס — ללא עמוד",
        False: "ללא רשימת רחובות בלמ״ס",
    }

    def _in_bucket(item: dict, has: bool) -> bool:
        return bool(item.get("has_lamas_streets", item.get("lamas", 0) > 0)) == has

    blocks = []
    for has in groups_order:
        sub = [i for i in items if _in_bucket(i, has)]
        if not sub:
            continue
        sub_groups = {key: [i for i in sub if i["category"] == key] for key in order}
        inner = ""
        for key in order:
            rows = sub_groups[key]
            if not rows:
                continue
            lis = "".join(
                f'<li><span>{escape(i["name"])}</span>'
                f'<small>{_fmt(i["lamas"])} רחובות בלמ״ס</small></li>'
                for i in rows
            )
            inner += (
                f'<details class="nr-group"><summary>{escape(rows[0]["reason"])} '
                f'<b>({_fmt(len(rows))})</b></summary>'
                f'<ul class="nr-list">{lis}</ul></details>'
            )
        blocks.append(
            f'<div class="nr-bucket"><h3>{escape(group_labels[has])} '
            f'<b>({_fmt(len(sub))})</b></h3>{inner}</div>'
        )
    if not blocks:
        return ""
    return (
        '<section class="no-results" id="no-results">'
        '<div class="shell"><h2>יישובים ללא תוצאות</h2>'
        f'<p>{_fmt(len(items))} יישובים ללא עמוד מפה, מחולקים לשתי קבוצות: '
        'בקבוצה הראשונה יש רשימת רחובות בלמ״ס אך טרם נוצר עמוד; '
        'בשנייה אין בכלל רחובות בלמ״ס, ולכן לא ניתן ליצור עמוד התאמה. '
        'המספר לצד כל יישוב הוא הרחובות הרשומים בלמ״ס.</p>'
        + "".join(blocks) + "</div></section>"
        '<style>.no-results{margin:3rem 0}'
        '.no-results details{margin:.5rem 0;border:1px solid var(--line-strong,#ccc);'
        'border-radius:10px;padding:.6rem 1rem}'
        '.no-results summary{cursor:pointer;font-weight:600}'
        '.nr-bucket{margin:1.5rem 0}'
        '.nr-bucket h3{margin:0 0 .4rem;font-size:1.05rem}'
        '.nr-list{columns:3 220px;list-style:none;padding:0;margin:.8rem 0 0}'
        '.nr-list li{display:flex;justify-content:space-between;gap:.5rem;padding:.15rem 0;break-inside:avoid}'
        '.nr-list small{opacity:.65}</style>'
    )


def _asset_url(path: str) -> str:
    """Invalidate browser/CDN caches whenever a local asset changes."""
    digest = hashlib.sha256((ROOT / path).read_bytes()).hexdigest()[:16]
    return f"{path}?v={digest}"


def _method_defaults() -> dict:
    """Read documented numeric defaults without importing the GIS/model stack."""
    wanted = {"DEFAULT_CONFIDENT_THRESHOLD", "AI_MIN_CONFIDENCE", "AI_MIN_CORE_WRATIO",
              "AI_MAX_RAW_HITS_PER_ID", "AI_MAX_APPROVED_HITS_PER_ID"}
    result = {}
    for filename in ("normalization.py", "pipeline.py"):
        tree = ast.parse((ROOT / filename).read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id in wanted:
                        result[target.id] = node.value.value
    if set(result) != wanted:
        raise ValueError("Method defaults changed; update the homepage documentation")
    return result


def _example_match(cities: list[dict]) -> str:
    """Use a real, inspectable record, never a fabricated example or code."""
    city = next((c for c in cities if c["name"] == "פתח תקווה"), None)
    if city:
        source = ROOT / HTML_DIR / city["file"]
        if source.exists():
            match = _RECORDS.search(source.read_text(encoding="utf-8"))
            try:
                records = json.loads(match.group(1)) if match else []
            except (ValueError, TypeError):
                records = []
            if not isinstance(records, list):
                records = []
            record = next((r for r in records if isinstance(r, list) and len(r) >= 7 and r[0] == "ai" and r[3]), None)
            if record:
                return f'''<aside class="evidence-card" aria-label="דוגמת התאמה אמיתית מפתח תקווה">
                  <div class="evidence-top"><span class="source-dot"></span> מתוך הנתונים · {escape(city['name'])}<span class="ai-label">בסיוע AI</span></div>
                  <div class="evidence-names"><span>השם ב־OpenStreetMap</span><strong>{escape(str(record[1]))}</strong><span class="match-arrow" aria-hidden="true">↓</span><span>השם הרשמי בלמ״ס</span><strong>{escape(str(record[2]))}</strong></div>
                  <div class="evidence-values"><div><span>קוד רחוב ביישוב</span><b>{escape(str(record[3]))}</b></div><div><span>ציון התאמת שמות</span><b dir="ltr">{escape(str(record[4]))}<small> / 100</small></b></div><div><span>ביטחון שדיווח AI</span><b>{escape(str(record[6]))}</b></div></div>
                  <p>הקישור עבר את סינון התהליך, אך אינו אישור בשטח ואינו מוסיף את הקוד ל־OSM. ציון השמות וביטחון המודל הם מדדים שונים.</p>
                  <a class="example-link" href="{escape(HTML_DIR + '/' + city['file'], quote=True)}">לבדיקת הנתונים בעמוד היישוב <span>←</span><small>{escape(city['name'])} · מפה מול מפה וטבלה מלאה</small></a>
                </aside>'''
    return '<aside class="evidence-card"><h2>קישור שניתן לבדיקה</h2><p>שם ב־OSM, שם רשמי, קוד רחוב ושיטת ההתאמה — יחד, בהקשר של היישוב.</p><a href="#cities">לנתוני היישובים ←</a></aside>'


def _page_range_html(totals: dict) -> str:
    if not totals.get("dated_pages"):
        return "דפי היישובים הקיימים נוצרו לפני שתועד מועד היצירה; הוא יוצג בכל דף שייווצר מחדש."
    oldest = format_local(totals["oldest_page_at"], with_time=False)
    newest = format_local(totals["newest_page_at"], with_time=False)
    span = oldest if oldest == newest else f"{oldest}–{newest}"
    return f"מועדי היצירה המתועדים: {escape(span)}."


def build_dashboard(cities: list[dict], output: str = OUTPUT_FILE, data_out: str | None = DATA_FILE) -> str:
    totals = aggregate(cities)
    method = _method_defaults()
    generated_at = build_time()
    batch_run = _last_batch_run()
    payload = {"schema_version": 3, "generated_at": generated_at, "batch_run": batch_run,
               "totals": totals,
               "method_defaults": method, "cities": cities}
    payload_json = json.dumps(payload, ensure_ascii=False).replace("<", "\\u003c")
    if data_out:
        Path(data_out).write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    replacements = {
        "PAYLOAD": payload_json,
        "GENERATED_AT": _time_html(generated_at),
        "BATCH_RUN": _time_html(batch_run, "לא נמצא סיכום"),
        "DATED_PAGES": _fmt(totals["dated_pages"]),
        "PAGE_RANGE": _page_range_html(totals),
        "CITIES": _fmt(totals["cities"]), "OSM": _fmt(totals["osm"]),
        "MATCHED": _fmt(totals["matched"]), "UNMATCHED": _fmt(totals["unmatched"]),
        "AI_APPROVED": _fmt(totals["ai_approved"]),
        "AI_PENDING": _fmt(totals["ai_pending"]), "AI_UNKNOWN": _fmt(totals["ai_unknown"]),
        "COVERAGE": f"{totals['coverage']:.1f}", "EXAMPLE": _example_match(cities),
        "NO_RESULTS": _no_results_section(),
        "BASE_CSS": _asset_url("assets/base.css"),
        "DASH_CSS": _asset_url("assets/dashboard.css"),
        "HOME_CSS": _asset_url("assets/project-home.css"),
        "DASH_JS": _asset_url("assets/dashboard.js"),
    }
    replacements.update({key: str(value) for key, value in method.items()})
    template = (ROOT / "dashboard_template.html").read_text(encoding="utf-8")
    # One-pass substitution prevents names/data from becoming template tokens.
    html = re.sub(r"@@([A-Z_]+)@@", lambda m: replacements[m[1]], template)
    Path(output).write_text(html, encoding="utf-8")
    print(f"Generated {output}: {len(cities)} settlements, {totals['osm']} OSM names, {totals['coverage']}% linked")
    return html


def generate_index_html(html_dir: str = HTML_DIR, output: str = OUTPUT_FILE,
                        data_out: str | None = DATA_FILE) -> list[dict]:
    print("Scanning settlement pages…")
    cities = scan_settlements(html_dir)
    build_dashboard(cities, output=output, data_out=data_out)
    return cities


def main():
    parser = argparse.ArgumentParser(description="Generate the national dashboard")
    parser.add_argument("--html-dir", default=HTML_DIR)
    parser.add_argument("--out", default=OUTPUT_FILE)
    parser.add_argument("--data-out", default=DATA_FILE, help="write aggregated JSON ('' to skip)")
    args = parser.parse_args()
    generate_index_html(args.html_dir, args.out, args.data_out or None)


if __name__ == "__main__":
    main()
