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
import glob
import json
import os
import re
from datetime import datetime
from html import escape
from pathlib import Path
from urllib.parse import quote

HTML_DIR = "HTML"
OUTPUT_FILE = "index.html"
DATA_FILE = "dashboard_data.json"

TIERS = {
    "excellent": "מצוין",
    "good": "טוב",
    "fair": "בינוני",
    "poor": "דורש טיפול",
    "missing": "ללא מאגר להשוואה",
    "none": "אין נתונים",
}

_DIAG_PAIR = re.compile(
    r'<div class="label">(.*?)</div>\s*<div class="value[^"]*">(.*?)</div>', re.S
)
_PAGE_META = re.compile(
    r'<script id="page-meta" type="application/json">(.*?)</script>', re.S
)
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

    return {
        "name": name,
        "file": os.path.basename(path),
        "href": f"{HTML_DIR}/{quote(os.path.basename(path), safe='')}",
        "osm": int(total_osm),
        "lamas": int(total_lamas),
        "matched": int(matched),
        "unmatched": int(unmatched),
        "ai": int(ai),
        "geo": int(geo),
        "coverage": coverage,
        "counts": status_counts,
        "tier": tier,
    }


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


def _last_updated() -> str:
    stamps = []
    for path in glob.glob("batch_reports/batch_summary_*.json"):
        try:
            with open(path, encoding="utf-8") as handle:
                stamps.append(json.load(handle).get("timestamp", ""))
        except Exception:
            continue
    stamp = max([s for s in stamps if s], default="")
    try:
        dt = datetime.fromisoformat(stamp)
    except ValueError:
        return "עדכון אחרון: לא ידוע"
    months = ["ינואר", "פברואר", "מרץ", "אפריל", "מאי", "יוני", "יולי", "אוגוסט",
              "ספטמבר", "אוקטובר", "נובמבר", "דצמבר"]
    return f"עודכן ב-{dt.day} ב{months[dt.month - 1]} {dt.year}"


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
    totals["lamas_coverage"] = round(totals["matched"] / totals["lamas"] * 100, 1) if totals["lamas"] else 0.0
    open_streets = max(totals["osm"] - totals["matched"] - totals["ai"] - totals["geo"], 0)
    totals["open"] = open_streets
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
        ("matched", "var(--perfect)"),
        ("ai", "var(--ai)"),
        ("geo", "var(--geo)"),
        ("open", "var(--surface-3)"),
    ]
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


def build_dashboard(cities: list[dict], output: str = OUTPUT_FILE, data_out: str | None = DATA_FILE) -> str:
    totals = aggregate(cities)
    circumference, offset, coverage_text = _ring(totals["coverage"])
    donut, matched_pct = _donut_style(totals)
    updated = _last_updated()
    buckets = totals["buckets"]
    max_bucket = max(buckets) or 1
    tier_counts = totals["tiers"]

    dist_bars = []
    labels = ["0–10", "10–20", "20–30", "30–40", "40–50", "50–60", "60–70", "70–80", "80–90", "90–100"]
    for index, count in enumerate(buckets):
        height = max(2.0, count / max_bucket * 100)
        dist_bars.append(
            f'<button class="col" type="button" data-bucket="{index * 10}" '
            f'title="{labels[index]}%: {count} יישובים" aria-label="{labels[index]} אחוזי כיסוי, {count} יישובים">'
            f'<span class="bar" style="height:{height:.2f}%"><span class="n">{count}</span></span>'
            f"</button>"
        )

    attention_items = "".join(
        f'<a class="attention-item" href="{escape(city["href"])}">'
        f'<span><span class="name">{escape(city["name"])}</span>'
        f'<span class="meta">{_fmt(city["unmatched"])} ללא התאמה · {_fmt(city["osm"])} רחובות</span></span>'
        f'<span class="pct">{city["coverage"]:.0f}%</span>'
        f"</a>"
        for city in _attention(cities)
    )

    tier_options = "".join(
        f'<option value="{key}">{TIERS[key]}</option>' for key in ("excellent", "good", "fair", "poor", "missing")
    )

    payload = {
        "updated": updated,
        "totals": totals,
        "cities": [
            {
                "name": city["name"],
                "file": city["file"],
                "href": city["href"],
                "osm": city["osm"],
                "lamas": city["lamas"],
                "matched": city["matched"],
                "unmatched": city["unmatched"],
                "ai": city["ai"],
                "geo": city["geo"],
                "coverage": city["coverage"],
                "tier": city["tier"],
                "counts": city["counts"],
            }
            for city in cities
        ],
    }
    payload_json = json.dumps(payload, ensure_ascii=False).replace("<", "\\u003c")
    if data_out:
        with open(data_out, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, indent=1))

    html = f"""<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>סקר התאמת רחובות — OSM ↔ למ״ס | ישראל</title>
<meta name="description" content="לוח בקרה ארצי להשוואת שמות רחובות בין OpenStreetMap למאגר הרשמי (למ״ס) ב-{_fmt(totals['cities'])} יישובים.">
<meta property="og:title" content="סקר התאמת רחובות — OSM ↔ למ״ס">
<meta property="og:description" content="{_fmt(totals['osm'])} רחובות, {_fmt(totals['matched'])} התאמות מאושרות, {coverage_text}% כיסוי ארצי.">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Heebo:wght@400;500;600;700;800&family=Outfit:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<link rel="stylesheet" href="assets/base.css">
<link rel="stylesheet" href="assets/dashboard.css">
</head>
<body class="dashboard-page">
<a class="skip-link" href="#cities">דלג לחיפוש יישובים</a>

<div class="shell">
  <header class="masthead">
    <span class="brand-mark" aria-hidden="true">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <path d="M4 6l5-2 6 2 5-2v14l-5 2-6-2-5 2z"/><path d="M9 4v14M15 6v14"/>
      </svg>
    </span>
    <div class="brand-text">
      <strong>סקר התאמת רחובות</strong>
      <span>OpenStreetMap ↔ מאגר הרחובות הרשמי (למ״ס)</span>
    </div>
    <span class="spacer"></span>
    <span class="updated-pill"><span class="live-dot"></span>{escape(updated)}</span>
    <button class="icon-btn" id="theme-toggle" type="button" title="מצב בהיר / כהה" aria-label="החלפת ערכת צבעים">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/></svg>
    </button>
  </header>

  <section class="hero">
    <div class="hero-grid">
      <div>
        <span class="hero-badge">🇮🇱 מפת נתונים ארצית · {_fmt(totals['cities'])} יישובים</span>
        <h1>כל הרחובות.<br><span class="grad">תמונת אמת אחת.</span></h1>
        <p class="lede">
          כמה מהרחובות בישראל כבר מופו בין OpenStreetMap למאגר הרשמי? בחרו יישוב,
          ראו את הכיסוי, את הרחובות שדורשים טיפול ואת ההצעות שממתינות לבדיקה —
          הכל על גבי מפה אינטראקטיבית.
        </p>
        <div class="hero-actions">
          <a class="btn btn-primary" href="#cities">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="11" cy="11" r="7"/><path d="M20 20l-3.5-3.5"/></svg>
            חיפוש יישוב
          </a>
          <button class="btn btn-ghost" type="button" data-sort-preset="unmatched" data-dir="desc">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 9v4m0 4h.01"/><path d="M10.3 3.9L2.6 17a2 2 0 0 0 1.7 3h15.4a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z"/></svg>
            היישובים שדורשים טיפול
          </button>
        </div>
        <p class="hero-caption">מבוסס על <code>OpenStreetMap</code> ועל קובצי הרחובות של הלשכה המרכזית לסטטיסטיקה · התאמות AI מוצגות כהצעה בלבד</p>
      </div>

      <aside class="score-card" aria-label="ציון כיסוי ארצי">
        <div class="score-main">
          <div class="ring">
            <svg viewBox="0 0 154 154" aria-hidden="true">
              <defs>
                <linearGradient id="ringGradient" x1="0" y1="0" x2="1" y2="1">
                  <stop offset="0%" stop-color="#4f8cff"/>
                  <stop offset="55%" stop-color="#8b5cf6"/>
                  <stop offset="100%" stop-color="#35d0a1"/>
                </linearGradient>
              </defs>
              <circle class="track" cx="77" cy="77" r="64" fill="none" stroke-width="11"/>
              <circle class="value" cx="77" cy="77" r="64" fill="none" stroke-width="11"
                      stroke-dasharray="{circumference}" stroke-dashoffset="{offset}"
                      data-offset="{offset}"/>
            </svg>
            <div class="ring-label">
              <span class="big metric">{coverage_text}%</span>
              <span class="small">כיסוי התאמות מאושרות</span>
            </div>
          </div>
          <div class="score-side">
            <h2>התמונה הארצית</h2>
            <p>אחוז הרחובות ב-OSM שקיבלו מזהה רשמי מאושר</p>
            <div class="mini-row"><span>רחובות שנבדקו</span><b>{_fmt(totals['osm'])}</b></div>
            <div class="mini-row"><span>התאמות מאושרות</span><b>{_fmt(totals['matched'])}</b></div>
            <div class="mini-row"><span>ממתינים לבדיקה</span><b>{_fmt(totals['unmatched'])}</b></div>
          </div>
        </div>
        <div class="score-foot">
          <div><b>{_fmt(tier_counts['excellent'])}</b><span>יישובים מעל 90%</span></div>
          <div><b>{_fmt(tier_counts['poor'])}</b><span>מתחת ל-40%</span></div>
          <div><b>{_fmt(tier_counts['missing'])}</b><span>ללא מאגר להשוואה</span></div>
        </div>
      </aside>
    </div>
  </section>

  <section class="kpi-row" aria-label="מדדים מרכזיים">
    <article class="kpi">
      <div class="kpi-top"><span class="kpi-label">יישובים במדד</span>
        <span class="kpi-icon" aria-hidden="true"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M3 21h18M5 21V7l7-4 7 4v14M9 9h.01M9 13h.01M15 9h.01M15 13h.01"/></svg></span>
      </div>
      <div class="kpi-value metric">{_fmt(totals['cities'])}</div>
      <div class="kpi-foot"><span>{_fmt(totals['comparable'])} עם מאגר רשמי · {_fmt(totals['missing_data'])} ללא מאגר</span></div>
    </article>
    <article class="kpi">
      <div class="kpi-top"><span class="kpi-label">רחובות ב-OSM</span>
        <span class="kpi-icon" aria-hidden="true"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M4 21V6a2 2 0 0 1 2-2h4v17M14 21V3h4a2 2 0 0 1 2 2v16"/><path d="M8 8h.01M8 12h.01M8 16h.01"/></svg></span>
      </div>
      <div class="kpi-value metric">{_fmt(totals['osm'])}</div>
      <div class="kpi-foot"><span>רחובות בעלי שם שנבדקו</span></div>
    </article>
    <article class="kpi">
      <div class="kpi-top"><span class="kpi-label">התאמות מאושרות</span>
        <span class="kpi-icon" style="background:var(--perfect-soft);color:var(--perfect)" aria-hidden="true"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6L9 17l-5-5"/></svg></span>
      </div>
      <div class="kpi-value metric">{_fmt(totals['matched'])}</div>
      <div class="kpi-foot"><span class="up">{coverage_text}%</span> מכיסוי הרחובות</div>
    </article>
    <article class="kpi">
      <div class="kpi-top"><span class="kpi-label">ממתינים לטיפול</span>
        <span class="kpi-icon" style="background:var(--unmatched-soft);color:var(--unmatched)" aria-hidden="true"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 8v5m0 3h.01"/></svg></span>
      </div>
      <div class="kpi-value metric">{_fmt(totals['unmatched'])}</div>
      <div class="kpi-foot"><span class="warn">{_fmt(totals['ai'])} הצעות AI</span> ממתינות לבדיקה</div>
    </article>
  </section>

  <section class="analytics" aria-label="ניתוח ארצי">
    <article class="card">
      <div class="card-head">
        <div>
          <h2>פיזור הכיסוי בין היישובים</h2>
          <p>לחיצה על עמודה מסננת את רשימת היישובים באותו טווח</p>
        </div>
      </div>
      <div class="dist" id="dist-chart">
        {''.join(dist_bars)}
      </div>
      <div class="dist-axis" aria-hidden="true">
        {''.join(f'<span>{label}</span>' for label in labels)}
      </div>
    </article>

    <article class="card">
      <div class="card-head">
        <div>
          <h2>הרכב הרחובות</h2>
          <p>חלוקה של כל הרחובות שנבדקו בארץ</p>
        </div>
      </div>
      <div class="mix">
        <div class="mix-donut" style="background:{donut}" role="img" aria-label="פילוח ארצי: {matched_pct}% מאושרים">
          <div class="mix-center"><b class="metric">{matched_pct}%</b><span>מאושרים</span></div>
        </div>
        <div class="mix-legend">
          <div class="row"><i style="background:var(--perfect)"></i><span>התאמה מאושרת</span><b>{_fmt(totals['matched'])}</b></div>
          <div class="row"><i style="background:var(--ai)"></i><span>הצעת AI</span><b>{_fmt(totals['ai'])}</b></div>
          <div class="row"><i style="background:var(--geo)"></i><span>בדיקה גאוגרפית</span><b>{_fmt(totals['geo'])}</b></div>
          <div class="row"><i style="background:var(--surface-3);box-shadow:inset 0 0 0 1px var(--line-strong)"></i><span>ללא התאמה / ללא מענה</span><b>{_fmt(totals['open'])}</b></div>
        </div>
      </div>
    </article>

    <article class="card attention">
      <div class="card-head">
        <div>
          <h2>דרוש טיפול</h2>
          <p>היישובים עם הכיסוי הנמוך ביותר (מעל 20 רחובות) — כאן כדאי להתחיל</p>
        </div>
        <button class="btn btn-ghost" type="button" data-sort-preset="coverage" data-dir="asc">לכל הרשימה</button>
      </div>
      <div class="attention-grid">{attention_items}</div>
    </article>
  </section>

  <section class="explorer" id="cities">
    <div class="explorer-head">
      <div>
        <h2>כל היישובים</h2>
        <p>חיפוש, סינון לפי רמת כיסוי ופתיחה של מפת הרחובות המלאה</p>
      </div>
    </div>

    <div class="explorer-toolbar">
      <label class="field" for="city-search">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" aria-hidden="true"><circle cx="11" cy="11" r="7"/><path d="M20 20l-3.5-3.5"/></svg>
        <input id="city-search" type="search" placeholder="חיפוש יישוב… (לחצו /)" autocomplete="off" aria-label="חיפוש יישוב">
      </label>
      <div class="filters" role="group" aria-label="סינון לפי רמת כיסוי">
        <button class="chip" type="button" data-tier="all" data-tone="all" aria-pressed="true"><span class="dot"></span>הכל<span class="count">{_fmt(totals['cities'])}</span></button>
        <button class="chip" type="button" data-tier="excellent" data-tone="perfect" aria-pressed="false"><span class="dot"></span>מעל 90%<span class="count">{_fmt(tier_counts['excellent'])}</span></button>
        <button class="chip" type="button" data-tier="good" data-tone="ai" aria-pressed="false"><span class="dot"></span>70–90%<span class="count">{_fmt(tier_counts['good'])}</span></button>
        <button class="chip" type="button" data-tier="fair" data-tone="partial" aria-pressed="false"><span class="dot"></span>40–70%<span class="count">{_fmt(tier_counts['fair'])}</span></button>
        <button class="chip" type="button" data-tier="poor" data-tone="unmatched" aria-pressed="false"><span class="dot"></span>מתחת ל-40%<span class="count">{_fmt(tier_counts['poor'])}</span></button>
        <button class="chip" type="button" data-tier="missing" data-tone="unnamed" aria-pressed="false"><span class="dot"></span>ללא מאגר<span class="count">{_fmt(tier_counts['missing'])}</span></button>
      </div>
      <span class="spacer"></span>
      <select class="select" id="sort-select" aria-label="מיון היישובים">
        <option value="streets:desc">היקף הרחובות</option>
        <option value="coverage:desc">כיסוי גבוה → נמוך</option>
        <option value="coverage:asc">כיסוי נמוך → גבוה</option>
        <option value="unmatched:desc">הכי הרבה ללא התאמה</option>
        <option value="name:asc">שם היישוב</option>
      </select>
      <div class="view-toggle" role="group" aria-label="תצוגה">
        <button type="button" data-view="grid" aria-pressed="true" title="תצוגת כרטיסים" aria-label="תצוגת כרטיסים">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9"><rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/></svg>
        </button>
        <button type="button" data-view="table" aria-pressed="false" title="תצוגת טבלה" aria-label="תצוגת טבלה">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round"><path d="M4 6h16M4 12h16M4 18h16"/></svg>
        </button>
      </div>
    </div>

    <div class="results-line" id="results-line" hidden>
      <span><b id="results-count">0</b> יישובים תואמים</span>
      <button class="clear-filter" id="clear-filters" type="button">איפוס הסינון</button>
    </div>

    <div class="city-grid" id="city-grid" aria-live="polite"></div>

    <div class="city-table-wrap" id="city-table-wrap" hidden>
      <table class="city-table" id="city-table">
        <thead>
          <tr>
            <th>יישוב</th>
            <th>כיסוי רחובות</th>
            <th>רחובות OSM</th>
            <th>מאושרים</th>
            <th>ללא התאמה</th>
            <th>הצעות AI</th>
            <th>רמה</th>
            <th></th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>

    <div class="empty-state" id="explorer-empty" hidden>
      <strong>לא נמצאו יישובים</strong>
      נסו לשנות את החיפוש, לאפס את הסינון או לבחור טווח כיסוי אחר.
    </div>

    <div class="load-more-row" id="load-more-wrap" hidden>
      <button class="btn btn-ghost" id="load-more" type="button">טעינת יישובים נוספים</button>
    </div>
  </section>
</div>

<footer class="site-footer">
  <div class="shell foot-grid">
    <p>
      סקר התאמת הרחובות משווה שמות רחובות ב-OpenStreetMap למזהים רשמיים של הלשכה המרכזית לסטטיסטיקה (למ״ס).
      התאמות אוטומטיות מסומנות כהצעה בלבד עד לאישור; כל מזהה ניתן לבדיקה בממשק המפורט של היישוב.
    </p>
    <div class="foot-links">
      <a href="https://github.com/baobabprince/streets_name_id" target="_blank" rel="noopener">מאגר הקוד</a>
      <a href="README.md">תיעוד השיטה</a>
    </div>
  </div>
</footer>

<script id="dash-data" type="application/json">{payload_json}</script>
<script src="assets/dashboard.js" defer></script>
</body>
</html>
"""

    with open(output, "w", encoding="utf-8") as handle:
        handle.write(html)
    print(f"Generated {output}: {len(cities)} settlements, {totals['osm']} streets, "
          f"{totals['coverage']}% coverage")
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
