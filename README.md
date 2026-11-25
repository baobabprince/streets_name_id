# Street Name Synonym Mapper

Pipeline that maps OpenStreetMap (OSM) street names to official **LAMAS** (Central Bureau of Statistics / CBS) street IDs for Israeli settlements. It combines Hebrew-aware normalization, 90% fuzzy matching, and optional whole-city Laya AI resolution for everything below 90%.

**Live dashboard:** [baobabprince.github.io/streets_name_id](https://baobabprince.github.io/streets_name_id/)

## Project homepage

The Hebrew index presents the output as **inspectable proposals for linking OSM
streets to official CBS street codes**, not as verified edits already present in
OSM. It explains the matching stages, AI guardrails, identifier scope and review
considerations, alongside the searchable settlement data.

- Source template: `dashboard_template.html`; presentation: `assets/project-home.css`.
- Rebuild after editing the template, explorer JS/CSS or settlement data:
  `python generate_index.py --data-out ''`. Local asset URLs are content-hashed
  to avoid mixed HTML/JavaScript versions in browsers and on static hosts.
- Preview the homepage and the settlement pages together:
  `python scripts/serve_project.py --port 8000` (open `/`; settlement pages are
  under `/HTML/`). The preview binds to `0.0.0.0` and disables response caching.
- Only AI records with an explicit final street ID count as AI links that passed
  filtering. Historical aggregate AI counts without per-record approval evidence
  are disclosed separately; approved AI is not added a second time to matched
  totals. Matching coverage is not a measured accuracy or OSM adoption rate.
- Numerical algorithm defaults displayed on the homepage are read from the
  current Python source without importing GIS packages or loading the model.
  The page explicitly distinguishes these defaults from historical run settings.
- Checks: `python -m unittest discover -s tests -p 'test_generate_index.py' -v`.
  Optional Playwright browser check: `python scripts/check_project_home.py` with
  the preview running (supports `PROJECT_URL` and `CHROMIUM_EXECUTABLE`).

## Optional Mazal entity unification (pilot)

Street identity and commemorative entity identity are separate. Two physical
streets can share `entity_key = mazal:<authority ID>` while retaining their
original OSM IDs, settlement-scoped `final_LAMAS_key`, official codes, geometry,
matching decisions and coverage statistics. This enrichment **does not approve
an OSM → LAMAS match** and does not merge map geometries or rename streets.

### Run with the existing GitHub secret

The manual **Mazal entity pilot** workflow (`.github/workflows/mazal-entities.yml`)
reads the repository secret **`MAZAL_API_KEY`**. It starts with Montefiore name
variants and Bialik, accepts one name per line, and can optionally sample names
from the structured settlement pages. The default budget is **50 requests / 50
canonical names**, with at least one second between requests. It needs no local
AI model, GIS download, write permissions or access to the secret outside Actions.
As with any new manual workflow, its definition must first be on the repository's
default branch before GitHub offers **Run workflow**; then the branch/ref can be
selected for the run.

Download the `mazal-entities-<run ID>` artifact. Its `mazal_entities.json` includes:

- `entries`: canonical name → resolution status and, only if resolved, authority
  ID, preferred name, authority-page URL and matching method;
- `entities`: groups of observed spellings sharing the same verified authority ID;
- `statistics`, request count and a **field/type-only** `response_shape` for
  diagnosing an unexpected response without publishing response bodies or keys.

The registry and lookup cache live under the already ignored `data/` and `cache/`
directories. No generated dataset or secret is committed. Lookups expire after
30 days; Actions caches successful runs separately from report artifacts.

**Live-validation boundary:** the request parameters are from the library's
[official Search API usage documentation](https://www.nli.org.il/en/research-and-teach/open-library/search-api/user-help)
(`material_type=Identity`, `output_format=json`). The JSON-LD parser is based on
public Search API clients, but its **Identity response shape has not yet been
verified with this repository's key**. Tests use explicitly synthetic fixtures,
not live captures. The first Actions run is a compatibility pilot: unsupported
schemas produce `schema_error`, fail the job and preserve the diagnostic report;
they never produce guessed entity links. An HTTP 200 with the library's `Errors`
header is also rejected, since NLI can silently ignore invalid filters.

### Local usage and pipeline integration

Install `requirements.txt`, then set `MAZAL_API_KEY` through your environment or
secret manager (do not pass it as a CLI argument or put it in source files):

```bash
python scripts/build_mazal_entities.py \
  --name 'משה מונטיפיורי' --name 'מונטיפיורי משה' \
  --html-dir HTML --max-names 50 --max-requests 50
```

Place the reviewed artifact at `data/mazal_entities.json`, or point
`MAZAL_ENTITIES_PATH` at it. Subsequent pipeline runs automatically add
`mazal_id`, `mazal_name`, `entity_key` and `mazal_status` to diagnostic CSVs.
Regenerated settlement pages show a shared-entities section with links to Mazal,
separate from street-code matching. Existing pages are not rewritten automatically; use
`--force` when reprocessing previously completed settlements. **Mapping runs
only read the local registry and never call the API.** With no registry the
pipeline behaves as before. No API key is embedded in any HTML or JSON report.

### Conservative matching rules

- Normalize punctuation, niqqud and street prefixes; token order may differ
  between a street name and a library heading.
- Match a complete preferred/alternative authority label, optionally removing
  a trailing MARC-style life-date range. Never pick the first search hit or use
  a fuzzy score as entity evidence.
- Only an explicit `nli.org.il/{language}/authorities/<18-digit ID>` URL is
  accepted. A bibliographic record ID is not a Mazal ID.
- Single-word names (e.g. `מונטיפיורי` or `הרצל`), numbers and ambiguous results
  stay unresolved/reviewable. This deliberately sacrifices recall to avoid
  misleading automatic merges; name equality alone is not contextual proof of
  the person a municipality intended to commemorate.
- Search all result pages up to the bounded page/request limits. A truncated
  result set is **not** proof of uniqueness. Quota/auth/network/schema failures
  stop further requests and are not cached as “not found”.
- Candidate `best_LAMAS_name` is never identity evidence: AI may have overridden
  that candidate. Conflicting explicit final/OSM identities are withheld.

Checks: `python -m unittest discover -s tests -p 'test_mazal.py' -v`.

## Features

- **Automated data fetching** from local OSM PBF extracts (or live Overpass fallback via osmnx) and data.gov.il (LAMAS)
- **High-speed offline OSM extraction** — automatically downloads Geofabrik's Israel PBF extract, parses and indexes all road segments, municipal boundaries and `place=*` features locally, eliminating Overpass API rate limits and timeouts.
- **Offline-first settlement matching** — one national index of OSM boundary/place names is built once per run and matched against the whole LAMAS settlement list with canonical Hebrew/Arabic normalization, curated aliases and geographic validation against LAMAS settlement centres; Nominatim is called only for names the local index cannot explain
- **Neighbourhood guard** — `place=suburb` / `neighbourhood` / `quarter`, `admin_level` 9+ and district-scale polygons can never win a LAMAS settlement match (offline index *and* Nominatim fallback). Features inside a differently named city boundary or Nominatim `address.city/town/village` are flagged, discarded candidates are listed in the match note, and rejections carry an explicit reason in the audit CSV
- **Minimal BBox-based Overpass fallback** — when the local PBF has no streets, the street fetch makes **one** Overpass query using the already-resolved bbox (or, without one, the matched OSM way/relation boundary, or the coordinates) with `graph_from_polygon`, instead of re-geocoding the whole settlement match dict through `ox.graph_from_place` (which caused `Nominatim API did not return a list of results` for small settlements). A PBF miss is almost always a data issue (no named streets in OSM), so there are no escalating retries: a short timeout (`OSM_OVERPASS_TIMEOUT`, default 30s), no larger-area/alternative-strategy retries, and OSMnx's built-in sleep-and-retry on HTTP 429/504 is disabled for this call
- **One consolidated match store** — positive *and* negative settlement resolutions are kept in `data/settlement_matches.json` (one atomic write per run), with a CSV audit trail in `batch_reports/settlement_match_report.csv`
- **One command for everything** — `python run_all.py` fetches, matches, processes, reports and rebuilds the dashboard in a single run
- **Hebrew-aware normalization** — expands abbreviations (`שד'` → `שדרות`, `רח'` → `רחוב`, …) and cleans punctuation
- **Cross-script Arabic↔Hebrew matching** — OSM streets named in Arabic script (`ابن سينا`, `شارع القدس`, `دوار …`) are also matched through bounded Hebrew transliteration variants (`אבן סינא`, `רחוב אלקדס`, `כיכר …`) that follow the LAMAS transliteration conventions (`א-רייף`/`אל סנאבל`/`אלקודס`, ח/כ, צ/ס, ף↔פ), so Arabic-script streets no longer score ~0 against the Hebrew LAMAS table; near-misses keep enough score to pass the AI gates
- **Fuzzy matching** with [rapidfuzz](https://github.com/rapidfuzz/rapidfuzz) (weighted ratio / token_sort / token_set, ≥90% auto-matched)
- **Whole-city Laya AI matching** — optional local `convaiinnovations/laya-multilingual` model resolves every OSM street below 90% against the city's LAMAS streets and writes confirmed matches directly to `final_LAMAS_id`
- **Batch processing** with parallel workers
- **Interactive HTML reports** — per-settlement maps + master index dashboard

## Project layout (core modules)

| Module | Role |
|--------|------|
| `run_all.py` | **Single-run orchestrator** (fetch → match → process → report → dashboard) |
| `settlement_index.py` | Offline OSM name index, geo validation, consolidated match store + resolver |
| `settlement_names.py` | Shared canonical settlement-name normalization (LAMAS ↔ OSM) |
| `settlement_matcher.py` | Nominatim fallback, bounded batch cache with negative results |
| `config/settlement_aliases.json` | Curated settlement aliases + renamed-city centre overrides |
| `pipeline.py` | Orchestrator (run one settlement end-to-end) |
| `data_loader.py` | Cache + fetch LAMAS / OSM (in-memory + pickle caches) |
| `normalization.py` | Street-name normalize + fuzzy candidates |
| `arabic_translit.py` | Arabic→Hebrew transliteration variants for cross-script matching |
| `diagnostics.py` | Match / coverage statistics |
| `laya_ai_resolver.py` | Optional local Laya decision-model resolver (whole-city matching) |
| `batch_process_settlements.py` | Batch runner / shared orchestration implementation |
| `generate_html.py` | Builds one settlement page from the pipeline results |
| `settlement_page.py` / `settlement_template.html` | Settlement page renderer + template ("חדר השוואה") |
| `scripts/convert_pages_to_room.py` | Re-render existing pages without re-matching |
| `generate_index.py` | National dashboard (`index.html`) |
| `assets/` | Shared UI (design tokens, dashboard, map, JS) |

## Installation

```bash
git clone https://github.com/baobabprince/streets_name_id.git
cd streets_name_id
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

**Optional — offline OSM PBF support (`osmium` executable):**

Install the system tool for your platform:

```bash
# Termux
pkg update && pkg install osmium-tool

# Debian / Ubuntu desktop or server
sudo apt install osmium-tool

# macOS
brew install osmium-tool

# Windows desktop (in an activated Conda environment)
conda install -c conda-forge osmium-tool
```

The program looks for `osmium` on `PATH`. If it is installed elsewhere, set
`OSMIUM_BIN` to the executable path (for example, `osmium.exe` on Windows).
`run_all.py` builds all three national caches (named streets, administrative
boundaries and `place=*` features) once, before any settlement is processed.
If the tool is not installed, local PBF extraction stops before downloading the
large regional extract; use `--no-pbf` to fall back to live Overpass data.
If Osmium detects a PBF decompression/format error, the pipeline downloads a
fresh extract and retries once before falling back to Overpass.
The Overpass fallback for a settlement missing from the PBF is a single, short
attempt (default 30 seconds; override with `OSM_OVERPASS_TIMEOUT=<seconds>`)
with no retries — if it is empty too, the settlement is reported as having no
OSM data.

**Optional — Laya local decision model:**

```bash
pip install "laya>=0.4.35,<0.5"
```

When `--use-ai` is enabled, every OSM street scoring below 90% in fuzzy matching
is sent together with the city's LAMAS street list to `LayaAIResolver`
(`convaiinnovations/laya-multilingual`). The model weights download from Hugging
Face on first use and the resolver is reused across settlements in the same
process. Every Laya suggestion passes strict output gates before it is written
to `final_LAMAS_id` (`mapping_status = APPROVED_AI`): confidence ≥ 0.70 and
prefix-stripped similarity ≥ 75 when Laya agrees with the fuzzy top-1 candidate,
stricter thresholds (≥ 0.80 / ≥ 80) when Laya overrides it, plus digit
compatibility, a settlement-name guard and magnet protection. Rejected
suggestions keep the diagnostic row unmatched with the rejection reason in
`ai_reasoning`.

## Usage

### One-shot run (recommended)

```bash
python run_all.py                          # every LAMAS settlement, start to finish
python run_all.py --cities --workers 4      # only the 83 configured cities
python run_all.py --non-cities --match-only --refresh-matches  # audit the long tail
python evaluate_settlement_matching.py      # prioritise settlement-ID failures
python run_all.py --only-file batch_reports/settlement_match_retry.txt --force --refresh-matches
python run_all.py --refresh-all             # refresh LAMAS + OSM + matches, reprocess all
python run_all.py --fast                    # no AI/HTML/dashboard/intermediates; pipelines still run
python run_all.py --match-only              # rebuild the settlement match map only
python run_all.py --dashboard-only          # just rebuild index.html
python run_all.py --dry-run --limit 10      # preview
```

The single pass does **five** things, each once:

1. loads LAMAS (network only on the first run / after `--refresh`) and builds the
   national OSM PBF caches (streets, boundaries, `place=*` features);
2. resolves all settlements **once** against the local OSM index, using
   `config/settlement_aliases.json` and `config/settlement_centers.json` for
   renamed/variant names and geographic sanity checks; only the leftovers are
   sent to Nominatim;
3. runs the per-settlement pipelines (skipping already processed ones) with the
   resolved OSM element/bbox and the authoritative LAMAS city name for street
   filtering;
4. writes one batch summary, one settlement-match CSV and the consolidated
   `data/settlement_matches.json`;
5. refreshes `index.html`.

Settlements whose pipeline completes with "nothing to map" (`NO_DATA` — OSM has
no named streets, or `NO_LAMAS_STREETS` — LAMAS has no street-level records for
the settlement) are counted as **no-data**, marked processed, and auto-documented
in `batch_reports/no_data_settlements.txt` (which `build_no_results.py` reads to
classify them in `no_results.json`). They do not fail the run.

`run_all.py` exits non-zero if any settlement pipeline fails, so CI can rely on
it. `batch_process_settlements.py` remains a compatible alias with the same
flags (without the strict exit code / presets).

| Flag | Meaning |
|------|---------|
| `--refresh` | Refresh LAMAS, national OSM caches and settlement matches |
| `--refresh-matches` | Ignore the settlement store and re-resolve every name |
| `--force` | Reprocess settlements even if already processed |
| `--no-nominatim` | Never call Nominatim; local OSM index only |
| `--no-intermediates` | Skip bulky step-1..5 pickles (CSV audit reports are kept) |
| `--match-only` / `--dashboard-only` | Run only that phase |
| `--only "חיפה,אילת"` | Process just these settlements |
| `--only-file PATH` | Read settlement names from a newline/comma-separated file; combines with `--only` |
| `--fast` / `--refresh-all` | Presets (see above) |
| `--skip-dashboard` | Keep `index.html` untouched |
| `--cities`, `--non-cities`, `--limit`, `--workers`, `--use-ai`, `--no-pbf`, `--skip-html`, … | Same as the batch flags below |

### Single settlement

```bash
python pipeline.py "Tel Aviv-Yafo"
python pipeline.py "Haifa" --no-ai
python pipeline.py "Eilat" --refresh
python pipeline.py "חיפה" --output-name haifa-laya
```

| Flag | Meaning |
|------|---------|
| `--refresh` | Ignore cache; re-download OSM/LAMAS |
| `--no-ai` | Fuzzy matching only (skip Laya AI) |
| `--no-pbf` | Disable local PBF extraction; query live Overpass API instead |
| `--output-name NAME` | Save outputs under a separate name |
| `--no-intermediates` | Skip bulky intermediate pickles |

### Batch (same CLI, without the presets)

```bash
python batch_process_settlements.py --workers 4
python batch_process_settlements.py --cities --use-ai
python batch_process_settlements.py --limit 5
python batch_process_settlements.py --dry-run
```

| Flag | Meaning |
|------|---------|
| `--workers N` | Parallel processes (default 1); forced to 1 when `--use-ai` is enabled |
| `--use-ai` | Enable Laya AI for streets below 90% fuzzy match (off by default in batch) |
| `--limit N` | First N settlements only |
| `--skip-html` | Skip HTML map generation |
| `--force` | Reprocess even if already done |
| `--refresh` / `--refresh-matches` | Refresh sources / re-resolve settlements |
| `--no-nominatim` | Offline settlement matching only |
| `--no-intermediates` | Skip bulky intermediate pickles |
| `--match-only` / `--dashboard-only` | Run only that phase |
| `--only NAMES` | Comma-separated settlement names |
| `--only-file PATH` | Settlement names from a file (one per line or comma-separated) |
| `--cities` | Process exactly the 83 cities listed in `config/israel_83_cities_lamas.json` |
| `--non-cities` | Process every LAMAS settlement except those 83 cities |

### Tuning settlement matching

`config/settlement_aliases.json` is the place to fix a settlement that keeps
failing to match. Add the LAMAS name as a key and any OSM/Hebrew/Arabic/English
spelling as aliases; keys are compared after canonicalization, so
`תל אביב - יפו`, `תל אביב־יפו` and `תל אביב יפו` are already equivalent.
`center_overrides` in the same file supplies ITM coordinates for settlements
renamed after `config/settlement_centers.json` was generated (e.g. נוף הגליל).

**שכונות שלא אמורות להיות יישוב:** רכיבי OSM מסוג `suburb`, `neighbourhood`,
`quarter`, `borough`, `isolated_dwelling`, גבולות ב־`admin_level` 9+, ופוליגונים
ברוחב מעל 0.6° (מחוזות/מועצות אזוריות) מסומנים כ־`is_settlement=False` ונדחים
גם אם שמם זהה לשם היישוב בלמ"ס וגם אם הם קרובים יותר למרכז היישוב. שכונה
שנמצאת בתוך גבול עיר אחרת (`admin_level=8`) מסומנת ב־`inside_city`. אם בכל זאת
קיים יישוב אמיתי ש־OSM מיפה אותו רק כשכונה, מוסיפים אותו ל־`osm_id_overrides`
עם ה־OSM id המדויק (`"n123456"` / `"r1392848"`) — זהו המנגנון היחיד שעוקף את
ההגנה, וההתאמה נרשמת בדוח עם `source=pinned`. הדחיות מופיעות בדוח עם סיבה
(`Only non-settlement OSM features matched (suburb…)` או סיבת הדחייה מ־Nominatim)
וב־`validation_message` של ההתאמה שנבחרה (כמה מועמדים שכונתיים נדחו). גרסאות
ה־cache קודמות (`STORE_VERSION<3`, `MATCH_CACHE_VERSION<v5`) לא נטענות אחרי
השינוי, כדי לא להחזיר שכונה שנשמרה בעבר כעיר או תוצאת Nominatim שנמצאת בתוך
עיר אחרת לפי שדה `address.city/town/village`.

Each run writes `batch_reports/settlement_match_report.csv` with one row per
settlement: status, source (`pbf` / `nominatim`), name score, distance from the
LAMAS centre, OSM id and the failure reason. Unresolved names are cached for
seven days so a rerun does not repeat failing Nominatim requests; use
`--refresh-matches` to retry them immediately. Duplicate assignments (two LAMAS
names pointing at the same OSM feature) are detected and the farther one is
flagged as unresolved instead of being silently mis-matched.

### התמקדות ביישובים הקטנים, לא רק ב־83 הערים

83 הערים הן בעיקר **sanity check**: הן כמעט תמיד נפתרות offline מה־PBF. עיקר
בעיות הזיהוי נמצא ביתר היישובים ברשימת הלמ"ס — שבטים בדואים, מושבים, כפרים
קטנים, מחנות, מוסדות ויישובים ש־OSM מיפה כ־`locality`, `hamlet`,
`landuse=residential` או אפילו כשכונה. לכן יש זרימת עבודה נפרדת:

```bash
# 1. בנה/רענן דוח התאמה לכל היישובים שאינן 83 הערים, בלי להריץ pipelines
python run_all.py --non-cities --match-only --refresh-matches

# 2. דרג את הכשלים לפי סיבה, וצור רשימת שמות לטיפול חוזר
python evaluate_settlement_matching.py

# 3. עבד/בדוק רק את השמות שסומנו P1/P2
python run_all.py --only-file batch_reports/settlement_match_retry.txt \
  --workers 2 --fast --force --refresh-matches
```

`evaluate_settlement_matching.py` כותב שלושה קבצים:

| קובץ | תוכן |
|---|---|
| `batch_reports/settlement_match_audit.csv` | כל שורה בדוח ההתאמה + `issues`, `priority`, `suggested_action` |
| `batch_reports/settlement_match_audit.md` | ספירות לפי מקור/עדיפות/סיבה ודוגמאות לכל סוג כשל |
| `batch_reports/settlement_match_retry.txt` | שמות מוכנים ל־`--only-file` |

הבאקטים המרכזיים: `unresolved_non_settlement`, `unresolved_nominatim`,
`unresolved_offline`, `duplicate_osm_id`, `inside_another_city`,
`empty_osm_type`, `nominatim_fallback`, `fuzzy_name`, `display_name_mismatch`,
`suburb_candidates_ignored`, `institution_or_camp`, `pinned_override`.
הפעולה המוצעת אומרת אם להוסיף alias, לבדוק `osm_id_override` מאומת, לתקן
מרכז למ"ס, או לעשות בדיקה ידנית. `--non-cities` ו־`--cities` סותרים זה את זה;
`--only` ו־`--only-file` מתאחדים לפי `canonical_key`.

### GitHub Actions: all 83 cities

Run **Actions → Run Laya for 83 Israeli cities → Run workflow** to process
the configured cities with the local Laya model on a GitHub-hosted runner.
The workflow invokes `python run_all.py --cities … --force --no-intermediates`,
which resolves all 83 settlements once from the national OSM PBF and fails if
the LAMAS dataset does not contain all 83 cities or if any city pipeline fails
(`run_all.py` returns a non-zero exit code in that case). No Gemini key is
required.

The workflow uploads batch summaries, per-city diagnostic and candidate-score
CSV files, and HTML maps as the `israel-cities-results` artifact, retained for
90 days. The artifact can be downloaded from the completed workflow run.

### Dashboard & visualization

The dashboard scans every page in `HTML/` (old and new formats), aggregates the
coverage data and writes a fully static `index.html` with an embedded JSON
payload. No server, CDN or API key is required; it works on GitHub Pages and
straight from a checkout.

```bash
python generate_index.py
# → index.html + dashboard_data.json (cache, git-ignored)
```

What you get:

- **National scorecard** — weighted coverage ring, 797 settlements, 58k streets,
  approved / pending / AI-proposal split, LAMAS coverage.
- **Interactive analytics** — coverage distribution histogram (click a column
  to filter the list), status-mix donut and an “attention needed” shortlist.
- **City explorer** — instant Hebrew-aware search, coverage-tier chips, sorting
  by size / coverage / unmatched streets, card and table views, and paged
  rendering so hundreds of settlements stay fast.
- **Settlement pages** (`HTML/*_roads.html`, "חדר השוואה") — see below.

#### Settlement pages

Every pipeline run (`pipeline.py`, `batch_process_settlements.py`,
`run_all.py`, or `python generate_html.py "<name>"` from an existing diagnostic
report) writes the same comparison-room page:

- **Overview** — OSM names and LAMAS codes, coverage in both directions
  (share of OSM names linked, share of official codes linked), the match mix,
  and the two gap counts.
- **Map vs map** — the OSM match layer (Leaflet, vendored under
  `assets/vendor/leaflet`, coloured by status, filtered together with the table)
  next to a GovMap reference panel for the same settlement or street. GovMap is
  reference only; it is not synchronised with the OSM map. If tiles or the
  GovMap embed are blocked, the table, search and sorting keep working.
- **Full findings table** — tabs for all OSM names, OSM without approved match,
  LAMAS names without an OSM street, and AI matches; search, sortable columns,
  a match-score range filter shared with the map (a missing score is not 0, and
  the similarity score is not a probability), CSV export.
- **Technical details** for the selected street: final code, score, AI fields,
  geographic validation and how the status was decided.
- Creation time of the page and the OSM / LAMAS source dates (see
  [Creation and source timestamps](#creation-and-source-timestamps)).

All data is embedded in the page as JSON (`#page-meta`, `#street-records`,
`#room-data`); the shared UI is `assets/room.js`, `assets/room.css` and
`assets/score-tools.js`, referenced with content hashes. Street geometry is
stored in WGS84 (about one metre precision, delta-encoded), so pages work from
GitHub Pages or straight from a checkout.

To apply template or asset changes to pages that are already generated —
without downloading data or re-running the matching — run:

```bash
python scripts/convert_pages_to_room.py             # all pages
python scripts/convert_pages_to_room.py "פתח תקווה"  # selected settlements
python generate_index.py --data-out ''
```

The converter also upgrades pages from the earlier SVG layout; their geometry
is recovered from the SVG coordinates and the page's rounded ITM centre
(about one metre). Their recorded creation time is kept, since the results did
not change. Single-street pages from `generate_single_street_pages.py` are left
as they are.

Browser checks (run the preview server first; Playwright required):
`python scripts/check_settlement_page.py` and `python scripts/check_project_home.py`.

## Methodology (short)

1. **Acquire once** — the full LAMAS street table and one national OSM cache
   (streets, boundaries and `place=*` features) are loaded before any
   settlement is processed; per-settlement street extracts come from that cache
   (stale caches are used as fallback if a refresh fails)
2. **Match settlements once** — canonical names + curated aliases + fuzzy
   fallback + LAMAS-centre distance checks, with a Nominatim fallback only for
   unresolved names, all persisted in one store
3. **Normalize streets** — abbreviations, punctuation, Hebrew preference
   (`name:he`); LAMAS rows are joined by canonical city key. Arabic-script
   OSM names additionally score through Hebrew transliteration variants
   (`arabic_translit.py`) because LAMAS spells those streets in Hebrew script
4. **Fuzzy match (≥90%)** — rapidfuzz weighted score against city LAMAS streets
   and synonyms; streets ≥90% are approved directly
5. **Whole-city Laya AI (optional)** — every street below 90% is matched against
   the city's LAMAS streets via Laya; confirmed matches are written directly to
   `final_LAMAS_id`
6. **Report** — diagnostic CSV and interactive HTML map

Threshold tuning is documented in `THRESHOLD_GUIDE.md`.

### Creation and source timestamps

Every settlement page rendered by the pipeline records, in its `#page-meta`
JSON (`provenance`) and visibly in the page header/footer:

- `generated_at` — when the page file was written (UTC, shown in Israel time);
- OSM data time — the `osmosis_replication_timestamp` from the PBF header the
  national streets cache was extracted from (or the Overpass retrieval time);
- LAMAS data time — when the table was downloaded from data.gov.il.

Cached inputs carry a `<file>.provenance.json` sidecar so later runs from the
cache keep reporting the original retrieval time. Caches built before this was
recorded fall back to their file write time (`cache_written_at`), and pages
generated before it show "לא תועד" — no time is ever estimated. See
`provenance.py`.

The national index shows its own creation time, the latest batch-summary time
(as recorded, without a time-zone claim) and how many settlement pages have a
recorded creation time. `index.html` honours `SOURCE_DATE_EPOCH` for
reproducible rebuilds.

## Outputs

- `data/` — pickle caches (national OSM extracts, LAMAS, per-settlement OSM),
  `data/settlement_matches.json` (consolidated settlement resolutions), and
  `diagnostic_report_*.csv`. Bulky intermediate pickles are optional
  (`--no-intermediates`).
- `HTML/` — per-settlement comparison pages (`*_roads.html`)
- `batch_reports/` — batch summaries, `settlement_match_report.csv` and
  `processed_settlements.json`
- `index.html` — national dashboard (search, filters, analytics)
- `assets/` — shared CSS/JS used by the dashboard and every settlement page

### LAMAS data fallback

LAMAS data is fetched from the official
[data.gov.il CKAN `datastore_search` API](https://data.gov.il/api/3/action/datastore_search)
in 32,000-record pages using the portal's recommended
`datagov-external-client` User-Agent. Page sizes and the reported total are
validated, and temporary network/server errors are retried.
If the LAMAS API is unavailable, the pipeline uses the existing LAMAS pickle
cache as a fallback regardless of age, and logs a warning. A manually downloaded
CSV can also be saved as `cache/lamas_data.csv`; it must contain either the
`official_code`, `street_name`, and `city_name` columns from the data portal, or
the normalized `LAMAS_id`, `LAMAS_name`, and `city` columns. Fresh CSV files are
used directly, and stale CSV or pickle files are used if an API refresh fails.
The source dataset is updated weekly, so LAMAS caches are refreshed after seven
days; an older cache remains available as a fallback if the API is unavailable.
OSM data is fetched as a simplified street graph for the requested settlement.
If an OSM refresh fails, a valid existing cache is used with a warning; without
a usable cache, the fetch error is surfaced instead of silently returning empty data.

### Which LAMAS street id is stored?

The portal resource is the national street **synonyms** table. Its live CKAN
schema is `_id, region_code, region_name, city_code, city_name, street_code,
street_name, street_name_status, official_code` ([data dictionary][lamas-dict],
[live API schema][lamas-schema]). `_id` is only a CKAN row number and
`city_code` is the settlement code — neither is the street id.

- `official_code` is the canonical `סמל רחוב` used by `LAMAS_id`. For an
  official row it equals `street_code`; for a synonym row it points back to the
  official street code. [Live example][lamas-example] from `city_code=52` (`לוזית`):
  `street_code=101 / שקד` has `street_name_status=official` and
  `official_code=101`, while `street_code=9521 / השקד` has
  `street_name_status="synonym of 101"` and the same `official_code=101`.
- `official_code` is **not globally unique**: the live API reports 1,503 rows
  with `official_code=101` across different settlements. The unambiguous id is
  therefore `LAMAS_key = city_code:official_code` (e.g. `52:101`). The pipeline
  filters LAMAS rows by canonical settlement before matching, so `LAMAS_id`
  remains the city-local official street code, while `final_LAMAS_key`,
  `final_city_code`, `final_official_code`, `final_street_code` and
  `final_street_name_status` are included in `diagnostic_report` for auditing.
- `street_name_status` — not the number of digits — decides whether a row is an
  official street. Four- and five-digit official codes are valid; the old
  "3 characters = street" diagnostic heuristic was wrong and was removed.
  Codes are stored as text, preserving leading zeroes; numeric `101.0` values
  are cleaned to `101`. Legacy `data/LAMAS_data.pkl` and
  `cache/lamas_data.csv` caches are upgraded to this schema on read.

[lamas-dict]: https://haifa.datacity.org.il/dataset/7b1b9245-e292-4572-82d7-92d408280d7c/resource/e02acb9c-dcd6-4fa4-895e-03902278a87d
[lamas-schema]: https://data.gov.il/api/3/action/datastore_search?resource_id=bf185c7f-1a4e-4662-88c5-fa118a244bda&limit=2
[lamas-example]: https://data.gov.il/api/3/action/datastore_search?resource_id=bf185c7f-1a4e-4662-88c5-fa118a244bda&filters=%7B%22city_code%22%3A52%2C%22official_code%22%3A101%7D&limit=3

## License

Apache-2.0 (see `LICENSE`).
