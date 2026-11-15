# Street Name Synonym Mapper

Pipeline that maps OpenStreetMap (OSM) street names to official **LAMAS** (Central Bureau of Statistics / CBS) street IDs for Israeli settlements. It combines name normalization, fuzzy matching, topological (adjacency) context, and a two-tier AI resolver (local model + Gemini).

**Live dashboard:** [baobabprince.github.io/streets_name_id](https://baobabprince.github.io/streets_name_id/)

## Features

- **Automated data fetching** from OSM (via osmnx) and data.gov.il (LAMAS)
- **Hebrew-aware normalization** — expands abbreviations (`שד'` → `שדרות`, `רח'` → `רחוב`, …) and cleans punctuation
- **Fuzzy matching** with [rapidfuzz](https://github.com/rapidfuzz/rapidfuzz) (weighted ratio / token_sort / token_set)
- **Topological context** — adjacency graph of connected OSM streets for AI prompts
- **Two-tier AI resolution**
  - Local model (optional, if torch/transformers available)
  - Cloud fallback: Google Gemini (`GEMINI_API_KEY`)
- **Batch processing** with parallel workers
- **Interactive HTML reports** — per-settlement maps + master index dashboard

## Project layout (core modules)

| Module | Role |
|--------|------|
| `pipeline.py` | Orchestrator (run one settlement end-to-end) |
| `data_loader.py` | Cache + fetch LAMAS / OSM |
| `normalization.py` | Name normalize + fuzzy candidates |
| `diagnostics.py` | Match / coverage statistics |
| `gemini_client.py` | Gemini API client |
| `local_ai_resolver.py` | Optional local Qwen-based resolver |
| `batch_process_settlements.py` | Multi-settlement runner |
| `generate_html.py` / `generate_index.py` | Per-city map + dashboard |

## Installation

```bash
git clone https://github.com/baobabprince/streets_name_id.git
cd streets_name_id
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

**Optional — Cloud AI:**

```bash
export GEMINI_API_KEY="YOUR_API_KEY_HERE"
# optional model override:
export GEMINI_MODEL="gemini-2.5-flash-preview-09-2025"
```

> Local AI pulls large `torch` / `transformers` wheels. For fuzzy-only runs you can skip them if install is slow.

## Usage

### Single settlement

```bash
python pipeline.py "Tel Aviv-Yafo"
python pipeline.py "Haifa" --no-ai
python pipeline.py "Eilat" --refresh
python pipeline.py "אלעד" --no-local-ai
```

| Flag | Meaning |
|------|---------|
| `--refresh` | Ignore cache; re-download OSM/LAMAS |
| `--no-ai` | Fuzzy matching only |
| `--no-local-ai` | Skip local model; use Gemini if key is set |

### Batch

```bash
python batch_process_settlements.py --workers 4 --use-ai
python batch_process_settlements.py --limit 5
python batch_process_settlements.py --dry-run
```

| Flag | Meaning |
|------|---------|
| `--workers N` | Parallel processes (default 1) |
| `--use-ai` | Enable AI (off by default in batch) |
| `--limit N` | First N settlements only |
| `--skip-html` | Skip HTML map generation |
| `--force` | Reprocess even if already done |

### Index dashboard

```bash
python generate_index.py
```

Opens as `index.html` (and on GitHub Pages when published).

## Methodology (short)

1. **Acquire** — LAMAS street codes + simplified OSM street geometries for one
   settlement at a time (cached under `data/`; stale OSM cache is used if a
   refresh fails)
2. **Normalize** — abbreviations, punctuation, Hebrew preference (`name:he`)
3. **Topology** — adjacency map of OSM segments
4. **Match** — confident / needs-AI / missing by fuzzy score thresholds
5. **AI (optional)** — local model, then Gemini, with neighbor context
6. **Report** — diagnostic CSV + interactive HTML map

Threshold tuning is documented in `THRESHOLD_GUIDE.md`.

## Outputs

- `data/` — pickle caches, intermediate steps, `diagnostic_report_*.csv`
- `HTML/` — per-settlement interactive maps (`*_roads.html`)
- `batch_reports/` — batch logs and summaries
- `index.html` — multi-settlement dashboard

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

## License

Apache-2.0 (see `LICENSE`).
