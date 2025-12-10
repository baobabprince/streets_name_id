# Street Name Synonym Mapper

This project provides a comprehensive pipeline to map street names from OpenStreetMap (OSM) to their official LAMAS (Central Bureau of Statistics) IDs. It leverages fuzzy matching, topological analysis, and a two-tier AI resolution system (Local AI + Cloud AI) to handle variations in street names, abbreviations, and synonyms.

## Features

- **Automated Data Fetching:** Downloads the latest street data from both OSM and the official Israeli government data portal (data.gov.il).
- **Intelligent Normalization:** Standardizes street names by expanding abbreviations (e.g., 'שד' -> 'שדרות') and cleaning punctuation.
- **Fuzzy Matching:** Utilizes fuzzy logic to identify potential matches between OSM and LAMAS street names.
- **Topological Context:** Builds an adjacency map of connected streets to provide geographical context for resolving ambiguous matches.
- **Two-Tier AI Resolution:**
    - **Local AI:** Uses a local embedding/reasoning model (if available) for fast, free resolution of ambiguous cases.
    - **Cloud AI (Gemini):** Falls back to Google's Gemini model for complex cases if local resolution fails or is disabled.
- **Batch Processing:** Supports parallel processing of multiple settlements with robust error handling.
- **Visual Reports:** Generates interactive HTML maps showing matched/unmatched streets and diagnostic statistics.

## Logic and Methodology

The pipeline operates in a series of sequential steps:

1.  **Data Acquisition:** Fetches official street codes from LAMAS and street geometries from OSM.
2.  **Preprocessing:** Normalizes street names in both datasets (standardizing abbreviations, removing punctuation).
3.  **Topological Analysis:** Builds a graph of connected streets in OSM to understand neighbor relationships.
4.  **Candidate Matching:**
    - **Confident:** Exact or high-score fuzzy matches (>= 98).
    - **Needs AI:** Ambiguous matches (score 80-98) sent for further resolution.
    - **Missing:** Streets with no plausible candidates in the official registry.
5.  **AI Resolution:**
    - Streets marked 'NEEDS_AI' are first checked by the Local AI resolver.
    - If unresolved, they are sent to the Gemini API (if key provided).
    - The AI is given the street name, its neighbors, and the list of official candidates to make a decision.
6.  **Reporting:** Merges results and generates CSV reports and HTML visualizations.

## Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/baobabprince/streets_name_id.git
    cd streets_name_id
    ```

2.  **Create a virtual environment and install dependencies:**
    ```bash
    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    ```

3.  **Set up Cloud AI (Optional):**
    To use the Gemini fallback for difficult cases, set your API key:
    ```bash
    export GEMINI_API_KEY="YOUR_API_KEY_HERE"
    ```

## Usage

### 1. Single Settlement (`pipeline.py`)

Run the pipeline for a specific city or settlement.

```bash
python pipeline.py "Settlement Name" [OPTIONS]
```

**Examples:**
```bash
# Run for Tel Aviv with full AI resolution
python pipeline.py "Tel Aviv-Yafo"

# Run without any AI (only fuzzy matching)
python pipeline.py "Haifa" --no-ai

# Force data refresh (ignore cache)
python pipeline.py "Eilat" --refresh
```

**Arguments:**
- `place`: Name of the settlement (e.g., "Tel Aviv").
- `--refresh`: Force re-download of data from APIs.
- `--no-ai`: Disable all AI resolution.
- `--no-local-ai`: Skip the local model and go straight to Gemini (or skip if no API key).

### 2. Batch Processing (`batch_process_settlements.py`)

Process multiple settlements automatically. This script reads the list of all settlements from the LAMAS file and processes them.

```bash
python batch_process_settlements.py [OPTIONS]
```

**Examples:**
```bash
# Process all settlements using 4 parallel workers, with AI enabled
python batch_process_settlements.py --workers 4 --use-ai

# Test run: process only the first 5 settlements, no AI
python batch_process_settlements.py --limit 5
```

**Arguments:**
- `--workers N`: Number of parallel processes (default: 1).
- `--use-ai`: Enable AI resolution (disabled by default in batch mode to save costs/time).
- `--limit N`: Process only the first N settlements.
- `--dry-run`: Print what would be done without running.
- `--skip-html`: Don't generate HTML reports (saves time).
- `--force`: Reprocess settlements even if they are already done.

### 3. Generating the Index

After running a batch, you can generate a master `index.html` linking to all individual reports:

```bash
python generate_index.py
```

## Outputs

- **`data/`**: functionality caches (PKL files) and intermediate CSVs.
- **`HTML/`**: Individual HTML reports for each settlement (e.g., `Tel_Aviv_report.html`).
- **`batch_reports/`**: Logs and summary CSVs from batch runs.
- **`index.html`**: The main dashboard linking to all generated reports.

## License

This project is licensed under the MIT License.
