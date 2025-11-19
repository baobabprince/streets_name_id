cond# Street Name Synonym Mapper

This project provides a comprehensive pipeline to map street names from OpenStreetMap (OSM) to their official LAMAS IDs, leveraging fuzzy matching, topological analysis, and an optional AI-driven resolution for ambiguous cases. The system is designed to handle variations in street names, including abbreviations and synonyms, by using an official government data source that includes synonym information.

## Features

- **Automated Data Fetching:** Downloads the latest street data from both OSM and the official Israeli government data portal (data.gov.il).
- **Intelligent Normalization:** Standardizes street names by expanding abbreviations (e.g., 'שד' -> 'שדרות') and cleaning punctuation to ensure consistent comparisons.
- **Fuzzy Matching:** Utilizes fuzzy logic to identify potential matches between OSM and LAMAS street names, even when they are not identical.
- **Topological Context:** Builds an adjacency map of connected streets from OSM to provide geographical context, which can help in resolving ambiguous matches.
- **Synonym Integration:** Leverages a LAMAS data source that includes official street name synonyms, significantly improving match rates.
- **AI-Powered Resolution (Optional):** For complex cases, the pipeline can consult a generative AI model (Google's Gemini) to make a final decision based on all available context.
- **Caching:** Caches downloaded data to speed up subsequent runs and reduce redundant API calls.
- **Exportable Results:** Saves the final mapping and intermediate analysis files to CSV for easy inspection and use in other systems.

## Logic and Methodology

The pipeline operates in a series of sequential steps to achieve a high-quality mapping:

1.  **Data Acquisition:**
    *   Fetches all official street names and their synonyms from the `data.gov.il` API. This data includes a unique `official_code` for each street, which serves as the LAMAS ID.
    *   Fetches all street geometries for a specified city (e.g., "בית שאן") from OpenStreetMap using the Overpass API.

2.  **Preprocessing and Normalization:**
    *   Both the OSM and LAMAS street names are passed through a normalization function. This function expands common abbreviations (like `שד`, `רח`, `כי`), removes punctuation, and standardizes whitespace. This ensures that, for example, "שד' רוטשילד" and "שדרות רוטשילד" are treated as identical.

3.  **Topological Analysis:**
    *   The pipeline analyzes the OSM data to understand which streets are connected to each other. It builds an "adjacency map" (a dictionary where each key is a street ID and the value is a list of adjacent street IDs). This map provides crucial geographic context for resolving ambiguities later.

4.  **Candidate Matching:**
    *   For each street in the OSM dataset, the system performs a fuzzy match against all LAMAS streets in the same city.
    *   It calculates a weighted similarity score based on several fuzzy matching algorithms.
    *   Matches with a very high score (>= 98) are marked as **'CONFIDENT'**.
    *   Matches with a good but not perfect score (80-98) are marked as **'NEEDS_AI'**.
    *   Streets with no good matches are marked as **'MISSING'**.

5.  **AI Resolution (Optional):**
    *   If enabled, all streets marked as 'NEEDS_AI' are sent to a generative AI model.
    *   A detailed prompt is constructed, including the OSM street name, its adjacent streets (from the topology map), and the list of potential LAMAS candidates with their fuzzy scores.
    *   The AI is instructed to act as a GIS expert and return the single best LAMAS ID or 'None' if no candidate is a clear match.

6.  **Final Merging and Output:**
    *   The system combines the 'CONFIDENT' matches with the successful AI-resolved matches to create the final mapping.
    *   This final mapping, along with intermediate dataframes, is saved to the `data/` directory.

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

3.  **Set up the AI (Optional):**
    If you wish to use the AI resolution feature, you must obtain a Gemini API key from Google AI Studio and set it as an environment variable.
    ```bash
    export GEMINI_API_KEY="YOUR_API_KEY_HERE"
    ```
    If you do not set this key, the pipeline will run but will skip the AI resolution step.

## How to Run the Pipeline

You can run the entire pipeline from the command line using the script in the `scripts/` directory.

### Basic Usage

To run the pipeline for a specific city, provide the city name as an argument.

```bash
python scripts/run_pipeline.py "בית שאן"
```

### Command-Line Arguments

-   **`place` (string):** The only required argument. The name of the city or place to process (e.g., `"תל אביב-יפו"`).
-   **`--no-ai`:** (Optional) Runs the pipeline without consulting the AI for ambiguous cases. This is useful for faster runs or if you don't have an API key.
-   **`--refresh`:** (Optional) Forces the script to re-download all data from the APIs, ignoring any cached data. Use this if you want to ensure you have the absolute latest data.

### Examples

-   **Run for Tel Aviv with AI resolution:**
    ```bash
    python scripts/run_pipeline.py "Tel Aviv-Yafo, Israel"
    ```

-   **Run for Beit She'an without AI and force a data refresh:**
    ```bash
    python scripts/run_pipeline.py "בית שאן" --no-ai --refresh
    ```

### Running Diagnostics

To diagnose the results of a pipeline run, you can use the `diagnose_pipeline.py` script:

```bash
python scripts/diagnose_pipeline.py "בית שאן"
```

## Outputs

The pipeline generates several files in the `data/` directory:

-   **`LAMAS_data.pkl`:** A cached pickle file of the raw data fetched from the LAMAS API.
-   **`osm_data_{place}.pkl`:** A cached pickle file of the GeoDataFrame containing street data from OSM for the specified place.
-   **`final_mapping_{place}.csv`:** The main output file. This CSV contains the final mapping between `osm_id`, `osm_name`, and the matched `final_LAMAS_id`.
-   **`analysis_{place}_{timestamp}_candidates.csv`:** (Generated by `analyze_results.py`) A detailed breakdown of all potential candidates considered for each OSM street.
-   **`analysis_{place}_{timestamp}_needs_ai.csv`:** (Generated by `analyze_results.py`) A filtered list of streets that were sent for AI resolution.
-   **`analysis_{place}_{timestamp}_merged_with_final.csv`:** (Generated by `analyze_results.py`) The final mapping merged with the intermediate candidate data for full traceability.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
