# Specification: Persistent Diagnostics and Logging System

## Overview
This track introduces a comprehensive diagnostic system to record the "reasoning" behind every street matching decision. It ensures that every pipeline run generates persistent logs that explain how normalization was performed, how fuzzy scores were calculated, and exactly what the AI was asked and how it responded. This information will be surfaced "nicely" in the HTML reports to facilitate rapid debugging.

## Functional Requirements
1.  **Diagnostic Data Collection**:
    -   **Normalization Trace**: Record `osm_name` -> `normalized_osm_name` and `lamas_name` -> `normalized_lamas_name`.
    -   **Scoring Breakdown**: Capture individual components (`fuzz.ratio`, `token_sort`, `token_set`) and the final weighted score for the top candidates.
    -   **Ambiguity Metrics**: Record the number of valid candidates identified and the score delta between the top two.
    -   **AI Transparency**: Log the full prompt sent to the AI (Gemini/Local), the raw text response, and any reasoning provided.
2.  **Persistent Storage**:
    -   For every run, generate a detailed JSON diagnostic file in `batch_reports/` alongside the summary reports.
3.  **HTML Integration & UI Polish**:
    -   Update the HTML generation logic to embed this diagnostic data.
    -   Add a visually appealing "Debug Info" component (Modal or Sidebar) accessible from street segments.
    -   Ensure data is presented cleanly (e.g., using tables for scores or formatted blocks for AI reasoning).

## Acceptance Criteria
-   [ ] Every pipeline run produces a JSON file in `batch_reports/` containing a segment-by-segment diagnostic trace.
-   [ ] Street segments in the generated HTML maps display a breakdown of the fuzzy matching scores in a nice UI.
-   [ ] If a segment was resolved by AI, the HTML report shows the reasoning or prompt context.
-   [ ] The diagnostic JSON includes the original name, normalized name, and all candidates considered.

## Out of Scope
-   External database integration or cloud-based logging.
-   Real-time monitoring dashboard.
