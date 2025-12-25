# Specification: Pipeline Performance Optimization

## Overview
This track aims to significantly reduce the execution time of the batch processing script and the matching pipeline. The primary optimizations will focus on batching AI requests at the city level, parallelizing settlement resolution, and upgrading the string similarity library.

## Functional Requirements
1.  **AI Batching (Full City Strategy)**:
    -   Modify `pipeline.py` to collect all street segments requiring AI resolution for a given settlement.
    -   Construct a single comprehensive prompt for the AI (Gemini or Local) containing all ambiguous cases for that city.
    -   Update the AI resolution logic to parse a batched response (e.g., a JSON map of OSM IDs to LAMAS IDs).
2.  **Faster Fuzzy Matching**:
    -   Replace the `fuzzywuzzy` library with `rapidfuzz` across the project (`normalization.py` and `pipeline.py`).
    -   `rapidfuzz` is a drop-in replacement that is significantly faster (written in C++) while maintaining the same algorithms (`ratio`, `token_set_ratio`, etc.).
3.  **Parallel Nominatim Resolution**:
    -   Update `BatchProcessor.run_batch` in `batch_process_settlements.py` to parallelize the initial settlement search phase.
    -   **Constraint**: Must respect the Nominatim usage policy (approx. 1 request per second). We will use a smaller pool of workers or a shared rate-limiter for this specific phase.

## Acceptance Criteria
-   [ ] Total pipeline execution time for a city with 50+ ambiguous streets is reduced by at least 50% (due to AI batching).
-   [ ] `rapidfuzz` is used for all string similarity calculations.
-   [ ] The batch processor resolves multiple settlements in parallel while staying within rate limits.
-   [ ] AI batching correctly maps multiple OSM IDs back to their respective LAMAS IDs from a single response.

## Out of Scope
-   Async database drivers.
-   GPU-specific optimizations for local AI (outside of existing torch/cuda support).
