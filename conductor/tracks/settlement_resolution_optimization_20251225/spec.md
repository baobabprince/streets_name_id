# Specification: Optimization of Settlement Resolution Speed and Logic

## Overview
This track addresses performance and efficiency issues in the settlement resolution phase. It introduces a thread-safe global rate limiter to allow parallelizing Nominatim queries while strictly adhering to API constraints. Additionally, it streamlines the query logic by removing redundant variations (like ", Israel" or region-based suffixes) to reduce unnecessary API calls and improve matching precision.

## Functional Requirements
1.  **Global Rate Limiter**: Implement a thread-safe mechanism (e.g., using `threading.Lock`) within `SettlementMatcher` to ensure that only one request is sent to Nominatim every 1.0 seconds, regardless of how many threads are calling it.
2.  **Parallel Resolution**: Update `BatchProcessor.run_batch` to initiate location resolution for settlements in parallel using `ThreadPoolExecutor`.
3.  **Pipeline Overlap**: Modify the batch logic so that as soon as a settlement's location is successfully resolved, its corresponding street-matching pipeline task can be submitted to the process pool.
4.  **Simplified Query Variants**:
    -   Refactor `SettlementMatcher.search_settlement` to use a "Selective Fallback" strategy.
    -   Primary Query: The full normalized settlement name.
    -   Secondary Query: If the primary fails and the original name contains parentheses (e.g., "Kfar Rosenwald (Zarit)"), attempt only the content inside the parentheses.
    -   **Strict Removal**: Remove the appending of ", Israel" and the splitting of names by dashes/hyphens from the query logic.

## Non-Functional Requirements
-   **Reliability**: The system must not exceed the 1 request/second limit.
-   **Performance**: Total time for the resolution phase should be approximately `N * 1.0` seconds (where N is the number of unique settlements not in cache).

## Acceptance Criteria
-   The "Resolving settlement locations" phase finishes significantly faster due to overlapping execution.
-   Zero "429 Too Many Requests" errors occur during large batch runs.
-   Nominatim queries are limited to the full name or the parenthetical variant, avoiding irrelevant fallback variations.

## Out of Scope
-   Changing the internal fuzzy matching scoring algorithm (ratio, token_set_ratio).
-   Modifying the AI street resolution logic.
