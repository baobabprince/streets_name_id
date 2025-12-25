# Plan: Pipeline Performance Optimization

## Phase 1: String Matching Optimization
- [x] Task: Replace `fuzzywuzzy` with `rapidfuzz` in `requirements.txt`. (b62a404)
- [x] Task: Update `normalization.py` to use `rapidfuzz` instead of `fuzzywuzzy`. (310e4c1)
- [x] Task: Update `pipeline.py` to use `rapidfuzz` instead of `fuzzywuzzy`. (310e4c1)
- [x] Task: Verify that all fuzzy matching tests pass with the new library. (310e4c1)
- [x] Task: Conductor - User Manual Verification 'Phase 1: String Matching Optimization' (Protocol in workflow.md)

## Phase 2: Parallel Settlement Resolution
- [x] Task: Implement a thread-safe rate limiter for Nominatim queries in `settlement_matcher.py`. (310e4c1)
- [~] Task: Update `BatchProcessor.run_batch` in `batch_process_settlements.py` to resolve multiple settlements in parallel using `ThreadPoolExecutor`.
- [x] Task: Verify that Nominatim queries are still throttled to ~1 per second across all threads. (3329538)
- [x] Task: Conductor - User Manual Verification 'Phase 2: Parallel Settlement Resolution' (Protocol in workflow.md)

## Phase 3: AI Batching
- [~] Task: Modify the AI resolution logic in `pipeline.py` to collect all ambiguous streets and send them in a single batch prompt.
- [ ] Task: Update `get_ai_resolution` and `LocalAIResolver` to handle multi-street prompts and structured JSON responses.
- [ ] Task: Update the segment-to-result mapping logic to handle the batched response.
- [ ] Task: Conductor - User Manual Verification 'Phase 3: AI Batching' (Protocol in workflow.md)
