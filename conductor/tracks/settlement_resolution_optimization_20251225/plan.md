# Plan: Optimization of Settlement Resolution Speed and Logic

## Phase 1: thread-safe rate limiter [checkpoint: 0eceb1c]
- [x] Task: Create unit tests for thread-safe rate limiting in `SettlementMatcher`. (236f0e9)
- [x] Task: Implement `threading.Lock` and updated `_rate_limit` in `settlement_matcher.py`. (236f0e9)
- [x] Task: Conductor - User Manual Verification 'thread-safe rate limiter' (Protocol in workflow.md)

## Phase 2: Parallel Resolution and Pipeline Overlap [checkpoint: ]
- [x] Task: Create integration tests for parallel settlement resolution using `ThreadPoolExecutor`. (0eceb1c)
- [x] Task: Update `BatchProcessor.run_batch` in `batch_process_settlements.py` to initiate location resolution in parallel. (3437751)
- [x] Task: Refactor `run_batch` logic to submit matching pipeline tasks immediately after each location is resolved. (3437751)
- [~] Task: Conductor - User Manual Verification 'Parallel Resolution and Pipeline Overlap' (Protocol in workflow.md)

## Phase 3: Simplified Query Variants [checkpoint: ]
- [ ] Task: Create unit tests for "Selective Fallback" query strategy in `SettlementMatcher.search_settlement`.
- [ ] Task: Refactor `search_settlement` to use full name primary query and parenthetical fallback.
- [ ] Task: Remove redundant variants (", Israel", dash-splitting) from `SettlementMatcher`.
- [ ] Task: Conductor - User Manual Verification 'Simplified Query Variants' (Protocol in workflow.md)
