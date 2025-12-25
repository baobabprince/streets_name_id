# Plan: Settlement Matching Accuracy Fix

## Phase 1: Baseline & Reproduction [checkpoint: 4f255af]
- [x] Task: Create reproduction unit tests in `tests/test_settlement_matcher.py` that demonstrate the "Kfar Saba hijacking" bug for "כפר רות" and "כפר ראש הנקרה". (3293444)
- [x] Task: Conductor - User Manual Verification 'Phase 1: Baseline & Reproduction' (Protocol in workflow.md)

## Phase 2: Deterministic Filtering Logic
- [x] Task: Update `SettlementMatcher._validate_result` to strictly enforce "Israel" or recognized districts in the address data. (4f255af)
- [x] Task: Implement strict entity type filtering in `SettlementMatcher` to reject `highway`, `street`, and `building` types. (4f255af)
- [x] Task: Implement "Distinctive Word Matching" logic to ensure tokens like "רות" or "ראש הנקרה" are present in the OSM name, preventing "Kfar" from matching "Kfar Saba". (3295602)
- [x] Task: Conductor - User Manual Verification 'Phase 2: Deterministic Filtering Logic' (Protocol in workflow.md)

## Phase 3: AI Ambiguity Resolution
- [x] Task: Update `SettlementMatcher.search_settlement` to trigger AI resolution via Gemini when multiple valid candidates exist or when distinctive word similarity is below 90%. (3295602)
- [x] Task: Implement the AI prompt logic to specifically evaluate and select the single best geographic settlement match. (3295602)
- [x] Task: Verify that all reproduction tests from Phase 1 now pass with correct OSM matches. (3296899)
- [x] Task: Conductor - User Manual Verification 'Phase 3: AI Ambiguity Resolution' (Protocol in workflow.md)
