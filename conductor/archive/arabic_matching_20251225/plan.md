# Plan: Arabic Street Name Matching Improvement

This plan outlines the steps to introduce an Arabic-to-Hebrew pre-processing layer to improve street name matching accuracy.

## Phase 1: Language Detection & Normalization [checkpoint: 61e18a7]
- [x] Task: Implement `is_arabic` detection logic in `normalization.py` (using Unicode ranges). 44d791e
- [x] Task: Create unit tests for Arabic language detection. 44d791e
- [x] Task: Implement Arabic prefix/suffix normalization (e.g., handling "Al-", "El-", "Al-") in `normalization.py`. (310e4c1)
- [x] Task: Create unit tests for Arabic normalization. (3368870)
- [x] Task: Conductor - User Manual Verification 'Language Detection & Normalization' (Protocol in workflow.md)

## Phase 2: Algorithmic Transliteration [checkpoint: 37b144f]
- [x] Task: Develop a phonetic mapping table for Arabic-to-Hebrew transliteration. (3371346)
- [x] Task: Implement the `transliterate_arabic_to_hebrew` function. (3371346)
- [x] Task: Create comprehensive unit tests for transliteration with various Arabic street name examples. (3371346)
- [x] Task: Conductor - User Manual Verification 'Algorithmic Transliteration' (Protocol in workflow.md)

## Phase 3: AI-Driven Final Polish [checkpoint: 69afb47]
- [x] Task: Implement `polish_transliteration_with_ai` using the Gemini API. (3373494)
- [x] Task: Design and refine the AI prompt to handle phonetic corrections and context-specific mapping. (3373494)
- [x] Task: Add caching/memoization for AI results to `normalization.py` or a dedicated cache module. (3373494)
- [x] Task: Create tests for the AI refinement logic (using mocks for the API). (3373494)
- [x] Task: Conductor - User Manual Verification 'AI-Driven Final Polish' (Protocol in workflow.md)

## Phase 4: Pipeline Integration & End-to-End Testing [checkpoint: ]
- [x] Task: Integrate the Arabic pre-processing flow into the main matching pipeline (e.g., in `pipeline.py` or `settlement_matcher.py`). (3373494)
- [x] Task: Update the `process_street` logic to apply Arabic pre-processing if Arabic text is detected. (3373494)
- [x] Task: Create integration tests using real Arabic street names from OSM data to verify improved matching against LAMAS. (3412598)
- [x] Task: Conductor - User Manual Verification 'Pipeline Integration & End-to-End Testing' (Protocol in workflow.md)
