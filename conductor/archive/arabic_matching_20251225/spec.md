# Specification: Arabic Street Name Matching Improvement

## Overview
The goal of this track is to significantly improve the matching accuracy of Arabic street names against official LAMAS Hebrew street names. This will be achieved by introducing a dedicated pre-processing layer that detects Arabic text, normalizes it, transliterates it into Hebrew characters, and uses AI for a final refinement "polish" before the standard fuzzy matching engine takes over.

## Functional Requirements
1.  **Language Detection:** Implement a mechanism to detect Arabic characters within street names using Unicode range checks (U+0600 to U+06FF).
2.  **Normalization:** Develop logic to handle Arabic-specific prefixes and suffixes (e.g., stripping or standardizing "Al-", "El-", "Ash-").
3.  **Algorithmic Transliteration:** Create a mapping and logic to convert Arabic script into Hebrew script based on phonetic and linguistic rules.
4.  **AI-Driven Final Polish:** Integrate the Gemini API to review the output of the algorithmic transliteration. The AI should correct phonetic errors, handle context-specific variations, and produce a high-quality Hebrew candidate.
5.  **Pipeline Integration:** Insert this pre-processing stage into the existing matching pipeline. Arabic names should be transformed into Hebrew *before* entering the fuzzy matching phase.

## Non-Functional Requirements
*   **Accuracy:** The transliteration and AI polish should prioritize phonetic accuracy to maximize fuzzy matching scores.
*   **Performance:** AI calls should be managed efficiently to avoid excessive latency or cost, potentially through batching or caching.
*   **Maintainability:** Transliteration rules and AI prompts should be clearly documented and easy to update.

## Acceptance Criteria
*   Street names containing Arabic characters are successfully detected and processed.
*   Arabic names like "شارע السلطان" are correctly transformed (e.g., to "רחוב הסולטן") and matched to their LAMAS equivalents.
*   The system shows a measurable improvement in match rates for settlements with high Arabic street name presence.
*   AI "Final Polish" successfully corrects at least 80% of common algorithmic transliteration artifacts in a test set.

## Out of Scope
*   Full translation of non-street-name entities.
*   Modification of the core fuzzy matching algorithm itself.
*   Support for languages other than Arabic, Hebrew, and English.
