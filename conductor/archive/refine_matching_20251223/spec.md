# Spec: Refine Street Name Matching Accuracy and AI Resolution for Edge Cases

## 1. Overview
This track focuses on improving the core logic of the street name matching pipeline. The goal is to increase the rate of high-confidence automatic matches and enhance the effectiveness of the AI resolution for ambiguous cases. This will be achieved by analyzing existing failure reports, refining the fuzzy matching thresholds and algorithms, and improving the prompts and context provided to the AI models.

## 2. Key Objectives
- **Reduce False Positives/Negatives:** Minimize the number of incorrectly matched or missed streets.
- **Improve Fuzzy Matching:** Fine-tune the `fuzzywuzzy` thresholds and potentially explore alternative scoring methods to better handle common Hebrew name variations.
- **Enhance AI Prompts:** Optimize the prompts sent to both local and cloud AI models to provide more decisive context, including better utilization of adjacent street data.
- **Analyze Edge Cases:** Systematically review reports in `batch_reports/` and `unmatched_streets_report.md` to identify recurring patterns of failure.
- **Add Robust Testing:** Introduce new test cases that specifically target the identified edge cases and failure patterns to prevent regressions.

## 3. Scope
### In Scope
- Modifying `normalization.py` to handle newly identified abbreviation or synonym patterns.
- Adjusting scoring thresholds in `settlement_matcher.py` and `pipeline.py`.
- Updating the AI context generation and prompts in `local_ai_resolver.py` and the main pipeline logic that calls the Gemini API.
- Creating a new test file, e.g., `tests/test_edge_cases.py`, with tests derived from real-world failure examples.
- Analyzing output files in `batch_reports/` to gather data for improvements.

### Out of Scope
- Major architectural changes to the pipeline.
- Replacing the core fuzzy matching or AI libraries.
- Adding new data sources beyond OSM and LAMAS.
- Building a new user interface for manual correction.

## 4. Success Metrics
- A 10% reduction in the number of streets categorized as "Needs AI" or "Missing" in a sample batch run (e.g., for "Tel Aviv-Yafo").
- Successful resolution of at least 75% of the manually identified top 10 recurring failure patterns when re-running the pipeline.
- All new and existing tests passing.
