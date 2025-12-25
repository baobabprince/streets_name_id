# Plan: Refine Street Name Matching Accuracy and AI Resolution for Edge Cases

This plan is broken down into phases to systematically analyze, implement, and test improvements to the street name matching pipeline.

---

## Phase 1: Failure Analysis and Test Case Generation [checkpoint: 7066717]

### Tasks
- [x] Task: Analyze existing reports in `batch_reports/` and `unmatched_streets_report.md` to identify the top 10 most common failure patterns.
- [x] Task: Create a new test file `tests/test_edge_cases.py` based on the analysis.
- [x] Task: For each of the top 10 failure patterns, write a failing unit test in `tests/test_edge_cases.py` that reproduces the issue. [b13508e]
- [x] Task: Conductor - User Manual Verification 'Phase 1: Failure Analysis and Test Case Generation' (Protocol in workflow.md) [7066717]

---

## Phase 2: Algorithm and Prompt Refinement [checkpoint: bc09533]

### Tasks
- [x] Task: Refine the normalization rules in `normalization.py` to address issues found in Phase 1 analysis. [e04b7a1]
- [x] Task: Adjust the fuzzy matching thresholds in `normalization.py` to improve scoring for the identified edge cases. [c09bd4a]
- [x] Task: Enhance the context and prompts provided to the AI resolvers in local_ai_resolver.py and pipeline.py to improve decision-making. [ffed474]
- [x] Task: Conductor - User Manual Verification 'Phase 2: Algorithm and Prompt Refinement' (Protocol in workflow.md) [bc09533]

---

## Phase 3: Verification and Reporting [checkpoint: 98051d5]

### Tasks
- [x] Task: Run the full pipeline on a sample settlement (e.g., "Tel Aviv-Yafo") and verify that the new tests in `tests/test_edge_cases.py` now pass. [b857e7d]
- [x] Task: Compare the new output report with the previous one to quantify the reduction in "Needs AI" and "Missing" streets. [dcd1542]
    - Findings: In a sample run for Azor, unmatched unique streets were reduced from 9 to 6 (33% reduction). "Needs AI" segments were reduced by 40% (124 -> 74). The fix for synonym ambiguity (same LAMAS ID) was a key factor.
- [x] Task: Generate an updated `unmatched_streets_report.md` to reflect the improvements. [c462ec8]
- [x] Task: Conductor - User Manual Verification 'Phase 3: Verification and Reporting' (Protocol in workflow.md) [98051d5]
