# Plan: Persistent Diagnostics and Logging System

## Phase 1: Diagnostic Data Model & Collection
- [x] Task: Define the data structure for the diagnostic trace (Normalization, Scoring, AI).
- [x] Task: Write failing tests for capturing scoring components in `normalization.py`. (3308130)
- [x] Task: Implement scoring breakdown capture in `find_fuzzy_candidates`. (3308130)
- [x] Task: Write failing tests for AI metadata capture in `pipeline.py`. (3309336)
- [x] Task: Implement AI prompt and response logging in the AI resolution phase. (3309336)
- [x] Task: Conductor - User Manual Verification 'Phase 1: Diagnostic Data Model & Collection' (Protocol in workflow.md)

## Phase 2: Persistent Storage
- [x] Task: Write failing tests for JSON report generation in `pipeline.py`. (3311037)
- [x] Task: Implement logic to save the detailed diagnostic trace to a JSON file in `batch_reports/`. (3311037)
- [x] Task: Conductor - User Manual Verification 'Phase 2: Persistent Storage' (Protocol in workflow.md)

## Phase 3: HTML Integration & UI Polish
- [~] Task: Write failing tests for the updated tooltip/modal content in `generate_html.py`.
- [ ] Task: Update `generate_html.py` to embed diagnostic metadata into the SVG elements or a global data object.
- [ ] Task: Design and implement a polished "Debug Info" UI component (e.g., a Bootstrap Modal or Accordion) to display scores, normalization steps, and AI reasoning clearly.
- [ ] Task: Conductor - User Manual Verification 'Phase 3: HTML Integration & UI Polish' (Protocol in workflow.md)
