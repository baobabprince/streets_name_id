# Specification: Settlement Matching Fix (v2)

## Overview
This track addresses inaccuracies in the settlement matching process where small settlements (e.g., כפר רות, כפר ראש הנקרה) are incorrectly matched to larger ones with similar prefixes (e.g., כפר סבא). The goal is "perfect" matching: exactly one correct OSM entity per LAMAS settlement, with zero tolerance for geographic or semantic mismatches.

## Functional Requirements
1.  **Strict Geographic Filtering**: 
    -   All matches must fall within `ISRAEL_BOUNDS`.
    -   **Geographic Sanity Check**: If a LAMAS settlement is expected in a certain district (based on LAMAS metadata if available, or AI context), reject matches that are hundreds of kilometers away.
2.  **Name Integrity & Distinctive Word Matching**:
    -   **Core Name Verification**: The matched OSM name MUST contain the distinctive part of the LAMAS name. For "כפר רות", the word "רות" must be a high-confidence match in the result.
    -   **Prefix Hijack Prevention**: Explicitly prevent "Kfar Saba" or "Tel Aviv" from being accepted as a match for any query that includes additional distinctive tokens (like "Rut" or "Rosh HaNikra").
3.  **Strict Entity Type Filtering**:
    -   Reject OSM entities typed as `highway`, `street`, or buildings. 
    -   Prioritize `city`, `town`, `village`, `municipality`, `hamlet`.
4.  **AI Ambiguity Resolution (Mandatory for "Kfar" cases)**:
    -   If Nominatim returns multiple candidates, or if the top candidate's score is boosted only by "Importance" rather than "Name Similarity", invoke Gemini.
    -   The AI must verify that the specific distinctive name (e.g., "Rut") is the primary subject of the OSM entity.

## Acceptance Criteria
-   [ ] "כפר רות" matches to Kfar Rut (OSM ID 1375113 or similar) and NOT Kfar Saba.
-   [ ] "כפר ראש הנקרה" matches to Rosh HaNikra and NOT Kfar Saba.
-   [ ] No settlement matches fall outside the geographic boundaries of Israel.
-   [ ] "Israel" (or recognized district) must be present in the address data of every match.
-   [ ] The `SettlementMatcher` produces a single high-confidence match or `None`.

## Out of Scope
-   Modifying street-level matching logic within the city.
