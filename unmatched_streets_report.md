# Report on Unmatched Streets and Recommendations for Improvement

## 1. Summary of Findings

Analysis of the street matching process between OpenStreetMap (OSM) and Israel Land Administration (LAMAS) data reveals a significant number of unmatched streets. This report outlines the primary causes for these mismatches and provides concrete recommendations to improve the matching rate.

The core issue stems from variations in naming conventions, abbreviations, and word order between the two datasets. While the current pipeline has a good baseline normalization and fuzzy matching process, it can be significantly improved.

Based on the data for Lod and Elyakhin, the percentage of unmatched streets is considerable. For example, in Elyakhin, 3 out of 41 unique OSM streets (7.3%) were unmatched, and 6 out of 44 LAMAS streets (13.6%) were unmatched.

## 2. Analysis of Unmatched Streets

By examining the lists of unmatched streets from the generated HTML reports, several key patterns emerge:


*   **Partial Names and Full Names:** One dataset might use a full name while the other uses a surname or a more common short name.
    *   **Example:** OSM has `שלום שבזי` while LAMAS has just `שבזי`.
    *   **Example:** OSM might have `זאב ז'בוטינסקי` while LAMAS has `ז'בוטינסקי`.

*   **Spelling Variations and Typos:** Although the fuzzy matching catches some of these, more subtle variations or typos can lead to scores below the confidence threshold.
    *   **Example from Lod:** `הנרייטה סולד` (OSM) vs. `הנריטה סולד` (LAMAS) was successfully matched with a score of 96, but required AI review. This highlights that even minor spelling differences can prevent a confident match.

*   **Word Order:** The current fuzzy matching (`fuzz.ratio`) is sensitive to word order. A street named `מרדכי וחווה פרימן` in OSM would not match well with `פרימן מרדכי וחווה` in LAMAS.

*   **Unhandled Abbreviations:** The normalization script handles a list of common abbreviations, but it's not exhaustive. Unmatched lists likely contain other abbreviation patterns that are not being expanded.

*   **Aggressive Normalization:** The current `normalize_street_name` function removes street type prefixes (like `רחוב`, `שדרות`, `סמטה`). This can lead to ambiguity. For example, `רחוב הגפן` (Gefen Street) and `סמטת הגפן` (Gefen Alley) would both be normalized to `הגפן`, making them indistinguishable and potentially leading to incorrect matches if both exist in a city.

## 3. Recommendations for Improvement

To address the issues identified above, the following improvements to the matching pipeline are recommended.

### 3.1. Enhance the Normalization Process

File to modify: `normalization.py`




### 3.2. Improve the Fuzzy Matching Algorithm

File to modify: `normalization.py`

The current use of `fuzz.ratio` is good, but other algorithms in the `fuzzywuzzy` library are better suited for this task.

1.  **Use `token_sort_ratio` for Word Order Invariance:** To handle cases like `שלום שבזי` vs. `שבזי שלום`, `fuzz.token_sort_ratio` is ideal. It tokenizes the strings, sorts the tokens alphabetically, and then calculates the ratio.

2.  **Use `token_set_ratio` for Partial Name Matches:** For cases like `שלום שבזי` vs. `שבזי`, `fuzz.token_set_ratio` is highly effective. It finds common tokens and is robust to differences in string length.

    **Implementation Suggestion:**
    In the `find_fuzzy_candidates` function, calculate a weighted average of different fuzzy scores to get a more robust final score.

    ```python
    # In find_fuzzy_candidates, inside the loop
    score_ratio = fuzz.ratio(osm_name, lamas_name)
    score_token_sort = fuzz.token_sort_ratio(osm_name, lamas_name)
    score_token_set = fuzz.token_set_ratio(osm_name, lamas_name)

    # Weighted average
    score = (score_ratio * 0.4) + (score_token_sort * 0.3) + (score_token_set * 0.3)
    ```
    The weights can be tuned based on performance.

### 3.3. Other Recommendations

*   **Analyze Thresholds:** The `analyze_threshold.py` script should be used to analyze a sample of matched and unmatched streets to fine-tune the `confident_threshold` and `needs_ai_threshold` values. This can help reduce the number of cases that are unnecessarily sent to AI and increase the number of confident matches.

*   **Leverage Adjacency for Scoring:** The adjacency map is currently only used for AI context. This information could be used earlier in the process. If two streets are candidates for a match, and their adjacent streets also have high matching scores, this should increase the confidence in the match.

By implementing these changes, the street matching pipeline can become more robust, leading to a higher match rate and more accurate data.
