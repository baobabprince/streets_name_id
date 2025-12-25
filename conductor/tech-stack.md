# Technology Stack

## Overview
This project is primarily built using **Python**, leveraging a suite of powerful libraries for data processing, geospatial analysis, and fuzzy string matching. The architecture is centered around a **data pipeline/batch processing** approach, with static **HTML reports** generated for visualization.

## Core Components

### Programming Language
*   **Python:** The primary language for all scripting, data manipulation, and logic implementation.

### Libraries and Frameworks
*   **Data Manipulation & Analysis:**
    *   `pandas`: For high-performance, easy-to-use data structures and data analysis tools.
    *   `numpy`: Fundamental package for scientific computing with Python, used for numerical operations.
    *   `geopandas`: Extends pandas to allow spatial operations on geographic types, crucial for handling OSM data.
    *   `shapely`: For manipulation and analysis of planar geometric objects.
*   **Fuzzy Matching:**
    *   `rapidfuzz`: High-performance fuzzy string matching (replaces fuzzywuzzy).
    *   `concurrent.futures`: For multi-threaded and multi-process parallelization.
    *   `python-Levenshtein`: (Legacy) provides Levenshtein distance support.
    *   `Unicode range detection & algorithmic transliteration`: Custom logic for Arabic-to-Hebrew pre-processing.
*   **HTTP Requests:**
    *   `requests`: For making HTTP requests to external APIs (e.g., OSM, LAMAS data portal).
*   **AI/ML & NLP (Potential):**
    *   `transformers`: Likely used for embedding or reasoning models, potentially for local AI resolution.
    *   `torch`: An open-source machine learning framework, indicating potential use of deep learning models for AI resolution.
    *   `pillow`: Python Imaging Library, possibly used for image manipulation related to AI models or report generation.
*   **Reporting:**
    *   Static HTML generation (via custom Python scripts) for visual reports.

### Data Storage & Caching
*   `data/nominatim_cache.json`: JSON files are used for caching external API responses.

## Architecture Pattern
The project follows a **parallelized data pipeline/batch processing** architecture. A hybrid parallel model is used: multi-threaded I/O for settlement resolution and multi-process execution for street-level pipelines. AI resolution is batched at the city level for maximum efficiency.

## Key Integrations
*   **OpenStreetMap (OSM):** Data source for street geometries.
*   **LAMAS (Central Bureau of Statistics):** Data source for official street names and IDs.
*   **Google Gemini API:** External AI service for complex street name and mandatory settlement disambiguation.
