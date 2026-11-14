# Refactor branch notes

This branch includes:
- rapidfuzz instead of fuzzywuzzy
- osmnx in requirements
- data_loader.py, diagnostics.py, gemini_client.py
- pipeline orchestrator with logging
- map UI: dark theme, search, filters, zoom/pan
- fixed score_to_color RGB bug

Core modules already on branch: requirements.txt, data_loader.py, diagnostics.py, gemini_client.py, README.md.
Still need merge of: normalization.py, pipeline.py, generate_html.py from local artifacts.
