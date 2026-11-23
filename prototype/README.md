# Comparison room — Petah Tikva prototype

An isolated desktop-first concept page. Existing generated settlement pages,
shared assets, the matching pipeline, and the national dashboard are unchanged.
The prototype is available at `prototype/index.html` on the same static host.

## Preview

From the repository root:

```sh
python scripts/serve_comparison_prototype.py --port 8000
```

Open `http://localhost:8000/` (redirects to `/prototype/`). The server binds to
`0.0.0.0` for Arena preview. Any static HTTP server can serve the repository;
open `/prototype/` in that case. No frontend build or Node dependency is needed.
Do not open the page with `file://`: it fetches its local data fixture.

The preview server sends `Cache-Control: no-store`, ignores old conditional
validators, and redirects `/` to the explicitly versioned preview URL. All
local script/style references have content-hash query parameters, including on
other static hosts. After editing JS/CSS, update them with:

```sh
python scripts/stamp_comparison_assets.py
```

A contract test verifies that the HTML references match the current asset
contents. The visible **version 2 / full table** marker identifies this revision.
A separate live status states whether the table handlers actually initialized;
it does not infer success from the HTML alone. The data fetch bypasses cache.

## What works

- Two coverage denominators: 885 / 989 OSM names (89.5%) and 863 / 1170
  official street codes (73.8%). Several OSM names can link to one official code.
- Overview metric cards and status-distribution segments filter the local map
  and findings table. The overview itself always retains settlement-wide totals.
- Search by either name or code; empty states; `/` to search. **All 989 OSM rows
  are rendered by default**, alphabetically, in a scrollable table with sticky
  headings. Optional page sizes are 10, 25, 50, 100 or all rows.
- Clicking **all OSM names** clears previous query/score filters, selects the
  complete OSM list, restores name-ascending order, and displays all rows. Other status tabs retain
  the active query/range. A visible filter strip explains any remaining filters
  and offers a clear-filters action that keeps the current category.
- Visible numeric match score (0–100) and a small meter in each findings row,
  plus a score in the selection panel. This is the algorithm's matching score
  (or candidate score for unmatched rows), **not** AI confidence or a probability.
- Inclusive min/max score filtering shared by map, table, query and status tabs;
  entering a bound switches to scored-only mode. All/scored-only/missing-only
  options distinguish missing scores from genuine zero. Invalid or inverted
  ranges show a validation message rather than silently changing the bounds.
- Ascending/descending sorting via the OSM name, official name, score and status
  column headers or the sort selector. The active header, arrow, and live sort
  summary show the applied order. The original findings-first order remains an
  option. Missing scores stay last in both score sort directions. CSV includes
  score columns and respects the current filter and sort order.
- An official code linked to multiple OSM names uses its **lowest available**
  score consistently for display, filtering, sorting and status/explanation.
  The full min/max range is available in its tooltip/details and CSV. Both linked
  geometries remain visible, as the official view's unit is the entire code.
- Leaflet map with the existing street geometry over an OSM tile background.
  Filtered-out streets remain as faint, noninteractive geographical context.
  Table initialization is independent of Leaflet: a failed map library cannot
  disable sorting, filtering, selection details or export.
- Selected street highlight and zoom. A prominent match-evidence panel appears
  inside findings, between the controls and the table, without a disclosure
  click. It shows the official code, method, name score, status, AI-reported
  confidence and OSM segment count in labeled cards. AI explanations directly
  name the linked official street/code; they do not imply human approval.
  Official names without a match have no invented location.
- Independent GovMap iframe with explicit selected-name search, city reset,
  and an external link preserving the current requested location/query.
- CSV export of **all filtered rows**, not only the current page, with a UTF-8
  BOM, quoting, and spreadsheet-formula escaping.

## Data provenance and limitations

`data.json` is a reproducible extraction from
`HTML/פתח_תקווה_roads.html`, not a fresh OSM/official data download or matching
run. Rebuild with:

```sh
# pyproj is also a transitive dependency of the project's geopandas stack
python -m pip install pyproj
python scripts/build_comparison_prototype.py
```

The source SVG stores local EPSG:3857 offsets; absolute coordinates are recovered
from the extent dimensions and the page's rounded EPSG:2039 centre. This adds
approximately metre-scale positioning uncertainty. Do not treat the fixture as
new survey-grade geometry. The build checks reconciliation of official totals.

The source contains 9 AI records **with final official IDs**. They are shown as
approved AI, not pending proposals, and included in approved coverage. A future
AI record without a final ID will display as a proposal and will not count as
approved. A score of 100 does not imply identical literal names or field
verification. No invented source or matching dates are displayed.

Unmatched official names come from the source page's diagnostic list; final
codes for these names are not available in that page and are not fabricated.
The official all-streets view combines 863 distinct approved codes with 307
unmatched names; the UI describes this distinction.

GovMap is a reference, not necessarily a rendering of the exact official
street-list dataset/version. Maps do not synchronize pan/zoom, and selecting a
row does not navigate GovMap until the explicit search action is used. GovMap
search results themselves depend on that service and may not resolve a name.

External browser requests: OSM tiles, GovMap, and an optional Google font (system
font fallback exists). The local Leaflet library is vendored, so the data,
geometry, filters, and table still work when external sources are blocked.
Tile failures show a warning. Cross-origin iframe load events cannot reliably
prove that a usable GovMap rendered, so after nine seconds a dismissible
**conditional** help card appears, rather than claiming successful loading or
diagnosing a specific error. Availability and frame-embedding permission must
be checked in the user's browser/on the eventual deployment host. Browser tests
intentionally block external requests and do not certify those services.

No write-back, review queue completion, or approval workflow is implemented.
The layout has basic narrow-screen wrapping but is intentionally desktop-first.

## Checks

```sh
python -m unittest discover -s tests -p 'test_comparison_prototype.py' -v
node --check prototype/comparison.js
node --test tests/test_comparison_scores.cjs
# Optional browser smoke test (preview server must already be running):
python -m pip install playwright
python -m playwright install chromium
python scripts/check_comparison_prototype.py
```

Browser tests cover metrics and reconciliation, filters, pagination, selection,
approved AI, missing official geometry, query search, CSV, keyboard shortcut,
map controls, score-column sorting in both directions, shared score range filters,
range validation, unavailable scores, grouped-code minimum semantics, ordered
CSV export, reset behavior, local operation when external requests fail, and JS errors.
`CHROMIUM_EXECUTABLE` can point to an already installed Chromium.

## Third-party asset

`vendor/leaflet/leaflet.js` and `leaflet.css`: Leaflet 1.9.4, downloaded from the
npm release tarball. The upstream BSD-2-Clause license is included at
`vendor/leaflet/LICENSE`. OSM attribution remains visible on the map.
