"""Browser smoke test of a generated settlement page (Petah Tikva).

Run the preview server first. Requires playwright.

    python scripts/serve_project.py --port 8000
    python scripts/check_settlement_page.py

Optional: COMPARISON_URL, CHROMIUM_EXECUTABLE, COMPARISON_SCREENSHOT.
External map/font requests are deliberately blocked: this checks local
functionality and fallbacks, NOT availability of OSM tiles or GovMap embedding.
"""
import re
import os
import csv
import io
import json
from pathlib import Path

from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[1]
PAGE = ROOT / "HTML/פתח_תקווה_roads.html"
DEFAULT_URL = "http://127.0.0.1:8000/HTML/%D7%A4%D7%AA%D7%97_%D7%AA%D7%A7%D7%95%D7%95%D7%94_roads.html"


def page_data():
    """Records embedded in the page under test (the table's source of truth)."""
    raw = PAGE.read_text(encoding="utf-8")
    records = re.search(r'<script id="street-records" type="application/json">(.*?)</script>', raw, re.S)
    return {"records": json.loads(records.group(1))}


def check_score_controls(page):
    fixture = page_data()
    def displayed_scores():
        return [float(text.replace(",", "")) for text in page.locator("#rows .score-value").all_text_contents()]
    def exported_rows():
        with page.expect_download() as info:
            page.locator("#export").click()
        content = Path(info.value.path()).read_text(encoding="utf-8-sig")
        return list(csv.reader(io.StringIO(content)))[1:]

    page.locator("#reset").click()
    page.locator("#page-size").select_option("10")
    expect(page.locator("#rows .score-value")).to_have_count(10)
    page.locator("#sort-score").click()
    expect(page.locator("#score-heading")).to_have_attribute("aria-sort", "ascending")
    scores = displayed_scores()
    assert scores == sorted(scores)
    assert scores[0] == min(r[4] for r in fixture["records"])
    page.locator("#sort-score").click()
    expect(page.locator("#sort-order")).to_have_value("score-desc")
    expect(page.locator("#score-heading")).to_have_attribute("aria-sort", "descending")
    assert displayed_scores() == [100] * 10

    page.locator("#score-min").fill("70")
    page.locator("#score-max").fill("90")
    expect(page.locator("#score-mode")).to_have_value("scored")
    expected = sum(70 <= r[4] <= 90 for r in fixture["records"])
    expect(page.locator("#map-count")).to_have_text(str(expected))
    # The map uses the same range as the table, not only the current page.
    assert page.locator('#osm-map .leaflet-overlay-pane path[style*="pointer-events: auto"]').count() == expected
    exported = exported_rows()
    scores = [float(row[3]) for row in exported]
    assert len(scores) == expected and all(70 <= score <= 90 for score in scores)
    assert scores == sorted(scores, reverse=True)
    page.locator("#next").click()
    page.locator("#score-min").fill("80")
    expect(page.locator("#prev")).to_be_disabled()
    page.locator("#rows button").first.click()
    expect(page.locator("#selected-score .score-value")).to_have_text(page.locator("#rows .score-value").first.inner_text())

    page.locator('#tabs [data-view="ai"]').click()
    expected = sum(r[0] == "ai" and 80 <= r[4] <= 90 for r in fixture["records"])
    expect(page.locator("#map-count")).to_have_text(str(expected))
    page.locator("#score-min").fill("95")
    expect(page.locator("#score-error")).to_be_visible()
    expect(page.locator("#score-min")).to_have_attribute("aria-invalid", "true")
    expect(page.locator("#map-count")).to_have_text("0")
    expect(page.locator("#export")).to_be_disabled()
    page.locator("#score-min").fill("-1")
    expect(page.locator("#score-error")).to_be_visible()
    page.locator("#score-min").fill("0")
    page.locator("#score-max").fill("101")
    expect(page.locator("#score-max")).to_have_attribute("aria-invalid", "true")
    page.locator("#clear-scores").click()
    expect(page.locator("#score-error")).to_be_hidden()
    expect(page.locator("#map-count")).to_have_text("9")
    expect(page.locator("#score-mode")).to_have_value("all")

    page.locator("#reset").click()
    page.locator("#page-size").select_option("10")
    page.locator('.metric-card[data-view="official-all"]').click()
    page.locator("#score-mode").select_option("missing")
    expect(page.locator("#map-count")).to_have_text("307")
    expect(page.locator("#score-min")).to_be_disabled()
    expect(page.locator("#rows .score-value")).to_have_count(0)
    expect(page.locator("#rows .score-missing")).to_have_count(10)
    page.locator("#clear-scores").click()
    expect(page.locator("#score-min")).to_be_enabled()
    expect(page.locator("#map-count")).to_have_text("1,170")
    page.locator("#sort-order").select_option("score-desc")
    exported = exported_rows()
    valid = [float(row[3]) for row in exported if row[3] != "לא זמין"]
    assert valid == sorted(valid, reverse=True)
    assert len(valid) == 863 and len(exported) == 1170
    assert all(row[3] == "לא זמין" for row in exported[-307:])
    page.locator("#sort-order").select_option("score-asc")
    exported = exported_rows()
    assert all(row[3] == "לא זמין" for row in exported[-307:])

    # Code 1125 has a 100-point match and an 85.7-point approved AI match.
    page.locator("#search").fill("1125")
    expect(page.locator("#map-count")).to_have_text("1")
    expect(page.locator("#rows .score-value")).to_have_text("85.7")
    expect(page.locator("#rows .status-tag")).to_have_text("AI מאושר")
    expect(page.locator("#score-filter-note")).to_contain_text("הציון הנמוך")
    page.locator("#rows button").click()
    expect(page.locator("#selected-score .score-value")).to_have_text("85.7")
    page.locator("#score-min").fill("90")
    expect(page.locator("#selection")).to_be_hidden()
    expect(page.locator("#map-count")).to_have_text("0")
    page.locator("#reset").click()
    expect(page.locator("#score-min")).to_have_value("")
    expect(page.locator("#sort-order")).to_have_value("name-asc")
    expect(page.locator("#map-count")).to_have_text("989")


def check_table_navigation_and_evidence(page):
    fixture = page_data()
    def expected_names(direction):
        return page.evaluate("""({names, direction}) => names.sort((a,b) => a.localeCompare(b,'he') * direction).slice(0,10)""",
                             {"names": [r[1] for r in fixture["records"]], "direction": direction})
    def table_names():
        return page.locator("#rows tr td:first-child").all_text_contents()[:10]

    page.locator("#reset").click()
    page.locator('#tabs [data-view="unmatched"]').click()
    page.locator("#search").fill("כוכבי")
    page.locator("#score-max").fill("90")
    page.locator("#sort-order").select_option("score-desc")
    expect(page.locator("#active-filters")).to_be_visible()
    page.locator('#tabs [data-view="all"]').click()
    expect(page.locator("#search")).to_have_value("")
    expect(page.locator("#score-max")).to_have_value("")
    expect(page.locator("#score-mode")).to_have_value("all")
    expect(page.locator("#active-filters")).to_be_hidden()
    expect(page.locator("#map-count")).to_have_text("989")
    expect(page.locator("#table-view-title")).to_have_text("כל שמות OSM · 989")
    expect(page.locator("#rows tr")).to_have_count(989)
    expect(page.locator("#page-size")).to_have_value("all")
    expect(page.locator("#sort-order")).to_have_value("name-asc")
    assert table_names() == expected_names(1)

    # Sort by the actual column headings, not only by the select widget.
    page.locator("#sort-name").click()
    expect(page.locator("#name-heading")).to_have_attribute("aria-sort", "descending")
    expect(page.locator("#sort-feedback")).to_contain_text("ת–א")
    assert table_names() == expected_names(-1)
    page.locator("#sort-name").click()
    assert table_names() == expected_names(1)
    for key in ["official", "status"]:
        page.locator("#sort-" + key).click()
        expect(page.locator("#sort-order")).to_have_value(key + "-asc")
        expect(page.locator("#" + key + "-heading")).to_have_attribute("aria-sort", "ascending")
        first_asc = table_names()
        column = 2 if key == "official" else 4
        cell_values = page.locator(f"#rows tr td:nth-child({column})").all_text_contents()
        assert cell_values == page.evaluate("values => [...values].sort((a,b) => a.localeCompare(b,'he'))", cell_values)
        page.locator("#sort-" + key).click()
        expect(page.locator("#" + key + "-heading")).to_have_attribute("aria-sort", "descending")
        assert table_names() != first_asc
        cell_values = page.locator(f"#rows tr td:nth-child({column})").all_text_contents()
        assert cell_values == page.evaluate("values => [...values].sort((a,b) => b.localeCompare(a,'he'))", cell_values)

    page.locator("#page-size").select_option("25")
    expect(page.locator("#rows tr")).to_have_count(25)
    page.locator("#next").click()
    expect(page.locator("#page-info")).to_have_text("26–50 מתוך 989")
    page.locator("#page-size").select_option("all")
    expect(page.locator("#rows tr")).to_have_count(989)
    expect(page.locator("#page-info")).to_have_text("כל 989 השורות מוצגות בטבלה")
    expect(page.locator("#next")).to_be_disabled()
    for direction in ["asc", "desc"]:
        page.locator("#sort-order").select_option("score-" + direction)
        scores = [float(v) for v in page.locator("#rows .score-value").all_text_contents()]
        assert len(scores) == 989
        assert scores == sorted(scores, reverse=direction == "desc")
        expect(page.locator("#sort-feedback")).to_contain_text("ציון")

    page.locator("#reset").click()
    page.locator('#tabs [data-view="ai"]').click()
    page.locator("#rows button").first.click()
    # Technical evidence is visible, inside findings, with no disclosure click.
    expect(page.locator("#findings #selection")).to_be_visible()
    expect(page.locator("#technical-title")).to_be_visible()
    expect(page.locator("#technical")).to_be_visible()
    expect(page.locator("#technical details")).to_have_count(0)
    expect(page.locator("#technical")).to_contain_text("קוד רחוב למ״ס")
    expect(page.locator("#technical")).to_contain_text("שיטת התאמה")
    expect(page.locator("#technical")).to_contain_text("ביטחון שדיווח AI")
    expect(page.locator("#selected-reason")).to_contain_text("ה־AI קישר את הרחוב לשם למ״ס")
    expect(page.locator("#selected-reason")).not_to_contain_text("בנתוני העמוד הקיים")
    technical = page.locator("#technical .technical-item").all()
    values = {item.locator("dt").inner_text(): item.locator("dd").inner_text() for item in technical}
    assert values["קוד רחוב למ״ס"].isdigit()
    assert values["שיטת התאמה"] == "AI"
    assert 0 <= float(values["ביטחון שדיווח AI (0–1)"]) <= 1
    assert values["ציון התאמת שמות (0–100)"] == page.locator("#selected-score .score-value").inner_text()
    page.locator("#sort-score").click()
    expect(page.locator("#selection")).to_be_hidden()
    expect(page.locator("#sort-order")).to_have_value("score-asc")
    page.locator("#search").fill("לאקיים123")
    expect(page.locator("#active-filters")).to_be_visible()
    page.locator("#clear-table-filters").click()
    expect(page.locator("#map-count")).to_have_text("9")
    expect(page.locator('#tabs [data-view="ai"]')).to_have_attribute("aria-pressed", "true")
    page.locator("#reset").click()


def check_loading_contract_and_map_independence(browser, page):
    url = os.environ.get("COMPARISON_URL", DEFAULT_URL)
    response = page.request.get(url, headers={"If-Modified-Since": "Wed, 01 Jan 2031 00:00:00 GMT"})
    assert response.status == 200
    assert "no-store" in response.headers.get("cache-control", "")
    for src in page.locator('script[src]').evaluate_all("scripts => scripts.map(script => script.src)"):
        assert "?v=" in src, src
        asset = page.request.get(src)
        assert asset.status == 200
        assert "no-store" in asset.headers.get("cache-control", "")

    # Failure of the map library must not leave a table with inert controls.
    isolated = page.context.new_page()
    errors = []
    isolated.on("pageerror", lambda error: errors.append(str(error)))
    isolated.route("https://**/*", lambda route: route.abort())
    isolated.route("**/vendor/leaflet/leaflet.js*", lambda route: route.abort())
    isolated.goto(url)
    expect(isolated.locator("#app-status")).to_have_attribute("data-state", "ready")
    expect(isolated.locator("#rows tr")).to_have_count(989)
    expect(isolated.locator("#tile-warning")).to_contain_text("הטבלה, החיפוש והמיון פעילים")
    first_name_before = isolated.locator("#rows tr td:first-child").first.inner_text()
    isolated.locator("#sort-score").click()
    scores = [float(v) for v in isolated.locator("#rows .score-value").all_text_contents()]
    assert scores == sorted(scores)
    assert scores[0] == 9.5 and scores[-1] == 100
    assert isolated.locator("#rows tr td:first-child").first.inner_text() != first_name_before
    expect(isolated.locator("#first-row-summary")).to_contain_text("9.5")
    isolated.locator("#sort-score").click()
    scores = [float(v) for v in isolated.locator("#rows .score-value").all_text_contents()]
    assert scores == sorted(scores, reverse=True)
    isolated.locator("#search").fill("כוכבי")
    assert 0 < isolated.locator("#rows tr").count() < 989
    isolated.locator('#tabs [data-view="all"]').click()
    expect(isolated.locator("#rows tr")).to_have_count(989)
    isolated.locator("#rows button").first.click()
    expect(isolated.locator("#technical")).to_be_visible()
    isolated.locator("#reset").click()
    expect(isolated.locator("#rows tr")).to_have_count(989)
    assert not errors, errors
    isolated.close()


def main():
    with sync_playwright() as p:
        executable = os.environ.get("CHROMIUM_EXECUTABLE")
        browser = p.chromium.launch(
            executable_path=executable,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--single-process", "--no-zygote"],
        )
        context = browser.new_context(viewport={"width": 1440, "height": 1100})
        page = context.new_page()
        errors = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.route("https://**/*", lambda route: route.abort())
        page.goto(os.environ.get("COMPARISON_URL", DEFAULT_URL))
        expect(page.locator("#rows tr")).to_have_count(989)
        expect(page.locator("#app-status")).to_have_attribute("data-state", "ready")
        expect(page.locator("#page-size")).to_have_value("all")
        expect(page.locator("#page-info")).to_have_text("כל 989 השורות מוצגות בטבלה")
        page.locator("#page-size").select_option("10")
        expect(page.locator("#rows tr")).to_have_count(10)
        expect(page.locator("#fatal")).to_be_hidden()
        expect(page.locator("#osm-total")).to_have_text("989")
        expect(page.locator("#official-total")).to_have_text("1,170")
        expect(page.locator("#osm-coverage")).to_have_text("89.5%")
        expect(page.locator("#official-coverage")).to_have_text("73.8%")
        assert page.locator("#osm-map .leaflet-overlay-pane path").count() == 989
        assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")

        page.locator("#next").click()
        expect(page.locator("#page-info")).to_have_text("11–20 מתוך 989")
        page.locator("#prev").click()
        expect(page.locator("#page-info")).to_have_text("1–10 מתוך 989")

        page.locator('#tabs [data-view="unmatched"]').click()
        expect(page.locator("#map-count")).to_have_text("104")
        page.locator("#rows button").first.click()
        expect(page.locator("#selection")).to_be_visible()
        expect(page.locator("#selected-reason")).to_contain_text("מועמד בלבד")
        expect(page.locator("#locate-osm")).to_be_enabled()
        expect(page.locator("#gov-context")).to_contain_text("תצוגת היישוב")
        page.locator("#locate-gov").click()
        expect(page.locator("#gov-context")).to_contain_text("חיפוש:")
        assert "q=" in page.locator("#gov-external").get_attribute("href")
        page.locator("#gov-city").click()
        assert "q=" not in page.locator("#gov-external").get_attribute("href")

        page.locator('#tabs [data-view="official-missing"]').click()
        expect(page.locator("#selection")).to_be_hidden()
        expect(page.locator("#map-count")).to_have_text("307")
        expect(page.locator("#no-geometry")).to_be_visible()
        page.locator("#rows button").first.click()
        expect(page.locator("#locate-osm")).to_be_disabled()
        expect(page.locator("#selected-reason")).to_contain_text("אין בנתונים האלה מיקום מזוהה")

        page.locator('#tabs [data-view="ai"]').click()
        expect(page.locator("#map-count")).to_have_text("9")
        expect(page.locator("#rows tr")).to_have_count(9)
        expect(page.locator("#rows .status-tag").first).to_have_text("AI מאושר")
        page.locator("#rows button").first.click()
        expect(page.locator("#selected-reason")).to_contain_text("ה־AI קישר")
        expect(page.locator('#tabs [data-view="geo-review"]')).to_have_count(0)
        expect(page.locator('#tabs button')).to_have_count(4)
        # Exercise the empty state without relying on the removed legacy tab.
        page.locator("#score-mode").select_option("missing")
        expect(page.locator("#empty")).to_be_visible()
        expect(page.locator("#export")).to_be_disabled()
        page.locator("#clear-scores").click()

        page.locator('.metric-card[data-view="official-all"]').click()
        expect(page.locator("#map-count")).to_have_text("1,170")
        page.locator('.metric-card[data-view="official-approved"]').click()
        expect(page.locator("#map-count")).to_have_text("863")
        page.locator('.metric-card[data-view="approved"]').click()
        expect(page.locator("#map-count")).to_have_text("885")
        page.locator('.metric-card[data-view="all"]').click()

        page.locator("#search").fill("לאקייםבשוםרחוב1234")
        expect(page.locator("#rows tr")).to_have_count(0)
        expect(page.locator("#empty")).to_be_visible()
        page.locator("#empty-reset").click()
        page.locator("#search").fill("כוכבי")
        assert page.locator("#rows tr").count() > 0
        expect(page.locator("#rows")).to_contain_text("כוכבי")
        with page.expect_download() as download_info:
            page.locator("#export").click()
        download = download_info.value
        content = Path(download.path()).read_text(encoding="utf-8-sig")
        assert '"שם OSM"' in content and "כוכבי" in content
        assert len(content.splitlines()) > 1
        page.locator("#reset").click()
        page.locator("#layers-toggle").click()
        expect(page.locator("#layers-toggle")).to_have_attribute("aria-pressed", "false")
        page.locator("#layers-toggle").click()
        page.locator('#legend button').first.click()
        expect(page.locator("#map-count")).to_have_text("832")
        page.locator("#reset").click()
        expect(page.locator(".source-meta")).to_contain_text(re.compile(r"הדף נוצר \d{2}\.\d{2}\.\d{4}, \d{2}:\d{2}"))
        expect(page.locator(".source-meta")).to_contain_text("נתוני OSM:")
        expect(page.locator(".source-meta")).to_contain_text("נתוני למ״ס:")
        expect(page.locator("h1")).to_have_text("פתח תקווה")
        assert "?" not in page.locator("#osm-total").inner_text()
        page.locator("h1").click()
        page.keyboard.press("/")
        expect(page.locator("#search")).to_be_focused()
        expect(page.locator("#tile-warning")).to_be_visible()
        expect(page.locator("#gov-help")).to_be_visible(timeout=12000)
        page.locator("#dismiss-gov-help").click()
        expect(page.locator("#gov-help")).to_be_hidden()
        check_table_navigation_and_evidence(page)
        check_score_controls(page)
        check_loading_contract_and_map_independence(browser, page)
        page.screenshot(path=os.environ.get("COMPARISON_SCREENSHOT", ".cache/settlement-page.png"), full_page=True)
        assert not errors, errors
        browser.close()
        print("PASS: metrics, 2-way coverage, filters, pagination, selection, AI approval, official missing geometry, search, CSV, keyboard shortcut, map controls, external-source fallback, score display/filter/sort/export, missing scores, grouped-code scoring, all-names reset, all column sorts, page sizes, visible technical evidence, default full table, versioned assets/no-store, sortable table without Leaflet; no JS errors")


if __name__ == "__main__":
    main()
