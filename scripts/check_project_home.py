"""Browser checks for the project-facing index; run serve_project.py first.

Requires playwright. Optional PROJECT_URL and CHROMIUM_EXECUTABLE.
External requests are blocked intentionally; these checks verify local behavior.
"""
import re
import hashlib
import json
import os
import sys
from pathlib import Path
from urllib.parse import urljoin, urlsplit, parse_qs

from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from generate_index import aggregate, scan_settlements  # noqa: E402


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(executable_path=os.environ.get("CHROMIUM_EXECUTABLE"),
                                    args=["--no-sandbox", "--single-process", "--no-zygote", "--disable-dev-shm-usage"])
        context = browser.new_context(viewport={"width": 1440, "height": 1100})
        page = context.new_page()
        errors = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.route("https://**/*", lambda route: route.abort())
        url = os.environ.get("PROJECT_URL", "http://127.0.0.1:8000/")
        response = page.goto(url)
        assert response.status == 200
        assert "no-store" in response.headers.get("cache-control", "")
        expect(page.locator("h1")).to_contain_text("התאמת רחובות OSM")
        dates = page.locator(".snapshot-dates")
        expect(dates).to_contain_text("הדף נוצר")
        expect(dates.locator("time").first).to_have_attribute("datetime", re.compile(r"^\d{4}-\d{2}-\d{2}T"))
        expect(dates).to_contain_text("הרצת אצווה אחרונה")
        on_disk = aggregate(scan_settlements(str(ROOT / "HTML")))
        expect(dates).to_contain_text(f"מתוך {on_disk['cities']:,} דפי יישוב")
        expect(page.locator(".project-footer time")).to_be_visible()
        expect(page.locator("#city-table tbody tr")).to_have_count(12)
        payload = json.loads(page.locator("#dash-data").text_content())
        # The page reflects the pages on disk (python generate_index.py --data-out '').
        assert payload["totals"]["cities"] == len(payload["cities"]) == on_disk["cities"]
        assert payload["totals"]["ai_approved"] == on_disk["ai_approved"]
        assert payload["totals"]["ai_unknown"] == on_disk["ai_unknown"]
        assert payload["totals"]["open"] == payload["totals"]["unmatched"]
        expect(page.locator(".stats-context")).to_contain_text("לא ספירה של מזהים שכבר הוטמעו")
        expect(page.locator("#shown-count")).to_have_text("12")
        expect(page.locator("#clear-filters")).to_be_disabled()
        assert not page.evaluate("document.documentElement.scrollWidth > innerWidth")

        # Local styles/scripts are content-addressed, not stale unversioned URLs.
        for asset in page.locator('script[src],link[rel="stylesheet"][href^="assets/"]').evaluate_all("nodes => nodes.map(n => n.src || n.href)"):
            parts = urlsplit(asset)
            path = parts.path.lstrip("/")
            digest = parse_qs(parts.query)["v"][0]
            assert digest == hashlib.sha256((ROOT / path).read_bytes()).hexdigest()[:16]

        page.locator("#city-search").fill("פתח תקווה")
        expect(page.locator("#city-table tbody tr")).to_have_count(1)
        expect(page.locator("#city-table tbody tr")).to_contain_text("פתח תקווה")
        expect(page.locator("#results-count")).to_have_text("1")
        original_href = page.locator("#city-table .t-name a").get_attribute("href")
        assert page.request.get(urljoin(url, original_href)).status == 200
        page.locator("#city-search").fill("לאקייםבשוםיישוב1234")
        expect(page.locator("#explorer-empty")).to_be_visible()
        page.locator("#clear-filters").click()
        expect(page.locator("#city-table tbody tr")).to_have_count(12)
        page.locator("#load-more").click()
        expect(page.locator("#city-table tbody tr")).to_have_count(24)
        expect(page.locator("#shown-count")).to_have_text("24")
        page.locator('[data-tier="excellent"]').click()
        visible = page.locator("#city-table .t-coverage b").all_text_contents()
        assert visible and all(float(value.rstrip("%")) >= 90 for value in visible)
        page.locator("#clear-filters").click()
        page.locator("#sort-select").select_option("name:asc")
        expected = page.evaluate("cities => cities.map(c=>c.name).sort((a,b)=>a.localeCompare(b,'he')).slice(0,12)", payload["cities"])
        assert page.locator("#city-table .t-name a").all_text_contents() == expected
        page.locator("#sort-select").select_option("ai:desc")
        approved = [int(value.replace(",", "")) for value in page.locator("#city-table tbody tr td:nth-child(6)").all_text_contents()]
        assert approved == sorted(approved, reverse=True)
        assert approved[0] == max(city["ai_approved"] for city in payload["cities"])
        page.locator('[data-view="grid"]').click()
        expect(page.locator(".city-card")).to_have_count(12)
        expect(page.locator("#city-table-wrap")).to_be_hidden()
        expect(page.locator(".city-card").first).to_contain_text("עברו סינון")
        page.locator('[data-view="table"]').click()
        expect(page.locator("#city-table-wrap")).to_be_visible()
        page.locator("h1").click()
        page.keyboard.press("/")
        expect(page.locator("#city-search")).to_be_focused()

        ai_details = page.locator(".method-details details").nth(1)
        ai_details.locator("summary").click()
        expect(ai_details).to_contain_text("0.7")
        expect(ai_details).to_contain_text("75.0")
        expect(ai_details).to_contain_text("10")
        expect(page.locator(".formula")).to_contain_text("0.30 × ratio")
        interpretation = page.locator(".method-details details").nth(2)
        interpretation.locator("summary").click()
        expect(interpretation).to_contain_text(f"{on_disk['ai_unknown']:,}")
        expect(interpretation).to_contain_text("אישור של קהילת OSM")
        identity = page.locator(".method-details details").nth(3)
        identity.locator("summary").click()
        expect(identity).to_contain_text("city_code:official_code")
        for removed in ("תמונת אמת אחת", "קישור שאפשר לבדוק", "הקשר לפני הכול", "אפשר לבחון את ההחלטה", "קופסה שחורה"):
            assert removed not in page.locator("body").inner_text()
        expect(page.locator(".value-strip")).to_have_count(0)
        expect(page.locator(".hero-principles")).to_have_count(0)
        page.screenshot(path=".cache/project-home-checked.png", full_page=True)
        page.set_viewport_size({"width": 390, "height": 844})
        assert not page.evaluate("document.documentElement.scrollWidth > innerWidth")
        page.set_viewport_size({"width": 1440, "height": 1100})

        # The homepage sample reaches the real Petah Tikva settlement page, not a mock.
        expect(page.locator(".evidence-card")).to_contain_text("הרב יצחק טייב")
        page.locator(".example-link").click()
        expect(page.locator("#app-status")).to_have_attribute("data-state", "ready")
        expect(page.locator("#rows tr")).to_have_count(989)
        page.locator("#sort-score").click()
        expect(page.locator("#rows .score-value").first).to_have_text("9.5")
        assert not errors, errors
        browser.close()
        print("PASS: homepage messaging, real data, AI accounting, versioned assets, search/filter/sort, table/cards, load-more, method defaults, narrow layout and comparison-room navigation; no JS errors")


if __name__ == "__main__":
    main()
