# govmap_screenshot.py
import os
from playwright.sync_api import sync_playwright, TimeoutError
from shapely.geometry import LineString
from pyproj import Transformer

# --- Constants ---
GOVMAP_URL = "https://www.govmap.gov.il/"
SCREENSHOT_DIR = os.path.join(os.path.dirname(__file__), "screenshots")
os.makedirs(SCREENSHOT_DIR, exist_ok=True)

# --- Coordinate Transformation ---
# Transformer to convert from WGS84 (EPSG:4326) to Israel Transverse Mercator (ITM)
transformer = Transformer.from_crs("EPSG:4326", "EPSG:2039", always_xy=True)

def take_govmap_screenshot(geometry: LineString, osm_id: str):
    """
    Takes a screenshot of a street's location on govmap.gov.il.

    Args:
        geometry (LineString): The street's geometry, containing the coordinates.
        osm_id (str): The OpenStreetMap ID of the street, used for the filename.

    Returns:
        str: The path to the saved screenshot, or None if an error occurred.
    """
    if not isinstance(geometry, LineString):
        print(f"  -> ERROR: Invalid geometry for OSM ID {osm_id}. Expected LineString.")
        return None

    # --- 1. Get the center point of the street ---
    centroid = geometry.centroid
    lon, lat = centroid.x, centroid.y

    # --- 2. Convert coordinates to ITM ---
    try:
        itm_x, itm_y = transformer.transform(lon, lat)
    except Exception as e:
        print(f"  -> ERROR: Failed to transform coordinates for OSM ID {osm_id}: {e}")
        return None

    # --- 3. Take the screenshot using Playwright ---
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()

            # Navigate to govmap.il and wait for the search bar
            page.goto(GOVMAP_URL, timeout=60000)
            search_bar = page.wait_for_selector("#MapSearchBox", timeout=30000)

            # Enter the ITM coordinates into the search bar
            search_bar.fill(f"{int(itm_x)}, {int(itm_y)}")
            search_bar.press("Enter")

            # Wait for the map to load and zoom in
            page.wait_for_timeout(5000)  # Give the map time to settle

            # Take the screenshot
            screenshot_path = os.path.join(SCREENSHOT_DIR, f"{osm_id}.png")
            page.screenshot(path=screenshot_path)

            browser.close()
            print(f"  -> Successfully captured screenshot for OSM ID {osm_id}")
            return screenshot_path

    except TimeoutError:
        print(f"  -> ERROR: Timed out waiting for govmap.il to load for OSM ID {osm_id}.")
        return None
    except Exception as e:
        print(f"  -> ERROR: An unexpected error occurred during screenshot capture for OSM ID {osm_id}: {e}")
        return None
