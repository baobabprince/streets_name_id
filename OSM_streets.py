import logging
import math
import os
from typing import Any, Callable

import osmnx as ox

from provenance import utc_now
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString, box

from settlement_names import (
    MAX_SETTLEMENT_BBOX_SPAN_DEG,
    MIN_SETTLEMENT_HALF_HEIGHT_LAT,
    MIN_SETTLEMENT_HALF_WIDTH_LON,
)

# ניתן להשתמש בשם יישוב או בתיבת גבולות (bounding box).
# ברירת מחדל מצומצמת כדי להימנע משליפת רשת הדרכים של מדינה שלמה.
place_name = "Tel Aviv-Yafo, Israel"
logger = logging.getLogger(__name__)

# A Nominatim/PBF place node can have a tiny or zero-area bbox.  Use a usable
# search box for the Overpass fallback instead of geocoding its display name
# again (which often fails for small/unrecognised settlements).
MIN_FALLBACK_HALF_LAT = MIN_SETTLEMENT_HALF_HEIGHT_LAT
MIN_FALLBACK_HALF_LON = MIN_SETTLEMENT_HALF_WIDTH_LON
FALLBACK_EXPANSION_FACTOR = 1.6

def is_hebrew(text):
    """
    בדיקה אם טקסט מכיל תווים עבריים.
    מחזיר True אם יש לפחות תו עברי אחד בטקסט.
    """
    if pd.isna(text) or text is None:
        return False
    text = str(text)
    # טווח תווים עבריים ב-Unicode: U+0590 to U+05FF
    return any('\u0590' <= char <= '\u05FF' for char in text)

def _bbox_bounds(place: dict, expansion: float = 1.0):
    """Return a usable ``(min_lon, min_lat, max_lon, max_lat)`` box.

    ``boundingbox`` from Nominatim is ``(min_lat, max_lat, min_lon, max_lon)``.
    Tiny/degenerate boxes are expanded, and huge ones are capped so a fallback
    Overpass query cannot accidentally request a whole district.
    """
    bbox = place.get("boundingbox")
    if not bbox or len(bbox) != 4:
        return None
    try:
        min_lat, max_lat, min_lon, max_lon = (float(value) for value in bbox)
    except (TypeError, ValueError):
        return None
    if not all(math.isfinite(value) for value in (min_lat, max_lat, min_lon, max_lon)):
        return None
    if min_lat > max_lat:
        min_lat, max_lat = max_lat, min_lat
    if min_lon > max_lon:
        min_lon, max_lon = max_lon, min_lon
    center_lat = (min_lat + max_lat) / 2
    center_lon = (min_lon + max_lon) / 2
    half_lat = max((max_lat - min_lat) / 2, MIN_FALLBACK_HALF_LAT) * expansion
    half_lon = max((max_lon - min_lon) / 2, MIN_FALLBACK_HALF_LON) * expansion
    max_half = MAX_SETTLEMENT_BBOX_SPAN_DEG / 2
    half_lat, half_lon = min(half_lat, max_half), min(half_lon, max_half)
    return (center_lon - half_lon, center_lat - half_lat, center_lon + half_lon, center_lat + half_lat)


def _bbox_polygon(place: dict, expansion: float = 1.0):
    bounds = _bbox_bounds(place, expansion)
    return box(*bounds) if bounds else None


def _graph_has_edges(G) -> bool:
    try:
        return len(G.edges) > 0
    except Exception:
        return True  # unknown graph object; let the caller inspect it


def _graph_by_osm_id(place: dict):
    """Fetch a graph from an exact OSM element id (no free-text geocoding)."""
    osm_prefix = {"node": "N", "way": "W", "relation": "R"}.get(
        str(place["osm_type"]).lower()
    )
    if not osm_prefix:
        raise ValueError(f"Unsupported OSM element type: {place['osm_type']}")
    place_gdf = ox.geocode_to_gdf(f"{osm_prefix}{place['osm_id']}", by_osmid=True)
    polygons = place_gdf.loc[
        place_gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"]), "geometry"
    ]
    if not polygons.empty:
        return ox.graph_from_polygon(polygons.iloc[0], network_type="all", simplify=True)
    return ox.graph_from_point(
        center_point=(float(place["lat"]), float(place["lon"])),
        dist=2500,
        network_type="all",
        simplify=True,
    )


def _graph_from_place_dict(place: dict):
    """Overpass fallback that never geocodes a whole place dict by mistake.

    ``ox.graph_from_place`` expects a string or an address-query dict.  Passing
    our settlement match dict makes OSMnx issue Nominatim queries for the dict
    keys, which is why small settlements used to fail with
    ``Nominatim API did not return a list of results``.  Use the already-known
    bbox/OSM id/coordinates instead.
    """
    strategies: list[tuple[str, Callable[[], Any]]] = []

    exact = _bbox_polygon(place)
    if exact is not None:
        strategies.append((
            "bbox",
            lambda polygon=exact: ox.graph_from_polygon(
                polygon, network_type="all", simplify=True
            ),
        ))

    expanded = _bbox_polygon(place, FALLBACK_EXPANSION_FACTOR)
    if expanded is not None and (exact is None or not expanded.equals(exact)):
        strategies.append((
            "expanded bbox",
            lambda polygon=expanded: ox.graph_from_polygon(
                polygon, network_type="all", simplify=True
            ),
        ))

    if place.get("osm_id") and place.get("osm_type"):
        strategies.append(("OSM id", lambda: _graph_by_osm_id(place)))

    if place.get("lat") is not None and place.get("lon") is not None:
        strategies.append((
            "point",
            lambda: ox.graph_from_point(
                center_point=(float(place["lat"]), float(place["lon"])),
                dist=2500,
                network_type="all",
                simplify=True,
            ),
        ))

    display_name = place.get("display_name")
    if display_name:
        strategies.append((
            "display name",
            lambda query=display_name: ox.graph_from_place(
                query, network_type="all", simplify=True
            ),
        ))

    errors = []
    for label, strategy in strategies:
        try:
            G = strategy()
            if G is not None and _graph_has_edges(G):
                logger.info("Overpass fallback succeeded via %s", label)
                return G
            errors.append(f"{label}: empty graph")
        except Exception as exc:
            errors.append(f"{label}: {exc}")
            logger.debug("Overpass fallback %s failed: %s", label, exc)
    raise RuntimeError("; ".join(errors) or "No usable fallback strategy")


def fetch_osm_street_data(place, use_pbf: bool = True):
    """
    מוריד את גרף הרחובות של האזור הנתון וממיר אותו
    ל-GeoDataFrame של קטעי רחובות (Ways/Edges).
    משתמש בקובץ PBF מקומי כברירת מחדל למהירות ואמינות מקסימלית.
    """
    logger.info("Fetching OSM street graph for %s...", place)

    # 0. נסיון שליפה מקובץ PBF מקומי תחילה (ללא תלות ברשת וללא חסימות Overpass)
    if use_pbf:
        try:
            from osm_pbf_loader import fetch_pbf_street_data
            pbf_gdf = fetch_pbf_street_data(place)
            if pbf_gdf is not None and not pbf_gdf.empty:
                try:
                    from osm_pbf_loader import national_streets_provenance
                    pbf_gdf.attrs["provenance"] = national_streets_provenance()
                except Exception as exc:  # provenance is informative only
                    logger.debug("Could not read PBF provenance: %s", exc)
                logger.info(
                    "Successfully loaded %s road segments from local PBF for %s",
                    len(pbf_gdf),
                    place,
                )
                return pbf_gdf
            if os.environ.get("OSM_PBF_ONLY") == "1":
                logger.info(
                    "Local PBF returned no named streets for %s (OSM_PBF_ONLY=1; skipping Overpass).",
                    place,
                )
                return None
            logger.info("Local PBF did not return streets for %s; attempting Overpass...", place)
        except Exception as pbf_err:
            logger.warning("Local PBF extraction encountered error (%s); falling back to Overpass", pbf_err)

    # הגדרת התגים הנוספים שאנו רוצים לשלוף, במיוחד 'name:he'
    if "name:he" not in ox.settings.useful_tags_way:
        ox.settings.useful_tags_way.append("name:he")

    # 1. Download and simplify the full street network for the requested place.
    overpass_retrieved_at = utc_now()
    try:
        if isinstance(place, dict):
            G = _graph_from_place_dict(place)
        else:
            G = ox.graph_from_place(place, network_type="all", simplify=True)
    except Exception as e:
        logger.exception("Failed to fetch OSM street graph for %s: %s", place, e)
        return None

    # 2. המרת קטעי הדרך (Edges) ל-GeoDataFrame
    osm_gdf = ox.graph_to_gdfs(G, nodes=False, edges=True)
    
    # 3. ניקוי ועיבוד ראשוני של ה-GeoDataFrame:
    # שמירת עמודות רלוונטיות, במיוחד שמות הרחובות (name)
    
    # oxmnx משתמש ב-u, v, key כמזהה של כל קטע. ניצור osm_id פשוט לשם נוחות.
    osm_gdf = osm_gdf.reset_index()
    osm_gdf['osm_id'] = (
        osm_gdf['u'].astype(str)
        + "-"
        + osm_gdf['v'].astype(str)
        + "-"
        + osm_gdf['key'].astype(str)
    )
    
    # נבחר את השדות החיוניים להמשך העבודה
    if "name" not in osm_gdf.columns:
        osm_gdf["name"] = pd.NA
    if "highway" not in osm_gdf.columns:
        osm_gdf["highway"] = pd.NA
    cols_to_keep = ['osm_id', 'name', 'highway', 'geometry']
    if 'name:he' in osm_gdf.columns:
        cols_to_keep.append('name:he')

    osm_gdf = osm_gdf[cols_to_keep].copy()
    
    # נוודא ששם העמודה הוא 'osm_name' ונוסיף 'city' (שנצטרך לחשב בהמשך)
    osm_gdf = osm_gdf.rename(columns={'name': 'osm_name'})
    
    # ב-OSM, שם הרחוב (name) יכול להיות מחרוזת בודדת או רשימה של שמות.
    # נטפל בכך כדי לקבל מחרוזת אחידה
    osm_gdf['osm_name'] = osm_gdf['osm_name'].apply(
        lambda value: value[0] if isinstance(value, list) and value else value
    )
    if 'name:he' in osm_gdf.columns:
        osm_gdf['name:he'] = osm_gdf['name:he'].apply(
            lambda value: value[0] if isinstance(value, list) and value else value
        )
    
    # 4. עדיפות לשם העברי: אם השם הראשי אינו בעברית ויש name:he, נשתמש בו
    # שמירת השם המקורי לצורך דיאגנוסטיקה
    if 'name:he' in osm_gdf.columns:
        osm_gdf['osm_name_original'] = osm_gdf['osm_name'].copy()
        
        has_hebrew_main = osm_gdf['osm_name'].map(is_hebrew)
        valid_hebrew_name = osm_gdf['name:he'].notna() & osm_gdf['name:he'].map(is_hebrew)
        use_hebrew_name = ~has_hebrew_main & valid_hebrew_name
        osm_gdf.loc[use_hebrew_name, 'osm_name'] = osm_gdf.loc[use_hebrew_name, 'name:he']
        logger.info("Preferred Hebrew names for %s road segments", use_hebrew_name.sum())
    else:
        # אם אין name:he, השם המקורי זהה לשם הראשי
        osm_gdf['osm_name_original'] = osm_gdf['osm_name'].copy()

    # 4. כיווץ ה-GeoDataFrame לדרכים בלבד (נאבד חלק מהתכונות של הגרף, אך זה מתאים למיפוי)
    logger.info("Fetched %s OSM road segments for %s", len(osm_gdf), place)
    osm_gdf.attrs["provenance"] = {"source": "overpass", "retrieved_at": overpass_retrieved_at}
    return osm_gdf

# טעינת נתוני OSM (יכול לקחת מספר דקות!)
# osm_gdf = fetch_osm_street_data(place_name)
# if osm_gdf is not None:
#     print("\n--- נתוני OSM ששולפו ---")
#     print(osm_gdf.head())
#     print(f"מערכת קואורדינטות (CRS): {osm_gdf.crs}")


if __name__ == "__main__":
    # ---
    # הדמיה (Mock Data) אם השליפה נכשלת או לצורך בדיקה מהירה:
    # ---
    if 'osm_gdf' not in locals():
        print("יצירת GeoDataFrame מדומה עבור OSM...")
        from shapely.geometry import LineString
        data_osm = {
            'osm_id': ['5001-A', '5002-B', '5003-C', '5004-D'],
            'osm_name': ['שדרות רוטשילד', 'אבא הלל', 'הרצל', 'הרב סילבר'],
            'highway': ['primary', 'residential', 'primary', 'residential'],
            'geometry': [
                LineString([(0, 0), (1, 0)]), 
                LineString([(1, 0), (1, 1)]), 
                LineString([(1, 1), (2, 1)]),
                LineString([(1, 0), (2, 0)]) # קטע חדש המשיק ל-5001-A ו-5002-B
            ]
        }
        osm_gdf = gpd.GeoDataFrame(data_osm, crs="EPSG:4326")
        # נוסיף עמודת עיר כרגע באופן ידני, נצטרך למפות אותה בהמשך
        osm_gdf['city'] = ['תל אביב-יפו', 'רמת גן', 'רמת גן', 'רמת גן']


    # ודא שהנתונים של הלמ"ס נשלפים (מהקוד הקודם)
    # LAMAS_data = fetch_all_LAMAS_data() # (יש להריץ את הפונקציה שהוגדרה קודם)
    if 'LAMAS_df' not in locals():
        print("שימוש בנתוני הלמ\"ס מדומה...")
        data_LAMAS = {
            '_id': [1001, 1002, 1003, 1004],
            'שם_ישוב': ['תל אביב-יפו', 'רמת גן', 'רמת גן', 'רמת גן'],
            'שם_רחוב': ['שד. רוטשילד', 'הרב סילבר', 'הרצל רח\'', 'אבא הלל סילבר'],
        }
        LAMAS_df = pd.DataFrame(data_LAMAS)
        LAMAS_df.rename(columns={'_id': 'LAMAS_id', 'שם_ישוב': 'city', 'שם_רחוב': 'LAMAS_name'}, inplace=True)