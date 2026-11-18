"""
OSM PBF Downloader and Offline Street Extractor.

Enables full offline extraction of streets and settlement boundaries from
OpenStreetMap .osm.pbf extracts (e.g. Geofabrik Israel and Palestine extract),
bypassing rate limits and network latency of Overpass/Nominatim.
"""

from __future__ import annotations

import json
import logging
import os
import platform
import re
import shutil
import subprocess
import urllib.request
from typing import Any, Dict, Optional, Tuple

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, box, shape
from shapely.geometry.base import BaseGeometry

from rapidfuzz import fuzz

logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

GEOFABRIK_URL = (
    "https://download.geofabrik.de/asia/israel-and-palestine-latest.osm.pbf"
)
DEFAULT_PBF_PATH = os.path.join(DATA_DIR, "israel-and-palestine-latest.osm.pbf")
DEFAULT_STREETS_PKL = os.path.join(DATA_DIR, "israel_named_streets.pkl")
DEFAULT_BOUNDARIES_PKL = os.path.join(DATA_DIR, "israel_boundaries.pkl")

# Cached in-memory datasets for instant repeated access in the same process
_CACHED_STREETS_GDF: Optional[gpd.GeoDataFrame] = None
_CACHED_BOUNDARIES_GDF: Optional[gpd.GeoDataFrame] = None


def is_hebrew(text: Any) -> bool:
    """Return True if text contains at least one Hebrew character."""
    if pd.isna(text) or text is None:
        return False
    return any("\u0590" <= char <= "\u05FF" for char in str(text))


def clean_place_name(place: str) -> str:
    """Strip country, common prefixes and noise from place name."""
    cleaned = re.sub(
        r",\s*(Israel|ישראל|Palestine|פלסטין).*$", "", place, flags=re.IGNORECASE
    )
    cleaned = re.sub(r"^(עיריית|עיריית\s+)", "", cleaned).strip()
    return cleaned


def download_osm_pbf(
    url: str = GEOFABRIK_URL,
    target_path: str = DEFAULT_PBF_PATH,
    force: bool = False,
) -> str:
    """Download the regional OSM .pbf extract if not already present."""
    if os.path.exists(target_path) and not force:
        logger.info("OSM PBF already exists: %s", target_path)
        return target_path

    logger.info("Downloading OSM PBF from %s to %s...", url, target_path)
    os.makedirs(os.path.dirname(target_path), exist_ok=True)

    # Try wget or curl first if available (they handle progress and resumes well)
    temp_target = target_path + ".tmp"
    if force and os.path.exists(temp_target):
        os.remove(temp_target)

    if shutil.which("wget"):
        cmd = ["wget", "-q", "--show-progress", "-c", url, "-O", temp_target]
        ret = subprocess.run(cmd)
        if ret.returncode == 0:
            os.replace(temp_target, target_path)
            return target_path

    if shutil.which("curl"):
        cmd = ["curl", "-L", "-f", "-o", temp_target, url]
        ret = subprocess.run(cmd)
        if ret.returncode == 0:
            os.replace(temp_target, target_path)
            return target_path

    # Fallback to python urllib
    urllib.request.urlretrieve(url, temp_target)
    os.replace(temp_target, target_path)
    return target_path


def _run_osmium_with_pbf_retry(
    command: list[str], pbf_path: str
) -> subprocess.CompletedProcess:
    """Retry once with a fresh PBF when Osmium reports a decompression error."""
    try:
        return subprocess.run(command, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as error:
        stderr = error.stderr or ""
        error_text = stderr.lower()
        corruption_markers = ("uncompress", "buffer error", "invalid pbf")
        if not any(marker in error_text for marker in corruption_markers):
            if stderr.strip():
                logger.error("Osmium failed: %s", stderr.strip())
            raise

        logger.warning(
            "Osmium reported a PBF decompression/format error; downloading a fresh "
            "extract and retrying once."
        )
        download_osm_pbf(target_path=pbf_path, force=True)
        try:
            return subprocess.run(command, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as retry_error:
            if retry_error.stderr and retry_error.stderr.strip():
                logger.error("Osmium retry failed: %s", retry_error.stderr.strip())
            raise


def _find_osmium_executable() -> Optional[str]:
    """Find osmium on PATH, or at an optional path supplied by the user."""
    return shutil.which(os.environ.get("OSMIUM_BIN", "osmium"))


def _osmium_install_instructions() -> str:
    """Return an installation command appropriate for the current platform."""
    prefix = os.environ.get("PREFIX", "").lower()
    if os.environ.get("TERMUX_VERSION") or "com.termux" in prefix:
        return "pkg install osmium-tool"

    system = platform.system()
    if system == "Windows":
        return "conda install -c conda-forge osmium-tool"
    if system == "Darwin":
        return "brew install osmium-tool"
    if system == "Linux":
        return "sudo apt install osmium-tool (Debian/Ubuntu), or install via conda-forge"
    return "see https://osmcode.org/osmium-tool/manual.html#installation"


def extract_pbf_to_cache(
    pbf_path: str = DEFAULT_PBF_PATH,
    streets_pkl: str = DEFAULT_STREETS_PKL,
    boundaries_pkl: str = DEFAULT_BOUNDARIES_PKL,
    force: bool = False,
) -> Tuple[str, str]:
    """
    Extract national named streets and administrative boundaries from PBF
    using osmium-tool, saving them as optimized GeoPandas pickle files.
    """
    needs_streets = force or not os.path.exists(streets_pkl)
    needs_boundaries = force or not os.path.exists(boundaries_pkl)

    if not needs_streets and not needs_boundaries:
        return streets_pkl, boundaries_pkl

    osmium_executable = _find_osmium_executable()
    if not osmium_executable:
        raise RuntimeError(
            "The osmium executable is required to build the local PBF caches. "
            f"Install it with: {_osmium_install_instructions()}. "
            "If it is installed outside PATH, set OSMIUM_BIN to its executable "
            "path. Alternatively, run with --no-pbf to use live Overpass data."
        )

    if not os.path.exists(pbf_path):
        download_osm_pbf(target_path=pbf_path)

    # 1. Extract highways if needed
    if needs_streets:
        logger.info("Filtering highways from PBF using osmium...")
        highways_pbf = os.path.join(DATA_DIR, "israel_highways.osm.pbf")
        highways_seq = os.path.join(DATA_DIR, "israel_highways.geojsonseq")

        _run_osmium_with_pbf_retry(
            [
                osmium_executable,
                "tags-filter",
                pbf_path,
                "w/highway",
                "-o",
                highways_pbf,
                "--overwrite",
            ],
            pbf_path,
        )

        logger.info("Exporting highway linestrings to GeoJSONSeq...")
        subprocess.run(
            [
                osmium_executable,
                "export",
                highways_pbf,
                "--geometry-types",
                "linestring",
                "-u",
                "type_id",
                "-a",
                "id",
                "-o",
                highways_seq,
                "--overwrite",
            ],
            check=True,
        )

        logger.info("Parsing named highway features into GeoDataFrame...")
        records = []
        with open(highways_seq, "r", encoding="utf-8") as f:
            for line in f:
                clean_line = line.strip().lstrip("\x1e")
                if not clean_line:
                    continue
                try:
                    data = json.loads(clean_line)
                except Exception:
                    continue
                props = data.get("properties", {})
                name = props.get("name")
                name_he = props.get("name:he")
                if not name and not name_he:
                    continue
                coords = data.get("geometry", {}).get("coordinates", [])
                if len(coords) < 2:
                    continue

                records.append({
                    "osm_id": str(data.get("id", "")),
                    "osm_name": name_he if (name_he and is_hebrew(name_he)) else name,
                    "osm_name_original": name,
                    "name:he": name_he,
                    "highway": props.get("highway"),
                    "geometry": LineString(coords),
                })

        streets_gdf = gpd.GeoDataFrame(records, crs="EPSG:4326")
        streets_gdf.to_pickle(streets_pkl)
        logger.info("Saved %d named street segments to %s", len(streets_gdf), streets_pkl)

        # Cleanup intermediate files
        for tmp in (highways_pbf, highways_seq):
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass

    # 2. Extract boundaries if needed
    if needs_boundaries:
        logger.info("Filtering administrative boundaries from PBF...")
        boundaries_pbf = os.path.join(DATA_DIR, "israel_boundaries.osm.pbf")
        boundaries_seq = os.path.join(DATA_DIR, "israel_boundaries.geojsonseq")

        _run_osmium_with_pbf_retry(
            [
                osmium_executable,
                "tags-filter",
                pbf_path,
                "r/boundary=administrative",
                "-o",
                boundaries_pbf,
                "--overwrite",
            ],
            pbf_path,
        )

        logger.info("Exporting boundary polygons to GeoJSONSeq...")
        subprocess.run(
            [
                osmium_executable,
                "export",
                boundaries_pbf,
                "--geometry-types",
                "polygon",
                "-u",
                "type_id",
                "-a",
                "id",
                "-o",
                boundaries_seq,
                "--overwrite",
            ],
            check=True,
        )

        b_records = []
        with open(boundaries_seq, "r", encoding="utf-8") as f:
            for line in f:
                clean_line = line.strip().lstrip("\x1e")
                if not clean_line:
                    continue
                try:
                    d = json.loads(clean_line)
                except Exception:
                    continue
                props = d.get("properties", {})
                geom_dict = d.get("geometry")
                if not geom_dict:
                    continue
                try:
                    geom = shape(geom_dict)
                    if not geom.is_valid:
                        geom = geom.buffer(0)
                except Exception:
                    continue

                b_records.append({
                    "osm_id": str(props.get("@id") or d.get("id")),
                    "admin_level": props.get("admin_level"),
                    "boundary": props.get("boundary"),
                    "name": props.get("name"),
                    "name:he": props.get("name:he"),
                    "name:en": props.get("name:en"),
                    "name:ar": props.get("name:ar"),
                    "geometry": geom,
                })

        boundaries_gdf = gpd.GeoDataFrame(b_records, crs="EPSG:4326")
        boundaries_gdf.to_pickle(boundaries_pkl)
        logger.info("Saved %d boundary polygons to %s", len(boundaries_gdf), boundaries_pkl)

        for tmp in (boundaries_pbf, boundaries_seq):
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass

    return streets_pkl, boundaries_pkl


def load_national_streets(pbf_path: str = DEFAULT_PBF_PATH) -> gpd.GeoDataFrame:
    """Load national street network into GeoDataFrame (cached in-memory)."""
    global _CACHED_STREETS_GDF
    if _CACHED_STREETS_GDF is not None:
        return _CACHED_STREETS_GDF

    if not os.path.exists(DEFAULT_STREETS_PKL):
        extract_pbf_to_cache(pbf_path=pbf_path)

    logger.info("Loading national streets from %s...", DEFAULT_STREETS_PKL)
    _CACHED_STREETS_GDF = pd.read_pickle(DEFAULT_STREETS_PKL)
    return _CACHED_STREETS_GDF


def load_national_boundaries(pbf_path: str = DEFAULT_PBF_PATH) -> gpd.GeoDataFrame:
    """Load national administrative boundaries into GeoDataFrame (cached in-memory)."""
    global _CACHED_BOUNDARIES_GDF
    if _CACHED_BOUNDARIES_GDF is not None:
        return _CACHED_BOUNDARIES_GDF

    if not os.path.exists(DEFAULT_BOUNDARIES_PKL):
        extract_pbf_to_cache(pbf_path=pbf_path)

    logger.info("Loading national boundaries from %s...", DEFAULT_BOUNDARIES_PKL)
    _CACHED_BOUNDARIES_GDF = pd.read_pickle(DEFAULT_BOUNDARIES_PKL)
    return _CACHED_BOUNDARIES_GDF


def find_settlement_boundary(
    place_name: str,
    boundaries_gdf: Optional[gpd.GeoDataFrame] = None,
) -> Optional[BaseGeometry]:
    """Find the polygon boundary for a settlement name."""
    if boundaries_gdf is None:
        boundaries_gdf = load_national_boundaries()

    if boundaries_gdf.empty:
        return None

    clean_name = clean_place_name(place_name)

    # 1. Exact match on Hebrew or primary name
    for col in ("name:he", "name", "name:en", "name:ar"):
        if col in boundaries_gdf.columns:
            matches = boundaries_gdf[boundaries_gdf[col].astype(str).str.strip() == clean_name]
            if not matches.empty:
                # Prefer municipal / city level admin_level (8 or 7 or 6)
                if "admin_level" in matches.columns:
                    for lvl in ("8", "7", "6"):
                        sub = matches[matches["admin_level"] == lvl]
                        if not sub.empty:
                            return sub.iloc[0].geometry
                return matches.iloc[0].geometry

    # 2. Case-insensitive / substring match
    for col in ("name:he", "name", "name:en", "name:ar"):
        if col in boundaries_gdf.columns:
            matches = boundaries_gdf[
                boundaries_gdf[col].astype(str).str.contains(clean_name, case=False, na=False)
            ]
            if not matches.empty:
                return matches.iloc[0].geometry

    # 3. Fuzzy match across boundary names
    best_match_geom = None
    best_score = 0.0
    clean_lower = clean_name.lower()
    for col in ("name:he", "name", "name:en"):
        if col in boundaries_gdf.columns:
            for _, row in boundaries_gdf.iterrows():
                val = str(row[col]) if pd.notna(row[col]) else ""
                if not val:
                    continue
                score = fuzz.ratio(clean_lower, val.lower())
                if score > best_score:
                    best_score = score
                    best_match_geom = row.geometry

    if best_match_geom is not None and best_score >= 80:
        logger.info("Fuzzy matched '%s' with boundary score %.1f%%", clean_name, best_score)
        return best_match_geom

    return None


def fetch_pbf_street_data(
    place: str | Dict[str, Any],
    pbf_path: str = DEFAULT_PBF_PATH,
) -> Optional[gpd.GeoDataFrame]:
    """
    Extract street network for a given place from the local OSM PBF cache.
    Returns a GeoDataFrame identical in structure to OSM_streets.fetch_osm_street_data.
    """
    try:
        streets_gdf = load_national_streets(pbf_path)
    except Exception as e:
        logger.warning("Could not load PBF street data: %s", e)
        return None

    if streets_gdf is None or streets_gdf.empty:
        return None

    query_geom: Optional[BaseGeometry] = None
    city_name: str = ""

    # Case A: place is a dictionary (from settlement_matcher or nominatim result)
    if isinstance(place, dict):
        city_name = place.get("settlement_name") or place.get("display_name", "")
        
        # Check if osm_id matches a known administrative boundary
        if place.get("osm_id"):
            osm_id_str = str(place["osm_id"]).lstrip("RNW")
            try:
                boundaries_gdf = load_national_boundaries()
                match = boundaries_gdf[boundaries_gdf["osm_id"] == osm_id_str]
                if not match.empty:
                    query_geom = match.iloc[0].geometry
            except Exception:
                pass

        # Check bounding box
        if query_geom is None:
            bbox = place.get("boundingbox")
            if bbox and len(bbox) == 4:
                try:
                    # Nominatim boundingbox format: [min_lat, max_lat, min_lon, max_lon]
                    min_lat, max_lat, min_lon, max_lon = (
                        float(bbox[0]),
                        float(bbox[1]),
                        float(bbox[2]),
                        float(bbox[3]),
                    )
                    query_geom = box(min_lon, min_lat, max_lon, max_lat)
                except (ValueError, TypeError):
                    pass

        # Try matching settlement name against boundary polygons if bbox was missing
        if query_geom is None and city_name:
            try:
                query_geom = find_settlement_boundary(city_name)
            except Exception:
                pass

        # Check point fallback only if no osm_id was specified (to avoid incorrect fallback for missing relations)
        if query_geom is None and not place.get("osm_id") and place.get("lat") and place.get("lon"):
            try:
                lat = float(place["lat"])
                lon = float(place["lon"])
                query_geom = box(lon - 0.018, lat - 0.015, lon + 0.018, lat + 0.015)
            except (ValueError, TypeError):
                pass

    # Case B: place is a string
    elif isinstance(place, str):
        city_name = clean_place_name(place)
        # Try finding polygon boundary from boundaries cache
        try:
            query_geom = find_settlement_boundary(city_name)
        except Exception as e:
            logger.debug("Failed finding boundary for %s: %s", city_name, e)

    if query_geom is None:
        logger.warning("Could not determine spatial geometry or bounding box for place: %s", place)
        return None

    # Spatial query against national streets index
    try:
        sindex = streets_gdf.sindex
        possible_matches_index = list(sindex.intersection(query_geom.bounds))
        possible_matches = streets_gdf.iloc[possible_matches_index]
        precise_matches = possible_matches[possible_matches.intersects(query_geom)].copy()
    except Exception:
        # Fallback without spatial index if necessary
        precise_matches = streets_gdf[streets_gdf.intersects(query_geom)].copy()

    if precise_matches.empty:
        logger.warning("No OSM road segments found within geometry for: %s", place)
        return None

    # Ensure required fields and formatting matching OSM_streets.py
    if "city" not in precise_matches.columns:
        precise_matches["city"] = city_name or clean_place_name(str(place))

    logger.info(
        "Extracted %d OSM road segments from local PBF for '%s'",
        len(precise_matches),
        city_name or place,
    )
    return precise_matches
