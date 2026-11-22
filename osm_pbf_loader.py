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
from shapely.geometry import LineString, Point, box, shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from rapidfuzz import fuzz, process

from settlement_names import (
    MAX_SETTLEMENT_BBOX_SPAN_DEG,
    MIN_SETTLEMENT_HALF_HEIGHT_LAT,
    MIN_SETTLEMENT_HALF_WIDTH_LON,
    NON_SETTLEMENT_ADMIN_LEVELS,
    admin_level,
    canonical_key,
    is_regional_boundary_name,
    load_osm_id_overrides,
    place_is_settlement,
)

logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

GEOFABRIK_URL = (
    "https://download.geofabrik.de/asia/israel-and-palestine-latest.osm.pbf"
)
DEFAULT_PBF_PATH = os.path.join(DATA_DIR, "israel-and-palestine-latest.osm.pbf")
DEFAULT_STREETS_PKL = os.path.join(DATA_DIR, "israel_named_streets.pkl")
DEFAULT_BOUNDARIES_PKL = os.path.join(DATA_DIR, "israel_boundaries.pkl")
DEFAULT_PLACES_PKL = os.path.join(DATA_DIR, "israel_places.pkl")

# Cached in-memory datasets for instant repeated access in the same process
_CACHED_STREETS_GDF: Optional[gpd.GeoDataFrame] = None
_CACHED_BOUNDARIES_GDF: Optional[gpd.GeoDataFrame] = None
_CACHED_PLACES_GDF: Optional[gpd.GeoDataFrame] = None
_BOUNDARY_INDEX_CACHE: Dict[tuple, tuple] = {}
_NEIGHBOR_POLYGONS_CACHE: Optional[gpd.GeoDataFrame] = None


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
                    "alt_name": props.get("alt_name"),
                    "official_name": props.get("official_name"),
                    "old_name": props.get("old_name"),
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


NAME_COLUMNS = (
    "name:he", "name", "name:en", "name:ar", "alt_name",
    "official_name", "old_name", "short_name",
)


def extract_places_to_cache(
    pbf_path: str = DEFAULT_PBF_PATH,
    places_pkl: str = DEFAULT_PLACES_PKL,
    force: bool = False,
) -> str:
    """Extract ``place=*`` features (villages/towns/neighbourhoods) from the PBF.

    Administrative boundaries alone miss many Israeli settlements that are
    mapped as a single ``place`` node.  This third national cache lets the
    settlement resolver match LAMAS names completely offline.
    """
    if os.path.exists(places_pkl) and not force:
        return places_pkl

    osmium_executable = _find_osmium_executable()
    if not osmium_executable:
        raise RuntimeError(
            "The osmium executable is required to build the local place cache. "
            f"Install it with: {_osmium_install_instructions()}."
        )
    if not os.path.exists(pbf_path):
        download_osm_pbf(target_path=pbf_path)

    places_pbf = os.path.join(DATA_DIR, "israel_places.osm.pbf")
    places_seq = os.path.join(DATA_DIR, "israel_places.geojsonseq")
    try:
        logger.info("Filtering place features from PBF using osmium...")
        _run_osmium_with_pbf_retry(
            [
                osmium_executable,
                "tags-filter",
                pbf_path,
                "n/place",
                "w/place",
                "r/place",
                "-o",
                places_pbf,
                "--overwrite",
            ],
            pbf_path,
        )
        subprocess.run(
            [
                osmium_executable,
                "export",
                places_pbf,
                "--geometry-types",
                "point,polygon,linestring",
                "-u",
                "type_id",
                "-a",
                "id",
                "-o",
                places_seq,
                "--overwrite",
            ],
            check=True,
        )

        records = []
        with open(places_seq, "r", encoding="utf-8") as handle:
            for line in handle:
                clean_line = line.strip().lstrip("\x1e")
                if not clean_line:
                    continue
                try:
                    data = json.loads(clean_line)
                except ValueError:
                    continue
                props = data.get("properties", {})
                geom_dict = data.get("geometry")
                if not geom_dict or not any(props.get(col) for col in NAME_COLUMNS if col in props):
                    continue
                try:
                    geom = shape(geom_dict)
                    if not geom.is_valid:
                        geom = geom.buffer(0)
                    if geom.is_empty:
                        continue
                except Exception:
                    continue
                raw_id = str(props.get("@id") or data.get("id") or "")
                records.append({
                    "osm_id": raw_id,
                    "osm_type": raw_id[0] if raw_id[:1] in {"n", "w", "r"} else "",
                    **{col: props.get(col) for col in NAME_COLUMNS},
                    "place": props.get("place"),
                    "admin_level": props.get("admin_level"),
                    "geometry": geom,
                })

        places_gdf = gpd.GeoDataFrame(records, crs="EPSG:4326")
        places_gdf.to_pickle(places_pkl)
        logger.info("Saved %d OSM place features to %s", len(places_gdf), places_pkl)
    finally:
        for tmp in (places_pbf, places_seq):
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass
    return places_pkl


def load_national_places(pbf_path: str = DEFAULT_PBF_PATH) -> gpd.GeoDataFrame:
    """Load the national ``place=*`` index (cached in-memory)."""
    global _CACHED_PLACES_GDF
    if _CACHED_PLACES_GDF is not None:
        return _CACHED_PLACES_GDF
    if not os.path.exists(DEFAULT_PLACES_PKL):
        extract_places_to_cache(pbf_path=pbf_path)
    logger.info("Loading national places from %s...", DEFAULT_PLACES_PKL)
    _CACHED_PLACES_GDF = pd.read_pickle(DEFAULT_PLACES_PKL)
    return _CACHED_PLACES_GDF


def prepare_national_caches(
    pbf_path: str = DEFAULT_PBF_PATH, force: bool = False, include_places: bool = True
) -> Dict[str, str]:
    """Build all national OSM caches once, before any settlement is processed."""
    streets_pkl, boundaries_pkl = extract_pbf_to_cache(pbf_path=pbf_path, force=force)
    caches = {"streets": streets_pkl, "boundaries": boundaries_pkl}
    if include_places:
        caches["places"] = extract_places_to_cache(pbf_path=pbf_path, force=force)
    return caches


def _boundary_name_index(boundaries_gdf: gpd.GeoDataFrame):
    """Build a canonical-key -> rows index once per GeoDataFrame."""
    cache_key = (id(boundaries_gdf), len(boundaries_gdf))
    cached = _BOUNDARY_INDEX_CACHE.get(cache_key)
    if cached is not None:
        return cached
    index: Dict[str, list] = {}
    for row_index, row in boundaries_gdf.iterrows():
        geometry = row.get("geometry")
        if geometry is None or getattr(geometry, "is_empty", True):
            continue
        level = admin_level(row.get("admin_level"))
        if level in NON_SETTLEMENT_ADMIN_LEVELS:
            continue  # district / subdistrict / neighbourhood subdivision
        row_names = [
            part.strip()
            for col in NAME_COLUMNS
            if not pd.isna(row.get(col))
            for part in str(row.get(col)).split(";")
            if part.strip()
        ]
        if any(is_regional_boundary_name(name) for name in row_names):
            continue  # regional council / subdistrict / district
        try:
            minx, miny, maxx, maxy = geometry.bounds
            if max(maxy - miny, maxx - minx) > MAX_SETTLEMENT_BBOX_SPAN_DEG:
                continue  # district / regional council, not a settlement
        except Exception:
            pass
        for col in NAME_COLUMNS:
            value = row.get(col)
            if pd.isna(value):
                continue
            for part in str(value).split(";"):
                key = canonical_key(part)
                if key:
                    index.setdefault(key, []).append((row_index, col))
    _BOUNDARY_INDEX_CACHE.clear()
    _BOUNDARY_INDEX_CACHE[cache_key] = (index, boundaries_gdf)
    return index, boundaries_gdf


def _preferred_row(boundaries_gdf: gpd.GeoDataFrame, rows: list):
    """Prefer Hebrew name and municipal-level boundaries (admin_level 8/7/6)."""
    def rank(item):
        row_index, col = item
        level = str(boundaries_gdf.at[row_index, "admin_level"]) if "admin_level" in boundaries_gdf else ""
        return (
            0 if col == "name:he" else 1 if col == "name" else 2,
            {"8": 0, "7": 1, "6": 2}.get(level, 3),
            str(row_index),
        )
    return sorted(rows, key=rank)[0][0]


def find_settlement_boundary(
    place_name: str,
    boundaries_gdf: Optional[gpd.GeoDataFrame] = None,
    expected_center: Optional[Tuple[float, float]] = None,
    max_distance_m: float = 25000.0,
) -> Optional[BaseGeometry]:
    """Find a settlement boundary by canonical name, then a bounded fuzzy lookup.

    ``expected_center`` is ``(lon, lat)`` from the LAMAS settlement centres;
    when supplied, candidates that are geographically implausible are rejected.
    """
    if boundaries_gdf is None:
        boundaries_gdf = load_national_boundaries()
    if boundaries_gdf is None or boundaries_gdf.empty:
        return None

    query_key = canonical_key(clean_place_name(place_name))
    if not query_key:
        return None
    index, gdf = _boundary_name_index(boundaries_gdf)

    def geometry_is_close(row_index) -> bool:
        if expected_center is None:
            return True
        geom = gdf.at[row_index, "geometry"]
        point = geom.representative_point()
        # Approximate metres per degree at Israel's latitude (~32°N).
        dx = (point.x - expected_center[0]) * 94_000
        dy = (point.y - expected_center[1]) * 111_000
        return (dx * dx + dy * dy) ** 0.5 <= max_distance_m

    exact_rows = index.get(query_key, [])
    if exact_rows:
        candidates = [row for row in exact_rows if geometry_is_close(row[0])]
        if candidates:
            return gdf.at[_preferred_row(gdf, candidates), "geometry"]

    # Bounded fuzzy lookup: one C-accelerated query over canonical keys.
    best = process.extract(
        query_key,
        list(index),
        scorer=fuzz.ratio,
        score_cutoff=88,
        limit=5,
    )
    for key, score, _ in best:
        rows = [row for row in index[key] if geometry_is_close(row[0])]
        if rows:
            logger.info(
                "Fuzzy matched '%s' with boundary '%s' (score %.1f)",
                place_name,
                key,
                score,
            )
            return gdf.at[_preferred_row(gdf, rows), "geometry"]
    return None


def _is_polygon_geom(geom: Optional[BaseGeometry]) -> bool:
    return (
        geom is not None
        and not getattr(geom, "is_empty", True)
        and getattr(geom, "geom_type", "") in {"Polygon", "MultiPolygon"}
    )


def _load_settlement_polygons_for_clipping() -> Optional[gpd.GeoDataFrame]:
    """Build a cached GeoDataFrame of all real settlement polygons (for clipping node fallback boxes)."""
    global _NEIGHBOR_POLYGONS_CACHE
    if _NEIGHBOR_POLYGONS_CACHE is not None:
        return _NEIGHBOR_POLYGONS_CACHE

    records = []
    try:
        boundaries_gdf = load_national_boundaries()
    except Exception:
        boundaries_gdf = None
    if boundaries_gdf is not None and not boundaries_gdf.empty:
        for _, row in boundaries_gdf.iterrows():
            geom = row.get("geometry")
            if not _is_polygon_geom(geom):
                continue
            if admin_level(row.get("admin_level")) in NON_SETTLEMENT_ADMIN_LEVELS:
                continue
            names = [
                part.strip()
                for col in NAME_COLUMNS
                if not pd.isna(row.get(col))
                for part in str(row.get(col)).split(";")
                if part.strip()
            ]
            if any(is_regional_boundary_name(n) for n in names):
                continue
            try:
                minx, miny, maxx, maxy = geom.bounds
                if max(maxy - miny, maxx - minx) > MAX_SETTLEMENT_BBOX_SPAN_DEG:
                    continue
            except Exception:
                continue
            keys = frozenset(canonical_key(n) for n in names if canonical_key(n))
            if keys:
                records.append({"keys": keys, "geometry": geom})

    try:
        places_gdf = load_national_places()
    except Exception:
        places_gdf = None
    if places_gdf is not None and not places_gdf.empty:
        for _, row in places_gdf.iterrows():
            geom = row.get("geometry")
            if not _is_polygon_geom(geom):
                continue
            if not place_is_settlement(row.get("place"), row.get("admin_level"), "place"):
                continue
            names = [
                part.strip()
                for col in NAME_COLUMNS
                if not pd.isna(row.get(col))
                for part in str(row.get(col)).split(";")
                if part.strip()
            ]
            keys = frozenset(canonical_key(n) for n in names if canonical_key(n))
            if keys:
                records.append({"keys": keys, "geometry": geom})

    if not records:
        _NEIGHBOR_POLYGONS_CACHE = gpd.GeoDataFrame(
            columns=["keys", "geometry"], crs="EPSG:4326"
        )
    else:
        _NEIGHBOR_POLYGONS_CACHE = gpd.GeoDataFrame(records, crs="EPSG:4326")
    return _NEIGHBOR_POLYGONS_CACHE


def _subtract_neighbor_settlements(
    fallback_box: BaseGeometry,
    city_name: str,
    center_point: Optional[Point] = None,
) -> BaseGeometry:
    """Carve adjacent settlements' polygons out of a node-only fallback box.

    Never subtracts polygons that share ``city_name`` or that enclose
    ``center_point`` (enclave settlements), so the target settlement itself is
    never erased.
    """
    try:
        neighbors_gdf = _load_settlement_polygons_for_clipping()
        if neighbors_gdf is None or neighbors_gdf.empty:
            return fallback_box
        target_key = canonical_key(clean_place_name(city_name)) if city_name else ""
        center = center_point or fallback_box.representative_point()
        cand_idx = list(neighbors_gdf.sindex.intersection(fallback_box.bounds))
        if not cand_idx:
            return fallback_box
        to_subtract = []
        for idx in cand_idx:
            row = neighbors_gdf.iloc[idx]
            if target_key and target_key in row["keys"]:
                continue
            poly = row["geometry"]
            if not poly.intersects(fallback_box):
                continue
            if poly.contains(center):
                continue  # target settlement sits inside this polygon (enclave)
            to_subtract.append(poly)
        if not to_subtract:
            return fallback_box
        clipped = fallback_box.difference(unary_union(to_subtract))
        if clipped is None or clipped.is_empty:
            return fallback_box
        return clipped
    except Exception as exc:
        logger.debug("Neighbor-polygon clipping skipped for '%s': %s", city_name, exc)
        return fallback_box


def _lookup_place_polygon_by_id(
    osm_id_str: str,
    osm_type: str = "",
    places_gdf: Optional[gpd.GeoDataFrame] = None,
) -> Optional[BaseGeometry]:
    """Return an exact polygon geometry from ``israel_places.pkl`` by OSM id."""
    if not osm_id_str:
        return None
    if places_gdf is None:
        try:
            places_gdf = load_national_places()
        except Exception:
            return None
    if places_gdf is None or places_gdf.empty:
        return None
    ids = places_gdf["osm_id"].astype(str).str.lstrip("rnwRNW")
    matches = places_gdf[ids == osm_id_str]
    if matches.empty:
        return None
    type_prefix = (osm_type or "")[:1].lower()
    if type_prefix in {"w", "r", "n"} and "osm_type" in matches.columns:
        typed = matches[matches["osm_type"].astype(str).str.lower() == type_prefix]
        if not typed.empty:
            matches = typed
    for _, row in matches.iterrows():
        geom = row.get("geometry")
        if _is_polygon_geom(geom):
            return geom
    return None


def find_settlement_place_area(
    place_name: str,
    places_gdf: Optional[gpd.GeoDataFrame] = None,
    expected_center: Optional[Tuple[float, float]] = None,
    max_distance_m: float = 25000.0,
    allow_non_settlement: bool = False,
    polygons_only: bool = False,
) -> Optional[BaseGeometry]:
    """Find a ``place=*`` polygon (preferred) or small search box around a node.

    Many Israeli settlements have no administrative boundary relation, but DO
    have an exact ``place=village`` / ``place=town`` polygon in OSM.  Always
    return that exact polygon when available instead of an inflated square.
    """
    if places_gdf is None:
        try:
            places_gdf = load_national_places()
        except Exception as exc:
            logger.debug("Place cache unavailable: %s", exc)
            return None
    if places_gdf is None or places_gdf.empty:
        return None
    query_key = canonical_key(clean_place_name(place_name))
    if not query_key:
        return None

    best_poly: Optional[Tuple[float, BaseGeometry]] = None
    best_point: Optional[Tuple[float, Point]] = None

    for _, row in places_gdf.iterrows():
        if not allow_non_settlement and not place_is_settlement(
            row.get("place"), row.get("admin_level"), "place"
        ):
            continue
        names = []
        for col in NAME_COLUMNS:
            value = row.get(col)
            if value is not None and str(value).strip().lower() not in {"nan", "none", ""}:
                names.extend(str(value).split(";"))
        if not any(canonical_key(name) == query_key for name in names):
            continue
        geometry = row.get("geometry")
        if geometry is None or getattr(geometry, "is_empty", True):
            continue
        point = geometry.representative_point()
        dist_m = 0.0
        if expected_center:
            dx = (point.x - expected_center[0]) * 94_000
            dy = (point.y - expected_center[1]) * 111_000
            dist_m = (dx * dx + dy * dy) ** 0.5
            if dist_m > max_distance_m:
                continue
        if _is_polygon_geom(geometry):
            if best_poly is None or dist_m < best_poly[0]:
                best_poly = (dist_m, geometry)
        elif not polygons_only:
            if best_point is None or dist_m < best_point[0]:
                best_point = (dist_m, point)

    if best_poly is not None:
        return best_poly[1]
    if best_point is not None:
        pt = best_point[1]
        return box(
            pt.x - MIN_SETTLEMENT_HALF_WIDTH_LON,
            pt.y - MIN_SETTLEMENT_HALF_HEIGHT_LAT,
            pt.x + MIN_SETTLEMENT_HALF_WIDTH_LON,
            pt.y + MIN_SETTLEMENT_HALF_HEIGHT_LAT,
        )
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
    used_node_fallback = False
    fallback_center: Optional[Point] = None
    city_name: str = ""

    # Case A: place is a dictionary (from settlement_matcher or nominatim result)
    if isinstance(place, dict):
        city_name = place.get("settlement_name") or place.get("display_name", "")
        expected_center = None
        if place.get("lat") is not None and place.get("lon") is not None:
            try:
                expected_center = (float(place["lon"]), float(place["lat"]))
                fallback_center = Point(expected_center[0], expected_center[1])
            except (ValueError, TypeError):
                expected_center = None

        # 1a. Check if osm_id matches a known non-regional administrative boundary
        if place.get("osm_id"):
            osm_id_str = str(place["osm_id"]).lstrip("rnwRNW")
            osm_type = str(place.get("osm_type") or "")
            try:
                boundaries_gdf = load_national_boundaries()
                ids = boundaries_gdf["osm_id"].astype(str).str.lstrip("rnwRNW")
                match = boundaries_gdf[ids == osm_id_str]
                if not match.empty:
                    row = match.iloc[0]
                    names = [
                        part.strip()
                        for col in NAME_COLUMNS
                        if col in match.columns and not pd.isna(row.get(col))
                        for part in str(row.get(col)).split(";")
                        if part.strip()
                    ]
                    if (
                        admin_level(row.get("admin_level")) not in NON_SETTLEMENT_ADMIN_LEVELS
                        and not any(is_regional_boundary_name(n) for n in names)
                        and _is_polygon_geom(row.geometry)
                    ):
                        query_geom = row.geometry
            except Exception:
                pass

            # 1b. Check if osm_id matches an exact place=* polygon in israel_places.pkl
            if query_geom is None:
                query_geom = _lookup_place_polygon_by_id(osm_id_str, osm_type=osm_type)

        # 2. If matched feature was only a point node (or missing from caches),
        #    prefer any exact boundary or place polygon within 3 km of the node.
        if query_geom is None and city_name:
            try:
                query_geom = find_settlement_boundary(
                    city_name,
                    expected_center=expected_center,
                    max_distance_m=3000.0 if expected_center else 25000.0,
                )
            except Exception:
                pass
        if query_geom is None and city_name:
            try:
                query_geom = find_settlement_place_area(
                    city_name,
                    expected_center=expected_center,
                    max_distance_m=3000.0 if expected_center else 25000.0,
                    polygons_only=True,
                )
            except Exception:
                pass

        # 3. Node-only bounding box fallback (compact radius + neighbor clipping)
        if query_geom is None:
            bbox = place.get("boundingbox")
            if bbox and len(bbox) == 4:
                try:
                    min_lat, max_lat, min_lon, max_lon = (
                        float(bbox[0]),
                        float(bbox[1]),
                        float(bbox[2]),
                        float(bbox[3]),
                    )
                    center_lat = float(place.get("lat") or (min_lat + max_lat) / 2)
                    center_lon = float(place.get("lon") or (min_lon + max_lon) / 2)
                    fallback_center = Point(center_lon, center_lat)
                    half_lat = max(
                        (max_lat - min_lat) / 2,
                        MIN_SETTLEMENT_HALF_HEIGHT_LAT,
                    )
                    half_lon = max(
                        (max_lon - min_lon) / 2,
                        MIN_SETTLEMENT_HALF_WIDTH_LON,
                    )
                    max_half = MAX_SETTLEMENT_BBOX_SPAN_DEG / 2
                    half_lat, half_lon = min(half_lat, max_half), min(half_lon, max_half)
                    min_lat, max_lat = center_lat - half_lat, center_lat + half_lat
                    min_lon, max_lon = center_lon - half_lon, center_lon + half_lon
                    query_geom = box(min_lon, min_lat, max_lon, max_lat)
                    used_node_fallback = True
                except (ValueError, TypeError):
                    pass

        # 4. Point-only fallback when neither polygon nor bbox was provided
        if query_geom is None and not place.get("osm_id") and expected_center is not None:
            lon, lat = expected_center
            query_geom = box(
                lon - MIN_SETTLEMENT_HALF_WIDTH_LON,
                lat - MIN_SETTLEMENT_HALF_HEIGHT_LAT,
                lon + MIN_SETTLEMENT_HALF_WIDTH_LON,
                lat + MIN_SETTLEMENT_HALF_HEIGHT_LAT,
            )
            used_node_fallback = True

    # Case B: place is a string
    elif isinstance(place, str):
        city_name = clean_place_name(place)
        expected_center = None
        try:
            from settlement_index import load_settlement_centers

            expected_center = load_settlement_centers().get(canonical_key(city_name))
        except Exception as e:
            logger.debug("Settlement centres unavailable for %s: %s", city_name, e)
        try:
            query_geom = find_settlement_boundary(
                city_name, expected_center=expected_center
            )
        except Exception as e:
            logger.warning("Failed finding boundary for %s: %s", city_name, e)

        if query_geom is None:
            try:
                pinned = canonical_key(city_name) in load_osm_id_overrides()
                query_geom = find_settlement_place_area(
                    city_name,
                    expected_center=expected_center,
                    allow_non_settlement=pinned,
                )
                if query_geom is not None and getattr(query_geom, "geom_type", "") == "Polygon":
                    # Check if find_settlement_place_area returned a node box (rectangle) vs real polygon
                    minx, miny, maxx, maxy = query_geom.bounds
                    if (
                        abs((maxx - minx) - 2 * MIN_SETTLEMENT_HALF_WIDTH_LON) < 1e-6
                        and abs((maxy - miny) - 2 * MIN_SETTLEMENT_HALF_HEIGHT_LAT) < 1e-6
                    ):
                        used_node_fallback = True
                        fallback_center = query_geom.representative_point()
            except Exception as exc:
                logger.debug("Place-area lookup failed for %s: %s", city_name, exc)

        if query_geom is None:
            from settlement_matcher import SettlementMatcher

            match = SettlementMatcher().search_settlement(city_name)
            if match is not None:
                fallback_center = Point(match.lon, match.lat)
                query_geom = box(
                    match.lon - MIN_SETTLEMENT_HALF_WIDTH_LON,
                    match.lat - MIN_SETTLEMENT_HALF_HEIGHT_LAT,
                    match.lon + MIN_SETTLEMENT_HALF_WIDTH_LON,
                    match.lat + MIN_SETTLEMENT_HALF_HEIGHT_LAT,
                )
                used_node_fallback = True
                logger.info(
                    "No local boundary found for '%s'; using a geocoded local "
                    "search area with the PBF.",
                    city_name,
                )

    if query_geom is None:
        logger.warning("Could not determine spatial geometry or bounding box for place: %s", place)
        return None

    if used_node_fallback:
        query_geom = _subtract_neighbor_settlements(
            query_geom, city_name, center_point=fallback_center
        )

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
