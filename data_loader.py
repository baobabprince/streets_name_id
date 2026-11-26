"""
Data loading and caching for LAMAS and OSM street data.
"""

from __future__ import annotations

import datetime
import logging
import os
import re
from collections import OrderedDict

import pandas as pd
import requests

from lamas_streets import CACHE_MAX_AGE_DAYS as LAMAS_CACHE_MAX_AGE_DAYS
from lamas_streets import RESOURCE_ID as LAMAS_RESOURCE_ID
from lamas_streets import fetch_all_LAMAS_data, normalize_lamas_data
from OSM_streets import fetch_osm_street_data
from provenance import cached_file_provenance, clear_sidecar, write_sidecar
from settlement_names import canonical_key

logger = logging.getLogger(__name__)

CACHE_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(CACHE_DIR, exist_ok=True)

LAMAS_CACHE = os.path.join(CACHE_DIR, "LAMAS_data.pkl")
OSM_CACHE_TEMPLATE = os.path.join(CACHE_DIR, "osm_data_{place}.pkl")
SIX_MONTHS_DAYS = 182

# A batch worker handles many settlements; avoid re-reading the same pickle
# from disk for every one of them.  Keys include the file mtime so a refresh
# invalidates the in-memory copy automatically.
_LAMAS_MEMORY_CACHE: dict[tuple[str, float, int], pd.DataFrame] = {}
LAMAS_SCHEMA_VERSION = 2
_OSM_MEMORY_CACHE: "OrderedDict[tuple[str, float], pd.DataFrame]" = OrderedDict()
_OSM_MEMORY_CACHE_MAX = 2


def _file_signature(path: str) -> tuple[str, float, int]:
    """Include the schema version so old in-memory caches cannot leak through."""
    try:
        return (path, os.path.getmtime(path), LAMAS_SCHEMA_VERSION)
    except OSError:
        return (path, -1.0, LAMAS_SCHEMA_VERSION)


def _ensure_lamas_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Upgrade legacy pickles to the canonical-id schema on every read."""
    attrs = dict(getattr(df, "attrs", None) or {})
    normalized = normalize_lamas_data(df)
    if attrs and isinstance(normalized, pd.DataFrame):
        normalized.attrs.update(attrs)
    return normalized


def lamas_provenance() -> dict:
    """When the cached LAMAS table was retrieved (or at least written)."""
    return {
        "source": "data.gov.il",
        "resource_id": LAMAS_RESOURCE_ID,
        **(cached_file_provenance(LAMAS_CACHE) or {}),
    }


def osm_cache_path(place: str | dict) -> str:
    if isinstance(place, dict):
        place = place.get("settlement_name") or place.get("display_name", "unknown_place")
    return OSM_CACHE_TEMPLATE.format(place=safe_place_name(place))


def osm_provenance(place: str | dict) -> dict | None:
    """Recorded origin of the cached OSM streets for ``place``."""
    return cached_file_provenance(osm_cache_path(place))


def _with_provenance(df, provenance):
    if provenance and hasattr(df, "attrs"):
        df.attrs["provenance"] = dict(provenance)
    return df


def city_key(series: pd.Series) -> pd.Series:
    """Canonical settlement key for joining LAMAS city names with OSM labels."""
    return series.map(canonical_key)


def is_fresh(path: str, max_age_days: int = SIX_MONTHS_DAYS) -> bool:
    """Return True if path exists and is newer than max_age_days."""
    if not os.path.exists(path):
        return False
    age_days = (
        datetime.datetime.now() - datetime.datetime.fromtimestamp(os.path.getmtime(path))
    ).days
    return age_days <= max_age_days


def safe_place_name(place: str) -> str:
    """Convert place to a filesystem-safe token."""
    return re.sub(r"[^0-9A-Za-z_\-\u0590-\u05FF]", "_", place)


def normalize_city(col: pd.Series) -> pd.Series:
    """Robustly normalize city names (dashes, whitespace, עיריית)."""
    return (
        col.astype(str)
        .str.replace(r"[\u2010\u2011\u2012\u2013\u2014\-]", " ", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.replace(r"עיריית", "", regex=False)
        .str.strip()
    )


def save_intermediate_df(df: pd.DataFrame, step_name: str, place: str) -> None:
    """Save an intermediate DataFrame/GeoDataFrame to the cache directory."""
    safe_place = safe_place_name(place)
    file_ext = (
        "csv"
        if step_name.startswith(("diagnostic_report", "step4_candidate_scores"))
        else "pkl"
    )
    filename = f"{step_name}_{safe_place}.{file_ext}"
    path = os.path.join(CACHE_DIR, filename)

    try:
        if file_ext == "pkl":
            df.to_pickle(path)
        else:
            df_to_save = df.drop(columns=["geometry"], errors="ignore")
            df_to_save.to_csv(path, index=False, encoding="utf-8")
        logger.info("Saved intermediate result for %s to %s", step_name, path)
    except Exception as e:
        logger.warning("Failed to save %s intermediate result: %s", step_name, e)
        if step_name == "step4_candidate_scores":
            raise


def load_or_fetch_LAMAS(
    force_refresh: bool = False, max_age_days: int = LAMAS_CACHE_MAX_AGE_DAYS
) -> pd.DataFrame:
    """Load LAMAS data from cache if fresh, otherwise fetch and cache it."""
    if not force_refresh and is_fresh(LAMAS_CACHE, max_age_days):
        signature = _file_signature(LAMAS_CACHE)
        memory_cached = _LAMAS_MEMORY_CACHE.get(signature)
        if memory_cached is not None and _is_valid_lamas_data(memory_cached):
            logger.debug("Loading LAMAS data from in-memory cache")
            return _with_provenance(memory_cached.copy(), lamas_provenance())
        try:
            logger.info("Loading LAMAS data from cache: %s", LAMAS_CACHE)
            cached_df = _ensure_lamas_schema(pd.read_pickle(LAMAS_CACHE))
            if not _is_valid_lamas_data(cached_df):
                raise ValueError("LAMAS cache is empty or missing required columns")
            _LAMAS_MEMORY_CACHE.clear()
            _LAMAS_MEMORY_CACHE[signature] = cached_df
            return _with_provenance(cached_df.copy(), lamas_provenance())
        except Exception as e:
            logger.warning("Failed to read LAMAS cache, will re-fetch: %s", e)

    logger.info("Fetching LAMAS data from API...")
    try:
        raw = fetch_all_LAMAS_data(force_refresh=force_refresh)
        fetched_provenance = dict(getattr(raw, "attrs", {}).get("provenance") or {})
        df = _ensure_lamas_schema(raw)
        if not _is_valid_lamas_data(df):
            raise ValueError("LAMAS fetch returned empty data or missing required columns")
    except (requests.exceptions.RequestException, ValueError) as fetch_error:
        if os.path.exists(LAMAS_CACHE):
            try:
                cached_df = _ensure_lamas_schema(pd.read_pickle(LAMAS_CACHE))
                if not _is_valid_lamas_data(cached_df):
                    raise ValueError("LAMAS cache is empty or missing required columns")
            except Exception:
                logger.exception("Could not load a usable LAMAS cache for fallback")
            else:
                logger.warning(
                    "LAMAS refresh failed (%s); using cached data from %s",
                    fetch_error,
                    LAMAS_CACHE,
                )
                _LAMAS_MEMORY_CACHE.clear()
                _LAMAS_MEMORY_CACHE[_file_signature(LAMAS_CACHE)] = cached_df
                return _with_provenance(cached_df.copy(), lamas_provenance())
        raise

    provenance = {
        "source": "data.gov.il",
        "resource_id": LAMAS_RESOURCE_ID,
        **fetched_provenance,
    }
    _with_provenance(df, provenance)
    try:
        df.to_pickle(LAMAS_CACHE)
        write_sidecar(LAMAS_CACHE, provenance)
        logger.info("Cached LAMAS data to %s", LAMAS_CACHE)
    except Exception as e:
        logger.warning("Failed to write LAMAS cache: %s", e)
    _LAMAS_MEMORY_CACHE.clear()
    _LAMAS_MEMORY_CACHE[_file_signature(LAMAS_CACHE)] = df.copy()
    return df


def _is_valid_lamas_data(df: pd.DataFrame) -> bool:
    required_columns = {"LAMAS_id", "LAMAS_name", "city"}
    return (
        isinstance(df, pd.DataFrame)
        and not df.empty
        and required_columns.issubset(df.columns)
    )


def load_or_fetch_osm(
    place: str | dict,
    force_refresh: bool = False,
    max_age_days: int = SIX_MONTHS_DAYS,
    use_pbf: bool = True,
):
    """Load OSM GeoDataFrame from cache if fresh, otherwise fetch and cache it."""
    if isinstance(place, dict):
        place_str_for_cache = place.get("settlement_name") or place.get(
            "display_name", "unknown_place"
        )
    else:
        place_str_for_cache = place

    safe = safe_place_name(place_str_for_cache)
    cache_path = OSM_CACHE_TEMPLATE.format(place=safe)

    if not force_refresh and is_fresh(cache_path, max_age_days):
        signature = _file_signature(cache_path)
        memory_cached = _OSM_MEMORY_CACHE.get(signature)
        if memory_cached is not None and _is_valid_osm_data(memory_cached):
            logger.debug("Loading OSM data from in-memory cache: %s", cache_path)
            _OSM_MEMORY_CACHE.move_to_end(signature)
            return _with_provenance(memory_cached.copy(), cached_file_provenance(cache_path))
        try:
            logger.info("Loading OSM data from cache: %s", cache_path)
            cached_gdf = pd.read_pickle(cache_path)
            if not _is_valid_osm_data(cached_gdf):
                raise ValueError("OSM cache is empty or missing required columns")
            _remember_osm(signature, cached_gdf)
            return _with_provenance(cached_gdf.copy(), cached_file_provenance(cache_path))
        except Exception as e:
            logger.warning("Failed to read OSM cache, will re-fetch: %s", e)

    logger.info("Fetching OSM street data for %s...", place_str_for_cache)
    try:
        gdf = fetch_osm_street_data(place, use_pbf=use_pbf)
        if isinstance(gdf, pd.DataFrame) and gdf.empty and gdf.attrs.get("pbf_no_named_streets"):
            return gdf
        if not _is_valid_osm_data(gdf):
            raise ValueError("OSM fetch returned no usable street data")
    except Exception as fetch_error:
        if os.path.exists(cache_path):
            try:
                cached_gdf = pd.read_pickle(cache_path)
                if not _is_valid_osm_data(cached_gdf):
                    raise ValueError("OSM cache is empty or missing required columns")
            except Exception:
                logger.exception("Could not load a usable OSM cache for fallback")
            else:
                logger.warning(
                    "OSM refresh failed (%s); using cached data from %s",
                    fetch_error,
                    cache_path,
                )
                _remember_osm(_file_signature(cache_path), cached_gdf)
                return _with_provenance(cached_gdf.copy(), cached_file_provenance(cache_path))
        raise

    provenance = dict(getattr(gdf, "attrs", {}).get("provenance") or {})
    try:
        gdf.to_pickle(cache_path)
        if provenance:
            write_sidecar(cache_path, provenance)
        else:  # never let an older fetch's metadata describe new data
            clear_sidecar(cache_path)
        logger.info("Cached OSM data to %s", cache_path)
    except Exception as e:
        logger.warning("Failed to write OSM cache: %s", e)
    _remember_osm(_file_signature(cache_path), gdf)
    return gdf


def _remember_osm(signature: tuple[str, float], gdf: pd.DataFrame) -> None:
    """Keep a small bounded in-memory OSM cache (GeoDataFrames can be big)."""
    _OSM_MEMORY_CACHE[signature] = gdf.copy()
    _OSM_MEMORY_CACHE.move_to_end(signature)
    while len(_OSM_MEMORY_CACHE) > _OSM_MEMORY_CACHE_MAX:
        _OSM_MEMORY_CACHE.popitem(last=False)


def _is_valid_osm_data(gdf: pd.DataFrame | None) -> bool:
    required_columns = {"osm_id", "osm_name", "geometry"}
    return (
        isinstance(gdf, pd.DataFrame)
        and not gdf.empty
        and required_columns.issubset(gdf.columns)
    )
