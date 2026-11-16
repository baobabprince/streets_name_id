"""
Data loading and caching for LAMAS and OSM street data.
"""

from __future__ import annotations

import datetime
import logging
import os
import re

import pandas as pd
import requests

from lamas_streets import CACHE_MAX_AGE_DAYS as LAMAS_CACHE_MAX_AGE_DAYS
from lamas_streets import fetch_all_LAMAS_data
from OSM_streets import fetch_osm_street_data

logger = logging.getLogger(__name__)

CACHE_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(CACHE_DIR, exist_ok=True)

LAMAS_CACHE = os.path.join(CACHE_DIR, "LAMAS_data.pkl")
OSM_CACHE_TEMPLATE = os.path.join(CACHE_DIR, "osm_data_{place}.pkl")
SIX_MONTHS_DAYS = 182


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
    file_ext = "csv" if step_name.startswith("diagnostic_report") else "pkl"
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


def load_or_fetch_LAMAS(
    force_refresh: bool = False, max_age_days: int = LAMAS_CACHE_MAX_AGE_DAYS
) -> pd.DataFrame:
    """Load LAMAS data from cache if fresh, otherwise fetch and cache it."""
    if not force_refresh and is_fresh(LAMAS_CACHE, max_age_days):
        try:
            logger.info("Loading LAMAS data from cache: %s", LAMAS_CACHE)
            cached_df = pd.read_pickle(LAMAS_CACHE)
            if not _is_valid_lamas_data(cached_df):
                raise ValueError("LAMAS cache is empty or missing required columns")
            return cached_df
        except Exception as e:
            logger.warning("Failed to read LAMAS cache, will re-fetch: %s", e)

    logger.info("Fetching LAMAS data from API...")
    try:
        df = fetch_all_LAMAS_data(force_refresh=force_refresh)
        if not _is_valid_lamas_data(df):
            raise ValueError("LAMAS fetch returned empty data or missing required columns")
    except (requests.exceptions.RequestException, ValueError) as fetch_error:
        if os.path.exists(LAMAS_CACHE):
            try:
                cached_df = pd.read_pickle(LAMAS_CACHE)
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
                return cached_df
        raise

    try:
        df.to_pickle(LAMAS_CACHE)
        logger.info("Cached LAMAS data to %s", LAMAS_CACHE)
    except Exception as e:
        logger.warning("Failed to write LAMAS cache: %s", e)
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
        place_str_for_cache = place.get("display_name", "unknown_place")
    else:
        place_str_for_cache = place

    safe = safe_place_name(place_str_for_cache)
    cache_path = OSM_CACHE_TEMPLATE.format(place=safe)

    if not force_refresh and is_fresh(cache_path, max_age_days):
        try:
            logger.info("Loading OSM data from cache: %s", cache_path)
            cached_gdf = pd.read_pickle(cache_path)
            if not _is_valid_osm_data(cached_gdf):
                raise ValueError("OSM cache is empty or missing required columns")
            return cached_gdf
        except Exception as e:
            logger.warning("Failed to read OSM cache, will re-fetch: %s", e)

    logger.info("Fetching OSM street data for %s...", place_str_for_cache)
    try:
        gdf = fetch_osm_street_data(place, use_pbf=use_pbf)
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
                return cached_gdf
        raise

    try:
        gdf.to_pickle(cache_path)
        logger.info("Cached OSM data to %s", cache_path)
    except Exception as e:
        logger.warning("Failed to write OSM cache: %s", e)
    return gdf


def _is_valid_osm_data(gdf: pd.DataFrame | None) -> bool:
    required_columns = {"osm_id", "osm_name", "geometry"}
    return (
        isinstance(gdf, pd.DataFrame)
        and not gdf.empty
        and required_columns.issubset(gdf.columns)
    )
