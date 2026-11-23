"""Creation-time and source-data timestamps for generated pages.

Every timestamp written by this module is an ISO-8601 string in UTC.  Values
are only recorded when they are actually known: a missing value means "not
recorded", never "now".  Display helpers convert aware timestamps to Israel
time; naive legacy timestamps are shown as recorded, without a time zone claim.

Cached source files get a small JSON sidecar (``<file>.provenance.json``) so a
page generated later from the cache still reports when the data was retrieved,
not when the page happened to be rendered.
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import struct
import zlib
from typing import Any, Optional

logger = logging.getLogger(__name__)

SIDECAR_SUFFIX = ".provenance.json"
DISPLAY_TZ_NAME = "Asia/Jerusalem"

try:  # tzdata may be missing on minimal systems; fall back to UTC then.
    from zoneinfo import ZoneInfo

    _DISPLAY_TZ: Optional[_dt.tzinfo] = ZoneInfo(DISPLAY_TZ_NAME)
except Exception:  # pragma: no cover - depends on the host tz database
    _DISPLAY_TZ = None


def to_iso(moment: _dt.datetime) -> str:
    """UTC ISO-8601 with seconds precision and an explicit offset."""
    if moment.tzinfo is None:
        moment = moment.astimezone()  # interpret naive values as host-local
    return moment.astimezone(_dt.timezone.utc).replace(microsecond=0).isoformat()


def utc_now() -> str:
    return to_iso(_dt.datetime.now(_dt.timezone.utc))


def build_time() -> str:
    """Generation time for reproducible artefacts.

    Honours ``SOURCE_DATE_EPOCH`` (reproducible-builds.org) so tests and
    deterministic rebuilds can pin the stamp; otherwise the current time.
    """
    epoch = os.environ.get("SOURCE_DATE_EPOCH", "").strip()
    if epoch:
        try:
            return to_iso(_dt.datetime.fromtimestamp(int(epoch), _dt.timezone.utc))
        except (ValueError, OverflowError, OSError):
            logger.warning("Ignoring invalid SOURCE_DATE_EPOCH=%r", epoch)
    return utc_now()


def file_mtime(path: str) -> Optional[str]:
    try:
        return to_iso(_dt.datetime.fromtimestamp(os.path.getmtime(path), _dt.timezone.utc))
    except OSError:
        return None


def sidecar_path(path: str) -> str:
    return path + SIDECAR_SUFFIX


def read_sidecar(path: str) -> Optional[dict]:
    try:
        with open(sidecar_path(path), encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def write_sidecar(path: str, data: dict) -> None:
    """Best effort: provenance must never break a pipeline run."""
    clean = {key: value for key, value in data.items() if value not in (None, "")}
    try:
        with open(sidecar_path(path), "w", encoding="utf-8") as handle:
            json.dump(clean, handle, ensure_ascii=False, indent=2, sort_keys=True)
    except OSError as exc:
        logger.warning("Could not write provenance for %s: %s", path, exc)


def clear_sidecar(path: str) -> None:
    try:
        os.remove(sidecar_path(path))
    except FileNotFoundError:
        pass
    except OSError as exc:
        logger.warning("Could not remove stale provenance for %s: %s", path, exc)


def cached_file_provenance(path: str) -> Optional[dict]:
    """Recorded provenance of a cache file, or its write time as a fallback.

    The fallback is labelled ``cache_written_at`` because a file timestamp
    shows when the cache was written, which is not necessarily when the
    upstream source was published.
    """
    recorded = read_sidecar(path)
    if recorded:
        return recorded
    written = file_mtime(path)
    return {"cache_written_at": written} if written else None


# --------------------------------------------------------------------------
# OSM PBF header: osmosis_replication_timestamp (seconds since epoch).
# Minimal protobuf reader, so no osmium/pyosmium dependency is needed.
# --------------------------------------------------------------------------

def _varint(buf: bytes, pos: int) -> tuple[int, int]:
    result = shift = 0
    while True:
        if pos >= len(buf):
            raise ValueError("truncated varint")
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7
        if shift > 63:
            raise ValueError("varint too long")


def _fields(buf: bytes):
    pos = 0
    while pos < len(buf):
        key, pos = _varint(buf, pos)
        number, wire = key >> 3, key & 7
        if wire == 0:
            value, pos = _varint(buf, pos)
        elif wire == 2:
            length, pos = _varint(buf, pos)
            value, pos = buf[pos:pos + length], pos + length
        elif wire == 1:
            value, pos = buf[pos:pos + 8], pos + 8
        elif wire == 5:
            value, pos = buf[pos:pos + 4], pos + 4
        else:
            raise ValueError(f"unsupported protobuf wire type {wire}")
        yield number, value


def pbf_replication_timestamp(path: str) -> Optional[str]:
    """Return the OSM data timestamp declared in a PBF header, if present.

    Geofabrik extracts set ``osmosis_replication_timestamp`` (HeaderBlock
    field 32); this is the time of the OSM database state in the file.
    """
    try:
        with open(path, "rb") as handle:
            (header_len,) = struct.unpack(">I", handle.read(4))
            if not 0 < header_len < 64 * 1024:
                return None
            blob_header = dict(_fields(handle.read(header_len)))
            if blob_header.get(1) != b"OSMHeader":
                return None
            blob = dict(_fields(handle.read(int(blob_header.get(3, 0)))))
        if 1 in blob:
            header_block = blob[1]
        elif 3 in blob:
            header_block = zlib.decompress(blob[3])
        else:
            return None
        seconds = dict(_fields(header_block)).get(32)
        if not isinstance(seconds, int) or seconds <= 0:
            return None
        return to_iso(_dt.datetime.fromtimestamp(seconds, _dt.timezone.utc))
    except (OSError, ValueError, struct.error, zlib.error, OverflowError):
        return None


# --------------------------------------------------------------------------
# Display
# --------------------------------------------------------------------------

def parse_iso(value: Any) -> Optional[_dt.datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return _dt.datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None


def format_local(value: Any, with_time: bool = True) -> Optional[str]:
    """``23.11.2026, 15:32`` (Israel time for aware values)."""
    moment = parse_iso(value)
    if moment is None:
        return None
    if moment.tzinfo is not None:
        moment = moment.astimezone(_DISPLAY_TZ or _dt.timezone.utc)
    text = f"{moment.day:02d}.{moment.month:02d}.{moment.year}"
    if with_time:
        text += f", {moment.hour:02d}:{moment.minute:02d}"
    return text


def zone_label(value: Any) -> str:
    moment = parse_iso(value)
    if moment is None or moment.tzinfo is None:
        return ""
    return "שעון ישראל" if _DISPLAY_TZ is not None else "UTC"


def osm_data_time(osm: Optional[dict]) -> tuple[Optional[str], str]:
    """Best documented time of the OSM data, with a Hebrew label."""
    osm = osm or {}
    if osm.get("osm_snapshot_at"):
        return osm["osm_snapshot_at"], "מצב OSM לפי קובץ ה־PBF"
    if osm.get("pbf_file_at"):
        return osm["pbf_file_at"], "מועד שמירת קובץ ה־PBF"
    if osm.get("retrieved_at"):
        return osm["retrieved_at"], "נשלפו מ־OSM"
    if osm.get("cache_written_at"):
        return osm["cache_written_at"], "נשמרו במטמון"
    return None, ""


def lamas_data_time(lamas: Optional[dict]) -> tuple[Optional[str], str]:
    lamas = lamas or {}
    if lamas.get("retrieved_at"):
        return lamas["retrieved_at"], "נשלפו מ־data.gov.il"
    if lamas.get("cache_written_at"):
        return lamas["cache_written_at"], "נשמרו במטמון"
    return None, ""


def _time_html(value: Any) -> str:
    from html import escape

    text = format_local(value)
    if not text:
        return "לא תועד"
    zone = zone_label(value)
    suffix = f" ({zone})" if zone else ""
    return f'<time datetime="{escape(str(value))}">{escape(text)}</time>{escape(suffix)}'


def page_stamp_html(provenance: Optional[dict]) -> str:
    """Short creation line shown under a settlement page title."""
    generated = (provenance or {}).get("generated_at")
    if not format_local(generated):
        return '<p class="page-stamp">מועד יצירת הדף לא תועד</p>'
    return f'<p class="page-stamp">הדף נוצר {_time_html(generated)}</p>'


def page_sources_html(provenance: Optional[dict]) -> str:
    """Footer line: page creation time and the time of each data source."""
    from html import escape

    provenance = provenance or {}
    osm_at, osm_label = osm_data_time(provenance.get("osm"))
    lamas_at, lamas_label = lamas_data_time(provenance.get("lamas"))
    parts = [
        f"<span>הדף נוצר: {_time_html(provenance.get('generated_at'))}</span>",
        f"<span>נתוני OSM: {_time_html(osm_at)}"
        + (f" · {escape(osm_label)}" if osm_at and osm_label else "")
        + "</span>",
        f"<span>נתוני למ״ס: {_time_html(lamas_at)}"
        + (f" · {escape(lamas_label)}" if lamas_at and lamas_label else "")
        + "</span>",
    ]
    return '<p class="footnote page-sources" aria-label="מועדי יצירה ומקורות">' + "".join(parts) + "</p>"


def page_provenance(sources: Optional[dict], generated_at: Optional[str] = None) -> dict:
    """Provenance block embedded in a page's ``#page-meta`` JSON."""
    sources = sources or {}
    block: dict = {"generated_at": generated_at or utc_now()}
    for key in ("osm", "lamas"):
        value = sources.get(key)
        if isinstance(value, dict) and value:
            block[key] = {k: v for k, v in value.items() if v not in (None, "")}
    return block


def display_dates(provenance: Any) -> Optional[dict]:
    """Compact display strings for a page's recorded creation/source times."""
    if not isinstance(provenance, dict):
        return None
    osm_at, _ = osm_data_time(provenance.get("osm"))
    lamas_at, _ = lamas_data_time(provenance.get("lamas"))
    generated_at = provenance.get("generated_at")
    dates = {
        "generated_at": generated_at if parse_iso(generated_at) else None,
        "generated": format_local(generated_at),
        "osm": format_local(osm_at, with_time=False),
        "lamas": format_local(lamas_at, with_time=False),
    }
    dates = {key: value for key, value in dates.items() if value}
    return dates or None
