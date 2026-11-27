#!/usr/bin/env python3
"""Benchmark OpenRouter Perplexity Decider V1.3 vs Local Laya across the 83 cities.

Reads the already-computed intermediate caches (`step2_osm_normalized`,
`step3_candidates`, `step4_ai_decisions`) without modifying production HTML,
`index.html`, or `data/` files, runs both AI decision tables through the exact
same `_finalize_mappings` output gates, and writes side-by-side comparison
reports under `batch_reports/`.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_loader import city_key, load_or_fetch_LAMAS, normalize_city, safe_place_name
from normalization import build_lamas_aliases, normalize_street_name
from openrouter_ai_resolver import (
    DEFAULT_CACHE_PATH,
    DEFAULT_MAX_RPM,
    DEFAULT_OPENROUTER_MODEL,
    DEFAULT_QUESTIONS_PER_REQUEST,
    DEFAULT_SHORTLIST_SIZE,
    HybridAIResolver,
    select_candidate_shortlist,
)
from pipeline import _clean_id, _filter_lamas_by_city, _finalize_mappings
from settlement_names import canonical_key, settlement_identity_key

OFFICIAL_CITIES_PATH = ROOT / "config" / "israel_83_cities_lamas.json"
DATA_DIR = ROOT / "data"
REPORTS_DIR = ROOT / "batch_reports"

_FILTER_TAG_RE = re.compile(r"\[filtered:\s*([^\]]+)\]")


def _extract_filter_reason(reasoning: Any) -> str | None:
    text = str(reasoning or "")
    match = _FILTER_TAG_RE.search(text)
    if not match:
        return None
    return match.group(1).split("(")[0].strip()


def _summarize_decisions(
    merged_df: pd.DataFrame,
    osm_total: int,
    fuzzy_approved: int,
    lamas_aliases: dict[str, list[str]],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Summarize unique-street AI outcomes from a `_finalize_mappings` DataFrame."""
    unm = merged_df[merged_df["status"] != "CONFIDENT"].drop_duplicates(
        subset=["normalized_name"]
    )
    rows_by_street: dict[str, dict[str, Any]] = {}
    ai_suggested = 0
    ai_no_match = 0
    ai_errors = 0
    ai_approved_agree = 0
    ai_approved_override = 0
    filter_counts: dict[str, int] = {}

    for row in unm.itertuples(index=False):
        street = str(row.normalized_name)
        best_id = _clean_id(getattr(row, "best_LAMAS_id", ""))
        best_score = float(getattr(row, "best_score", 0.0) or 0.0)
        ai_id = _clean_id(getattr(row, "ai_suggested_LAMAS_id", ""))
        conf_val = getattr(row, "ai_confidence", None)
        conf = None if pd.isna(conf_val) else float(conf_val)
        reason = str(getattr(row, "ai_reasoning", "") or "")
        method = str(getattr(row, "ai_method", "") or "")
        status = str(getattr(row, "mapping_status", "UNMATCHED"))

        if "error" in method or "refusal" in method:
            ai_errors += 1
        elif not ai_id:
            ai_no_match += 1
        else:
            ai_suggested += 1
            if status == "APPROVED_AI":
                if best_id and ai_id == best_id:
                    ai_approved_agree += 1
                else:
                    ai_approved_override += 1
            else:
                ftag = _extract_filter_reason(reason) or "other"
                filter_counts[ftag] = filter_counts.get(ftag, 0) + 1

        matched_name = ""
        if ai_id and ai_id in lamas_aliases:
            matched_name = lamas_aliases[ai_id][0]
        elif status == "APPROVED_AI":
            matched_name = str(getattr(row, "best_LAMAS_name", "") or "")

        rows_by_street[street] = {
            "normalized_name": street,
            "best_LAMAS_id": best_id,
            "best_LAMAS_name_fuzzy": (lamas_aliases.get(best_id, [""])[0] if best_id else ""),
            "best_score": best_score,
            "ai_suggested_LAMAS_id": ai_id,
            "ai_suggested_name": matched_name,
            "ai_confidence": conf,
            "ai_reasoning": reason,
            "ai_method": method,
            "mapping_status": status,
            "filter_reason": _extract_filter_reason(reason) if status != "APPROVED_AI" else None,
        }

    ai_approved = ai_approved_agree + ai_approved_override
    total_matched = fuzzy_approved + ai_approved
    summary = {
        "osm_named": osm_total,
        "fuzzy_approved": fuzzy_approved,
        "pre_ai_unmatched": int(len(unm)),
        "ai_suggested": ai_suggested,
        "ai_no_match": ai_no_match,
        "ai_errors": ai_errors,
        "ai_approved": ai_approved,
        "ai_approved_agree": ai_approved_agree,
        "ai_approved_override": ai_approved_override,
        "ai_rejected": sum(filter_counts.values()),
        "filter_counts": filter_counts,
        "total_matched": total_matched,
        "coverage_pct": round(100.0 * total_matched / osm_total, 2) if osm_total else 0.0,
    }
    return summary, rows_by_street


def run_benchmark(
    *,
    model: str = DEFAULT_OPENROUTER_MODEL,
    shortlist_size: int = DEFAULT_SHORTLIST_SIZE,
    questions_per_request: int = DEFAULT_QUESTIONS_PER_REQUEST,
    max_rpm: float = DEFAULT_MAX_RPM,
    cache_path: str = DEFAULT_CACHE_PATH,
    verify_shortlist_only: bool = False,
) -> dict[str, Any]:
    with open(OFFICIAL_CITIES_PATH, "r", encoding="utf-8") as handle:
        cities = json.load(handle)["cities"]

    lamas_df = load_or_fetch_LAMAS(force_refresh=False)
    lamas_df = lamas_df.copy()
    lamas_df["city"] = normalize_city(lamas_df["city"])
    lamas_df["city_key"] = city_key(lamas_df["city"])
    lamas_df["city_identity_key"] = lamas_df["city"].map(settlement_identity_key)
    lamas_df["normalized_name"] = lamas_df["LAMAS_name"].apply(normalize_street_name)
    lamas_df = lamas_df.dropna(subset=["normalized_name"]).copy()

    city_contexts: list[dict[str, Any]] = []
    shortlist_hits = 0
    shortlist_total = 0
    shortlist_misses: list[tuple[str, str, str, str]] = []

    for city in cities:
        safe = safe_place_name(city)
        diag_path = DATA_DIR / f"diagnostic_report_{safe}.csv"
        if not diag_path.exists():
            raise FileNotFoundError(
                f"Missing diagnostic report for '{city}' ({safe}) in {DATA_DIR}"
            )

        diag_df = pd.read_csv(diag_path, low_memory=False)
        diag_df["osm_id"] = diag_df["osm_id"].astype(str)
        osm_gdf = diag_df[["osm_id", "osm_name", "normalized_name", "city"]].copy()
        candidates_df = diag_df[
            ["osm_id", "status", "best_score", "best_LAMAS_id", "best_LAMAS_name"]
        ].copy()
        candidates_df["best_LAMAS_id"] = candidates_df["best_LAMAS_id"].map(_clean_id)
        laya_df = diag_df[diag_df["status"] != "CONFIDENT"][
            [
                "osm_id",
                "ai_suggested_LAMAS_id",
                "ai_confidence",
                "ai_reasoning",
                "ai_method",
            ]
        ].copy()
        laya_df["ai_suggested_LAMAS_id"] = laya_df["ai_suggested_LAMAS_id"].map(
            lambda v: _clean_id(v) or None
        )
        laya_df["ai_reasoning"] = (
            laya_df["ai_reasoning"]
            .fillna("")
            .astype(str)
            .str.replace(r"\s*\[filtered:[^\]]+\]", "", regex=True)
            .str.strip()
        )
        osm_city_label = str(osm_gdf["city"].iloc[0])

        target_key = canonical_key(city)
        target_id_key = settlement_identity_key(city)
        lamas_city_df = _filter_lamas_by_city(
            lamas_df, target_key, target_identity_key=target_id_key
        ).copy()
        settlement_mask = lamas_city_df["LAMAS_name"].map(
            lambda n: bool(target_key) and pd.notna(n) and canonical_key(n) == target_key
        )
        settlement_ids = set(
            lamas_city_df.loc[settlement_mask, "LAMAS_id"].astype(str)
        )
        lamas_street_df = lamas_city_df[
            ~lamas_city_df["LAMAS_id"].astype(str).isin(settlement_ids)
        ]

        lamas_aliases = build_lamas_aliases(lamas_city_df)
        ai_lamas_aliases = build_lamas_aliases(lamas_street_df)
        lamas_name_by_id = {lid: names[0] for lid, names in lamas_aliases.items() if names}
        osm_names_by_id = dict(
            zip(osm_gdf["osm_id"], osm_gdf["normalized_name"].fillna("").astype(str))
        )

        # Evaluate local Laya baseline through identical _finalize_mappings
        base_merged_raw, _ = _finalize_mappings(
            candidates_df,
            laya_df,
            lamas_name_by_id=lamas_name_by_id,
            city_name=osm_city_label,
            osm_names_by_id=osm_names_by_id,
            lamas_aliases_by_id=lamas_aliases,
        )
        base_merged = osm_gdf[["osm_id", "normalized_name"]].merge(
            base_merged_raw, on="osm_id", how="left"
        )
        osm_total = int(osm_gdf["normalized_name"].nunique())
        fuzzy_approved = int(
            base_merged.loc[
                base_merged["mapping_status"] == "APPROVED_FUZZY", "normalized_name"
            ].nunique()
        )
        base_summary, base_rows = _summarize_decisions(
            base_merged, osm_total, fuzzy_approved, lamas_aliases
        )

        osm_gdf_in_city = osm_gdf[osm_gdf["city"] == osm_city_label]
        unmatched_rows = candidates_df[
            (candidates_df["status"] != "CONFIDENT")
            & (candidates_df["osm_id"].isin(osm_gdf_in_city["osm_id"]))
        ].merge(osm_gdf_in_city[["osm_id", "normalized_name"]], on="osm_id")
        unique_unmatched = unmatched_rows.dropna(
            subset=["normalized_name"]
        ).drop_duplicates(subset=["normalized_name"])
        unmatched_names = [str(r.normalized_name) for r in unique_unmatched.itertuples(index=False)]
        preferred_ids = {
            str(r.normalized_name): _clean_id(getattr(r, "best_LAMAS_id", ""))
            for r in unique_unmatched.itertuples(index=False)
            if _clean_id(getattr(r, "best_LAMAS_id", ""))
        }

        # Check shortlist recall on local Laya approved hits before calling API
        if unmatched_names and ai_lamas_aliases:
            sl = select_candidate_shortlist(
                unmatched_names,
                ai_lamas_aliases,
                shortlist_size=shortlist_size,
                preferred_ids=preferred_ids,
            )
            for st, brow in base_rows.items():
                if brow["mapping_status"] == "APPROVED_AI":
                    shortlist_total += 1
                    tid = brow["ai_suggested_LAMAS_id"]
                    if tid in sl.get(st, []):
                        shortlist_hits += 1
                    else:
                        shortlist_misses.append((city, st, tid, brow["ai_suggested_name"]))

        city_contexts.append(
            {
                "city": city,
                "osm_city_label": osm_city_label,
                "osm_total": osm_total,
                "fuzzy_approved": fuzzy_approved,
                "osm_gdf": osm_gdf,
                "candidates_df": candidates_df,
                "unmatched_rows": unmatched_rows,
                "unmatched_names": unmatched_names,
                "preferred_ids": preferred_ids,
                "lamas_aliases": lamas_aliases,
                "ai_lamas_aliases": ai_lamas_aliases,
                "lamas_name_by_id": lamas_name_by_id,
                "osm_names_by_id": osm_names_by_id,
                "base_summary": base_summary,
                "base_rows": base_rows,
            }
        )

    recall_pct = 100.0 * shortlist_hits / max(1, shortlist_total)
    print(
        f"Shortlist K={shortlist_size} recall vs Local Laya approved hits: "
        f"{shortlist_hits}/{shortlist_total} ({recall_pct:.2f}%)"
    )
    if shortlist_misses:
        print("Shortlist misses:", shortlist_misses)

    if verify_shortlist_only:
        return {
            "shortlist_hits": shortlist_hits,
            "shortlist_total": shortlist_total,
            "shortlist_recall_pct": round(recall_pct, 2),
            "shortlist_misses": shortlist_misses,
        }
    if recall_pct < 99.0:
        raise RuntimeError(
            f"Shortlist recall {recall_pct:.2f}% is below 99.0%; aborting before API calls."
        )

    # Resolve all 83 cities together using batched HybridAIResolver
    city_tasks = [
        (ctx["osm_city_label"], ctx["unmatched_names"], ctx["ai_lamas_aliases"])
        for ctx in city_contexts
        if ctx["unmatched_names"] and ctx["ai_lamas_aliases"]
    ]
    preferred_by_city = {
        ctx["osm_city_label"]: ctx["preferred_ids"] for ctx in city_contexts
    }

    hybrid = HybridAIResolver(
        model=model,
        shortlist_size=shortlist_size,
        questions_per_request=questions_per_request,
        max_rpm=max_rpm,
        cache_path=cache_path,
    )
    t_start = time.time()
    all_decisions = hybrid.resolve_multi_city(
        city_tasks, preferred_ids_by_city=preferred_by_city
    )
    wall_seconds = time.time() - t_start

    per_city_rows: list[dict[str, Any]] = []
    gained_diffs: list[dict[str, Any]] = []
    dropped_diffs: list[dict[str, Any]] = []
    changed_diffs: list[dict[str, Any]] = []
    semantic_blocked_by_wratio: list[dict[str, Any]] = []

    agg_base = {
        "osm_named": 0,
        "fuzzy_approved": 0,
        "pre_ai_unmatched": 0,
        "ai_suggested": 0,
        "ai_no_match": 0,
        "ai_errors": 0,
        "ai_approved": 0,
        "ai_approved_agree": 0,
        "ai_approved_override": 0,
        "ai_rejected": 0,
        "filter_counts": {},
    }
    agg_or = {k: (dict(v) if isinstance(v, dict) else 0) for k, v in agg_base.items()}

    for ctx in city_contexts:
        label = ctx["osm_city_label"]
        city_dec = all_decisions.get(label, {})
        valid_ids = set(ctx["ai_lamas_aliases"])
        ai_rows = []
        for row in ctx["unmatched_rows"].itertuples(index=False):
            dec = city_dec.get(row.normalized_name)
            if not dec:
                continue
            lid = dec.get("lamas_id")
            reasoning = dec.get("reasoning", "")
            if lid is not None:
                lid = str(lid).strip()
                if lid.lower() in {"", "none", "null", "nan"}:
                    lid = None
                elif lid not in valid_ids:
                    lid = None
                    reasoning = "AI returned an ID outside the city LAMAS list"
            ai_rows.append(
                {
                    "osm_id": row.osm_id,
                    "ai_suggested_LAMAS_id": lid,
                    "ai_confidence": dec.get("confidence"),
                    "ai_reasoning": reasoning,
                    "ai_method": dec.get("method", f"openrouter:{model}"),
                }
            )
        or_df = pd.DataFrame(
            ai_rows,
            columns=[
                "osm_id",
                "ai_suggested_LAMAS_id",
                "ai_confidence",
                "ai_reasoning",
                "ai_method",
            ],
        )
        or_merged_raw, _ = _finalize_mappings(
            ctx["candidates_df"],
            or_df,
            lamas_name_by_id=ctx["lamas_name_by_id"],
            city_name=label,
            osm_names_by_id=ctx["osm_names_by_id"],
            lamas_aliases_by_id=ctx["lamas_aliases"],
        )
        or_merged = ctx["osm_gdf"][["osm_id", "normalized_name"]].merge(
            or_merged_raw, on="osm_id", how="left"
        )
        or_summary, or_rows = _summarize_decisions(
            or_merged, ctx["osm_total"], ctx["fuzzy_approved"], ctx["lamas_aliases"]
        )
        base_summary = ctx["base_summary"]
        base_rows = ctx["base_rows"]

        for agg, src in ((agg_base, base_summary), (agg_or, or_summary)):
            for key in (
                "osm_named",
                "fuzzy_approved",
                "pre_ai_unmatched",
                "ai_suggested",
                "ai_no_match",
                "ai_errors",
                "ai_approved",
                "ai_approved_agree",
                "ai_approved_override",
                "ai_rejected",
            ):
                agg[key] += src[key]
            for fk, fv in src["filter_counts"].items():
                agg["filter_counts"][fk] = agg["filter_counts"].get(fk, 0) + fv

        per_city_rows.append(
            {
                "city": ctx["city"],
                "osm_named": ctx["osm_total"],
                "fuzzy_approved": ctx["fuzzy_approved"],
                "pre_ai_unmatched": base_summary["pre_ai_unmatched"],
                "laya_suggested": base_summary["ai_suggested"],
                "laya_approved": base_summary["ai_approved"],
                "laya_rejected": base_summary["ai_rejected"],
                "laya_cov_pct": base_summary["coverage_pct"],
                "or_suggested": or_summary["ai_suggested"],
                "or_approved": or_summary["ai_approved"],
                "or_approved_agree": or_summary["ai_approved_agree"],
                "or_approved_override": or_summary["ai_approved_override"],
                "or_rejected": or_summary["ai_rejected"],
                "or_errors": or_summary["ai_errors"],
                "or_cov_pct": or_summary["coverage_pct"],
                "delta_ai_approved": or_summary["ai_approved"] - base_summary["ai_approved"],
                "delta_cov_pct": round(
                    or_summary["coverage_pct"] - base_summary["coverage_pct"], 2
                ),
            }
        )

        # Record per-street diffs
        for st, or_r in or_rows.items():
            b_r = base_rows.get(st, {})
            or_ok = or_r["mapping_status"] == "APPROVED_AI"
            b_ok = b_r.get("mapping_status") == "APPROVED_AI"

            if or_ok and not b_ok:
                gained_diffs.append(
                    {
                        "city": ctx["city"],
                        "osm_street": st,
                        "or_lamas_id": or_r["ai_suggested_LAMAS_id"],
                        "or_lamas_name": or_r["ai_suggested_name"],
                        "or_conf": round(or_r["ai_confidence"] or 0.0, 3),
                        "laya_status": (
                            f"filtered:{b_r.get('filter_reason')} ({b_r.get('ai_suggested_name','')})"
                            if b_r.get("ai_suggested_LAMAS_id")
                            else "no_match"
                        ),
                        "fuzzy_top1": b_r.get("best_LAMAS_name_fuzzy", ""),
                        "fuzzy_score": round(or_r["best_score"], 1),
                    }
                )
            elif b_ok and not or_ok:
                dropped_diffs.append(
                    {
                        "city": ctx["city"],
                        "osm_street": st,
                        "laya_lamas_id": b_r.get("ai_suggested_LAMAS_id"),
                        "laya_lamas_name": b_r.get("ai_suggested_name"),
                        "laya_conf": round(b_r.get("ai_confidence") or 0.0, 3),
                        "or_status": (
                            f"filtered:{or_r.get('filter_reason')} ({or_r.get('ai_suggested_name','')}, conf={round(or_r['ai_confidence'] or 0, 3)})"
                            if or_r.get("ai_suggested_LAMAS_id")
                            else f"no_match (conf={round(or_r['ai_confidence'] or 0, 3)})"
                        ),
                    }
                )
            elif or_ok and b_ok and or_r["ai_suggested_LAMAS_id"] != b_r.get("ai_suggested_LAMAS_id"):
                changed_diffs.append(
                    {
                        "city": ctx["city"],
                        "osm_street": st,
                        "laya_lamas_id": b_r.get("ai_suggested_LAMAS_id"),
                        "laya_lamas_name": b_r.get("ai_suggested_name"),
                        "laya_conf": round(b_r.get("ai_confidence") or 0.0, 3),
                        "or_lamas_id": or_r["ai_suggested_LAMAS_id"],
                        "or_lamas_name": or_r["ai_suggested_name"],
                        "or_conf": round(or_r["ai_confidence"] or 0.0, 3),
                    }
                )

            if (
                not or_ok
                and or_r.get("filter_reason") in {"low_core_wratio", "override_low_core_wratio"}
                and (or_r.get("ai_confidence") or 0.0) >= 0.85
            ):
                semantic_blocked_by_wratio.append(
                    {
                        "city": ctx["city"],
                        "osm_street": st,
                        "or_lamas_id": or_r["ai_suggested_LAMAS_id"],
                        "or_lamas_name": or_r["ai_suggested_name"],
                        "or_conf": round(or_r["ai_confidence"] or 0.0, 3),
                        "reason": or_r["ai_reasoning"],
                    }
                )

    agg_base["total_matched"] = agg_base["fuzzy_approved"] + agg_base["ai_approved"]
    agg_base["coverage_pct"] = round(
        100.0 * agg_base["total_matched"] / max(1, agg_base["osm_named"]), 2
    )
    agg_or["total_matched"] = agg_or["fuzzy_approved"] + agg_or["ai_approved"]
    agg_or["coverage_pct"] = round(
        100.0 * agg_or["total_matched"] / max(1, agg_or["osm_named"]), 2
    )

    report = {
        "model": model,
        "cities_count": len(cities),
        "wall_seconds": round(wall_seconds, 2),
        "shortlist": {
            "k": shortlist_size,
            "hits": shortlist_hits,
            "total": shortlist_total,
            "recall_pct": round(recall_pct, 2),
            "misses": shortlist_misses,
        },
        "telemetry": {
            **hybrid.primary.stats,
            "hybrid": hybrid.stats,
            "total_api_seconds": round(hybrid.primary.stats["total_api_seconds"], 2),
        },
        "baseline_local_laya": agg_base,
        "openrouter_decider": agg_or,
        "per_city": per_city_rows,
        "diffs": {
            "gained_count": len(gained_diffs),
            "dropped_count": len(dropped_diffs),
            "changed_id_count": len(changed_diffs),
            "semantic_blocked_by_wratio_count": len(semantic_blocked_by_wratio),
            "gained": gained_diffs,
            "dropped": dropped_diffs,
            "changed_id": changed_diffs,
            "semantic_blocked_by_wratio": semantic_blocked_by_wratio,
        },
    }

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = REPORTS_DIR / "benchmark_openrouter_vs_laya_83cities.csv"
    json_path = REPORTS_DIR / "benchmark_openrouter_vs_laya_83cities.json"
    md_path = REPORTS_DIR / "benchmark_openrouter_vs_laya_83cities.md"

    pd.DataFrame(per_city_rows).sort_values("delta_ai_approved", ascending=False).to_csv(
        csv_path, index=False, encoding="utf-8-sig"
    )
    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)

    # Render readable Markdown report
    lines = [
        f"# 83-City AI Benchmark: `{model}` vs Local Laya",
        "",
        "## Executive Summary",
        f"- **Model**: `{model}` (`shortlist_size={shortlist_size}`, `questions_per_request={questions_per_request}`)",
        f"- **Shortlist recall vs Local Laya approved hits**: **{shortlist_hits}/{shortlist_total} ({recall_pct:.2f}%)**",
        f"- **Wall time (83 cities / {agg_or['pre_ai_unmatched']} unmatched streets)**: **{wall_seconds:.2f}s** (pure API time: {report['telemetry']['total_api_seconds']}s)",
        f"- **OpenRouter usage**: `api_requests={hybrid.primary.stats['api_requests']}`, `cache_hits={hybrid.primary.stats['cache_hits']}`, `input_tokens={hybrid.primary.stats['total_input_tokens']:,}`, `cost=${hybrid.primary.stats['total_cost']:.4f}`",
        f"- **Fallback / errors**: `fallback_events={hybrid.stats['fallback_events']}`, `local_fallback_resolved={hybrid.stats['local_fallback_resolved']}`, `errors={hybrid.stats['errors']}`",
        "",
        "| Metric | Local Laya | OpenRouter Decider V1.3 | Delta |",
        "|---|---:|---:|---:|",
        f"| Pre-AI unmatched streets | {agg_base['pre_ai_unmatched']:,} | {agg_or['pre_ai_unmatched']:,} | 0 |",
        f"| AI raw suggestions | {agg_base['ai_suggested']:,} | {agg_or['ai_suggested']:,} | {agg_or['ai_suggested'] - agg_base['ai_suggested']:+,} |",
        f"| AI explicit `no_match` | {agg_base['ai_no_match']:,} | {agg_or['ai_no_match']:,} | {agg_or['ai_no_match'] - agg_base['ai_no_match']:+,} |",
        f"| **APPROVED_AI total** | **{agg_base['ai_approved']:,}** | **{agg_or['ai_approved']:,}** | **{agg_or['ai_approved'] - agg_base['ai_approved']:+,}** |",
        f"| APPROVED_AI agree / override | {agg_base['ai_approved_agree']} / {agg_base['ai_approved_override']} | {agg_or['ai_approved_agree']} / {agg_or['ai_approved_override']} | - |",
        f"| Filtered out by guardrails | {agg_base['ai_rejected']:,} | {agg_or['ai_rejected']:,} | {agg_or['ai_rejected'] - agg_base['ai_rejected']:+,} |",
        f"| Total city coverage | {agg_base['coverage_pct']:.2f}% ({agg_base['total_matched']:,}/{agg_base['osm_named']:,}) | {agg_or['coverage_pct']:.2f}% ({agg_or['total_matched']:,}/{agg_or['osm_named']:,}) | {agg_or['coverage_pct'] - agg_base['coverage_pct']:+.2f}pp |",
        "",
        "## Guardrail Rejection Breakdown",
        f"- Local Laya: `{json.dumps(agg_base['filter_counts'], ensure_ascii=False)}`",
        f"- OpenRouter Decider: `{json.dumps(agg_or['filter_counts'], ensure_ascii=False)}`",
        "",
        f"## Match Differences Summary: Gained={len(gained_diffs)}, Dropped={len(dropped_diffs)}, Changed ID={len(changed_diffs)}, High-Conf Semantic Blocked by WRatio={len(semantic_blocked_by_wratio)}",
        "",
        "### Top Cities by Net APPROVED_AI Gain",
        "| City | Named | Unmatched | Laya AI | OpenRouter AI | Delta | Laya % | OpenRouter % |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in sorted(per_city_rows, key=lambda r: (-r["delta_ai_approved"], -r["or_approved"]))[:25]:
        lines.append(
            f"| {row['city']} | {row['osm_named']} | {row['pre_ai_unmatched']} | "
            f"{row['laya_approved']} | {row['or_approved']} | {row['delta_ai_approved']:+d} | "
            f"{row['laya_cov_pct']:.1f}% | {row['or_cov_pct']:.1f}% |"
        )

    def _add_diff_section(title: str, items: list[dict[str, Any]], cols: list[str], limit: int = 80) -> None:
        lines.append("")
        lines.append(f"### {title} ({len(items)} total, showing up to {limit})")
        if not items:
            lines.append("_None_")
            return
        lines.append("| " + " | ".join(cols) + " |")
        lines.append("|" + "|".join(["---"] * len(cols)) + "|")
        for item in items[:limit]:
            lines.append("| " + " | ".join(str(item.get(c, "")).replace("|", "/") for c in cols) + " |")

    _add_diff_section(
        "Gained Matches (OpenRouter Approved, Laya Missed)",
        gained_diffs,
        ["city", "osm_street", "or_lamas_name", "or_conf", "fuzzy_score", "laya_status"],
        limit=100,
    )
    _add_diff_section(
        "Dropped Matches (Laya Approved, OpenRouter Missed/Rejected)",
        dropped_diffs,
        ["city", "osm_street", "laya_lamas_name", "laya_conf", "or_status"],
        limit=100,
    )
    _add_diff_section(
        "Changed LAMAS ID (Both Approved, Different Target)",
        changed_diffs,
        ["city", "osm_street", "laya_lamas_name", "laya_conf", "or_lamas_name", "or_conf"],
        limit=100,
    )
    _add_diff_section(
        "High-Confidence OpenRouter Matches (>=0.85) Blocked by WRatio Guardrail",
        semantic_blocked_by_wratio,
        ["city", "osm_street", "or_lamas_name", "or_conf", "reason"],
        limit=80,
    )

    md_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote benchmark reports:\n  {csv_path}\n  {json_path}\n  {md_path}")
    return report


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = argparse.ArgumentParser(description="Benchmark OpenRouter vs Laya on 83 cities")
    parser.add_argument("--model", default=DEFAULT_OPENROUTER_MODEL)
    parser.add_argument("--shortlist-size", type=int, default=DEFAULT_SHORTLIST_SIZE)
    parser.add_argument("--questions-per-request", type=int, default=DEFAULT_QUESTIONS_PER_REQUEST)
    parser.add_argument("--max-rpm", type=float, default=DEFAULT_MAX_RPM)
    parser.add_argument("--cache-path", default=DEFAULT_CACHE_PATH)
    parser.add_argument("--verify-shortlist-only", action="store_true")
    args = parser.parse_args()

    run_benchmark(
        model=args.model,
        shortlist_size=args.shortlist_size,
        questions_per_request=args.questions_per_request,
        max_rpm=args.max_rpm,
        cache_path=args.cache_path,
        verify_shortlist_only=args.verify_shortlist_only,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
