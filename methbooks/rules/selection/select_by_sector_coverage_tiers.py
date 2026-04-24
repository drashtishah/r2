"""
Purpose: Select securities per GICS sector from the ranked Eligible Universe using four sequential tiers until 50% cumulative free float-adjusted market capitalization coverage is reached; apply marginal-company logic and enforce a 45% coverage floor.
Datapoints: ff_market_cap, msci_esg_rating, is_current_constituent, gics_sector, rank.
Thresholds: TARGET_COVERAGE = 0.5, TIER1_CUMULATIVE_THRESHOLD = 0.35, TIER2_CUMULATIVE_THRESHOLD = 0.5, TIER3_CUMULATIVE_THRESHOLD = 0.65, MIN_COVERAGE_FLOOR = 0.45.
Source: methbooks/data/markdown/MSCI_Selection_Indexes_Methodology_20251211.md section "3.1.3 Selection of Eligible Securities" near line 382.
See also: methbooks/rules/ranking/rank_by_esg_rating_membership_industry_score_ff_mcap.py (produces rank consumed here).
"""
from __future__ import annotations

import polars as pl

TARGET_COVERAGE = 0.5
TIER1_CUMULATIVE_THRESHOLD = 0.35
TIER2_CUMULATIVE_THRESHOLD = 0.5
TIER3_CUMULATIVE_THRESHOLD = 0.65
MIN_COVERAGE_FLOOR = 0.45


def _select_sector(sector_df: pl.DataFrame) -> list[str]:
    rows = sector_df.sort("rank").to_dicts()
    total_mcap = sum(r["ff_market_cap"] for r in rows)
    if total_mcap == 0:
        return []

    selected_ids: list[str] = []
    cumulative = 0.0

    for r in rows:
        frac = r["ff_market_cap"] / total_mcap
        cumul_with = cumulative + frac

        cumul_before = cumulative
        in_tier1 = cumul_before < TIER1_CUMULATIVE_THRESHOLD
        in_tier2 = (r["msci_esg_rating"] == "AAA") and (cumul_before < TIER2_CUMULATIVE_THRESHOLD)
        in_tier3 = bool(r["is_current_constituent"]) and (cumul_before < TIER3_CUMULATIVE_THRESHOLD)
        in_tier4 = True

        if not (in_tier1 or in_tier2 or in_tier3 or in_tier4):
            continue

        if cumulative >= TARGET_COVERAGE:
            if not (in_tier1 or in_tier2 or in_tier3):
                break

        if cumulative < TARGET_COVERAGE <= cumul_with:
            is_marginal = True
        else:
            is_marginal = False

        if is_marginal and not r["is_current_constituent"]:
            diff_with = abs(cumul_with - TARGET_COVERAGE)
            diff_without = abs(cumulative - TARGET_COVERAGE)
            if diff_with >= diff_without:
                if cumulative >= MIN_COVERAGE_FLOOR:
                    break
                selected_ids.append(r["security_id"])
                cumulative = cumul_with
                break
            else:
                selected_ids.append(r["security_id"])
                cumulative = cumul_with
                break
        else:
            selected_ids.append(r["security_id"])
            cumulative = cumul_with

        if cumulative >= TARGET_COVERAGE and not (in_tier1 or in_tier2 or in_tier3):
            break

    if cumulative < MIN_COVERAGE_FLOOR:
        selected_set = set(selected_ids)
        for r in rows:
            if r["security_id"] in selected_set:
                continue
            selected_ids.append(r["security_id"])
            cumulative += r["ff_market_cap"] / total_mcap
            if cumulative >= MIN_COVERAGE_FLOOR:
                break

    return selected_ids


def select_by_sector_coverage_tiers(df: pl.DataFrame) -> pl.DataFrame:
    all_selected: set[str] = set()
    for sector in df["gics_sector"].unique().to_list():
        sector_df = df.filter(pl.col("gics_sector") == sector)
        all_selected.update(_select_sector(sector_df))

    out = df.filter(pl.col("security_id").is_in(list(all_selected)))

    assert out["security_id"].null_count() == 0, f"null security_id in output: {out['security_id'].null_count()}"
    for sector in df["gics_sector"].unique().to_list():
        sector_eligible = df.filter(pl.col("gics_sector") == sector)
        sector_selected = out.filter(pl.col("gics_sector") == sector)
        if sector_eligible.height == 0:
            continue
        total = float(sector_eligible["ff_market_cap"].sum())
        if total == 0:
            continue
        coverage = float(sector_selected["ff_market_cap"].sum()) / total
        assert coverage >= MIN_COVERAGE_FLOOR, (
            f"sector={sector}: coverage {coverage:.4f} < floor {MIN_COVERAGE_FLOOR}"
        )
    return out
