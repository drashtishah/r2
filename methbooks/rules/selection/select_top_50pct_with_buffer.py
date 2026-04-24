"""
Purpose: Select top securities per sector: guaranteed top 40%, buffer band up to 60%
    preferring existing constituents, then fill to reach 50% target.
Datapoints: sector_rank, gics_sector, is_current_constituent.
Thresholds: 0.40 (guaranteed cutoff), 0.50 (target), 0.60 (buffer band upper bound).
Source: methbooks/data/markdown/MSCI_Climate_Action_Indexes_Methodology_20251211.md section "Selection" near line 1.
See also: methbooks/rules/ranking/rank_by_assessment_then_mcap.py (provides sector_rank).
"""
from __future__ import annotations

import math

import polars as pl


def select_top_50pct_with_buffer(df: pl.DataFrame) -> pl.DataFrame:
    selected_ids: set[str] = set()

    for sector in df["gics_sector"].unique().to_list():
        sector_df = df.filter(pl.col("gics_sector") == sector).sort("sector_rank")
        n = sector_df.height
        cutoff_40 = math.floor(0.40 * n)
        cutoff_60 = math.ceil(0.60 * n)
        target_50 = math.ceil(0.50 * n)

        # Step 1: guaranteed top 40%
        top_ids = sector_df.filter(pl.col("sector_rank") <= cutoff_40)["security_id"].to_list()
        selected_ids.update(top_ids)

        # Step 2: band: rank > cutoff_40 and rank <= cutoff_60
        band = sector_df.filter(
            (pl.col("sector_rank") > cutoff_40) & (pl.col("sector_rank") <= cutoff_60)
        )
        constituents_in_band = band.filter(pl.col("is_current_constituent"))["security_id"].to_list()
        non_constituents_in_band = band.filter(~pl.col("is_current_constituent"))["security_id"].to_list()

        # Add existing constituents in band first
        selected_ids.update(constituents_in_band)

        # Step 3: fill with non-constituents until >= 50% target (include the one that tips it over)
        current_count = len([sid for sid in sector_df["security_id"].to_list() if sid in selected_ids])
        for sid in non_constituents_in_band:
            if current_count >= target_50:
                break
            selected_ids.add(sid)
            current_count += 1

    out = df.with_columns(
        pl.col("security_id").is_in(list(selected_ids)).alias("selected")
    )
    assert "selected" in out.columns, f"selected column missing: {out.columns}"
    return out
