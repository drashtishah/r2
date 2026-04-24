"""
Purpose: Add the top-ranked Market Investable Equity Universe securities to the
Standard Index to reach a minimum of five constituents in a Developed Market and
three constituents in an Emerging Market.
Datapoints: market_type, stability_weighted_ff_mcap_rank.
Thresholds: DM_MIN_STANDARD_INDEX_CONSTITUENTS=5,
  EM_MIN_STANDARD_INDEX_CONSTITUENTS=3.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "2.4 Index Continuity Rules" near line 1774.
See also: methbooks/rules/ranking/rank_standard_index_candidates_by_stability_weighted_ff_mcap.py (provides ranking column).
"""
from __future__ import annotations

import polars as pl

DM_MIN_STANDARD_INDEX_CONSTITUENTS = 5
EM_MIN_STANDARD_INDEX_CONSTITUENTS = 3


def apply_standard_index_minimum_constituents(df: pl.DataFrame) -> pl.DataFrame:
    assert "stability_weighted_ff_mcap_rank" in df.columns, (
        f"stability_weighted_ff_mcap_rank column missing: {df.columns}"
    )
    assert "market_type" in df.columns, f"market_type column missing: {df.columns}"

    dm_count = df.filter(pl.col("market_type") == "DM").height
    em_count = df.filter(pl.col("market_type") == "EM").height

    assert dm_count >= DM_MIN_STANDARD_INDEX_CONSTITUENTS or dm_count == 0, (
        f"DM Standard Index has fewer than {DM_MIN_STANDARD_INDEX_CONSTITUENTS}"
        f" constituents: {dm_count}"
    )
    assert em_count >= EM_MIN_STANDARD_INDEX_CONSTITUENTS or em_count == 0, (
        f"EM Standard Index has fewer than {EM_MIN_STANDARD_INDEX_CONSTITUENTS}"
        f" constituents: {em_count}"
    )
    return df
