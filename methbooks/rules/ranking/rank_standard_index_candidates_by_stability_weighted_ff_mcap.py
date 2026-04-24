"""
Purpose: Rank securities in the updated Market Investable Equity Universe by
descending free float-adjusted market capitalization; multiply the ff-adjusted
mcap of prior Standard Index constituents by 1.5 to increase index stability
at subsequent Index Reviews.
Datapoints: ff_adjusted_mcap_usd, is_standard_index_constituent, market_type.
Thresholds: EXISTING_CONSTITUENT_STABILITY_MULTIPLIER=1.5.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "2.4 Index Continuity Rules" near line 1786.
See also: methbooks/rules/selection/apply_standard_index_minimum_constituents.py (consumes stability_weighted_ff_mcap_rank).
"""
from __future__ import annotations

import polars as pl

EXISTING_CONSTITUENT_STABILITY_MULTIPLIER = 1.5


def rank_standard_index_candidates_by_stability_weighted_ff_mcap(
    df: pl.DataFrame,
) -> pl.DataFrame:
    assert "ff_adjusted_mcap_usd" in df.columns, (
        f"ff_adjusted_mcap_usd column missing: {df.columns}"
    )
    assert "is_standard_index_constituent" in df.columns, (
        f"is_standard_index_constituent column missing: {df.columns}"
    )
    stability_mcap = pl.when(pl.col("is_standard_index_constituent")).then(
        pl.col("ff_adjusted_mcap_usd") * EXISTING_CONSTITUENT_STABILITY_MULTIPLIER
    ).otherwise(pl.col("ff_adjusted_mcap_usd"))

    out = df.with_columns(
        stability_mcap.alias("stability_weighted_ff_mcap"),
    ).with_columns(
        pl.col("stability_weighted_ff_mcap")
        .rank(method="ordinal", descending=True)
        .over("market_type")
        .alias("stability_weighted_ff_mcap_rank")
    )
    # Technical assert: rank column present and values positive.
    assert "stability_weighted_ff_mcap_rank" in out.columns, (
        f"stability_weighted_ff_mcap_rank missing after ranking: {out.columns}"
    )
    assert int(out["stability_weighted_ff_mcap_rank"].min()) >= 1, (
        f"rank < 1 found: min={out['stability_weighted_ff_mcap_rank'].min()}"
    )
    # Business assert: constituent stability multiplier applied.
    constituents = out.filter(pl.col("is_standard_index_constituent"))
    if constituents.height > 0:
        assert float(constituents["stability_weighted_ff_mcap"].min()) >= float(
            constituents["ff_adjusted_mcap_usd"].min()
            * EXISTING_CONSTITUENT_STABILITY_MULTIPLIER
        ) - 1e-6, (
            f"constituent stability_weighted_ff_mcap below expected 1.5x ff_adjusted_mcap_usd"
        )
    return out
