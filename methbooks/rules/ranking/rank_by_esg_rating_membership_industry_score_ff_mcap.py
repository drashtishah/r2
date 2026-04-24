"""
Purpose: Rank eligible securities within each GICS sector by ESG Rating (highest first), then by current Selection Index membership (existing constituents above non-constituents), then by industry-adjusted ESG score (highest first), then by free float-adjusted market capitalization (largest first), to produce stable ESG-quality-led ordering before coverage selection.
Datapoints: msci_esg_rating, is_current_constituent, industry_adjusted_esg_score, ff_market_cap, gics_sector.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Selection_Indexes_Methodology_20251211.md section "3.1.2 Ranking of Eligible Securities" near line 362.
See also: methbooks/rules/selection/select_by_sector_coverage_tiers.py (consumes the rank column produced here).
"""
from __future__ import annotations

import polars as pl

_RATING_ORDER = {"AAA": 0, "AA": 1, "A": 2, "BBB": 3, "BB": 4, "B": 5, "CCC": 6}


def rank_by_esg_rating_membership_industry_score_ff_mcap(df: pl.DataFrame) -> pl.DataFrame:
    sectors = df["gics_sector"].unique().to_list()
    ranked_parts: list[pl.DataFrame] = []
    for sector in sectors:
        part = (
            df.filter(pl.col("gics_sector") == sector)
            .with_columns(
                pl.col("msci_esg_rating").replace(_RATING_ORDER).cast(pl.Int32).alias("_rating_ord"),
                pl.when(pl.col("is_current_constituent")).then(pl.lit(0)).otherwise(pl.lit(1)).cast(pl.Int32).alias("_constituent_ord"),
                (-pl.col("industry_adjusted_esg_score")).alias("_neg_score"),
                (-pl.col("ff_market_cap")).alias("_neg_mcap"),
            )
            .sort(["_rating_ord", "_constituent_ord", "_neg_score", "_neg_mcap"])
            .with_columns(
                (pl.int_range(pl.len(), dtype=pl.UInt32) + 1).alias("rank")
            )
            .drop(["_rating_ord", "_constituent_ord", "_neg_score", "_neg_mcap"])
        )
        ranked_parts.append(part)
    out = pl.concat(ranked_parts)

    assert "rank" in out.columns, f"rank column missing: {out.columns}"
    assert out["rank"].null_count() == 0, f"null ranks present: {out['rank'].null_count()}"
    sector_rank_unique = out.group_by(["gics_sector", "rank"]).len().filter(pl.col("len") > 1)
    assert sector_rank_unique.height == 0, (
        f"duplicate ranks within sector: {sector_rank_unique.head(5).to_dicts()}"
    )
    for sector in out["gics_sector"].unique().to_list():
        s = out.filter(pl.col("gics_sector") == sector).with_columns(
            pl.col("msci_esg_rating").replace(_RATING_ORDER).cast(pl.Int32).alias("_ro")
        )
        if s.height < 2:
            continue
        ratings_present = sorted(s["_ro"].unique().to_list())
        for i in range(len(ratings_present) - 1):
            better_rating = ratings_present[i]
            worse_rating = ratings_present[i + 1]
            max_rank_better = int(s.filter(pl.col("_ro") == better_rating)["rank"].max())
            min_rank_worse = int(s.filter(pl.col("_ro") == worse_rating)["rank"].min())
            assert max_rank_better < min_rank_worse, (
                f"sector={sector}: row with rating_ord={better_rating} has rank {max_rank_better} "
                f">= min rank {min_rank_worse} of rating_ord={worse_rating}"
            )
    return out
