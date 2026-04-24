"""
Purpose: Exclude preferred shares whose terms resemble fixed-income instruments
(fixed dividend entitlement or par-value-capped liquidation claim), retaining only
those whose only difference from common shares is limited voting power.
Datapoints: preferred_share_flag, fixed_dividend_flag, par_value_liquidation_flag.
Thresholds: none.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "2.1.1 Identifying Eligible Equity Securities" near line 866.
See also: methbooks/rules/eligibility/exclude_mutual_funds_etfs_equity_derivatives.py (companion eligibility screen).
"""
from __future__ import annotations

import polars as pl


def exclude_ineligible_preferred_shares(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(
        ~(
            pl.col("preferred_share_flag")
            & (pl.col("fixed_dividend_flag") | pl.col("par_value_liquidation_flag"))
        )
    )
    assert "preferred_share_flag" in out.columns, (
        f"preferred_share_flag column missing: {out.columns}"
    )
    bad = out.filter(
        pl.col("preferred_share_flag")
        & (pl.col("fixed_dividend_flag") | pl.col("par_value_liquidation_flag"))
    )
    assert bad.height == 0, (
        f"preferred share with fixed_dividend_flag=True or par_value_liquidation_flag=True"
        f" survived: {bad.height} rows"
    )
    return out
