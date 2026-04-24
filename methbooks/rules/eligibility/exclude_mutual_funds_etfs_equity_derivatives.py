"""
Purpose: Remove instruments that are not direct equity exposures (mutual funds, ETFs,
equity derivatives, most investment trusts) from the Equity Universe.
Datapoints: security_type.
Thresholds: none.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "2.1.1 Identifying Eligible Equity Securities" near line 845.
See also: methbooks/rules/eligibility/exclude_ineligible_preferred_shares.py (companion eligibility screen).
"""
from __future__ import annotations

import polars as pl

_INELIGIBLE_TYPES = {"mutual_fund", "etf", "equity_derivative"}


def exclude_mutual_funds_etfs_equity_derivatives(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(~pl.col("security_type").is_in(_INELIGIBLE_TYPES))
    assert "security_type" in out.columns, (
        f"security_type column missing after filter: {out.columns}"
    )
    surviving = out.filter(pl.col("security_type").is_in(_INELIGIBLE_TYPES))
    assert surviving.height == 0, (
        f"mutual fund/ETF/equity derivative rows survived: {surviving.height}"
    )
    return out
