"""
Purpose: Exclude from Micro Cap companies whose full company and full security
market cap falls below the Micro Cap Minimum Size Requirement (99.8% DM Equity
Universe coverage equivalent).
Datapoints: full_market_cap_usd, full_security_market_cap_usd, micro_cap_min_size_usd.
Thresholds: DM_EQUITY_UNIVERSE_COVERAGE_TARGET_PCT=99.8.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "4.1.2 Micro Cap Minimum Size Requirement" near line 3341.
See also: methbooks/rules/eligibility/apply_micro_cap_maximum_size_requirement.py (Micro Cap upper bound).
"""
from __future__ import annotations

import polars as pl

DM_EQUITY_UNIVERSE_COVERAGE_TARGET_PCT = 99.8


def apply_micro_cap_minimum_size_requirement(df: pl.DataFrame) -> pl.DataFrame:
    assert "micro_cap_min_size_usd" in df.columns, (
        f"micro_cap_min_size_usd column missing: {df.columns}"
    )
    out = df.filter(
        (pl.col("full_market_cap_usd") >= pl.col("micro_cap_min_size_usd"))
        & (pl.col("full_security_market_cap_usd") >= pl.col("micro_cap_min_size_usd"))
    )
    bad_company = out.filter(
        pl.col("full_market_cap_usd") < pl.col("micro_cap_min_size_usd")
    )
    assert bad_company.height == 0, (
        f"Micro Cap company with full_market_cap_usd < micro_cap_min_size_usd"
        f" survived: {bad_company.height} rows;"
        f" min full_market_cap_usd={bad_company['full_market_cap_usd'].min()}"
    )
    bad_security = out.filter(
        pl.col("full_security_market_cap_usd") < pl.col("micro_cap_min_size_usd")
    )
    assert bad_security.height == 0, (
        f"Micro Cap security with full_security_market_cap_usd < micro_cap_min_size_usd"
        f" survived: {bad_security.height} rows;"
        f" min full_security_market_cap_usd={bad_security['full_security_market_cap_usd'].min()}"
    )
    return out
