"""
Purpose: Exclude US-classified companies that have not filed Form 10-K or 10-Q
from the USA Investable Equity Universe.
Datapoints: country_classification, has_filed_10k_10q.
Thresholds: none.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "2.2.9 Financial Reporting Requirement" near line 1248.
See also: methbooks/rules/eligibility/apply_equity_universe_minimum_size_requirement.py (companion eligibility screen).
"""
from __future__ import annotations

import polars as pl

_US_CLASSIFICATION = "US"


def apply_financial_reporting_requirement(df: pl.DataFrame) -> pl.DataFrame:
    assert "country_classification" in df.columns, (
        f"country_classification column missing: {df.columns}"
    )
    assert "has_filed_10k_10q" in df.columns, (
        f"has_filed_10k_10q column missing: {df.columns}"
    )
    out = df.filter(
        (pl.col("country_classification") != _US_CLASSIFICATION)
        | pl.col("has_filed_10k_10q")
    )
    bad = out.filter(
        (pl.col("country_classification") == _US_CLASSIFICATION)
        & (~pl.col("has_filed_10k_10q"))
    )
    assert bad.height == 0, (
        f"US-classified company with has_filed_10k_10q=False survived: {bad.height} rows"
    )
    return out
