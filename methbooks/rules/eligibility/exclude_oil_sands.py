"""
Purpose: Exclude companies with >= 5% oil sands extraction revenue AND proven oil sands reserves.
Datapoints: oil_sands_extraction_revenue_pct, has_oil_sands_reserves.
Thresholds: 5.0 (revenue pct).
Source: methbooks/data/markdown/MSCI_Climate_Action_Indexes_Methodology_20251211.md section "Appendix I: ESG Business Involvement Eligibility" near line 613.
See also: methbooks/rules/event_handling/quarterly_controversies_bisr_deletion.py (re-applied at quarterly review).
"""
from __future__ import annotations

import polars as pl

OIL_SANDS_REVENUE_THRESHOLD = 5.0


def exclude_oil_sands(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(
        ~(
            (pl.col("oil_sands_extraction_revenue_pct") >= OIL_SANDS_REVENUE_THRESHOLD)
            & pl.col("has_oil_sands_reserves")
        )
    )
    assert "oil_sands_extraction_revenue_pct" in out.columns, (
        f"oil_sands_extraction_revenue_pct column missing: {out.columns}"
    )
    survivors = out.filter(
        (pl.col("oil_sands_extraction_revenue_pct") >= OIL_SANDS_REVENUE_THRESHOLD)
        & pl.col("has_oil_sands_reserves")
    )
    assert survivors.height == 0, (
        f"oil sands rows survived: {survivors.height}"
    )
    return out
