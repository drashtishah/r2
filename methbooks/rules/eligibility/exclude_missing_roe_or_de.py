"""
Purpose: Remove securities that cannot receive a Quality Score because a mandatory
    fundamental variable is absent.
Datapoints: roe, debt_to_equity.
Thresholds: none.
Source: methbooks/data/markdown/msci_quality_indexes_methodology_20250520.md section
    "Appendix II: Quality Z-Score Computation" near line 444.
"""
from __future__ import annotations

import polars as pl


def exclude_missing_roe_or_de(df: pl.DataFrame) -> pl.DataFrame:
    assert "roe" in df.columns, f"roe column missing: {df.columns}"
    assert "debt_to_equity" in df.columns, f"debt_to_equity column missing: {df.columns}"

    out = df.filter(pl.col("roe").is_not_null() & pl.col("debt_to_equity").is_not_null())

    assert out["roe"].null_count() == 0, (
        f"null roe survives after exclusion: {out['roe'].null_count()} rows"
    )
    assert out["debt_to_equity"].null_count() == 0, (
        f"null debt_to_equity survives after exclusion: {out['debt_to_equity'].null_count()} rows"
    )
    return out
