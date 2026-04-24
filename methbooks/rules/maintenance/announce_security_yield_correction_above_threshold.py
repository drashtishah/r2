"""
Purpose: Trigger a security-level announcement with revised valuation ratios when an error causes the incorrect security yield to deviate from the correct yield by more than 0.25 percent (0.5 percent for book value yield); historical security data is always revised regardless of magnitude.
Datapoints: security_yield_correct, security_yield_incorrect, ratio_type.
Thresholds: SECURITY_YIELD_THRESHOLD = 0.0025, SECURITY_BV_YIELD_THRESHOLD = 0.005.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "Appendix III: Correction Policy" near line 4467.
See also: methbooks/rules/maintenance/apply_index_revision_above_yield_threshold.py (index-level equivalent).
"""
from __future__ import annotations

import polars as pl

SECURITY_YIELD_THRESHOLD = 0.0025
SECURITY_BV_YIELD_THRESHOLD = 0.005


def announce_security_yield_correction_above_threshold(df: pl.DataFrame) -> pl.DataFrame:
    for col in ("security_yield_correct", "security_yield_incorrect", "ratio_type"):
        assert col in df.columns, f"{col} column missing: {df.columns}"

    threshold_expr = pl.when(pl.col("ratio_type") == "book_value_yield").then(
        pl.lit(SECURITY_BV_YIELD_THRESHOLD)
    ).otherwise(pl.lit(SECURITY_YIELD_THRESHOLD))

    yield_error = (pl.col("security_yield_correct") - pl.col("security_yield_incorrect")).abs()
    out = df.with_columns(
        pl.lit(True).alias("security_historical_revision_required"),
        pl.when(yield_error > threshold_expr)
        .then(pl.lit(True))
        .otherwise(pl.lit(False))
        .alias("security_announcement_required"),
    )
    assert "security_announcement_required" in out.columns, (
        f"security_announcement_required column missing after compute: {out.columns}"
    )
    assert "security_historical_revision_required" in out.columns, (
        f"security_historical_revision_required column missing after compute: {out.columns}"
    )
    all_revised = out["security_historical_revision_required"].all()
    assert all_revised, (
        f"security_historical_revision_required not True for all rows: "
        f"all_revised={all_revised}"
    )
    bv_rows = out.filter(
        (pl.col("ratio_type") == "book_value_yield")
        & ((pl.col("security_yield_correct") - pl.col("security_yield_incorrect")).abs() > SECURITY_BV_YIELD_THRESHOLD)
        & (~pl.col("security_announcement_required"))
    ).height
    assert bv_rows == 0, (
        f"security_announcement_required False for {bv_rows} book_value_yield rows exceeding threshold"
    )
    non_bv_rows = out.filter(
        (pl.col("ratio_type") != "book_value_yield")
        & ((pl.col("security_yield_correct") - pl.col("security_yield_incorrect")).abs() > SECURITY_YIELD_THRESHOLD)
        & (~pl.col("security_announcement_required"))
    ).height
    assert non_bv_rows == 0, (
        f"security_announcement_required False for {non_bv_rows} non-BV yield rows exceeding threshold"
    )
    return out
