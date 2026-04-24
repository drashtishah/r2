"""
Purpose: Trigger recalculation and announcement of index-level valuation ratios only when the error in country or industry index yield exceeds 0.1 percent (0.5 percent for book value yield); below threshold, no index revision or announcement is made.
Datapoints: index_yield_correct, index_yield_incorrect, ratio_type.
Thresholds: INDEX_YIELD_THRESHOLD = 0.001, INDEX_BV_YIELD_THRESHOLD = 0.005.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "Appendix III: Correction Policy" near line 4475.
See also: methbooks/rules/maintenance/announce_security_yield_correction_above_threshold.py (security-level equivalent).
"""
from __future__ import annotations

import polars as pl

INDEX_YIELD_THRESHOLD = 0.001
INDEX_BV_YIELD_THRESHOLD = 0.005


def apply_index_revision_above_yield_threshold(df: pl.DataFrame) -> pl.DataFrame:
    for col in ("index_yield_correct", "index_yield_incorrect", "ratio_type"):
        assert col in df.columns, f"{col} column missing: {df.columns}"

    threshold_expr = pl.when(pl.col("ratio_type") == "book_value_yield").then(
        pl.lit(INDEX_BV_YIELD_THRESHOLD)
    ).otherwise(pl.lit(INDEX_YIELD_THRESHOLD))

    yield_error = (pl.col("index_yield_correct") - pl.col("index_yield_incorrect")).abs()
    out = df.with_columns(
        pl.when(yield_error > threshold_expr)
        .then(pl.lit(True))
        .otherwise(pl.lit(False))
        .alias("index_revision_required"),
    )
    assert "index_revision_required" in out.columns, (
        f"index_revision_required column missing after compute: {out.columns}"
    )
    no_action = out.filter(~pl.col("index_revision_required"))
    if no_action.height > 0:
        bv_over = no_action.filter(
            (pl.col("ratio_type") == "book_value_yield")
            & ((pl.col("index_yield_correct") - pl.col("index_yield_incorrect")).abs() > INDEX_BV_YIELD_THRESHOLD)
        ).height
        assert bv_over == 0, (
            f"index_revision_required False for {bv_over} book_value_yield rows exceeding threshold"
        )
        non_bv_over = no_action.filter(
            (pl.col("ratio_type") != "book_value_yield")
            & ((pl.col("index_yield_correct") - pl.col("index_yield_incorrect")).abs() > INDEX_YIELD_THRESHOLD)
        ).height
        assert non_bv_over == 0, (
            f"index_revision_required False for {non_bv_over} non-BV yield rows exceeding threshold"
        )
    return out
