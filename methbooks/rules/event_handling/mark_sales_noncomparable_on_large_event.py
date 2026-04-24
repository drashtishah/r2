"""
Purpose: Mark the post-event fiscal year as non-comparable in sales and earnings, dropping the 5-year growth rate calculation, when a corporate event causes annual sales to increase by more than 50% or decrease by more than 33% and the variation is not due to normal operations.
Datapoints: post_event_annual_sales, pre_event_annual_sales, event_type_flag.
Thresholds: SALES_INCREASE_THRESHOLD = 0.5, SALES_DECREASE_THRESHOLD = 0.33.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "2.2.1 Long-Term Historical Growth Trends" near line 1217.
See also: methbooks/rules/scoring/compute_egro_ols.py (growth rate suppressed for marked securities).
"""
from __future__ import annotations

import polars as pl

SALES_INCREASE_THRESHOLD = 0.5
SALES_DECREASE_THRESHOLD = 0.33


def mark_sales_noncomparable_on_large_event(df: pl.DataFrame) -> pl.DataFrame:
    for col in ("post_event_annual_sales", "pre_event_annual_sales", "event_type_flag"):
        assert col in df.columns, f"{col} column missing: {df.columns}"

    sales_change = (
        (pl.col("post_event_annual_sales") - pl.col("pre_event_annual_sales"))
        / pl.col("pre_event_annual_sales").abs()
    )
    out = df.with_columns(
        pl.when(
            pl.col("event_type_flag").is_not_null()
            & pl.col("pre_event_annual_sales").is_not_null()
            & (
                (sales_change > SALES_INCREASE_THRESHOLD)
                | (sales_change < -SALES_DECREASE_THRESHOLD)
            )
        )
        .then(pl.lit(True))
        .otherwise(pl.lit(False))
        .alias("sales_noncomparable_flag")
    )
    assert "sales_noncomparable_flag" in out.columns, (
        f"sales_noncomparable_flag column missing after mark: {out.columns}"
    )
    flagged = out.filter(pl.col("sales_noncomparable_flag"))
    if flagged.height > 0:
        change_vals = (
            (flagged["post_event_annual_sales"] - flagged["pre_event_annual_sales"])
            / flagged["pre_event_annual_sales"].abs()
        )
        bad = sum(
            1 for v in change_vals.to_list()
            if v is not None and not (v > SALES_INCREASE_THRESHOLD or v < -SALES_DECREASE_THRESHOLD)
        )
        assert bad == 0, (
            f"{bad} flagged rows do not meet sales change thresholds "
            f"(>{SALES_INCREASE_THRESHOLD*100}% increase or <{-SALES_DECREASE_THRESHOLD*100}% decrease)"
        )
    return out
