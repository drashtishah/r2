"""
Purpose: When a real or combined capital structure event increases shares and earnings are negative, recalculate loss per share using post-event shares to prevent artificial amplification of losses in P/E and P/CE index calculations.
Datapoints: trailing_12m_earnings, shares_outstanding_pre_event, shares_outstanding_post_event.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "4.2 Real Changes in Capital Structure" near line 3640.
See also: methbooks/rules/event_handling/compute_proforma_bvps_real_event.py (same real-event trigger).
"""
from __future__ import annotations

import polars as pl


def apply_loss_per_share_restatement(df: pl.DataFrame) -> pl.DataFrame:
    for col in ("trailing_12m_earnings", "shares_outstanding_pre_event", "shares_outstanding_post_event"):
        assert col in df.columns, f"{col} column missing: {df.columns}"

    out = df.with_columns(
        pl.when(
            (pl.col("trailing_12m_earnings") < 0)
            & (pl.col("shares_outstanding_post_event") > pl.col("shares_outstanding_pre_event"))
        )
        .then(
            pl.col("trailing_12m_earnings") / pl.col("shares_outstanding_post_event")
        )
        .otherwise(pl.lit(None).cast(pl.Float64))
        .alias("loss_per_share_restated")
    )
    assert "loss_per_share_restated" in out.columns, (
        f"loss_per_share_restated column missing after restatement: {out.columns}"
    )
    loss_rows = out.filter(
        (pl.col("trailing_12m_earnings") < 0)
        & (pl.col("shares_outstanding_post_event") > pl.col("shares_outstanding_pre_event"))
        & pl.col("loss_per_share_restated").is_not_null()
    )
    if loss_rows.height > 0:
        bad = loss_rows.filter(
            (pl.col("loss_per_share_restated") * pl.col("shares_outstanding_post_event") - pl.col("trailing_12m_earnings")).abs() > 1e-6
        ).height
        assert bad == 0, (
            f"loss_per_share_restated not equal to earnings / post_event_shares for {bad} rows"
        )
    non_trigger = out.filter(
        ~(
            (pl.col("trailing_12m_earnings") < 0)
            & (pl.col("shares_outstanding_post_event") > pl.col("shares_outstanding_pre_event"))
        )
        & pl.col("loss_per_share_restated").is_not_null()
    ).height
    assert non_trigger == 0, (
        f"loss_per_share_restated populated for {non_trigger} rows without trigger "
        f"(earnings < 0 and post > pre shares)"
    )
    return out
