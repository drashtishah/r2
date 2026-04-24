"""
Purpose: Do not adjust the current dividend rate when the PAF from a nominal or combined nominal and real capital structure event falls within 0.80 to 1.25, assuming the event will not change dividend policy.
Datapoints: price_adjustment_factor, annualized_dps.
Thresholds: PAF_LOWER_BOUND = 0.8, PAF_UPPER_BOUND = 1.25.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "4.1 Nominal Changes in Capital Structure" near line 3590.
See also: methbooks/rules/event_handling/apply_paf_to_per_share_data_nominal_event.py (PAF applied to other per-share data).
"""
from __future__ import annotations

import polars as pl

PAF_LOWER_BOUND = 0.8
PAF_UPPER_BOUND = 1.25


def suppress_small_event_dps_adjustment(df: pl.DataFrame) -> pl.DataFrame:
    assert "price_adjustment_factor" in df.columns, (
        f"price_adjustment_factor column missing: {df.columns}"
    )
    assert "annualized_dps" in df.columns, (
        f"annualized_dps column missing: {df.columns}"
    )
    original_dps = df["annualized_dps"].clone()
    out = df.with_columns(
        pl.when(
            (pl.col("price_adjustment_factor") >= PAF_LOWER_BOUND)
            & (pl.col("price_adjustment_factor") <= PAF_UPPER_BOUND)
        )
        .then(pl.col("annualized_dps"))
        .otherwise(pl.col("annualized_dps") * pl.col("price_adjustment_factor"))
        .alias("annualized_dps")
    )
    assert "annualized_dps" in out.columns, (
        f"annualized_dps column missing after suppress: {out.columns}"
    )
    exempt_rows = out.filter(
        (pl.col("price_adjustment_factor") >= PAF_LOWER_BOUND)
        & (pl.col("price_adjustment_factor") <= PAF_UPPER_BOUND)
    )
    if exempt_rows.height > 0:
        orig_exempt = df.filter(
            (pl.col("price_adjustment_factor") >= PAF_LOWER_BOUND)
            & (pl.col("price_adjustment_factor") <= PAF_UPPER_BOUND)
        )["annualized_dps"]
        changed = (exempt_rows["annualized_dps"] - orig_exempt).abs().max()
        assert changed is None or changed < 1e-9, (
            f"annualized_dps changed for rows with PAF in [{PAF_LOWER_BOUND}, {PAF_UPPER_BOUND}]: "
            f"max_change={changed}"
        )
    return out
