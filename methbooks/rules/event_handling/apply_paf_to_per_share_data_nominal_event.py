"""
Purpose: Adjust all fundamental per-share data by the price adjustment factor for nominal capital structure changes (no change in market cap), ensuring per-share figures remain consistent with the adjusted price; also applies to combined nominal and real events.
Datapoints: price_adjustment_factor, per_share_fundamental_data.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "4.1 Nominal Changes in Capital Structure" near line 3563.
See also: methbooks/rules/event_handling/suppress_small_event_dps_adjustment.py (DPS exempt when PAF in [0.80, 1.25]).
"""
from __future__ import annotations

import polars as pl


def apply_paf_to_per_share_data_nominal_event(df: pl.DataFrame) -> pl.DataFrame:
    assert "price_adjustment_factor" in df.columns, (
        f"price_adjustment_factor column missing: {df.columns}"
    )
    assert "per_share_fundamental_data" in df.columns, (
        f"per_share_fundamental_data column missing: {df.columns}"
    )
    paf_min = float(df["price_adjustment_factor"].min())
    assert paf_min != 0, (
        f"price_adjustment_factor contains zero: min={paf_min}"
    )
    out = df.with_columns(
        (pl.col("per_share_fundamental_data") * pl.col("price_adjustment_factor"))
        .alias("per_share_fundamental_data")
    )
    assert "per_share_fundamental_data" in out.columns, (
        f"per_share_fundamental_data column missing after adjust: {out.columns}"
    )
    sample = out.filter(pl.col("price_adjustment_factor").is_not_null()).head(1)
    if sample.height > 0:
        original = df["per_share_fundamental_data"][0]
        paf = df["price_adjustment_factor"][0]
        adjusted = out["per_share_fundamental_data"][0]
        if original is not None and paf is not None:
            assert abs(adjusted - original * paf) < 1e-9, (
                f"per_share_fundamental_data not correctly adjusted: "
                f"expected {original * paf}, got {adjusted}"
            )
    return out
