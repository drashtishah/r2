"""
Purpose: Adjust all fundamental per-share data by the price adjustment factor for nominal capital structure changes (no change in market cap), ensuring per-share figures remain consistent with the adjusted price; also applies to combined nominal and real events.
Datapoints: price_adjustment_factor, per_share_fundamental_data.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "4.1 Nominal Changes in Capital Structure" near line 3563.
See also: methbooks/rules/event_handling/suppress_small_event_dps_adjustment.py (DPS exempt when PAF in [0.80, 1.25]).
"""
from __future__ import annotations

import polars as pl

_NOMINAL_EVENTS = {"split_consolidation", "stock_dividend", "rights_issue", "merger", "spinoff"}


def apply_paf_to_per_share_data_nominal_event(df: pl.DataFrame) -> pl.DataFrame:
    assert "price_adjustment_factor" in df.columns, (
        f"price_adjustment_factor column missing: {df.columns}"
    )
    assert "per_share_fundamental_data" in df.columns, (
        f"per_share_fundamental_data column missing: {df.columns}"
    )
    assert "event_type_flag" in df.columns, (
        f"event_type_flag column missing: {df.columns}"
    )
    paf_min = float(df["price_adjustment_factor"].min())
    assert paf_min != 0, (
        f"price_adjustment_factor contains zero: min={paf_min}"
    )
    out = df.with_columns(
        pl.when(pl.col("event_type_flag").is_in(list(_NOMINAL_EVENTS)))
        .then(pl.col("per_share_fundamental_data") * pl.col("price_adjustment_factor"))
        .otherwise(pl.col("per_share_fundamental_data"))
        .alias("per_share_fundamental_data")
    )
    assert "per_share_fundamental_data" in out.columns, (
        f"per_share_fundamental_data column missing after adjust: {out.columns}"
    )
    nominal_mask = df["event_type_flag"].is_in(list(_NOMINAL_EVENTS))
    nominal_idx = [i for i, v in enumerate(nominal_mask.to_list()) if v]
    if nominal_idx:
        i = nominal_idx[0]
        original = df["per_share_fundamental_data"][i]
        paf = df["price_adjustment_factor"][i]
        adjusted = out["per_share_fundamental_data"][i]
        if original is not None and paf is not None:
            assert abs(adjusted - original * paf) < 1e-9, (
                f"per_share_fundamental_data not correctly adjusted for nominal event row {i}: "
                f"expected {original * paf}, got {adjusted}"
            )
    non_nominal_idx = [i for i, v in enumerate(nominal_mask.to_list()) if not v]
    if non_nominal_idx:
        j = non_nominal_idx[0]
        original = df["per_share_fundamental_data"][j]
        unchanged = out["per_share_fundamental_data"][j]
        if original is not None:
            assert original == unchanged, (
                f"per_share_fundamental_data modified for non-nominal row {j}: "
                f"original={original}, got={unchanged}"
            )
    return out
