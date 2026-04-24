"""
Purpose: Delete existing constituents outside of Index Reviews when they file for
bankruptcy, face delisting, have FIF fall below 0.15, are subject to prolonged
suspension, or are involved in corporate events that render them ineligible.
Datapoints: bankruptcy_flag, delisting_flag, fif, suspension_flag.
Thresholds: MIN_FIF_FOR_RETENTION=0.15.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "3.2.6 Early Deletions of Existing Index Constituents" near line 3115.
See also: methbooks/rules/event_handling/prolonged_suspension_deletion.py (prolonged suspension sub-rule from corporate_events methbook).
"""
from __future__ import annotations

import polars as pl

MIN_FIF_FOR_RETENTION = 0.15


def early_deletion_policy(df: pl.DataFrame) -> pl.DataFrame:
    for col in ("bankruptcy_flag", "delisting_flag", "fif", "suspension_flag"):
        assert col in df.columns, f"{col} column missing: {df.columns}"
    out = df.filter(
        (~pl.col("bankruptcy_flag"))
        & (~pl.col("delisting_flag"))
        & (pl.col("fif") >= MIN_FIF_FOR_RETENTION)
        & (~pl.col("suspension_flag"))
    )
    assert out.filter(pl.col("bankruptcy_flag")).height == 0, (
        f"constituent with bankruptcy_flag=True survived: "
        f"{out.filter(pl.col('bankruptcy_flag')).height} rows"
    )
    assert out.filter(pl.col("delisting_flag")).height == 0, (
        f"constituent with delisting_flag=True survived: "
        f"{out.filter(pl.col('delisting_flag')).height} rows"
    )
    assert out.filter(pl.col("fif") < MIN_FIF_FOR_RETENTION).height == 0, (
        f"constituent with fif < {MIN_FIF_FOR_RETENTION} survived:"
        f" min fif={out['fif'].min()}"
    )
    return out
