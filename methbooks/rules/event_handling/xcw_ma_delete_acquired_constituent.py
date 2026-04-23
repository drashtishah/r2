"""
Purpose: Delete an existing index constituent that is acquired by a non-index-constituent
    at the time of the M&A event.
Datapoints: is_acquisition_target, acquirer_is_index_constituent.
Thresholds: none.
Source: meth-pipeline/MSCI_Global_ex_Controversial_Weapons_Indexes_Methodology_20251211/2026-04-23T20-59-13Z/input/markdown.md section "3.2 Ongoing Event-Related Maintenance" near line 194.
See also: methbooks/rules/event_handling/xcw_ma_no_add_nonconstituent_acquirer.py (companion rule preventing addition of the non-constituent acquirer).
"""
from __future__ import annotations

import polars as pl


def xcw_ma_delete_acquired_constituent(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(
        ~(
            (pl.col("is_acquisition_target") == True)  # noqa: E712
            & (pl.col("acquirer_is_index_constituent") == False)  # noqa: E712
        )
    )
    assert "is_acquisition_target" in out.columns, (
        f"is_acquisition_target column missing: {out.columns}"
    )
    assert "acquirer_is_index_constituent" in out.columns, (
        f"acquirer_is_index_constituent column missing: {out.columns}"
    )
    survivors = out.filter(
        (pl.col("is_acquisition_target") == True)  # noqa: E712
        & (pl.col("acquirer_is_index_constituent") == False)  # noqa: E712
    )
    assert survivors.height == 0, (
        f"constituents acquired by non-constituent survived: {survivors.height} rows"
    )
    return out
