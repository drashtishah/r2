"""
Purpose: Make no fundamental data adjustment for share buybacks, special cash dividends, or debt-to-equity swaps; these event types are treated as pass-through by default.
Datapoints: event_type_flag, per_share_fundamental_data.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "4 Fundamental Data Treatments for Corporate Events" near line 3555.
See also: methbooks/rules/event_handling/apply_paf_to_per_share_data_nominal_event.py (adjustment applied for nominal events).
"""
from __future__ import annotations

import polars as pl

_NO_ADJUST_EVENTS = {"share_buyback", "special_cash_dividend", "debt_to_equity_swap"}


def no_adjustment_for_buyback_special_dividend_debt_swap(df: pl.DataFrame) -> pl.DataFrame:
    assert "event_type_flag" in df.columns, (
        f"event_type_flag column missing: {df.columns}"
    )
    assert "per_share_fundamental_data" in df.columns, (
        f"per_share_fundamental_data column missing: {df.columns}"
    )
    original_psfd = df["per_share_fundamental_data"].clone()
    out = df
    exempt = out.filter(pl.col("event_type_flag").is_in(list(_NO_ADJUST_EVENTS)))
    orig_exempt = df.filter(pl.col("event_type_flag").is_in(list(_NO_ADJUST_EVENTS)))
    if exempt.height > 0:
        changed = (exempt["per_share_fundamental_data"] - orig_exempt["per_share_fundamental_data"]).abs().max()
        assert changed is None or changed < 1e-9, (
            f"per_share_fundamental_data changed for exempt event types "
            f"(share_buyback / special_cash_dividend / debt_to_equity_swap): max_change={changed}"
        )
    return out
