"""
Purpose: Remove securities listed on exchange alert/watch/suspension boards
(e.g. Tokyo Securities Under Supervision, Borsa Istanbul Watch List,
India Graded Surveillance Measure) from the Equity Universe; re-addition
blocked for 12 months after deletion.
Datapoints: alert_board_flag.
Thresholds: READMISSION_LOCKOUT_MONTHS=12.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "Appendix I: Ineligible Alert Boards" near line 4064.
See also: methbooks/rules/event_handling/early_deletion_policy.py (deletion trigger).
"""
from __future__ import annotations

import polars as pl

READMISSION_LOCKOUT_MONTHS = 12


def exclude_ineligible_alert_board_securities(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("alert_board_flag") == False)  # noqa: E712
    assert "alert_board_flag" in out.columns, (
        f"alert_board_flag column missing: {out.columns}"
    )
    assert not out["alert_board_flag"].any(), (
        f"security with alert_board_flag=True survived: "
        f"count={out['alert_board_flag'].sum()}"
    )
    return out
