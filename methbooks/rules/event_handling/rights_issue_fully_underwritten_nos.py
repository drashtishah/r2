# methbooks/rules/event_handling/rights_issue_fully_underwritten_nos.py
"""
Purpose: Always increase NOS on ex-date for fully underwritten rights issues, treating the underwriter as purchaser of any unsubscribed shares.
Datapoints: security_id, nos, is_fully_underwritten
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "3.6.3 Number of Shares, FIF and/or DIF Changes Following Rights" near line 4204.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl


def rights_issue_fully_underwritten_nos(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "nos", "is_fully_underwritten"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    nos_min = float(out["nos"].min())
    assert nos_min >= 0, f"nos must be >= 0 after update, actual min: {nos_min}"

    # Government or strategic underwriter: new shares considered non-free-float.
    # Investment banker underwriter: new shares considered pro rata free float.
    if "underwriter_type" in out.columns and "fif" in out.columns:
        fif_min = float(out["fif"].min())
        fif_max = float(out["fif"].max())
        assert fif_min >= 0, f"fif must be >= 0, actual min: {fif_min}"
        assert fif_max <= 1, f"fif must be <= 1, actual max: {fif_max}"

        gov_strategic = out.filter(
            pl.col("is_fully_underwritten")
            & pl.col("underwriter_type").is_in(["government", "strategic"])
        )
        if not gov_strategic.is_empty() and "fif_increased_from_new_shares" in out.columns:
            invalid = gov_strategic.filter(pl.col("fif_increased_from_new_shares"))
            assert invalid.is_empty(), (
                f"new shares underwritten by government/strategic investor must be "
                f"non-free-float; offending security_ids: {invalid['security_id'].to_list()}"
            )

    return out
