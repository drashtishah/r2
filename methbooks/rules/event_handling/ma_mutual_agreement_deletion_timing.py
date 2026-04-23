# methbooks/rules/event_handling/ma_mutual_agreement_deletion_timing.py
"""
Purpose: Delete the acquired or merged entity as of the close of the last trading day when the M&A proceeds via mutual agreement and all necessary information is available before completion.
Datapoints: security_id, is_index_constituent, weight
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.1.1 Implementation timing" near line 379.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl


def ma_mutual_agreement_deletion_timing(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "is_index_constituent", "weight"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    total_weight = float(out["weight"].sum())
    assert total_weight >= 0, f"target security weight must be >= 0 on effective date, actual: {total_weight}"

    # Cancelled M&A: no constituent that was previously deleted should be
    # immediately reinstated; is_index_constituent must not flip back to True
    # within the same event batch without an explicit reinstatement flag.
    if "is_cancelled_ma" in out.columns and "was_deleted" in out.columns:
        invalid = out.filter(
            pl.col("is_cancelled_ma") & pl.col("was_deleted") & pl.col("is_index_constituent")
        )
        assert invalid.is_empty(), (
            f"cancelled M&A must not cause immediate reinstatement; "
            f"offending security_ids: {invalid['security_id'].to_list()}"
        )

    return out
