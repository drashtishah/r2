# methbooks/rules/event_handling/nonindex_acquiring_index_early_inclusion.py
"""
Purpose: When a listed non-index constituent acquires an index constituent with newly issued or exchanged shares, consider the acquirer for immediate inclusion via early inclusion rules at the time of the event.
Datapoints: security_id, is_index_constituent
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.3.3" near line 766.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl


def nonindex_acquiring_index_early_inclusion(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "is_index_constituent"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    # Non-index acquirer must be evaluated against IPO early-inclusion thresholds at event time.
    # If early_inclusion_evaluated flag is present, verify it is set for non-index acquirers
    # that issued or exchanged shares in the acquisition.
    if "early_inclusion_evaluated" in out.columns and "is_share_issuing_acquirer" in out.columns:
        non_index_acquirers = out.filter(
            ~pl.col("is_index_constituent") & pl.col("is_share_issuing_acquirer")
        )
        not_evaluated = non_index_acquirers.filter(~pl.col("early_inclusion_evaluated"))
        assert not_evaluated.is_empty(), (
            f"non-index acquirers that issued shares must be evaluated for early inclusion; "
            f"not evaluated: {not_evaluated['security_id'].to_list()}"
        )

    return out
