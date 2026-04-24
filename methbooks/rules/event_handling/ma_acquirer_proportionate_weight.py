"""
Purpose: Pass-through stub for M&A acquirer proportionate weight assignment; business
    logic requires case-by-case review and is not transformable in a batch pipeline.
Datapoints: .
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Climate_Action_Indexes_Methodology_20251211.md section "3.3 Ongoing Event-Related Changes" near line 418.
See also: methbooks/rules/event_handling/spinoff_immediate_addition.py (companion event stub).
"""
from __future__ import annotations

import polars as pl


def ma_acquirer_proportionate_weight(df: pl.DataFrame) -> pl.DataFrame:
    assert isinstance(df, pl.DataFrame), f"expected pl.DataFrame, got {type(df)}"
    return df
