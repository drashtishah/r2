"""
Purpose: Pass-through stub for spinoff immediate addition policy; business logic
    requires case-by-case review and is not transformable in a batch pipeline.
Datapoints: .
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Climate_Action_Indexes_Methodology_20251211.md section "Event Handling" near line 1.
See also: methbooks/rules/event_handling/ma_acquirer_proportionate_weight.py (companion event stub).
"""
from __future__ import annotations

import polars as pl


def spinoff_immediate_addition(df: pl.DataFrame) -> pl.DataFrame:
    assert isinstance(df, pl.DataFrame), f"expected pl.DataFrame, got {type(df)}"
    return df
