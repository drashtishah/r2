"""
Purpose: Preserve blended, digital assets, and ETF-linked index calculation by carrying forward the latest available component index level when a component is temporarily or permanently unavailable.
Datapoints: component_index_level, component_availability_flag.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Index_Policies.md section "Index Calculation and Discretion" near line 537.
See also: methbooks/rules/event_handling/market_closure_price_carryforward.py (security-level carryforward).
"""
from __future__ import annotations
import polars as pl

def component_index_carryforward_on_unavailability(df: pl.DataFrame) -> pl.DataFrame:
    required = ["component_index_level", "component_availability_flag"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df
    null_count = out["component_index_level"].null_count()
    assert null_count == 0, (
        f"no component may have a null index level after carryforward is applied; "
        f"null_count={null_count}"
    )
    return out
