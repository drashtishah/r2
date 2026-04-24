"""
Purpose: Set each selected security's weight proportional to its parent index weight,
    renormalized to sum to 1.
Datapoints: parent_index_weight.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Climate_Action_Indexes_Methodology_20251211.md section "Weighting" near line 1.
See also: methbooks/rules/weighting/cap_issuer_weight_5pct.py (applied after this).
"""
from __future__ import annotations

import polars as pl


def renormalize_parent_weights(df: pl.DataFrame) -> pl.DataFrame:
    total = df["parent_index_weight"].sum()
    assert total > 0, f"parent_index_weight sum is zero or negative: {total}"
    out = df.with_columns(
        (pl.col("parent_index_weight") / total).alias("weight")
    )
    assert abs(float(out["weight"].sum()) - 1.0) < 1e-9, (
        f"weights do not sum to 1 after renormalization: {out['weight'].sum()}"
    )
    return out
