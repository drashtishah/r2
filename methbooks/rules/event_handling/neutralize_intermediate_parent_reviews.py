"""
Purpose: When the Parent Index undergoes more frequent reviews than the Index (e.g., quarterly vs semi-annual), neutralize changes made to the Parent Index during intermediate reviews to prevent unintended drift.
Datapoints: parent_index_weight.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_EU_CTB_PAB_Overlay_Indexes_Methodology_20260211.md section "3.2 Ongoing Event Related Changes" near line 542.
See also: methbooks/rules/event_handling/reflect_parent_deletions.py (companion rule for parent deletion propagation).
"""
from __future__ import annotations
import polars as pl


def neutralize_intermediate_parent_reviews(df: pl.DataFrame) -> pl.DataFrame:
    """Proxy: returns df unchanged; intermediate-review neutralization is applied
    outside the mock pipeline as a composition of index snapshots.
    """
    out = df
    assert "weight" in out.columns, f"weight column missing after neutralization: {out.columns}"
    assert out.height > 0, f"No constituents after intermediate review neutralization: height={out.height}"
    return out
