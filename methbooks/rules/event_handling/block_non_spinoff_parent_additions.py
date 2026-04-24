"""
Purpose: Prevent new Parent Index additions (IPOs and other early inclusions) from being added to the Index between Index Reviews. Spin-offs of existing Index constituents are the sole exception, added at event implementation and reevaluated at the subsequent review.
Datapoints:
Thresholds: none.
Source: methbooks/data/markdown/MSCI_EU_CTB_PAB_Overlay_Indexes_Methodology_20260211.md section "3.2 Ongoing Event Related Changes" near line 551.
See also: methbooks/rules/event_handling/neutralize_intermediate_parent_reviews.py (companion rule for intermediate-review neutralization).
"""
from __future__ import annotations
import polars as pl


def block_non_spinoff_parent_additions(df: pl.DataFrame) -> pl.DataFrame:
    """Proxy: returns df unchanged; addition-blocking is enforced at the event
    processing layer, not as a row filter over constituent data.
    """
    out = df
    assert "weight" in out.columns, f"weight column missing after addition-block: {out.columns}"
    assert out.height > 0, f"No constituents after blocking non-spinoff parent additions: height={out.height}"
    return out
