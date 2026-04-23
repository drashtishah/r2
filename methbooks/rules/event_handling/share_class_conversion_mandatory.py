# methbooks/rules/event_handling/share_class_conversion_mandatory.py
"""
Purpose: Implement mandatory full or partial conversions of an index constituent share class into another share class at the time of the event.
Datapoints: security_id, share_class, is_index_constituent, conversion_type
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.5.1 Mandatory conversions" near line 874.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

_MANDATORY_CONVERSION_TYPE = "mandatory"


def share_class_conversion_mandatory(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "share_class", "is_index_constituent", "conversion_type"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    # Conversions of non-index constituent share class into index constituent must be deferred
    # to Index Review, not implemented at the time of event.
    if "is_converting_into_index_constituent" in out.columns:
        invalid = out.filter(
            (pl.col("conversion_type") == _MANDATORY_CONVERSION_TYPE)
            & ~pl.col("is_index_constituent")
            & pl.col("is_converting_into_index_constituent")
            & ~pl.col("deferred_to_index_review").cast(pl.Boolean)
        )
        assert invalid.is_empty(), (
            f"conversions of non-index constituent share class into index constituent "
            f"must be deferred to Index Review; "
            f"offending security_ids: {invalid['security_id'].to_list()}"
        )

    if "has_historical_link" in out.columns:
        mandatory_rows = out.filter(
            (pl.col("conversion_type") == _MANDATORY_CONVERSION_TYPE)
            & pl.col("is_index_constituent")
        )
        missing_links = mandatory_rows.filter(~pl.col("has_historical_link"))
        assert missing_links.is_empty(), (
            f"historical links must be created for mandatory index-constituent conversions; "
            f"missing for: {missing_links['security_id'].to_list()}"
        )

    return out
