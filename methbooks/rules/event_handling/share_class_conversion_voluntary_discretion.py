# methbooks/rules/event_handling/share_class_conversion_voluntary_discretion.py
"""
Purpose: Evaluate voluntary share class conversions using qualitative factors; MSCI retains discretion over event-time vs Index Review implementation.
Datapoints: security_id, conversion_type
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.5.2 Voluntary conversions" near line 874.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

_VOLUNTARY_CONVERSION_TYPE = "voluntary"
_PERIODICAL_CONVERSION_TYPE = "periodical"


def share_class_conversion_voluntary_discretion(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "conversion_type"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    # Periodical conversions must always be deferred to Index Review.
    if "deferred_to_index_review" in out.columns:
        periodical_rows = out.filter(pl.col("conversion_type") == _PERIODICAL_CONVERSION_TYPE)
        not_deferred = periodical_rows.filter(~pl.col("deferred_to_index_review").cast(pl.Boolean))
        assert not_deferred.is_empty(), (
            f"periodical conversions must be deferred to Index Review; "
            f"offending security_ids: {not_deferred['security_id'].to_list()}"
        )

    return out
