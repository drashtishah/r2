# methbooks/rules/event_handling/tender_offer_standard_size_override.py
"""
Purpose: Maintain a Standard Index constituent whose FIF falls below 0.15 if its post-event float-adjusted market cap exceeds two thirds of 1.8 times one half of the Standard Index Interim Size-Segment Cutoff.
Datapoints: security_id, fif, float_adj_market_cap, interim_size_segment_cutoff, size_segment
Thresholds: OVERRIDE_NUMERATOR = 2, OVERRIDE_DENOMINATOR = 3, FLOAT_ADJ_MCAP_MULTIPLE = 1.8, FLOAT_ADJ_MCAP_HALF_FACTOR = 0.5
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.2.3 Implementation timing" near line 608.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

OVERRIDE_NUMERATOR = 2
OVERRIDE_DENOMINATOR = 3
FLOAT_ADJ_MCAP_MULTIPLE = 1.8
FLOAT_ADJ_MCAP_HALF_FACTOR = 0.5

_STANDARD_SIZE_SEGMENT = "Standard"


def _override_threshold(interim_cutoff: float) -> float:
    return (
        (OVERRIDE_NUMERATOR / OVERRIDE_DENOMINATOR)
        * FLOAT_ADJ_MCAP_MULTIPLE
        * FLOAT_ADJ_MCAP_HALF_FACTOR
        * interim_cutoff
    )


def tender_offer_standard_size_override(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "fif", "float_adj_market_cap", "interim_size_segment_cutoff", "size_segment"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    if "has_size_override" in out.columns:
        invalid = out.filter(
            pl.col("has_size_override")
            & (pl.col("size_segment") != _STANDARD_SIZE_SEGMENT)
        )
        assert invalid.is_empty(), (
            f"override applies only to Standard size segment, "
            f"actual size_segment values: {invalid['size_segment'].to_list()}"
        )

    return out
