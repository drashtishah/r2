# methbooks/rules/event_handling/partial_tender_dutch_auction_no_paf.py
"""
Purpose: Apply no PAF on the ex-date for Dutch auction partial tender offers.
Datapoints: security_id, offer_type
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.4 Partial tender offers and buyback offers" near line 791.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

_DUTCH_AUCTION_OFFER_TYPE = "dutch_auction"


def partial_tender_dutch_auction_no_paf(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "offer_type"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    if "paf" in out.columns:
        dutch_rows = out.filter(pl.col("offer_type") == _DUTCH_AUCTION_OFFER_TYPE)
        invalid = dutch_rows.filter(pl.col("paf").is_not_null())
        assert invalid.is_empty(), (
            f"no PAF must be applied when offer_type == dutch_auction; "
            f"offending security_ids: {invalid['security_id'].to_list()}"
        )

    return out
