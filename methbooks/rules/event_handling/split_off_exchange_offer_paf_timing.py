# methbooks/rules/event_handling/split_off_exchange_offer_paf_timing.py
"""
Purpose: Apply PAF = 1 on the first business day after the end of a split-off or exchange offer.
Datapoints: security_id, offer_type
Thresholds: SPLIT_OFF_PAF_VALUE = 1
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.4.4 Split-Off / Exchange Offer" near line 791.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

SPLIT_OFF_PAF_VALUE = 1

_SPLIT_OFF_OFFER_TYPES = {"split_off", "exchange_offer"}


def split_off_exchange_offer_paf_timing(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "offer_type"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert SPLIT_OFF_PAF_VALUE == 1, (
        f"PAF for split-off / exchange offer must equal 1, actual: {SPLIT_OFF_PAF_VALUE}"
    )

    if "paf" in out.columns:
        split_off_rows = out.filter(pl.col("offer_type").is_in(list(_SPLIT_OFF_OFFER_TYPES)))
        invalid = split_off_rows.filter(pl.col("paf") != SPLIT_OFF_PAF_VALUE)
        assert invalid.is_empty(), (
            f"PAF must = {SPLIT_OFF_PAF_VALUE} applied day after end of exchange offer; "
            f"offending paf values: {invalid['paf'].to_list()}"
        )

    return out
