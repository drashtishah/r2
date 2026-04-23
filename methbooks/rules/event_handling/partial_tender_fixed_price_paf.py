# methbooks/rules/event_handling/partial_tender_fixed_price_paf.py
"""
Purpose: Apply PAF = 1 on the ex-date of fixed-price partial tender offers.
Datapoints: security_id, offer_type
Thresholds: FIXED_PRICE_PAF_VALUE = 1
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.4 Partial tender offers and buyback offers" near line 791.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

FIXED_PRICE_PAF_VALUE = 1

_FIXED_PRICE_OFFER_TYPE = "fixed_price"


def partial_tender_fixed_price_paf(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "offer_type"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert FIXED_PRICE_PAF_VALUE == 1, (
        f"PAF for fixed-price partial tenders must equal 1, actual: {FIXED_PRICE_PAF_VALUE}"
    )

    if "paf" in out.columns:
        fixed_price_rows = out.filter(pl.col("offer_type") == _FIXED_PRICE_OFFER_TYPE)
        invalid = fixed_price_rows.filter(pl.col("paf") != FIXED_PRICE_PAF_VALUE)
        assert invalid.is_empty(), (
            f"PAF must = {FIXED_PRICE_PAF_VALUE} when offer_type == fixed_price, "
            f"offending paf values: {invalid['paf'].to_list()}"
        )

    return out
