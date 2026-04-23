# methbooks/rules/event_handling/capital_repayment_paf.py
"""
Purpose: Apply PAF = [(P(t) + Cash) / P(t)] on ex-date for extraordinary capital repayments; treat ordinary capital repayments as regular cash dividends with no PAF adjustment.
Datapoints: security_id, closing_price, is_extraordinary_distribution
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "3.3 Capital Repayments" near line 4003.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl


def capital_repayment_paf(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "closing_price", "is_extraordinary_distribution"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    cp_min = float(out["closing_price"].min())
    assert cp_min > 0, (
        f"closing_price must be > 0 for PAF = (P(t) + cash) / P(t) for extraordinary "
        f"capital repayments; actual min closing_price: {cp_min}"
    )

    # Ordinary (non-extraordinary) capital repayments must not have PAF applied.
    if "paf" in out.columns:
        ordinary_rows = out.filter(~pl.col("is_extraordinary_distribution"))
        with_paf = ordinary_rows.filter(pl.col("paf").is_not_null())
        assert with_paf.is_empty(), (
            f"ordinary capital repayments must not have PAF applied; "
            f"offending security_ids: {with_paf['security_id'].to_list()}"
        )

    return out
