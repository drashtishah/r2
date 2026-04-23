# methbooks/rules/event_handling/rights_issue_premium_paf.py
"""
Purpose: Apply PAF = 1 when the rights issue subscription price is greater than or equal to the market price.
Datapoints: security_id, closing_price, subscription_price
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "3.6.2 Rights Issues: premium subscription" near line 4118.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

_PREMIUM_PAF_VALUE = 1


def rights_issue_premium_paf(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "closing_price", "subscription_price"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    cp_min = float(out["closing_price"].min())
    assert cp_min > 0, (
        f"if subscription_price >= closing_price: PAF = 1; "
        f"closing_price must be > 0, actual min: {cp_min}"
    )

    if "paf" in out.columns:
        premium_rows = out.filter(pl.col("subscription_price") >= pl.col("closing_price"))
        invalid = premium_rows.filter(pl.col("paf") != _PREMIUM_PAF_VALUE)
        assert invalid.is_empty(), (
            f"PAF must = 1 when subscription_price >= closing_price; "
            f"offending paf values: {invalid['paf'].to_list()}"
        )

    return out
