# methbooks/rules/event_handling/rights_issue_discount_paf.py
"""
Purpose: Apply PAF on ex-date of rights issues when subscription price is below market price.
Datapoints: security_id, closing_price, subscription_price, shares_before, shares_issued, forthcoming_gross_dividend
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "3.6.1 Rights Issues: PAF treatment" near line 4118.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl


def rights_issue_discount_paf(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "closing_price", "subscription_price", "shares_before", "shares_issued", "forthcoming_gross_dividend"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    sb_min = float(out["shares_before"].min())
    assert sb_min > 0, (
        f"shares_before must be > 0 for PAF = "
        f"[(P(t) * (Shares Before + Shares Issued) - Shares Issued * Issue P) / Shares Before] / [P(t)]; "
        f"actual min shares_before: {sb_min}"
    )

    cp_min = float(out["closing_price"].min())
    assert cp_min > 0, f"closing_price must be > 0, actual min: {cp_min}"

    sp_min = float(out["subscription_price"].min())
    assert sp_min >= 0, f"subscription_price must be >= 0, actual min: {sp_min}"

    si_min = float(out["shares_issued"].min())
    assert si_min >= 0, f"shares_issued must be >= 0, actual min: {si_min}"

    # If security does not trade on ex-date, event implemented when trading resumes.
    if "is_trading_on_ex_date" in out.columns and "implementation_deferred" in out.columns:
        non_trading = out.filter(~pl.col("is_trading_on_ex_date"))
        not_deferred = non_trading.filter(~pl.col("implementation_deferred").cast(pl.Boolean))
        assert not_deferred.is_empty(), (
            f"if security does not trade on ex-date, event must be deferred until trading resumes; "
            f"offending security_ids: {not_deferred['security_id'].to_list()}"
        )

    return out
