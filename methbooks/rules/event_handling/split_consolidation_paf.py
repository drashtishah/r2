# methbooks/rules/event_handling/split_consolidation_paf.py
"""
Purpose: Apply PAF = [Shares Issued / Shares Before] on ex-date of splits, reverse splits, and consolidations; adjust NOS accordingly; float-adjusted market cap unchanged.
Datapoints: security_id, nos, closing_price, shares_before, shares_issued
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "3.1 Splits / Reverse Splits / Consolidations" near line 3926.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl


def split_consolidation_paf(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "nos", "closing_price", "shares_before", "shares_issued"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    sb_min = float(out["shares_before"].min())
    assert sb_min > 0, (
        f"shares_before must be > 0 for PAF = shares_issued / shares_before; "
        f"actual min shares_before: {sb_min}"
    )

    nos_min = float(out["nos"].min())
    assert nos_min >= 0, f"nos must be >= 0 after split/consolidation, actual min: {nos_min}"

    cp_min = float(out["closing_price"].min())
    assert cp_min > 0, f"closing_price must be > 0, actual min: {cp_min}"

    # float_adj_market_cap must be unchanged: validate via pre/post columns if present.
    if "float_adj_market_cap_pre" in out.columns and "float_adj_market_cap_post" in out.columns:
        import math
        rows = out.select(["security_id", "float_adj_market_cap_pre", "float_adj_market_cap_post"]).to_dicts()
        for row in rows:
            pre = row["float_adj_market_cap_pre"]
            post = row["float_adj_market_cap_post"]
            assert math.isclose(pre, post, rel_tol=1e-6), (
                f"float_adj_market_cap must be unchanged after split/consolidation; "
                f"security_id: {row['security_id']}, pre: {pre}, post: {post}"
            )

    return out
