# methbooks/rules/event_handling/optional_dividend_stock_default.py
"""
Purpose: When the default election is stock, apply PAF and increase NOS on ex-date assuming investors elect the default distribution.
Datapoints: security_id, nos, closing_price, default_distribution_type
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "3.5 Optional Dividends" near line 4053.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

_STOCK_DISTRIBUTION_TYPE = "stock"


def optional_dividend_stock_default(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "nos", "closing_price", "default_distribution_type"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    cp_min = float(out["closing_price"].min())
    assert cp_min > 0, (
        f"if default is stock: PAF applied and NOS increased as of close of ex-date; "
        f"closing_price must be > 0, actual min: {cp_min}"
    )

    nos_min = float(out["nos"].min())
    assert nos_min >= 0, f"nos must be >= 0, actual min: {nos_min}"

    # Free float unchanged: distribution assumed pro rata; validate via fif columns if present.
    if "fif_pre" in out.columns and "fif_post" in out.columns:
        import math
        rows = out.filter(
            pl.col("default_distribution_type") == _STOCK_DISTRIBUTION_TYPE
        ).select(["security_id", "fif_pre", "fif_post"]).to_dicts()
        for row in rows:
            assert math.isclose(row["fif_pre"], row["fif_post"], rel_tol=1e-6), (
                f"free float must be unchanged when default election is stock (pro rata); "
                f"security_id: {row['security_id']}, fif_pre: {row['fif_pre']}, "
                f"fif_post: {row['fif_post']}"
            )

    return out
