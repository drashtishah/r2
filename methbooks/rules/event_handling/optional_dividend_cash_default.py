# methbooks/rules/event_handling/optional_dividend_cash_default.py
"""
Purpose: When the default election for an optional dividend is cash, reinvest proceeds in DTR indexes and defer any NOS increase to the next Index Review.
Datapoints: security_id, nos, default_distribution_type
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "3.5 Optional Dividends" near line 4053.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

_CASH_DISTRIBUTION_TYPE = "cash"


def optional_dividend_cash_default(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "nos", "default_distribution_type"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert "default_distribution_type" in out.columns, (
        "if no default specified: cash option assumed, treated as regular cash dividend; "
        "default_distribution_type column must be present"
    )

    nos_min = float(out["nos"].min())
    assert nos_min >= 0, f"nos must be >= 0, actual min: {nos_min}"

    # When default is cash, NOS must not be immediately increased; verify via deferred flag.
    if "nos_increase_deferred" in out.columns:
        cash_rows = out.filter(pl.col("default_distribution_type") == _CASH_DISTRIBUTION_TYPE)
        not_deferred = cash_rows.filter(
            ~pl.col("nos_increase_deferred").cast(pl.Boolean)
            & (pl.col("nos") > pl.col("nos").shift(1).fill_null(0))
        )
        assert not_deferred.is_empty(), (
            f"NOS increase must be deferred to next Index Review when default election is cash; "
            f"offending security_ids: {not_deferred['security_id'].to_list()}"
        )

    return out
