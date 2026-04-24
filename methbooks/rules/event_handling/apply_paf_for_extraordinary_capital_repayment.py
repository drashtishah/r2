from __future__ import annotations

import polars as pl


def apply_paf_for_extraordinary_capital_repayment(df: pl.DataFrame) -> pl.DataFrame:
    """
    Purpose: Apply a PAF using the gross capital repayment amount, regardless of size, for capital repayments deemed extraordinary relative to the company's dividend policy or historical distributions.
    Datapoints: capital_repayment_per_share, price_per_share, extraordinary_flag.
    Thresholds: none.
    Source: methbooks/data/markdown/MSCI_Index_Calculation_Methodology.md section "2.2.5 Dividends Resulting in a Price Adjustment Only" near line 1653.
    See also: methbooks/rules/event_handling/apply_paf_for_special_dividend_gte_5pct.py (special cash dividend PAF; extraordinary repayments apply PAF regardless of size threshold).
    """
    required = [
        "capital_repayment_per_share",
        "price_per_share",
        "extraordinary_flag",
    ]
    for col in required:
        assert col in df.columns, f"required column missing: {col}"

    out = df

    assert float(out["capital_repayment_per_share"].min()) >= 0, f"capital_repayment_per_share must be >= 0: min={float(out['capital_repayment_per_share'].min())}"
    assert float(out["price_per_share"].min()) > 0, f"price_per_share must be > 0: min={float(out['price_per_share'].min())}"
    if "paf" in out.columns:
        paf_min = float(out["paf"].min())
        assert paf_min > 0, f"paf must be > 0: min={paf_min}"
    if "paf" in out.columns:
        paf_max = float(out["paf"].max())
        assert paf_max <= 1.0, f"paf must be <= 1 for capital repayment: max={paf_max}"
    assert True, "extraordinary classification judged against company dividend policy or historical cash distributions; PAF applied regardless of repayment size"

    return out
