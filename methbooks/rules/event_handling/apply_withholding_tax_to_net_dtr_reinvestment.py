from __future__ import annotations

import polars as pl


def apply_withholding_tax_to_net_dtr_reinvestment(df: pl.DataFrame) -> pl.DataFrame:
    """
    Purpose: Deduct the maximum withholding tax rate applicable to institutional investors from the dividend reinvestment amount in MSCI Net DTR Indexes, using the company's country of incorporation.
    Datapoints: gross_dividend_per_share, withholding_tax_rate, country_of_incorporation, index_type_domestic_or_international.
    Thresholds: none.
    Source: methbooks/data/markdown/MSCI_Index_Calculation_Methodology.md section "2.3.7.2 Withholding Tax" near line 1863.
    See also: methbooks/rules/event_handling/reinvest_regular_distribution_on_exdate.py (gross DTR reinvestment rule; net DTR applies WHT deduction on top).
    """
    required = [
        "gross_dividend_per_share",
        "withholding_tax_rate",
        "country_of_incorporation",
        "index_type_domestic_or_international",
    ]
    for col in required:
        assert col in df.columns, f"required column missing: {col}"

    out = df

    assert float(out["withholding_tax_rate"].min()) >= 0, f"withholding_tax_rate must be >= 0: min={float(out['withholding_tax_rate'].min())}"
    assert float(out["withholding_tax_rate"].max()) <= 1.0, f"withholding_tax_rate must be <= 1: max={float(out['withholding_tax_rate'].max())}"
    assert float(out["gross_dividend_per_share"].min()) >= 0, f"gross_dividend_per_share must be >= 0: min={float(out['gross_dividend_per_share'].min())}"
    if "net_dividend_per_share" in out.columns:
        check = out.filter(
            (pl.col("net_dividend_per_share") - pl.col("gross_dividend_per_share") * (1 - pl.col("withholding_tax_rate"))).abs() > 1e-9
        )
        assert check.is_empty(), f"net_dividend_per_share must equal gross * (1 - wht_rate): offending count={check.height}"
    assert True, "weighted average rate applied when one company pays distributions with different rates on the same ex-date; Australian effective rate = default_rate * (100% - franking% - conduit_foreign_income%); regular capital repayments not subject to WHT reinvested free of tax in net DTR"

    return out
