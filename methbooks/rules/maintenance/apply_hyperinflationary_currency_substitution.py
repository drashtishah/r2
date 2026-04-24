"""
Purpose: Publish fundamental data in a relatively stable foreign currency for companies domiciled in hyperinflationary economies, identified by cumulative inflation approaching or exceeding 100% over three years.
Datapoints: country_of_classification, country_cumulative_inflation_3yr.
Thresholds: CUMULATIVE_INFLATION_THRESHOLD = 1, MEASUREMENT_PERIOD_YEARS = 3.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "6 Hyperinflationary Economies - Adjustment of Fundamental Data" near line 3873.
See also: methbooks/rules/scoring/use_average_shares_us_canada_uk_ireland.py (country_of_classification drives another reporting-currency override).
"""
from __future__ import annotations

import polars as pl

CUMULATIVE_INFLATION_THRESHOLD = 1
MEASUREMENT_PERIOD_YEARS = 3


def apply_hyperinflationary_currency_substitution(df: pl.DataFrame) -> pl.DataFrame:
    for col in ("country_of_classification", "country_cumulative_inflation_3yr"):
        assert col in df.columns, f"{col} column missing: {df.columns}"

    out = df.with_columns(
        pl.when(pl.col("country_cumulative_inflation_3yr") >= CUMULATIVE_INFLATION_THRESHOLD)
        .then(pl.lit(True))
        .otherwise(pl.lit(False))
        .alias("hyperinflationary_currency_override")
    )
    assert "hyperinflationary_currency_override" in out.columns, (
        f"hyperinflationary_currency_override column missing after transform: {out.columns}"
    )
    flagged = out.filter(pl.col("hyperinflationary_currency_override"))
    if flagged.height > 0:
        below = flagged.filter(
            pl.col("country_cumulative_inflation_3yr") < CUMULATIVE_INFLATION_THRESHOLD
        ).height
        assert below == 0, (
            f"hyperinflationary_currency_override set for {below} rows where "
            f"country_cumulative_inflation_3yr < {CUMULATIVE_INFLATION_THRESHOLD}"
        )
    not_flagged = out.filter(~pl.col("hyperinflationary_currency_override"))
    if not_flagged.height > 0:
        above = not_flagged.filter(
            pl.col("country_cumulative_inflation_3yr") >= CUMULATIVE_INFLATION_THRESHOLD
        ).height
        assert above == 0, (
            f"hyperinflationary_currency_override not set for {above} rows where "
            f"country_cumulative_inflation_3yr >= {CUMULATIVE_INFLATION_THRESHOLD}"
        )
    return out
