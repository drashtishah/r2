"""
Purpose: Apply average number of shares (not period-end) for EPS, CEPS, and SPS per-share calculations for US, Canada, UK, and Ireland, matching local standard practice.
Datapoints: country_of_classification, number_of_shares_outstanding, average_shares_outstanding.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "1.1.1 Number of Shares" near line 584.
See also: methbooks/rules/scoring/compute_eps12f.py (uses the shares denominator set here).
"""
from __future__ import annotations

import polars as pl

_AVG_SHARES_COUNTRIES = {"US", "CA", "GB", "IE"}


def use_average_shares_us_canada_uk_ireland(df: pl.DataFrame) -> pl.DataFrame:
    assert "country_of_classification" in df.columns, (
        f"country_of_classification column missing: {df.columns}"
    )
    assert "average_shares_outstanding" in df.columns, (
        f"average_shares_outstanding column missing: {df.columns}"
    )
    out = df.with_columns(
        pl.when(pl.col("country_of_classification").is_in(list(_AVG_SHARES_COUNTRIES)))
        .then(pl.col("average_shares_outstanding"))
        .otherwise(pl.col("number_of_shares_outstanding"))
        .alias("shares_denominator")
    )
    assert "shares_denominator" in out.columns, (
        f"shares_denominator column missing after transform: {out.columns}"
    )
    avg_countries = out.filter(
        pl.col("country_of_classification").is_in(list(_AVG_SHARES_COUNTRIES))
    )
    if avg_countries.height > 0:
        mismatches = avg_countries.filter(
            pl.col("shares_denominator") != pl.col("average_shares_outstanding")
        ).height
        assert mismatches == 0, (
            f"shares_denominator not equal to average_shares_outstanding for "
            f"{mismatches} US/CA/GB/IE rows"
        )
    return out
