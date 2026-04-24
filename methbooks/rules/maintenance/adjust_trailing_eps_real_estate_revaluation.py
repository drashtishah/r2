"""
Purpose: Adjust trailing EPS and five-year historical EPS for Real Estate companies (GICS Sector 60) by excluding gains or losses from revaluation of investment property, treating them as non-recurring.
Datapoints: gics_industry_group, trailing_12m_earnings, property_revaluation_gain_loss.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "Appendix I: MSCI's Treatment of Some Specific Aspects of IFRS" near line 3918.
See also: methbooks/rules/maintenance/apply_ifrs_goodwill_amortization_adjustment.py (another IFRS-related earnings adjustment).
"""
from __future__ import annotations

import polars as pl

_REAL_ESTATE_INDUSTRY_GROUPS = {"6010", "6020"}


def adjust_trailing_eps_real_estate_revaluation(df: pl.DataFrame) -> pl.DataFrame:
    for col in ("gics_industry_group", "trailing_12m_earnings", "property_revaluation_gain_loss"):
        assert col in df.columns, f"{col} column missing: {df.columns}"

    real_estate_missing = df.filter(
        pl.col("gics_industry_group").is_in(list(_REAL_ESTATE_INDUSTRY_GROUPS))
        & pl.col("property_revaluation_gain_loss").is_null()
    ).height
    assert real_estate_missing == 0, (
        f"property_revaluation_gain_loss missing for {real_estate_missing} Real Estate rows"
    )

    out = df.with_columns(
        pl.when(pl.col("gics_industry_group").is_in(list(_REAL_ESTATE_INDUSTRY_GROUPS)))
        .then(pl.col("trailing_12m_earnings") - pl.col("property_revaluation_gain_loss"))
        .otherwise(pl.col("trailing_12m_earnings"))
        .alias("trailing_12m_earnings_adjusted")
    )
    assert "trailing_12m_earnings_adjusted" in out.columns, (
        f"trailing_12m_earnings_adjusted column missing after adjust: {out.columns}"
    )
    re_rows = out.filter(pl.col("gics_industry_group").is_in(list(_REAL_ESTATE_INDUSTRY_GROUPS)))
    if re_rows.height > 0:
        orig_re = df.filter(pl.col("gics_industry_group").is_in(list(_REAL_ESTATE_INDUSTRY_GROUPS)))
        bad = re_rows.filter(
            (pl.col("trailing_12m_earnings_adjusted")
             - orig_re["trailing_12m_earnings"] + orig_re["property_revaluation_gain_loss"]).abs() > 1e-6
        ).height
        assert bad == 0, (
            f"adjusted_trailing_eps != trailing_eps - property_revaluation for {bad} Real Estate rows"
        )
    return out
