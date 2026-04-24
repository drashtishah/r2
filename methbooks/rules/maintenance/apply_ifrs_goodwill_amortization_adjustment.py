"""
Purpose: Add back goodwill amortization to pre-IFRS historical earnings to maintain 5-year EPS comparability when IFRS adoption changes reported earnings by 10% or more.
Datapoints: historical_earnings_non_ifrs, historical_earnings_ifrs, goodwill_amortization.
Thresholds: MIN_EARNINGS_DIFF_PCT = 0.1.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "Appendix I: MSCI's Treatment of Some Specific Aspects of IFRS" near line 3906.
See also: methbooks/rules/maintenance/adjust_trailing_eps_real_estate_revaluation.py (another IFRS-related earnings adjustment).
"""
from __future__ import annotations

import polars as pl

MIN_EARNINGS_DIFF_PCT = 0.1


def apply_ifrs_goodwill_amortization_adjustment(df: pl.DataFrame) -> pl.DataFrame:
    for col in ("historical_earnings_non_ifrs", "historical_earnings_ifrs", "goodwill_amortization"):
        assert col in df.columns, f"{col} column missing: {df.columns}"

    out = df.with_columns(
        pl.when(
            pl.col("historical_earnings_non_ifrs").is_not_null()
            & pl.col("historical_earnings_ifrs").is_not_null()
            & (pl.col("historical_earnings_non_ifrs") != 0)
            & (
                (
                    (pl.col("historical_earnings_ifrs") - pl.col("historical_earnings_non_ifrs")).abs()
                    / pl.col("historical_earnings_non_ifrs").abs()
                ) >= MIN_EARNINGS_DIFF_PCT
            )
        )
        .then(pl.col("historical_earnings_non_ifrs") + pl.col("goodwill_amortization"))
        .otherwise(pl.col("historical_earnings_non_ifrs"))
        .alias("adjusted_historical_earnings_non_ifrs")
    )
    assert "adjusted_historical_earnings_non_ifrs" in out.columns, (
        f"adjusted_historical_earnings_non_ifrs column missing after adjust: {out.columns}"
    )
    adjusted = out.filter(
        pl.col("historical_earnings_non_ifrs").is_not_null()
        & pl.col("historical_earnings_ifrs").is_not_null()
        & (pl.col("historical_earnings_non_ifrs") != 0)
        & (
            (
                (pl.col("historical_earnings_ifrs") - pl.col("historical_earnings_non_ifrs")).abs()
                / pl.col("historical_earnings_non_ifrs").abs()
            ) >= MIN_EARNINGS_DIFF_PCT
        )
    )
    if adjusted.height > 0:
        bad = adjusted.join(out.select(["security_id", "adjusted_historical_earnings_non_ifrs"]), on="security_id").filter(
            (pl.col("adjusted_historical_earnings_non_ifrs") - pl.col("historical_earnings_non_ifrs") - pl.col("goodwill_amortization")).abs() > 1e-6
        ).height
        assert bad == 0, (
            f"adjusted_historical_earnings_non_ifrs != non_ifrs + goodwill_amortization for {bad} rows"
        )
    return out
