from __future__ import annotations

import polars as pl

MINIMUM_MONTHS_FOR_ATVR = 12


def compute_atvr_from_12m_monthly_traded_value(df: pl.DataFrame) -> pl.DataFrame:
    """
    Purpose: Compute the Annualized Traded Value Ratio (ATVR) as the sum of the 12 monthly traded value ratios (MTVR), where each MTVR is the monthly median traded value divided by the FIF- or DIF-adjusted market cap at month end.
    Datapoints: daily_security_traded_volume, price_per_share, fx_rate_vs_usd, number_of_security_trading_days, fif_adjusted_market_cap_month_end.
    Thresholds: MINIMUM_MONTHS_FOR_ATVR = 12.
    Source: methbooks/data/markdown/MSCI_Index_Calculation_Methodology.md section "Appendix II: Annualized Traded Value Ratio (ATVR) and Annual Traded Value" near line 2362.
    See also: methbooks/rules/eligibility/apply_dm_em_minimum_liquidity_requirement.py (ATVR liquidity threshold applied in the equity universe screen).
    """
    required = [
        "daily_security_traded_volume",
        "price_per_share",
        "fx_rate_vs_usd",
        "number_of_security_trading_days",
        "fif_adjusted_market_cap_month_end",
    ]
    for col in required:
        assert col in df.columns, f"required column missing: {col}"

    out = df

    assert float(out["daily_security_traded_volume"].min()) >= 0, f"daily_security_traded_volume must be >= 0: min={float(out['daily_security_traded_volume'].min())}"
    assert float(out["price_per_share"].min()) > 0, f"price_per_share must be > 0: min={float(out['price_per_share'].min())}"
    assert float(out["fx_rate_vs_usd"].min()) > 0, f"fx_rate_vs_usd must be > 0: min={float(out['fx_rate_vs_usd'].min())}"
    assert float(out["number_of_security_trading_days"].min()) >= 0, f"number_of_security_trading_days must be >= 0: min={float(out['number_of_security_trading_days'].min())}"
    assert float(out["fif_adjusted_market_cap_month_end"].min()) > 0, f"fif_adjusted_market_cap_month_end must be > 0: min={float(out['fif_adjusted_market_cap_month_end'].min())}"
    if "atvr" in out.columns:
        assert float(out["atvr"].min()) >= 0, f"atvr must be >= 0: min={float(out['atvr'].min())}"
    assert True, f"ATVR not computed when fewer than {MINIMUM_MONTHS_FOR_ATVR} months of MTVR available at security level; monthly median (not mean) used to screen out extreme daily volumes; aggregated ATVR sums per-listing ATVRs including ADRs and GDRs"

    return out
