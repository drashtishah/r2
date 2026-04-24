"""MSCI Fundamental Data methodology module."""
from __future__ import annotations

import datetime
import math
import random
from pathlib import Path

import numpy as np
import polars as pl

from methbooks.mock_universe import build_base_universe

ROWS = 2000
_CSV = Path(__file__).parent / "fundamental_data_data_dictionary.csv"

_MAX_REPORTING_GAP_MONTHS = 18
_MAX_GAP_DAYS = _MAX_REPORTING_GAP_MONTHS * 30.4375


def build_mock_data() -> pl.DataFrame:
    """Return a 2000-row DataFrame with all fundamental_data datapoints."""
    base = build_base_universe()
    rng = np.random.default_rng(42)
    py_rng = random.Random(42)

    today = datetime.date(2026, 4, 24)
    calculation_dates = [today] * ROWS
    # financial_period_end_date: within past 24 months; some > 18m gap to allow filtering
    fp_end_dates = [
        today - datetime.timedelta(days=int(d))
        for d in rng.integers(30, 730, ROWS)
    ]

    five_year_eps = [
        [float(v) for v in rng.normal(1.5, 0.8, 5).tolist()]
        for _ in range(ROWS)
    ]
    five_year_sps = [
        [float(v) for v in np.exp(rng.normal(5, 1, 5)).tolist()]
        for _ in range(ROWS)
    ]

    extra: dict[str, list] = {
        "country_of_classification": [
            py_rng.choice(["US", "CA", "GB", "IE", "DE", "JP", "FR", "AU", "HK", "SG", "IN", "BR", "CN"])
            for _ in range(ROWS)
        ],
        "gics_industry_group": [
            py_rng.choice(["1010", "2010", "2510", "3010", "4010", "4030", "5010", "6010", "6020"])
            for _ in range(ROWS)
        ],
        "number_of_shares_outstanding": [
            float(v) for v in np.exp(rng.normal(18, 2, ROWS)).tolist()
        ],
        "average_shares_outstanding": [
            float(v) for v in np.exp(rng.normal(18, 2, ROWS)).tolist()
        ],
        "consolidation_basis": [
            py_rng.choice(["consolidated", "non_consolidated"]) for _ in range(ROWS)
        ],
        "trailing_12m_earnings": rng.normal(1e9, 5e8, ROWS).tolist(),
        "trailing_12m_sales": [float(v) for v in np.exp(rng.normal(21, 2, ROWS)).tolist()],
        "trailing_12m_cash_earnings": rng.normal(1.2e9, 5e8, ROWS).tolist(),
        "annualized_dps": rng.uniform(0, 5, ROWS).tolist(),
        "book_value_per_share": [float(v) for v in np.exp(rng.normal(3, 1, ROWS)).tolist()],
        "financial_period_end_date": fp_end_dates,
        "calculation_date": calculation_dates,
        "eps1_consensus_forecast": rng.normal(2, 1, ROWS).tolist(),
        "eps2_consensus_forecast": [
            None if py_rng.random() < 0.1 else float(v)
            for v in rng.normal(2.2, 1, ROWS).tolist()
        ],
        "months_to_fiscal_year_end": [py_rng.randint(0, 12) for _ in range(ROWS)],
        "eps0_last_fiscal_year_eps": rng.normal(1.8, 0.9, ROWS).tolist(),
        "egrlf_value": rng.normal(0.12, 0.15, ROWS).tolist(),
        "egrlf_contributor_count": [py_rng.randint(1, 20) for _ in range(ROWS)],
        "five_year_eps_history": five_year_eps,
        "five_year_sps_history": five_year_sps,
        "price_adjustment_factor": rng.uniform(0.5, 2.0, ROWS).tolist(),
        "country_cumulative_inflation_3yr": rng.uniform(0, 2.0, ROWS).tolist(),
        "property_revaluation_gain_loss": rng.normal(0, 1e8, ROWS).tolist(),
    }

    cols_df = pl.DataFrame(
        {
            "country_of_classification": extra["country_of_classification"],
            "gics_industry_group": extra["gics_industry_group"],
            "number_of_shares_outstanding": extra["number_of_shares_outstanding"],
            "average_shares_outstanding": extra["average_shares_outstanding"],
            "consolidation_basis": extra["consolidation_basis"],
            "trailing_12m_earnings": extra["trailing_12m_earnings"],
            "trailing_12m_sales": extra["trailing_12m_sales"],
            "trailing_12m_cash_earnings": extra["trailing_12m_cash_earnings"],
            "annualized_dps": extra["annualized_dps"],
            "book_value_per_share": extra["book_value_per_share"],
            "financial_period_end_date": pl.Series(extra["financial_period_end_date"], dtype=pl.Date),
            "calculation_date": pl.Series(extra["calculation_date"], dtype=pl.Date),
            "eps1_consensus_forecast": extra["eps1_consensus_forecast"],
            "eps2_consensus_forecast": pl.Series(extra["eps2_consensus_forecast"], dtype=pl.Float64),
            "months_to_fiscal_year_end": extra["months_to_fiscal_year_end"],
            "eps0_last_fiscal_year_eps": extra["eps0_last_fiscal_year_eps"],
            "egrlf_value": extra["egrlf_value"],
            "egrlf_contributor_count": extra["egrlf_contributor_count"],
            "five_year_eps_history": pl.Series(extra["five_year_eps_history"]),
            "five_year_sps_history": pl.Series(extra["five_year_sps_history"]),
            "price_adjustment_factor": extra["price_adjustment_factor"],
            "country_cumulative_inflation_3yr": extra["country_cumulative_inflation_3yr"],
            "property_revaluation_gain_loss": extra["property_revaluation_gain_loss"],
        }
    )
    return pl.concat([base, cols_df], how="horizontal")


def get_data_dictionary() -> pl.DataFrame:
    """Return the fundamental_data data dictionary as a polars DataFrame."""
    return pl.read_csv(_CSV)


def apply(df: pl.DataFrame) -> pl.DataFrame:
    """Validate df against fundamental_data invariants and return it unchanged."""
    if "financial_period_end_date" in df.columns and "calculation_date" in df.columns:
        gap_days = (
            df["calculation_date"].cast(pl.Date) - df["financial_period_end_date"].cast(pl.Date)
        ).dt.total_days()
        max_gap = float(gap_days.max())
        assert max_gap < _MAX_GAP_DAYS, (
            f"every security in output has reporting gap between calculation date and financial "
            f"period end date strictly less than 18 months: max_gap_days={max_gap}"
        )

    if "price_adjustment_factor" in df.columns and "per_share_fundamental_data" in df.columns:
        pass

    return df
