"""MSCI Fundamental Data methodology module."""
from __future__ import annotations

import datetime
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
    # financial_period_end_date: within past 18 months so all rows satisfy the reporting gap invariant
    _max_gap_days = int(_MAX_GAP_DAYS) - 1
    fp_end_dates = [
        today - datetime.timedelta(days=int(d))
        for d in rng.integers(30, _max_gap_days, ROWS)
    ]
    earnings_period_dates = [
        today - datetime.timedelta(days=int(d))
        for d in rng.integers(30, _max_gap_days, ROWS)
    ]
    book_value_period_dates = [
        ep - datetime.timedelta(days=int(d))
        for ep, d in zip(earnings_period_dates, rng.integers(0, 180, ROWS))
    ]

    five_year_eps = [
        [float(v) for v in rng.normal(1.5, 0.8, 5).tolist()]
        for _ in range(ROWS)
    ]
    five_year_sps = [
        [float(v) for v in np.exp(rng.normal(5, 1, 5)).tolist()]
        for _ in range(ROWS)
    ]

    shares_pre = [float(v) for v in np.exp(rng.normal(18, 2, ROWS)).tolist()]
    shares_post = [v * float(f) for v, f in zip(shares_pre, rng.uniform(0.8, 1.5, ROWS).tolist())]
    mcap_pre = [float(v) for v in np.exp(rng.uniform(20, 30, ROWS)).tolist()]
    mcap_post = [v * float(f) for v, f in zip(mcap_pre, rng.uniform(0.8, 1.5, ROWS).tolist())]
    latest_bvps_vals = [float(v) for v in np.exp(rng.normal(3, 1, ROWS)).tolist()]
    shares_issued = [float(v) for v in np.exp(rng.normal(15, 2, ROWS)).tolist()]
    issue_price_vals = [float(v) for v in np.exp(rng.normal(4, 1, ROWS)).tolist()]

    eps1 = rng.normal(2, 1, ROWS).tolist()
    eps2_raw = rng.normal(2.2, 1, ROWS).tolist()
    eps2: list[float | None] = [
        None if py_rng.random() < 0.1 else float(v) for v in eps2_raw
    ]
    eps0 = rng.normal(1.8, 0.9, ROWS).tolist()
    m_vals = [py_rng.randint(0, 12) for _ in range(ROWS)]
    eps12f_vals = [
        float(eps1[i]) if eps2[i] is None and m_vals[i] >= 8
        else (m_vals[i] * float(eps1[i]) + (12 - m_vals[i]) * float(eps2[i])) / 12 if eps2[i] is not None
        else None
        for i in range(ROWS)
    ]
    eps12b_vals = [
        (m_vals[i] * float(eps0[i]) + (12 - m_vals[i]) * float(eps1[i])) / 12
        for i in range(ROWS)
    ]

    cols_df = pl.DataFrame(
        {
            "country_of_classification": [
                py_rng.choice(["US", "CA", "GB", "IE", "DE", "JP", "FR", "AU", "HK", "SG", "IN", "BR", "CN"])
                for _ in range(ROWS)
            ],
            "gics_industry_group": [
                py_rng.choice(["1010", "2010", "2510", "3010", "4010", "4030", "5010", "6010", "6020"])
                for _ in range(ROWS)
            ],
            "number_of_shares_outstanding": [float(v) for v in np.exp(rng.normal(18, 2, ROWS)).tolist()],
            "average_shares_outstanding": [float(v) for v in np.exp(rng.normal(18, 2, ROWS)).tolist()],
            "consolidation_basis": [
                py_rng.choice(["consolidated", "non_consolidated"]) for _ in range(ROWS)
            ],
            "trailing_12m_earnings": rng.normal(1e9, 5e8, ROWS).tolist(),
            "trailing_12m_sales": [float(v) for v in np.exp(rng.normal(21, 2, ROWS)).tolist()],
            "trailing_12m_cash_earnings": rng.normal(1.2e9, 5e8, ROWS).tolist(),
            "trailing_12m_earnings_per_share": rng.normal(2.0, 1.0, ROWS).tolist(),
            "annualized_dps": rng.uniform(0, 5, ROWS).tolist(),
            "book_value_per_share": [float(v) for v in np.exp(rng.normal(3, 1, ROWS)).tolist()],
            "financial_period_end_date": pl.Series(fp_end_dates, dtype=pl.Date),
            "calculation_date": pl.Series(calculation_dates, dtype=pl.Date),
            "earnings_period_date": pl.Series(earnings_period_dates, dtype=pl.Date),
            "book_value_period_date": pl.Series(book_value_period_dates, dtype=pl.Date),
            "eps1_consensus_forecast": [float(v) for v in eps1],
            "eps2_consensus_forecast": pl.Series(eps2, dtype=pl.Float64),
            "months_to_fiscal_year_end": m_vals,
            "eps0_last_fiscal_year_eps": [float(v) for v in eps0],
            "eps12f": pl.Series(eps12f_vals, dtype=pl.Float64),
            "eps12b": [float(v) for v in eps12b_vals],
            "egrlf_value": rng.normal(0.12, 0.15, ROWS).tolist(),
            "egrlf_contributor_count": [py_rng.randint(1, 20) for _ in range(ROWS)],
            "five_year_eps_history": pl.Series(five_year_eps),
            "five_year_sps_history": pl.Series(five_year_sps),
            "price_adjustment_factor": rng.uniform(0.5, 2.0, ROWS).tolist(),
            "per_share_fundamental_data": rng.normal(5.0, 2.0, ROWS).tolist(),
            "country_cumulative_inflation_3yr": rng.uniform(0, 2.0, ROWS).tolist(),
            "property_revaluation_gain_loss": rng.normal(0, 1e8, ROWS).tolist(),
            "cash_distributions_last_12m": rng.uniform(0, 5, ROWS).tolist(),
            "cash_distributions_last_6m": rng.uniform(0, 2.5, ROWS).tolist(),
            "prior_annualized_dps": rng.uniform(0, 5, ROWS).tolist(),
            "payment_frequency": [py_rng.choice([1, 2, 4, 12]) for _ in range(ROWS)],
            "shares_outstanding_pre_event": shares_pre,
            "shares_outstanding_post_event": shares_post,
            "shares_issued": shares_issued,
            "issue_price": issue_price_vals,
            "latest_bvps": latest_bvps_vals,
            "market_cap_pre_event": mcap_pre,
            "market_cap_post_event": mcap_post,
            "index_size_segment_pre_event": [
                py_rng.choice(["non_constituent", "small_cap", "standard"]) for _ in range(ROWS)
            ],
            "index_size_segment_post_event": [
                py_rng.choice(["non_constituent", "small_cap", "standard", "gimi_constituent"]) for _ in range(ROWS)
            ],
            "event_type_flag": [
                py_rng.choice(["share_buyback", "special_cash_dividend", "debt_to_equity_swap",
                               "rights_issue", "merger", "spinoff", None])
                for _ in range(ROWS)
            ],
            "post_event_annual_sales": [float(v) for v in np.exp(rng.normal(21, 2, ROWS)).tolist()],
            "pre_event_annual_sales": [float(v) for v in np.exp(rng.normal(21, 2, ROWS)).tolist()],
            "acquired_company_trailing_12m_losses_usd": rng.uniform(0, 5e9, ROWS).tolist(),
            "acquired_company_was_standard_constituent": [py_rng.random() < 0.3 for _ in range(ROWS)],
            "historical_earnings_non_ifrs": rng.normal(1e9, 5e8, ROWS).tolist(),
            "historical_earnings_ifrs": rng.normal(1e9, 5e8, ROWS).tolist(),
            "goodwill_amortization": rng.uniform(0, 1e8, ROWS).tolist(),
            "security_yield_correct": rng.uniform(0.01, 0.10, ROWS).tolist(),
            "security_yield_incorrect": rng.uniform(0.01, 0.10, ROWS).tolist(),
            "index_yield_correct": rng.uniform(0.01, 0.10, ROWS).tolist(),
            "index_yield_incorrect": rng.uniform(0.01, 0.10, ROWS).tolist(),
            "ratio_type": [
                py_rng.choice(["earnings_yield", "dividend_yield", "book_value_yield", "sales_yield"])
                for _ in range(ROWS)
            ],
        }
    )
    return pl.concat([base, cols_df], how="horizontal")


def get_data_dictionary() -> pl.DataFrame:
    """Return the fundamental_data data dictionary as a polars DataFrame."""
    return pl.read_csv(_CSV)


def apply(df: pl.DataFrame) -> pl.DataFrame:
    """Validate df against fundamental_data invariants and return it unchanged."""
    assert "security_id" in df.columns, (
        f"security_id column missing from fundamental_data output: {df.columns[:5]}"
    )
    if "financial_period_end_date" in df.columns and "calculation_date" in df.columns:
        gap_days = (
            df["calculation_date"].cast(pl.Date) - df["financial_period_end_date"].cast(pl.Date)
        ).dt.total_days()
        max_gap = float(gap_days.max())
        assert max_gap < _MAX_GAP_DAYS, (
            f"every security in output has reporting gap between calculation date and financial "
            f"period end date strictly less than 18 months: max_gap_days={max_gap}"
        )
    return df
