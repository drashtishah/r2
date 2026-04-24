"""MSCI Index Calculation Methodology module."""
from __future__ import annotations

import random
from pathlib import Path

import numpy as np
import polars as pl

from methbooks.mock_universe import build_base_universe
from methbooks.rules.event_handling.optional_dividend_cash_default import (
    optional_dividend_cash_default,
)

_CSV = Path(__file__).parent / "index_calculation_data_dictionary.csv"

ROWS = 2000


def build_mock_data() -> pl.DataFrame:
    """Return a 2000-row DataFrame with all index_calculation datapoints."""
    base = build_base_universe()
    rng = np.random.default_rng(42)
    py_rng = random.Random(42)

    extra: dict[str, list] = {
        "price_per_share": rng.lognormal(4.0, 1.5, ROWS).tolist(),
        "end_of_day_number_of_shares": [
            int(v) for v in np.exp(rng.uniform(13.8, 23.0, ROWS)).tolist()
        ],
        "inclusion_factor": rng.uniform(0.05, 1.0, ROWS).tolist(),
        "price_adjustment_factor": rng.uniform(0.5, 1.0, ROWS).tolist(),
        "fx_rate_vs_usd": rng.uniform(0.5, 200.0, ROWS).tolist(),
        "internal_currency_index": [1.0] * ROWS,
        "gross_dividend_per_share": rng.uniform(0.0, 5.0, ROWS).tolist(),
        "withholding_tax_rate": rng.uniform(0.0, 0.35, ROWS).tolist(),
        "component_index_weight": (lambda w: (w / w.sum()).tolist())(
            rng.uniform(0.05, 0.5, ROWS)
        ),
        "component_index_level": rng.uniform(100.0, 10000.0, ROWS).tolist(),
        "daily_security_traded_volume": rng.uniform(0.0, 1e8, ROWS).tolist(),
        "fif_adjusted_market_cap_month_end": np.exp(
            rng.uniform(18.0, 30.0, ROWS)
        ).tolist(),
        "number_of_security_trading_days": [
            int(v) for v in rng.uniform(0, 23, ROWS).tolist()
        ],
        "wmr_closing_spot_rate": rng.uniform(0.5, 200.0, ROWS).tolist(),
        "error_impact_bps": rng.uniform(0.0, 100.0, ROWS).tolist(),
        "affected_index_type": [
            py_rng.choice(["country", "world_industry_group", "em_industry_group", "frontier_region"])
            for _ in range(ROWS)
        ],
        "security_traded_flag": [py_rng.random() > 0.05 for _ in range(ROWS)],
        "special_dividend_per_share": rng.uniform(0.0, 10.0, ROWS).tolist(),
        "announcement_status": [
            py_rng.choice(["confirmed", "provisional", "unconfirmed"])
            for _ in range(ROWS)
        ],
        "stock_dividend_ratio": rng.uniform(0.0, 0.5, ROWS).tolist(),
        "capital_repayment_per_share": rng.uniform(0.0, 5.0, ROWS).tolist(),
        "extraordinary_flag": [py_rng.random() < 0.1 for _ in range(ROWS)],
        "payment_default_flag": [py_rng.random() < 0.02 for _ in range(ROWS)],
        "original_dividend_amount": rng.uniform(0.0, 5.0, ROWS).tolist(),
        "shares_cum_date": np.exp(rng.uniform(13.8, 23.0, ROWS)).tolist(),
        "country_of_listing": [
            py_rng.choice(["US", "TW", "JP", "KR", "GB", "DE", "AU", "other"])
            for _ in range(ROWS)
        ],
        "country_of_incorporation": [
            py_rng.choice(["US", "TW", "JP", "KR", "GB", "DE", "AU", "other"])
            for _ in range(ROWS)
        ],
        "index_type_domestic_or_international": [
            py_rng.choice(["international", "domestic"]) for _ in range(ROWS)
        ],
        "calculation_day_of_week": [
            py_rng.choice(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Sunday"])
            for _ in range(ROWS)
        ],
        "is_sunday_intermediary_index": [py_rng.random() < 0.1 for _ in range(ROWS)],
        "is_sunday_thursday_index": [py_rng.random() < 0.1 for _ in range(ROWS)],
        "dividend_per_share": rng.uniform(0.0, 5.0, ROWS).tolist(),
        "wmr_closing_spot_rate_friday": rng.uniform(0.5, 200.0, ROWS).tolist(),
        "prior_year_dividend_same_period": rng.uniform(0.0, 5.0, ROWS).tolist(),
        "announced_dividend_estimate": rng.uniform(0.0, 5.0, ROWS).tolist(),
        "capital_change_adjustment": rng.uniform(0.8, 1.2, ROWS).tolist(),
        "ratified_dividend_per_share": rng.uniform(0.0, 5.0, ROWS).tolist(),
        "shares_prior_to_exdate": np.exp(rng.uniform(13.8, 23.0, ROWS)).tolist(),
        "redemption_right_flag": [py_rng.random() < 0.02 for _ in range(ROWS)],
        "stock_dividend_shares_received_subject_to_wht": rng.uniform(0.0, 100.0, ROWS).tolist(),
        "par_value_per_share": rng.uniform(0.01, 10.0, ROWS).tolist(),
        "end_of_day_number_of_shares_ex_date_minus_1": np.exp(
            rng.uniform(13.8, 23.0, ROWS)
        ).tolist(),
        "net_dividend_per_share": rng.uniform(0.0, 5.0, ROWS).tolist(),
        # for correct_dividend_within_12m_window - use date columns
        "dividend_amount_corrected": rng.uniform(0.0, 5.0, ROWS).tolist(),
    }

    import datetime
    base_date = datetime.date(2025, 1, 1)
    extra["ex_date"] = [
        base_date + datetime.timedelta(days=int(d))
        for d in rng.uniform(0, 300, ROWS).tolist()
    ]
    extra["correction_discovery_date"] = [
        d + datetime.timedelta(days=int(dd))
        for d, dd in zip(extra["ex_date"], rng.uniform(0, 364, ROWS).tolist())
    ]
    extra["default_discovery_date"] = [
        d + datetime.timedelta(days=int(dd))
        for d, dd in zip(extra["ex_date"], rng.uniform(0, 364, ROWS).tolist())
    ]
    extra["reception_date"] = [
        d + datetime.timedelta(days=int(dd))
        for d, dd in zip(extra["ex_date"], rng.uniform(0, 5, ROWS).tolist())
    ]
    extra["calculation_date"] = [
        base_date + datetime.timedelta(days=int(d))
        for d in rng.uniform(0, 365, ROWS).tolist()
    ]
    extra["error_discovery_date"] = [
        base_date + datetime.timedelta(days=int(d))
        for d in rng.uniform(0, 365, ROWS).tolist()
    ]

    return base.with_columns(
        [pl.Series(name=k, values=v) for k, v in extra.items()]
    )


def get_data_dictionary() -> pl.DataFrame:
    return pl.read_csv(_CSV)


def apply(df: pl.DataFrame) -> pl.DataFrame:
    out = df

    # composition_order is empty; no rule calls.

    assert float(out["price_per_share"].min()) > 0, (
        f"price index level > 0 on all calculation days: price_per_share min={float(out['price_per_share'].min())}"
    )
    assert abs(float(out["weight"].sum()) - 1.0) < 1e-6, (
        f"initial security weights sum to 100 across all constituents: weight sum={float(out['weight'].sum())}"
    )
    assert float(out["gross_dividend_per_share"].min()) >= 0, (
        f"DTR index level >= price index level when cumulative dividends reinvested are positive: gross_dividend_per_share min={float(out['gross_dividend_per_share'].min())}"
    )
    assert float(out["daily_security_traded_volume"].min()) >= 0, (
        f"ATVR >= 0: daily_security_traded_volume min={float(out['daily_security_traded_volume'].min())}"
    )
    assert abs(float(out["component_index_weight"].sum()) - 1.0) < 1e-6, (
        f"component_index_weights sum to 1 for blended indexes: sum={float(out['component_index_weight'].sum())}"
    )

    return out
