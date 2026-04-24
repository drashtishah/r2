"""MSCI Index Policies methodology module."""
from __future__ import annotations

import math
import random
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from methbooks.mock_universe import build_base_universe

ROWS = 2000
_CSV = Path(__file__).parent / "index_policies_data_dictionary.csv"
_BASE_DATE = date(2024, 1, 1)


def build_mock_data() -> pl.DataFrame:
    """Return base universe extended with index_policies rule datapoints."""
    base = build_base_universe()
    rng = random.Random(42)

    def rand_date(offset_days_min: int, offset_days_max: int) -> list[date]:
        return [
            _BASE_DATE + timedelta(days=rng.randint(offset_days_min, offset_days_max))
            for _ in range(ROWS)
        ]

    occurrence_dates = rand_date(-400, -1)
    discovery_dates = [
        occ + timedelta(days=rng.randint(0, 400)) for occ in occurrence_dates
    ]
    announcement_dates = rand_date(-180, -31)
    termination_effective_dates = [
        ann + timedelta(days=rng.randint(90, 365)) for ann in announcement_dates
    ]
    change_announcement_dates = rand_date(-90, -31)
    change_effective_dates = [
        ann + timedelta(days=rng.randint(30, 180)) for ann in change_announcement_dates
    ]

    total_market = [rng.randint(50, 500) for _ in range(ROWS)]
    affected = [rng.randint(0, t) for t in total_market]
    eligible = [rng.randint(0, 20) for _ in range(ROWS)]
    method_min = [rng.randint(1, 10) for _ in range(ROWS)]

    extra = pl.DataFrame(
        {
            "closing_price": [math.exp(rng.gauss(3.9, 1.0)) for _ in range(ROWS)],
            "market_status": [rng.choice(["open", "closed"]) for _ in range(ROWS)],
            "is_trading": [rng.random() > 0.05 for _ in range(ROWS)],
            "intraday_price": [math.exp(rng.gauss(3.9, 1.0)) for _ in range(ROWS)],
            "market_outage_timestamp": [
                None if rng.random() > 0.05 else "2024-06-01T14:30:00"
                for _ in range(ROWS)
            ],
            "component_index_level": [rng.uniform(100.0, 5000.0) for _ in range(ROWS)],
            "component_availability_flag": [rng.random() > 0.02 for _ in range(ROWS)],
            "error_impact_bps": [rng.uniform(0.0, 200.0) for _ in range(ROWS)],
            "index_level_scope": [
                rng.choice(["country", "regional", "global"]) for _ in range(ROWS)
            ],
            "restatement_flag": [rng.random() > 0.7 for _ in range(ROWS)],
            "error_occurrence_date": occurrence_dates,
            "error_discovery_date": discovery_dates,
            "correction_eligible_flag": [rng.random() > 0.5 for _ in range(ROWS)],
            "error_type": [
                rng.choice(["price_error", "constituent_omission", "weight_error"])
                for _ in range(ROWS)
            ],
            "is_constituent_omission": [rng.random() > 0.9 for _ in range(ROWS)],
            "affected_security_count": affected,
            "total_market_security_count": total_market,
            "termination_announcement_date": announcement_dates,
            "termination_effective_date": termination_effective_dates,
            "termination_cause": [
                rng.choice(["standard", "eic_exception"]) for _ in range(ROWS)
            ],
            "is_provisional_index": [rng.random() > 0.9 for _ in range(ROWS)],
            "eligible_security_count": eligible,
            "methodology_minimum_securities": method_min,
            "discontinued_flag": [e < m for e, m in zip(eligible, method_min)],
            "change_announcement_date": change_announcement_dates,
            "change_effective_date": change_effective_dates,
            "eic_fast_track_flag": [rng.random() > 0.9 for _ in range(ROWS)],
        }
    )
    return pl.concat([base, extra], how="horizontal")


def get_data_dictionary() -> pl.DataFrame:
    """Return the index_policies data dictionary as a polars DataFrame."""
    return pl.read_csv(_CSV)


def apply(df: pl.DataFrame) -> pl.DataFrame:
    """Validate base invariants; index_policies composition_order is empty."""
    required = ["security_id", "weight"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing base columns: {missing}"

    weight_sum = float(df["weight"].sum())
    assert abs(weight_sum - 1.0) < 1e-6, f"weights do not sum to 1: sum={weight_sum}"

    weight_min = float(df["weight"].min())
    assert weight_min >= 0, f"negative weight found: min={weight_min}"

    return df
