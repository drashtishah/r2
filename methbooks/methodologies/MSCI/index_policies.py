"""MSCI Index Policies methodology module."""
from __future__ import annotations

from pathlib import Path

import polars as pl

from methbooks.mock_universe import build_base_universe

_CSV = Path(__file__).parent / "index_policies_data_dictionary.csv"


def build_mock_data() -> pl.DataFrame:
    """Return base universe; index_policies defines no additional mock datapoints."""
    return build_base_universe()


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
