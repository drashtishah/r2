"""Shared base universe for methbooks rules.

Purpose (why):
    Deterministic polars DataFrame of 2000 rows with a random security_id
    and a random weight distribution summing to 1. Methbooks extend this
    base with methbook-specific datapoints in their own build_mock_data().
Input:
    seed: int, defaults to 42 for determinism.
Output:
    pl.DataFrame with columns (security_id, weight), 2000 rows.
Side effects:
    none. Raises AssertionError on invariant violation.
"""
from __future__ import annotations

import random
import string

import polars as pl

ROWS = 2000
SEED = 42


def _random_id(rng: random.Random) -> str:
    return "".join(rng.choices(string.ascii_uppercase + string.digits, k=6))


def build_base_universe(seed: int = SEED) -> pl.DataFrame:
    rng = random.Random(seed)
    raw = [rng.random() for _ in range(ROWS)]
    total = sum(raw)
    df = pl.DataFrame(
        {
            "security_id": [_random_id(rng) for _ in range(ROWS)],
            "weight": [w / total for w in raw],
        }
    )
    assert df.height == ROWS, f"expected {ROWS} rows, got {df.height}"
    assert set(df.columns) == {"security_id", "weight"}, f"unexpected columns: {set(df.columns)}"
    assert abs(float(df["weight"].sum()) - 1.0) < 1e-9, f"weights must sum to 1, got {df['weight'].sum()}"
    return df
