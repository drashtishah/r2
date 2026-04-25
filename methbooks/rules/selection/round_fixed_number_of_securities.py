"""
Purpose: Apply upward rounding to raw target count per tier.
Datapoints: raw_fixed_number.
Thresholds: TIER1_UPPER = 100, TIER1_ROUND = 10, TIER2_LOWER = 100, TIER2_UPPER = 300,
    TIER2_ROUND = 25, TIER3_LOWER = 300, TIER3_ROUND = 50.
Source: methbooks/data/markdown/msci_quality_indexes_methodology_20250520.md section
    "Appendix III: Rules to Determine Fixed Number of Securities at Initial Construction
    and in Ongoing Rebalancing" near line 488.
"""
from __future__ import annotations

import math

import polars as pl

TIER1_UPPER = 100
TIER1_ROUND = 10
TIER2_LOWER = 100
TIER2_UPPER = 300
TIER2_ROUND = 25
TIER3_LOWER = 300
TIER3_ROUND = 50


def _round_up(x: int, multiple: int) -> int:
    return math.ceil(x / multiple) * multiple


def round_fixed_number_of_securities(df: pl.DataFrame) -> pl.DataFrame:
    assert "raw_fixed_number" in df.columns, f"raw_fixed_number missing: {df.columns}"

    raw = int(df["raw_fixed_number"][0])

    if raw < TIER1_UPPER:
        rounded = _round_up(raw, TIER1_ROUND)
    elif raw < TIER2_UPPER:
        rounded = _round_up(raw, TIER2_ROUND)
    else:
        rounded = _round_up(raw, TIER3_ROUND)

    out = df.with_columns(pl.lit(rounded).cast(pl.Int64).alias("fixed_number_securities"))

    assert "fixed_number_securities" in out.columns, (
        f"fixed_number_securities missing: {out.columns}"
    )
    val = int(out["fixed_number_securities"][0])
    assert val > 0, f"fixed_number_securities not positive: {val}"
    assert val >= raw, f"fixed_number_securities {val} < raw_fixed_number {raw}"

    if raw < TIER1_UPPER:
        assert val % TIER1_ROUND == 0, f"fixed_number_securities {val} not multiple of {TIER1_ROUND}"
    elif raw < TIER2_UPPER:
        assert val % TIER2_ROUND == 0, f"fixed_number_securities {val} not multiple of {TIER2_ROUND}"
    else:
        assert val % TIER3_ROUND == 0, f"fixed_number_securities {val} not multiple of {TIER3_ROUND}"

    return out
