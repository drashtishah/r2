"""
Purpose: Under Market Stress Light Rebalancing conditions, replace standard buffer
zones (2/3x and 1.5x) with tighter zones (0.5x and 1.8x) and restrict Standard
Index additions and migrations per Appendix VIII criteria.
Datapoints: light_rebalancing_flag, full_market_cap_usd,
  market_size_segment_cutoff_usd.
Thresholds: LIGHT_REBALANCING_LOWER_BUFFER_MULTIPLIER=0.5,
  LIGHT_REBALANCING_UPPER_BUFFER_MULTIPLIER=1.8,
  STANDARD_LOWER_BUFFER_MULTIPLIER=0.667,
  STANDARD_UPPER_BUFFER_MULTIPLIER=1.5.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "Appendix VIII: Market Monitoring Framework and Potential Switch to a Light Rebalancing under Conditions of Market Stress" near line 5475.
See also: methbooks/rules/eligibility/apply_equity_universe_minimum_size_requirement.py (size-based eligibility rule).
"""
from __future__ import annotations

import polars as pl

LIGHT_REBALANCING_LOWER_BUFFER_MULTIPLIER = 0.5
LIGHT_REBALANCING_UPPER_BUFFER_MULTIPLIER = 1.8
STANDARD_LOWER_BUFFER_MULTIPLIER = 0.667
STANDARD_UPPER_BUFFER_MULTIPLIER = 1.5


def light_rebalancing_tighter_buffers(df: pl.DataFrame) -> pl.DataFrame:
    assert "light_rebalancing_flag" in df.columns, (
        f"light_rebalancing_flag column missing: {df.columns}"
    )
    assert "full_market_cap_usd" in df.columns, (
        f"full_market_cap_usd column missing: {df.columns}"
    )
    assert "market_size_segment_cutoff_usd" in df.columns, (
        f"market_size_segment_cutoff_usd column missing: {df.columns}"
    )
    is_light = pl.col("light_rebalancing_flag")
    lower = pl.when(is_light).then(
        LIGHT_REBALANCING_LOWER_BUFFER_MULTIPLIER * pl.col("market_size_segment_cutoff_usd")
    ).otherwise(
        STANDARD_LOWER_BUFFER_MULTIPLIER * pl.col("market_size_segment_cutoff_usd")
    )
    upper = pl.when(is_light).then(
        LIGHT_REBALANCING_UPPER_BUFFER_MULTIPLIER * pl.col("market_size_segment_cutoff_usd")
    ).otherwise(
        STANDARD_UPPER_BUFFER_MULTIPLIER * pl.col("market_size_segment_cutoff_usd")
    )
    out = df.with_columns(
        lower.alias("buffer_lower_usd"),
        upper.alias("buffer_upper_usd"),
    )
    # Business assert: light rebalancing rows use tighter lower buffer (0.5x vs 0.667x).
    light_rows = out.filter(is_light)
    if light_rows.height > 0:
        expected_lower = (
            LIGHT_REBALANCING_LOWER_BUFFER_MULTIPLIER
            * light_rows["market_size_segment_cutoff_usd"]
        )
        actual_lower = light_rows["buffer_lower_usd"]
        assert (
            (actual_lower - expected_lower).abs().max() < 1e-6
        ), (
            f"buffer_lower_usd does not equal {LIGHT_REBALANCING_LOWER_BUFFER_MULTIPLIER}x"
            f" cutoff under light rebalancing: max diff={(actual_lower - expected_lower).abs().max()}"
        )
    return out
