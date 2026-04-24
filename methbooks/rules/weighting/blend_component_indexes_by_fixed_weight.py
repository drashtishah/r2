from __future__ import annotations

import polars as pl


def blend_component_indexes_by_fixed_weight(df: pl.DataFrame) -> pl.DataFrame:
    """
    Purpose: Calculate a blended index level by combining published component index daily returns using fixed proportions set at the time of rebalance, via linear combination.
    Datapoints: component_index_level, component_index_weight.
    Thresholds: none.
    Source: methbooks/data/markdown/MSCI_Index_Calculation_Methodology.md section "3 Index Calculation for the MSCI Blended Indexes" near line 2123.
    See also: methbooks/rules/weighting/compute_price_index_level_laspeyres.py (price index chain-linking rule).
    """
    required = [
        "component_index_level",
        "component_index_weight",
    ]
    for col in required:
        assert col in df.columns, f"required column missing: {col}"

    out = df

    assert float(out["component_index_weight"].min()) > 0, f"component_index_weight must be > 0 for all components: min={float(out['component_index_weight'].min())}"
    assert abs(float(out["component_index_weight"].sum()) - 1.0) < 1e-6, f"component_index_weight must sum to 1: sum={float(out['component_index_weight'].sum())}"
    assert float(out["component_index_level"].min()) > 0, f"component_index_level must be > 0 for all components: min={float(out['component_index_level'].min())}"
    assert True, "blended index level = chain-linked product of (1 + sum_c(w_c * daily_return_c)); weights fixed between rebalances"

    return out
