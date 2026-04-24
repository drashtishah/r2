"""
Purpose: Weight each security by quality_score * parent_index_weight, then normalize to 1.
Datapoints: quality_score, parent_index_weight.
Thresholds: none.
Source: methbooks/data/markdown/msci_quality_indexes_methodology_20250520.md section
    "2.4 Weighting Scheme" near line 238.
"""
from __future__ import annotations

import polars as pl


def apply_quality_score_tilt_weight(df: pl.DataFrame) -> pl.DataFrame:
    assert "quality_score" in df.columns, f"quality_score missing: {df.columns}"
    assert "parent_index_weight" in df.columns, f"parent_index_weight missing: {df.columns}"

    raw = (df["quality_score"] * df["parent_index_weight"]).to_list()
    total = sum(raw)
    weights = [r / total for r in raw]

    out = df.with_columns(pl.Series("weight", weights))

    assert "weight" in out.columns, f"weight missing: {out.columns}"
    assert out["weight"].null_count() == 0, (
        f"null weight after tilt: {out['weight'].null_count()} rows"
    )
    assert abs(float(out["weight"].sum()) - 1.0) < 1e-9, (
        f"weights do not sum to 1.0: {out['weight'].sum()}"
    )
    assert out.filter(pl.col("weight") <= 0).height == 0, (
        f"non-positive weight: {out.filter(pl.col('weight') <= 0).height} rows"
    )

    # Relative ordering: weight order matches quality_score * parent_index_weight order.
    tilt = (out["quality_score"] * out["parent_index_weight"]).to_list()
    wts = out["weight"].to_list()
    if len(tilt) > 1:
        pair = sorted(zip(tilt, wts))
        for i in range(len(pair) - 1):
            assert pair[i][1] <= pair[i + 1][1] + 1e-12, (
                f"weight order inconsistent with tilt order at index {i}: "
                f"tilt={pair[i][0]}, weight={pair[i][1]}"
            )

    return out
