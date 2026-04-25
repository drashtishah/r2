"""
Purpose: Apply flexible issuer cap for narrow Parent Indexes: max(10%, max_parent_issuer_weight).
Datapoints: issuer_id, weight, is_narrow_parent_index, max_parent_issuer_weight.
Thresholds: NARROW_FLOOR = 0.1.
Source: methbooks/data/markdown/msci_quality_indexes_methodology_20250520.md section
    "Appendix IV: Issuer Weight Capping" near line 521.
"""
from __future__ import annotations

import polars as pl

NARROW_FLOOR = 0.1
MAX_ITERATIONS = 1000


def cap_issuer_weight_narrow_quality_index(df: pl.DataFrame) -> pl.DataFrame:
    assert "issuer_id" in df.columns, f"issuer_id missing: {df.columns}"
    assert "weight" in df.columns, f"weight missing: {df.columns}"
    assert "is_narrow_parent_index" in df.columns, (
        f"is_narrow_parent_index missing: {df.columns}"
    )
    assert "max_parent_issuer_weight" in df.columns, (
        f"max_parent_issuer_weight missing: {df.columns}"
    )

    if not df["is_narrow_parent_index"].any():
        return df

    cap_level = max(NARROW_FLOOR, float(df["max_parent_issuer_weight"][0]))

    weights = df["weight"].to_list()
    issuer_ids = df["issuer_id"].to_list()

    for _ in range(MAX_ITERATIONS):
        issuer_weight: dict[str, float] = {}
        for i, iid in enumerate(issuer_ids):
            issuer_weight[iid] = issuer_weight.get(iid, 0.0) + weights[i]

        capped = {iid: w for iid, w in issuer_weight.items() if w > cap_level + 1e-12}
        if not capped:
            break

        excess = sum(w - cap_level for w in capped.values())
        uncapped_total = sum(w for iid, w in issuer_weight.items() if iid not in capped)

        if uncapped_total <= 0:
            break

        for i, iid in enumerate(issuer_ids):
            if iid in capped:
                weights[i] = weights[i] * cap_level / capped[iid]
            else:
                weights[i] = weights[i] * (uncapped_total + excess) / uncapped_total

    total = sum(weights)
    weights = [w / total for w in weights]

    out = df.with_columns(pl.Series("weight", weights))

    assert "weight" in out.columns, f"weight missing: {out.columns}"
    assert abs(float(out["weight"].sum()) - 1.0) < 1e-9, (
        f"weights do not sum to 1.0 after narrow cap: {out['weight'].sum()}"
    )

    issuer_agg: dict[str, float] = {}
    for iid, w in zip(out["issuer_id"].to_list(), out["weight"].to_list()):
        issuer_agg[iid] = issuer_agg.get(iid, 0.0) + w
    max_issuer_w = max(issuer_agg.values()) if issuer_agg else 0.0
    assert max_issuer_w <= cap_level + 1e-9, (
        f"issuer weight exceeds cap {cap_level} in narrow index: {max_issuer_w}"
    )

    return out
