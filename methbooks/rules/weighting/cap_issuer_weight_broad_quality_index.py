"""
Purpose: Cap aggregate issuer weight at 5% for broad-market Quality Indexes.
Datapoints: issuer_id, weight, is_broad_parent_index.
Thresholds: BROAD_ISSUER_CAP = 0.05.
Source: methbooks/data/markdown/msci_quality_indexes_methodology_20250520.md section
    "2.4 Weighting Scheme" near line 251.
See also: methbooks/rules/weighting/cap_issuer_weight_5pct.py (same cap value, different
    context).
"""
from __future__ import annotations

import polars as pl

BROAD_ISSUER_CAP = 0.05
MAX_ITERATIONS = 1000


def cap_issuer_weight_broad_quality_index(df: pl.DataFrame) -> pl.DataFrame:
    assert "issuer_id" in df.columns, f"issuer_id missing: {df.columns}"
    assert "weight" in df.columns, f"weight missing: {df.columns}"
    assert "is_broad_parent_index" in df.columns, f"is_broad_parent_index missing: {df.columns}"

    if not df["is_broad_parent_index"].any():
        return df

    weights = df["weight"].to_list()
    issuer_ids = df["issuer_id"].to_list()

    for _ in range(MAX_ITERATIONS):
        issuer_weight: dict[str, float] = {}
        for i, iid in enumerate(issuer_ids):
            issuer_weight[iid] = issuer_weight.get(iid, 0.0) + weights[i]

        capped = {iid: w for iid, w in issuer_weight.items() if w > BROAD_ISSUER_CAP + 1e-12}
        if not capped:
            break

        excess = sum(w - BROAD_ISSUER_CAP for w in capped.values())
        uncapped_total = sum(w for iid, w in issuer_weight.items() if iid not in capped)

        if uncapped_total <= 0:
            break

        for i, iid in enumerate(issuer_ids):
            if iid in capped:
                weights[i] = weights[i] * BROAD_ISSUER_CAP / capped[iid]
            else:
                weights[i] = weights[i] * (uncapped_total + excess) / uncapped_total

    total = sum(weights)
    weights = [w / total for w in weights]

    out = df.with_columns(pl.Series("weight", weights))

    assert "weight" in out.columns, f"weight missing: {out.columns}"
    assert abs(float(out["weight"].sum()) - 1.0) < 1e-9, (
        f"weights do not sum to 1.0 after broad cap: {out['weight'].sum()}"
    )

    issuer_agg: dict[str, float] = {}
    for iid, w in zip(out["issuer_id"].to_list(), out["weight"].to_list()):
        issuer_agg[iid] = issuer_agg.get(iid, 0.0) + w
    max_issuer_w = max(issuer_agg.values()) if issuer_agg else 0.0
    assert max_issuer_w <= BROAD_ISSUER_CAP + 1e-9, (
        f"issuer weight exceeds {BROAD_ISSUER_CAP} in broad index: {max_issuer_w}"
    )

    return out
