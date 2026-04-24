"""
Purpose: Iteratively cap any single issuer's aggregate weight at 5%; redistribute
    excess pro-rata to uncapped issuers; repeat until no issuer exceeds cap.
Datapoints: issuer_id, weight.
Thresholds: 0.05 (issuer weight cap).
Source: methbooks/data/markdown/MSCI_Climate_Action_Indexes_Methodology_20251211.md section "2.4 Weighting Of Selected Securities" near line 300.
See also: methbooks/rules/weighting/apply_active_sector_weight_cap_floor.py (applied after this).
"""
from __future__ import annotations

import polars as pl

ISSUER_WEIGHT_CAP = 0.05
MAX_ITERATIONS = 1000


def cap_issuer_weight_5pct(df: pl.DataFrame) -> pl.DataFrame:
    weights = df["weight"].to_list()
    issuer_ids = df["issuer_id"].to_list()
    n = len(weights)

    for _ in range(MAX_ITERATIONS):
        # Aggregate weight per issuer
        issuer_weight: dict[str, float] = {}
        for i, iid in enumerate(issuer_ids):
            issuer_weight[iid] = issuer_weight.get(iid, 0.0) + weights[i]

        # Find issuers exceeding cap
        capped = {iid: w for iid, w in issuer_weight.items() if w > ISSUER_WEIGHT_CAP + 1e-12}
        if not capped:
            break

        excess = sum(w - ISSUER_WEIGHT_CAP for w in capped.values())
        uncapped_total = sum(w for iid, w in issuer_weight.items() if iid not in capped)

        if uncapped_total <= 0:
            break

        # Set capped issuers to cap; redistribute excess to uncapped
        for i, iid in enumerate(issuer_ids):
            if iid in capped:
                # Scale down proportionally within the issuer
                weights[i] = weights[i] * ISSUER_WEIGHT_CAP / capped[iid]
            else:
                weights[i] = weights[i] * (uncapped_total + excess) / uncapped_total

    # Renormalize to sum=1
    total = sum(weights)
    weights = [w / total for w in weights]

    out = df.with_columns(pl.Series("weight", weights))
    assert abs(float(out["weight"].sum()) - 1.0) < 1e-9, (
        f"weights do not sum to 1 after issuer capping: {out['weight'].sum()}"
    )
    return out
