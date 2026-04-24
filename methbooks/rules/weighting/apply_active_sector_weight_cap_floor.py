"""
Purpose: Cap active sector weight at +5% and floor at -5% relative to parent index;
    redistribute excess/deficit across uncapped sectors proportionally; renormalize to 1.
    Single pass (per methbook clarification).
Datapoints: weight, parent_index_weight, gics_sector.
Thresholds: +0.05 active weight cap, -0.05 active weight floor.
Source: methbooks/data/markdown/MSCI_Climate_Action_Indexes_Methodology_20251211.md section "2.4.1 Active GICS Sector Weight Cap and Floor" near line 305.
See also: methbooks/rules/weighting/cap_issuer_weight_5pct.py (applied before this).
"""
from __future__ import annotations

import polars as pl

ACTIVE_CAP = 0.05
ACTIVE_FLOOR = -0.05


def apply_active_sector_weight_cap_floor(df: pl.DataFrame) -> pl.DataFrame:
    # Compute sector-level sums
    sector_weights = (
        df.group_by("gics_sector")
        .agg(
            pl.col("weight").sum().alias("sector_weight"),
            pl.col("parent_index_weight").sum().alias("sector_parent_weight"),
        )
        .with_columns(
            (pl.col("sector_weight") - pl.col("sector_parent_weight")).alias("active_weight")
        )
    )

    # Identify capped and uncapped sectors (single pass)
    capped_sectors = sector_weights.filter(
        (pl.col("active_weight") > ACTIVE_CAP) | (pl.col("active_weight") < ACTIVE_FLOOR)
    )
    uncapped_sectors = sector_weights.filter(
        (pl.col("active_weight") <= ACTIVE_CAP) & (pl.col("active_weight") >= ACTIVE_FLOOR)
    )

    if capped_sectors.height == 0:
        # Nothing to do; renormalize
        total = df["weight"].sum()
        out = df.with_columns((pl.col("weight") / total).alias("weight"))
        return out

    # Compute net redistribution needed
    net_excess = 0.0
    target_sector_weights: dict[str, float] = {}

    for row in capped_sectors.iter_rows(named=True):
        sector = row["gics_sector"]
        sw = row["sector_weight"]
        spw = row["sector_parent_weight"]
        active = row["active_weight"]
        if active > ACTIVE_CAP:
            target_sector_weights[sector] = spw + ACTIVE_CAP
            net_excess += sw - target_sector_weights[sector]
        else:
            target_sector_weights[sector] = spw + ACTIVE_FLOOR
            net_excess += sw - target_sector_weights[sector]

    uncapped_total = float(uncapped_sectors["sector_weight"].sum())

    # Redistribute net_excess to uncapped sectors proportionally
    uncapped_multiplier = (uncapped_total + net_excess) / uncapped_total if uncapped_total > 0 else 1.0

    # Build sector multiplier map
    sector_scale: dict[str, float] = {}
    for row in capped_sectors.iter_rows(named=True):
        sector = row["gics_sector"]
        sw = row["sector_weight"]
        sector_scale[sector] = target_sector_weights[sector] / sw if sw > 0 else 1.0

    for row in uncapped_sectors.iter_rows(named=True):
        sector = row["gics_sector"]
        sector_scale[sector] = uncapped_multiplier

    # Apply per-security scale
    sectors = df["gics_sector"].to_list()
    weights = df["weight"].to_list()
    new_weights = [w * sector_scale.get(s, 1.0) for w, s in zip(weights, sectors)]

    # Renormalize to 1
    total = sum(new_weights)
    new_weights = [w / total for w in new_weights]

    out = df.with_columns(pl.Series("weight", new_weights))
    assert abs(float(out["weight"].sum()) - 1.0) < 1e-9, (
        f"weights do not sum to 1 after sector cap/floor: {out['weight'].sum()}"
    )
    return out
