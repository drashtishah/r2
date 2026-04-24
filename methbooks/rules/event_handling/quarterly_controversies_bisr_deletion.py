"""
Purpose: Apply controversies and exclusion screens at quarterly review and renormalize
    weights; removes red flag controversies, tobacco, thermal coal mining, oil sands,
    and nuclear weapons in that order.
Datapoints: msci_controversies_score, tobacco_producer_flag, tobacco_revenue_pct,
    thermal_coal_mining_revenue_pct, oil_sands_extraction_revenue_pct, has_oil_sands_reserves,
    nuclear_weapons_flag, weight.
Thresholds: see individual eligibility rules.
Source: methbooks/data/markdown/MSCI_Climate_Action_Indexes_Methodology_20251211.md section "Event Handling" near line 1.
See also: methbooks/rules/eligibility/exclude_red_flag_controversies.py (rule reused here).
"""
from __future__ import annotations

import polars as pl

from methbooks.rules.eligibility.exclude_red_flag_controversies import exclude_red_flag_controversies
from methbooks.rules.eligibility.exclude_tobacco import exclude_tobacco
from methbooks.rules.eligibility.exclude_thermal_coal_mining import exclude_thermal_coal_mining
from methbooks.rules.eligibility.exclude_oil_sands import exclude_oil_sands
from methbooks.rules.eligibility.exclude_nuclear_weapons import exclude_nuclear_weapons
from methbooks.rules.weighting.renormalize_parent_weights import renormalize_parent_weights


def quarterly_controversies_bisr_deletion(df: pl.DataFrame) -> pl.DataFrame:
    assert isinstance(df, pl.DataFrame), f"expected pl.DataFrame, got {type(df)}"
    out = exclude_red_flag_controversies(df)
    out = exclude_tobacco(out)
    out = exclude_thermal_coal_mining(out)
    out = exclude_oil_sands(out)
    out = exclude_nuclear_weapons(out)
    out = renormalize_parent_weights(out)
    return out
