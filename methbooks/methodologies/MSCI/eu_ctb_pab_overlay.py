"""MSCI EU CTB/PAB Overlay Indexes Methodology."""
from __future__ import annotations

import random
from pathlib import Path

import polars as pl

from methbooks.mock_universe import build_base_universe
from methbooks.rules.eligibility.exclude_controversial_weapons_involvement import (
    exclude_controversial_weapons_involvement,
)
from methbooks.rules.eligibility.exclude_red_flag_controversies import (
    exclude_red_flag_controversies,
)
from methbooks.rules.eligibility.exclude_red_orange_flag_environmental_controversies import (
    exclude_red_orange_flag_environmental_controversies,
)
from methbooks.rules.eligibility.exclude_tobacco import exclude_tobacco
from methbooks.rules.eligibility.exclude_thermal_coal_mining import exclude_thermal_coal_mining
from methbooks.rules.eligibility.exclude_thermal_coal_distribution import (
    exclude_thermal_coal_distribution,
)
from methbooks.rules.eligibility.exclude_oil_gas_activities_pab import (
    exclude_oil_gas_activities_pab,
)
from methbooks.rules.eligibility.exclude_fossil_fuel_power_generation_pab import (
    exclude_fossil_fuel_power_generation_pab,
)
from methbooks.rules.eligibility.exclude_unrated_msci_solutions import (
    exclude_unrated_msci_solutions,
)
from methbooks.rules.weighting.optimize_index_weights import optimize_index_weights
from methbooks.rules.event_handling.neutralize_intermediate_parent_reviews import (
    neutralize_intermediate_parent_reviews,
)
from methbooks.rules.event_handling.block_non_spinoff_parent_additions import (
    block_non_spinoff_parent_additions,
)
from methbooks.rules.event_handling.reflect_parent_deletions import reflect_parent_deletions

SEED = 42
RNG_SEED = 44
MIN_ROWS = 1
MAX_ROWS = 2000


def build_mock_data() -> pl.DataFrame:
    df = build_base_universe(seed=SEED)
    rng = random.Random(RNG_SEED)
    n = df.height
    return df.with_columns(
        pl.Series("msci_controversy_score", [rng.uniform(0.01, 10) for _ in range(n)]),
        pl.Series("msci_environmental_controversy_score", [rng.uniform(1.01, 10) for _ in range(n)]),
        pl.Series("bisr_controversial_weapons_flag", [False] * n),
        pl.Series("bisr_tobacco_manufacturing_flag", [False] * n),
        pl.Series("bisr_thermal_coal_mining_revenue_pct", [rng.uniform(0, 0.9) for _ in range(n)]),
        pl.Series("bisr_thermal_coal_distribution_flag", [False] * n),
        pl.Series("bisr_oil_gas_revenue_pct", [rng.uniform(0, 9.9) for _ in range(n)]),
        pl.Series("bisr_fossil_fuel_power_generation_revenue_pct", [rng.uniform(0, 49.9) for _ in range(n)]),
        pl.Series("ghg_intensity_scope123", [rng.lognormvariate(5, 1.5) for _ in range(n)]),
        pl.Series("parent_index_weight", [rng.uniform(0.0001, 0.05) for _ in range(n)]),
        pl.Series("reference_index_weight", [rng.uniform(0.0001, 0.05) for _ in range(n)]),
        pl.Series("high_climate_impact_sector", [rng.random() < 0.3 for _ in range(n)]),
        pl.Series("is_rated_msci_controversies", [True] * n),
        pl.Series("is_rated_msci_climate", [True] * n),
        pl.Series("is_rated_msci_bisr", [True] * n),
    )


def get_data_dictionary() -> pl.DataFrame:
    csv = Path(__file__).with_name("eu_ctb_pab_overlay_data_dictionary.csv")
    return pl.read_csv(csv)


def apply(df: pl.DataFrame) -> pl.DataFrame:
    out = exclude_controversial_weapons_involvement(df)
    out = exclude_red_flag_controversies(out)
    out = exclude_red_orange_flag_environmental_controversies(out)
    out = exclude_tobacco(out)
    out = exclude_thermal_coal_mining(out)
    out = exclude_thermal_coal_distribution(out)
    out = exclude_oil_gas_activities_pab(out)
    out = exclude_fossil_fuel_power_generation_pab(out)
    out = exclude_unrated_msci_solutions(out)
    out = optimize_index_weights(out)
    assert abs(float(out["weight"].sum()) - 1.0) < 1e-6, f"Final weights do not sum to 1: sum={out['weight'].sum()}"
    assert (out["weight"] >= 0).all(), f"Negative weight in final index: min={out['weight'].min()}"
    assert (out["msci_controversy_score"] > 0).all(), f"Red Flag controversy constituent in final index: min score={out['msci_controversy_score'].min()}"
    assert (out["msci_environmental_controversy_score"] > 1).all(), f"Red/Orange Flag environmental controversy constituent: min score={out['msci_environmental_controversy_score'].min()}"
    assert not out["bisr_controversial_weapons_flag"].any(), f"Controversial weapons constituent in final index"
    assert not out["bisr_tobacco_manufacturing_flag"].any(), f"Tobacco manufacturer in final index"
    assert out["is_rated_msci_controversies"].all(), f"Unrated Controversies company in final index"
    assert out["is_rated_msci_climate"].all(), f"Unrated Climate company in final index"
    assert out["is_rated_msci_bisr"].all(), f"Unrated BISR company in final index"
    return out
