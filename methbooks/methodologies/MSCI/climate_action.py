"""MSCI Climate Action Indexes Methodology."""
from __future__ import annotations

import random
import string
from pathlib import Path

import polars as pl

from methbooks.mock_universe import build_base_universe
from methbooks.rules.eligibility.exclude_red_flag_controversies import exclude_red_flag_controversies
from methbooks.rules.eligibility.exclude_controversial_weapons_involvement import (
    exclude_controversial_weapons_involvement,
)
from methbooks.rules.eligibility.exclude_tobacco import exclude_tobacco
from methbooks.rules.eligibility.exclude_thermal_coal_mining import exclude_thermal_coal_mining
from methbooks.rules.eligibility.exclude_oil_sands import exclude_oil_sands
from methbooks.rules.eligibility.exclude_nuclear_weapons import exclude_nuclear_weapons
from methbooks.rules.eligibility.exclude_high_emitters_without_sbti import (
    exclude_high_emitters_without_sbti,
)
from methbooks.rules.eligibility.exclude_bottom_quartile_climate_risk_management import (
    exclude_bottom_quartile_climate_risk_management,
)
from methbooks.rules.scoring.score_intensity import score_intensity
from methbooks.rules.scoring.score_green_business_revenue import score_green_business_revenue
from methbooks.rules.scoring.score_climate_risk_management import score_climate_risk_management
from methbooks.rules.scoring.score_emission_track_record import score_emission_track_record
from methbooks.rules.scoring.assess_credible_track_record import assess_credible_track_record
from methbooks.rules.scoring.compute_security_level_assessment import (
    compute_security_level_assessment,
)
from methbooks.rules.ranking.rank_by_assessment_then_mcap import rank_by_assessment_then_mcap
from methbooks.rules.selection.select_top_50pct_with_buffer import select_top_50pct_with_buffer
from methbooks.rules.weighting.renormalize_parent_weights import renormalize_parent_weights
from methbooks.rules.weighting.cap_issuer_weight_5pct import cap_issuer_weight_5pct
from methbooks.rules.weighting.apply_active_sector_weight_cap_floor import (
    apply_active_sector_weight_cap_floor,
)
from methbooks.rules.event_handling.spinoff_immediate_addition import spinoff_immediate_addition
from methbooks.rules.event_handling.ma_acquirer_proportionate_weight import (
    ma_acquirer_proportionate_weight,
)
from methbooks.rules.event_handling.quarterly_controversies_bisr_deletion import (
    quarterly_controversies_bisr_deletion,
)

SEED = 42
RNG_SEED = 43
MIN_ROWS = 1
MAX_ROWS = 2000

_GICS_SECTORS = [
    "Energy",
    "Materials",
    "Industrials",
    "Consumer Discretionary",
    "Consumer Staples",
    "Health Care",
    "Financials",
    "Information Technology",
    "Communication Services",
    "Utilities",
    "Real Estate",
]


def _random_id(rng: random.Random, k: int = 6) -> str:
    return "".join(rng.choices(string.ascii_uppercase + string.digits, k=k))


def build_mock_data() -> pl.DataFrame:
    df = build_base_universe(seed=SEED)
    rng = random.Random(RNG_SEED)
    n = df.height

    # Generate issuer_ids: ~N/10 unique issuers, assigned randomly
    n_issuers = max(1, n // 10)
    issuer_pool = [_random_id(rng, 8) for _ in range(n_issuers)]

    return df.with_columns(
        pl.Series(
            "msci_controversies_score",
            [0.0 if rng.random() < 0.8 else rng.uniform(0.01, 10) for _ in range(n)],
        ),
        pl.Series("tobacco_producer_flag", [rng.random() < 0.05 for _ in range(n)]),
        pl.Series("tobacco_revenue_pct", [rng.uniform(0, 30) for _ in range(n)]),
        pl.Series("thermal_coal_mining_revenue_pct", [rng.uniform(0, 20) for _ in range(n)]),
        pl.Series("oil_sands_extraction_revenue_pct", [rng.uniform(0, 20) for _ in range(n)]),
        pl.Series("has_oil_sands_reserves", [rng.random() < 0.1 for _ in range(n)]),
        pl.Series("nuclear_weapons_flag", [rng.random() < 0.02 for _ in range(n)]),
        pl.Series("bisr_controversial_weapons_flag", [rng.random() < 0.02 for _ in range(n)]),
        pl.Series("emission_intensity", [rng.lognormvariate(1.5, 1.2) for _ in range(n)]),
        pl.Series("has_emission_intensity_data", [rng.random() < 0.9 for _ in range(n)]),
        pl.Series(
            "total_potential_carbon_emissions_ex_met_coal",
            [rng.lognormvariate(4.0, 2.0) for _ in range(n)],
        ),
        pl.Series(
            "fossil_fuel_reserves_energy_application", [rng.random() < 0.15 for _ in range(n)]
        ),
        pl.Series("approved_sbti", [rng.random() < 0.2 for _ in range(n)]),
        pl.Series("crm_weighted_avg_score", [rng.uniform(0, 10) for _ in range(n)]),
        pl.Series("carbon_emissions_mgmt_score", [rng.uniform(0, 10) for _ in range(n)]),
        pl.Series("carbon_emissions_key_issue_weight", [rng.uniform(0, 0.5) for _ in range(n)]),
        pl.Series("green_business_revenue_pct", [rng.uniform(0, 50) for _ in range(n)]),
        pl.Series(
            "emission_track_record",
            [
                rng.gauss(-0.02, 0.05) if rng.random() < 0.25 else None
                for _ in range(n)
            ],
        ),
        pl.Series("reports_scope_1_2_emissions", [rng.random() < 0.7 for _ in range(n)]),
        pl.Series("has_emission_reduction_target", [rng.random() < 0.4 for _ in range(n)]),
        pl.Series("gics_sector", [rng.choice(_GICS_SECTORS) for _ in range(n)]),
        pl.Series("ff_mcap", [rng.lognormvariate(23.0, 2.0) for _ in range(n)]),
        pl.Series("is_current_constituent", [rng.random() < 0.6 for _ in range(n)]),
        pl.Series("parent_index_weight", [rng.uniform(0.0001, 0.02) for _ in range(n)]),
        pl.Series("issuer_id", [rng.choice(issuer_pool) for _ in range(n)]),
        pl.Series("is_new_parent_addition", [rng.random() < 0.005 for _ in range(n)]),
        pl.Series("is_parent_deletion_event", [rng.random() < 0.005 for _ in range(n)]),
        pl.Series("is_acquisition_target", [rng.random() < 0.002 for _ in range(n)]),
        pl.Series("acquirer_is_index_constituent", [rng.random() < 0.5 for _ in range(n)]),
        pl.Series("has_characteristics_change", [rng.random() < 0.01 for _ in range(n)]),
        # Computed columns mocked for unit testing of downstream rules in isolation
        pl.Series("intensity_score", [rng.randint(1, 4) for _ in range(n)]),
        pl.Series("green_business_score", [rng.randint(1, 4) for _ in range(n)]),
        pl.Series("climate_risk_management_score", [rng.randint(1, 4) for _ in range(n)]),
        pl.Series(
            "track_record_score",
            [rng.choice([1, 2, 3, 4, None]) for _ in range(n)],
            dtype=pl.Int32,
        ),
        pl.Series(
            "credible_track_record",
            [True if rng.random() < 0.15 else False for _ in range(n)],
        ),
        pl.Series("security_level_assessment", [rng.randint(1, 4) for _ in range(n)]),
        pl.Series("sector_rank", [rng.randint(1, 50) for _ in range(n)]),
        pl.Series("selected", [rng.random() < 0.5 for _ in range(n)]),
    )


def get_data_dictionary() -> pl.DataFrame:
    csv = Path(__file__).with_name("climate_action_data_dictionary.csv")
    return pl.read_csv(csv)


def apply(df: pl.DataFrame) -> pl.DataFrame:
    # Eligibility
    out = exclude_red_flag_controversies(df)
    out = exclude_controversial_weapons_involvement(out)
    out = exclude_tobacco(out)
    out = exclude_thermal_coal_mining(out)
    out = exclude_oil_sands(out)
    out = exclude_nuclear_weapons(out)
    out = exclude_high_emitters_without_sbti(out)
    out = exclude_bottom_quartile_climate_risk_management(out)

    # Scoring
    out = score_intensity(out)
    out = score_green_business_revenue(out)
    out = score_climate_risk_management(out)
    out = score_emission_track_record(out)
    out = assess_credible_track_record(out)
    out = compute_security_level_assessment(out)

    # Ranking
    out = rank_by_assessment_then_mcap(out)

    # Selection
    out = select_top_50pct_with_buffer(out)
    out = out.filter(pl.col("selected"))

    # Weighting
    out = renormalize_parent_weights(out)
    out = cap_issuer_weight_5pct(out)
    out = apply_active_sector_weight_cap_floor(out)

    # Final asserts
    assert MIN_ROWS <= out.height <= MAX_ROWS, (
        f"unexpected row count: {out.height} not in [{MIN_ROWS}, {MAX_ROWS}]"
    )
    assert out["msci_controversies_score"].max() == 0 or out.height == 0, (
        f"red flag controversy rows in output: {out['msci_controversies_score'].max()}"
    )
    assert out["nuclear_weapons_flag"].sum() == 0, (
        f"nuclear weapons rows in output: {out['nuclear_weapons_flag'].sum()}"
    )
    weight_sum = float(out["weight"].sum())
    assert abs(weight_sum - 1.0) < 1e-6, (
        f"constituent weights do not sum to 1.0: sum={weight_sum}"
    )
    assert float(out["weight"].min()) >= 0.0, (
        f"negative constituent weight found: min={out['weight'].min()}"
    )
    assert out["selected"].all(), "non-selected rows in output"
    return out
