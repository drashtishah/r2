"""MSCI Selection Indexes methodology."""
from __future__ import annotations

import random
from pathlib import Path

import numpy as np
import polars as pl

from methbooks.mock_universe import build_base_universe
from methbooks.rules.eligibility.require_esg_rating_bb_or_above import require_esg_rating_bb_or_above
from methbooks.rules.eligibility.require_controversies_score_3_or_above import require_controversies_score_3_or_above
from methbooks.rules.eligibility.retain_existing_if_controversies_score_1_or_above import retain_existing_if_controversies_score_1_or_above
from methbooks.rules.eligibility.exclude_controversial_weapons_involvement import exclude_controversial_weapons_involvement
from methbooks.rules.eligibility.exclude_nuclear_weapons import exclude_nuclear_weapons
from methbooks.rules.eligibility.exclude_civilian_firearms_producer import exclude_civilian_firearms_producer
from methbooks.rules.eligibility.exclude_civilian_firearms_revenue_above_5pct import exclude_civilian_firearms_revenue_above_5pct
from methbooks.rules.eligibility.exclude_tobacco_producer import exclude_tobacco_producer
from methbooks.rules.eligibility.exclude_tobacco_revenue_above_5pct import exclude_tobacco_revenue_above_5pct
from methbooks.rules.eligibility.exclude_alcohol_above_10pct import exclude_alcohol_above_10pct
from methbooks.rules.eligibility.exclude_conventional_weapons_above_10pct import exclude_conventional_weapons_above_10pct
from methbooks.rules.eligibility.exclude_gambling_above_10pct import exclude_gambling_above_10pct
from methbooks.rules.eligibility.exclude_nuclear_power_above_10pct import exclude_nuclear_power_above_10pct
from methbooks.rules.eligibility.exclude_fossil_fuel_extraction_above_5pct import exclude_fossil_fuel_extraction_above_5pct
from methbooks.rules.eligibility.exclude_thermal_coal_power_above_5pct import exclude_thermal_coal_power_above_5pct
from methbooks.rules.eligibility.exclude_palm_oil_above_5pct import exclude_palm_oil_above_5pct
from methbooks.rules.eligibility.exclude_arctic_oil_gas_above_5pct import exclude_arctic_oil_gas_above_5pct
from methbooks.rules.ranking.rank_by_esg_rating_membership_industry_score_ff_mcap import rank_by_esg_rating_membership_industry_score_ff_mcap
from methbooks.rules.selection.select_by_sector_coverage_tiers import select_by_sector_coverage_tiers

SEED = 42
MIN_ROWS = 1
MAX_ROWS = 2000

_RATINGS = ["AAA", "AA", "A", "BBB", "BB", "B", "CCC"]
_RATING_WEIGHTS = [0.05, 0.10, 0.15, 0.25, 0.20, 0.15, 0.10]
_GICS_SECTORS = [
    "Energy", "Materials", "Industrials", "Consumer Discretionary", "Consumer Staples",
    "Health Care", "Financials", "Information Technology", "Communication Services",
    "Utilities", "Real Estate",
]
_ELIGIBLE_RATINGS = {"AAA", "AA", "A", "BBB", "BB"}


def build_mock_data() -> pl.DataFrame:
    df = build_base_universe(seed=SEED)
    rng = random.Random(SEED + 1)
    np_rng = np.random.default_rng(SEED + 2)
    n = df.height
    return df.with_columns(
        pl.Series("msci_esg_rating", rng.choices(_RATINGS, weights=_RATING_WEIGHTS, k=n)),
        pl.Series("msci_controversies_score", [rng.randint(0, 10) for _ in range(n)]),
        pl.Series("industry_adjusted_esg_score", np_rng.uniform(0.0, 10.0, n).tolist()),
        pl.Series("ff_market_cap", np_rng.lognormal(mean=20.0, sigma=2.0, size=n).tolist()),
        pl.Series("gics_sector", [rng.choice(_GICS_SECTORS) for _ in range(n)]),
        pl.Series("is_current_constituent", [rng.random() < 0.6 for _ in range(n)]),
        pl.Series("civilian_firearms_producer_flag", [rng.random() < 0.005 for _ in range(n)]),
        pl.Series("civilian_firearms_revenue_pct", np_rng.beta(0.5, 20.0, n).tolist()),
        pl.Series("tobacco_producer_flag", [rng.random() < 0.005 for _ in range(n)]),
        pl.Series("tobacco_revenue_pct", np_rng.beta(0.5, 20.0, n).tolist()),
        pl.Series("alcohol_revenue_pct", np_rng.beta(0.5, 15.0, n).tolist()),
        pl.Series("conventional_weapons_revenue_pct", np_rng.beta(0.5, 15.0, n).tolist()),
        pl.Series("gambling_revenue_pct", np_rng.beta(0.5, 15.0, n).tolist()),
        pl.Series("nuclear_power_revenue_pct", np_rng.beta(0.5, 15.0, n).tolist()),
        pl.Series("thermal_coal_mining_revenue_pct", np_rng.beta(0.5, 20.0, n).tolist()),
        pl.Series("unconventional_oil_gas_revenue_pct", np_rng.beta(0.5, 20.0, n).tolist()),
        pl.Series("thermal_coal_power_revenue_pct", np_rng.beta(0.5, 20.0, n).tolist()),
        pl.Series("palm_oil_revenue_pct", np_rng.beta(0.5, 20.0, n).tolist()),
        pl.Series("arctic_oil_gas_revenue_pct", np_rng.beta(0.5, 20.0, n).tolist()),
        pl.Series("sector_current_ff_mcap_coverage", np_rng.uniform(0.3, 0.7, n).tolist()),
        pl.Series("is_eligible_section_2_2", [rng.random() < 0.7 for _ in range(n)]),
        pl.Series("cbi_screen_flag", [rng.random() < 0.05 for _ in range(n)]),
        pl.Series("bisr_controversial_weapons_flag", [rng.random() < 0.02 for _ in range(n)]),
        pl.Series("nuclear_weapons_flag", [rng.random() < 0.01 for _ in range(n)]),
        pl.Series("rank", [0] * n, dtype=pl.UInt32),
    )


def get_data_dictionary() -> pl.DataFrame:
    csv = Path(__file__).with_name("selection_data_dictionary.csv")
    return pl.read_csv(csv)


def apply(df: pl.DataFrame) -> pl.DataFrame:
    out = require_esg_rating_bb_or_above(df)
    out = require_controversies_score_3_or_above(out)
    out = retain_existing_if_controversies_score_1_or_above(out)
    out = exclude_controversial_weapons_involvement(out)
    out = exclude_nuclear_weapons(out)
    out = exclude_civilian_firearms_producer(out)
    out = exclude_civilian_firearms_revenue_above_5pct(out)
    out = exclude_tobacco_producer(out)
    out = exclude_tobacco_revenue_above_5pct(out)
    out = exclude_alcohol_above_10pct(out)
    out = exclude_conventional_weapons_above_10pct(out)
    out = exclude_gambling_above_10pct(out)
    out = exclude_nuclear_power_above_10pct(out)
    out = exclude_fossil_fuel_extraction_above_5pct(out)
    out = exclude_thermal_coal_power_above_5pct(out)
    out = exclude_palm_oil_above_5pct(out)
    out = exclude_arctic_oil_gas_above_5pct(out)
    out = rank_by_esg_rating_membership_industry_score_ff_mcap(out)
    out = select_by_sector_coverage_tiers(out)

    assert MIN_ROWS <= out.height <= MAX_ROWS, (
        f"unexpected final row count: {out.height} not in [{MIN_ROWS}, {MAX_ROWS}]"
    )
    ineligible_rating = out.filter(~pl.col("msci_esg_rating").is_in(list(_ELIGIBLE_RATINGS)))
    assert ineligible_rating.height == 0, (
        f"selected securities with ESG rating below BB: {ineligible_rating.height} rows, "
        f"ratings={ineligible_rating['msci_esg_rating'].to_list()}"
    )
    non_constituent_low_score = out.filter(
        (~pl.col("is_current_constituent")) & (pl.col("msci_controversies_score") < 3)
    )
    assert non_constituent_low_score.height == 0, (
        f"selected non-constituent with controversies score < 3: {non_constituent_low_score.height} rows"
    )
    low_score_all = out.filter(pl.col("msci_controversies_score") < 1)
    assert low_score_all.height == 0, (
        f"selected security with controversies score < 1: {low_score_all.height} rows"
    )
    assert float(out["weight"].min()) >= 0.0, f"negative weight found: min={out['weight'].min()}"
    weight_sum = float(out["weight"].sum())
    assert weight_sum > 0.0, f"all weights zeroed: sum={weight_sum}"
    return out
