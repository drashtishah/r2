"""MSCI Global Investable Market Indexes (GIMI) Methodology."""
from __future__ import annotations

import math
import random
from pathlib import Path

import polars as pl

from methbooks.mock_universe import build_base_universe
from methbooks.rules.eligibility.apply_china_suspension_screen import (
    apply_china_suspension_screen,
)
from methbooks.rules.eligibility.apply_dm_em_minimum_liquidity_requirement import (
    apply_dm_em_minimum_liquidity_requirement,
)
from methbooks.rules.eligibility.apply_equity_universe_minimum_float_adjusted_mcap_requirement import (
    apply_equity_universe_minimum_float_adjusted_mcap_requirement,
)
from methbooks.rules.eligibility.apply_equity_universe_minimum_size_requirement import (
    apply_equity_universe_minimum_size_requirement,
)
from methbooks.rules.eligibility.apply_financial_reporting_requirement import (
    apply_financial_reporting_requirement,
)
from methbooks.rules.eligibility.apply_fm_minimum_liquidity_requirement import (
    apply_fm_minimum_liquidity_requirement,
)
from methbooks.rules.eligibility.apply_global_minimum_fif_requirement import (
    apply_global_minimum_fif_requirement,
)
from methbooks.rules.eligibility.apply_micro_cap_maximum_size_requirement import (
    apply_micro_cap_maximum_size_requirement,
)
from methbooks.rules.eligibility.apply_micro_cap_minimum_liquidity_requirement import (
    apply_micro_cap_minimum_liquidity_requirement,
)
from methbooks.rules.eligibility.apply_micro_cap_minimum_size_requirement import (
    apply_micro_cap_minimum_size_requirement,
)
from methbooks.rules.eligibility.apply_minimum_foreign_room_requirement import (
    apply_minimum_foreign_room_requirement,
)
from methbooks.rules.eligibility.apply_minimum_length_of_trading_requirement import (
    apply_minimum_length_of_trading_requirement,
)
from methbooks.rules.eligibility.apply_non_constituent_high_price_liquidity_screen import (
    apply_non_constituent_high_price_liquidity_screen,
)
from methbooks.rules.eligibility.exclude_ineligible_alert_board_securities import (
    exclude_ineligible_alert_board_securities,
)
from methbooks.rules.eligibility.exclude_ineligible_preferred_shares import (
    exclude_ineligible_preferred_shares,
)
from methbooks.rules.eligibility.exclude_mutual_funds_etfs_equity_derivatives import (
    exclude_mutual_funds_etfs_equity_derivatives,
)
from methbooks.rules.maintenance.apply_foreign_room_adjustment_factor import (
    apply_foreign_room_adjustment_factor,
)
from methbooks.rules.maintenance.apply_liquidity_adjustment_factor_em_fm import (
    apply_liquidity_adjustment_factor_em_fm,
)
from methbooks.rules.maintenance.update_equity_universe_minimum_size_at_review import (
    update_equity_universe_minimum_size_at_review,
)
from methbooks.rules.maintenance.update_segment_number_of_companies_at_review import (
    update_segment_number_of_companies_at_review,
)
from methbooks.rules.ranking.rank_standard_index_candidates_by_stability_weighted_ff_mcap import (
    rank_standard_index_candidates_by_stability_weighted_ff_mcap,
)
from methbooks.rules.selection.apply_extreme_price_increase_standard_index_block import (
    apply_extreme_price_increase_standard_index_block,
)
from methbooks.rules.selection.apply_standard_index_minimum_constituents import (
    apply_standard_index_minimum_constituents,
)
from methbooks.rules.weighting.apply_initial_foreign_room_adjustment_factor import (
    apply_initial_foreign_room_adjustment_factor,
)

SEED = 42
MIN_ROWS = 1
MAX_ROWS = 2000

_SECURITY_TYPES = [
    "ordinary_share",
    "ordinary_share",
    "ordinary_share",
    "ordinary_share",
    "ordinary_share",
    "ordinary_share",
    "ordinary_share",
    "ordinary_share",
    "preferred_share",
    "reit",
    "trust",
    "adr",
    "etf",
    "mutual_fund",
]
_MARKET_TYPES = ["DM", "DM", "DM", "DM", "DM", "EM", "EM", "EM", "FM", "Standalone"]
_FM_CATEGORIES = ["very_low", "low", "low", "average", "average"]
_COUNTRY_CLASSIFICATIONS = ["US", "US", "US", "CA", "GB", "DE", "JP", "FR", "AU", "CN"]
_ADJ_FACTORS = [1.0, 1.0, 1.0, 0.5, 0.25, 0.0]
_GICS_SUB_INDUSTRIES = [str(i) for i in range(10101010, 10101010 + 163)]
_REVIEW_EFFECTIVE_DATE = "2026-05-30"
_ANNOUNCEMENT_DATE = "2026-05-14"
_MOCK_EQUITY_UNIVERSE_MIN_SIZE_USD = 507_000_000.0
_MOCK_STANDARD_CUTOFF_USD = 2_000_000_000.0
_MOCK_IMI_CUTOFF_USD = 200_000_000.0
_MOCK_MICRO_CAP_MIN_SIZE_USD = 50_000_000.0
_MOCK_GLOBAL_MIN_SIZE_REF_EM_USD = 300_000_000.0
_MOCK_INITIAL_SEGMENT_NUMBER = 0  # 0 skips segment number stability assert (no prior review)


def _lognormal(rng: random.Random, mean_log: float, sigma: float) -> float:
    return math.exp(rng.gauss(mean_log, sigma))


def build_mock_data() -> pl.DataFrame:
    df = build_base_universe(seed=SEED)
    rng = random.Random(SEED + 1)
    n = df.height

    def _col(name: str, vals: list) -> pl.Series:
        return pl.Series(name, vals)

    market_types = [rng.choice(_MARKET_TYPES) for _ in range(n)]
    is_existing = [rng.random() < 0.7 for _ in range(n)]
    security_types = [rng.choice(_SECURITY_TYPES) for _ in range(n)]
    preferred = [s == "preferred_share" for s in security_types]
    full_mcap = [_lognormal(rng, math.log(1e9), 2.0) for _ in range(n)]
    ff_mcap = [_lognormal(rng, math.log(5e8), 2.0) for _ in range(n)]
    fif_vals = [rng.random() for _ in range(n)]
    atvr_12m = [_lognormal(rng, math.log(30), 1.5) for _ in range(n)]
    atvr_3m = [_lognormal(rng, math.log(25), 1.5) for _ in range(n)]
    freq_3m = [min(100.0, max(0.0, rng.betavariate(8, 2) * 100)) for _ in range(n)]
    freq_12m = [min(100.0, max(0.0, rng.betavariate(8, 2) * 100)) for _ in range(n)]
    has_fol = [rng.random() < 0.3 for _ in range(n)]
    foreign_room = [rng.uniform(15.0, 100.0) for _ in range(n)]
    stock_price = [_lognormal(rng, math.log(50), 1.5) for _ in range(n)]
    months_trading = [_lognormal(rng, math.log(48), 1.0) for _ in range(n)]
    is_large_ipo = [rng.random() < 0.05 for _ in range(n)]
    alert_board = [rng.random() < 0.02 for _ in range(n)]
    fixed_div = [rng.random() < 0.05 if preferred[i] else False for i in range(n)]
    par_val_liq = [rng.random() < 0.04 if preferred[i] else False for i in range(n)]
    fm_cat = [rng.choice(_FM_CATEGORIES) for _ in range(n)]
    adj_factor = [rng.choice(_ADJ_FACTORS) for _ in range(n)]
    light_reb = [rng.random() < 0.05 for _ in range(n)]
    china_susp = [rng.random() < 0.02 for _ in range(n)]
    china_days = [min(250, max(0, int(rng.expovariate(0.05)))) for _ in range(n)]
    country_class = [rng.choice(_COUNTRY_CLASSIFICATIONS) for _ in range(n)]
    has_10k = [False if country_class[i] == "US" and rng.random() < 0.02 else True for i in range(n)]
    gics_sub = [rng.choice(_GICS_SUB_INDUSTRIES) for _ in range(n)]
    is_standard_constituent = [rng.random() < 0.6 for _ in range(n)]
    is_std_addition_candidate = [
        rng.random() < 0.3 if not is_standard_constituent[i] else False
        for i in range(n)
    ]
    # Excess returns: normally distributed around 0 with most below thresholds.
    def _excess_return(rng: random.Random) -> float:
        return rng.gauss(0, 20)
    # Country weight, global min size reference, LAF, consecutive reviews.
    country_weight = [rng.uniform(0, 15) for _ in range(n)]
    laf = [1.0 for _ in range(n)]
    consec_reviews = [int(rng.expovariate(0.5)) for _ in range(n)]
    # Maintenance datapoints.
    cum_coverage = [rng.uniform(98.5, 100.0) for _ in range(n)]
    prior_ref_rank = [1 if i == 0 else 0 for i in range(n)]
    prior_micro_rank = [1 if i == 1 else 0 for i in range(n)]
    global_min_lower = [_MOCK_EQUITY_UNIVERSE_MIN_SIZE_USD * 0.5 for _ in range(n)]
    global_min_upper = [_MOCK_EQUITY_UNIVERSE_MIN_SIZE_USD * 1.15 for _ in range(n)]
    initial_seg = [_MOCK_INITIAL_SEGMENT_NUMBER for _ in range(n)]
    free_float = [rng.uniform(5, 100) for _ in range(n)]
    current_dif = [rng.uniform(5, 100) for _ in range(n)]
    updated_ff = [rng.uniform(5, 100) for _ in range(n)]
    is_addition = [rng.random() < 0.05 for _ in range(n)]
    fol_change = [rng.random() < 0.02 for _ in range(n)]
    current_nos = [int(_lognormal(rng, math.log(1e8), 1.0)) for _ in range(n)]
    updated_nos = [int(v * rng.uniform(0.98, 1.02)) for v in current_nos]
    last_reduction_date = ["2025-01-01" for _ in range(n)]
    # Market size segment cutoff (for buffer zone calc).
    mss_cutoff = [_MOCK_STANDARD_CUTOFF_USD for _ in range(n)]

    return df.with_columns(
        _col("security_type", security_types),
        _col("preferred_share_flag", preferred),
        _col("fixed_dividend_flag", fixed_div),
        _col("par_value_liquidation_flag", par_val_liq),
        _col("alert_board_flag", alert_board),
        _col("market_type", market_types),
        _col("is_existing_imi_constituent", is_existing),
        _col("full_market_cap_usd", full_mcap),
        _col("ff_adjusted_mcap_usd", ff_mcap),
        _col("fif", fif_vals),
        _col("atvr_12m_pct", atvr_12m),
        _col("atvr_3m_pct", atvr_3m),
        _col("freq_of_trading_3m_pct", freq_3m),
        _col("freq_of_trading_12m_pct", freq_12m),
        _col("has_fol_flag", has_fol),
        _col("foreign_room_pct", foreign_room),
        _col("stock_price_usd", stock_price),
        _col("months_since_first_trading", months_trading),
        _col("is_large_ipo_flag", is_large_ipo),
        _col("country_classification", country_class),
        _col("has_filed_10k_10q", has_10k),
        _col("fm_liquidity_category", fm_cat),
        _col("current_adjustment_factor", [str(a) for a in adj_factor]),
        _col("light_rebalancing_flag", light_reb),
        _col("china_suspension_flag", china_susp),
        _col("china_suspension_consecutive_business_days", china_days),
        _col("gics_sub_industry", gics_sub),
        _col("is_standard_index_constituent", is_standard_constituent),
        _col("is_standard_index_addition_candidate", is_std_addition_candidate),
        _col("equity_universe_min_size_usd", [_MOCK_EQUITY_UNIVERSE_MIN_SIZE_USD] * n),
        _col("standard_index_interim_cutoff_usd", [_MOCK_STANDARD_CUTOFF_USD] * n),
        _col("imi_market_size_segment_cutoff_usd", [_MOCK_IMI_CUTOFF_USD] * n),
        _col("micro_cap_min_size_usd", [_MOCK_MICRO_CAP_MIN_SIZE_USD] * n),
        _col("full_security_market_cap_usd", ff_mcap),
        _col("atvr_3m_available_flag", [True] * n),
        _col("cumulative_ff_mcap_coverage_pct", cum_coverage),
        _col("prior_reference_rank", prior_ref_rank),
        _col("prior_micro_cap_reference_rank", prior_micro_rank),
        _col("global_min_size_range_lower_usd", global_min_lower),
        _col("global_min_size_range_upper_usd", global_min_upper),
        _col("initial_segment_number_of_companies", initial_seg),
        _col("free_float_pct", free_float),
        _col("current_dif_pct", current_dif),
        _col("updated_free_float_pct", updated_ff),
        _col("is_addition_flag", is_addition),
        _col("fol_change_flag", fol_change),
        _col("current_nos", current_nos),
        _col("updated_nos", updated_nos),
        _col("last_weight_reduction_date", last_reduction_date),
        _col("country_weight_pct", country_weight),
        _col("global_min_size_reference_em_usd", [_MOCK_GLOBAL_MIN_SIZE_REF_EM_USD] * n),
        _col("liquidity_adjustment_factor", laf),
        _col("consecutive_reviews_meeting_new_constituent_liquidity", consec_reviews),
        _col("market_size_segment_cutoff_usd", mss_cutoff),
        *[
            _col(f"excess_return_{d}d_pct", [_excess_return(rng) for _ in range(n)])
            for d in [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 90, 120, 150, 180, 250]
        ],
    )


def get_data_dictionary() -> pl.DataFrame:
    csv = Path(__file__).with_name("gimi_data_dictionary.csv")
    return pl.read_csv(csv)


def apply(df: pl.DataFrame) -> pl.DataFrame:
    out = exclude_mutual_funds_etfs_equity_derivatives(df)
    out = exclude_ineligible_preferred_shares(out)
    out = exclude_ineligible_alert_board_securities(out)
    out = apply_equity_universe_minimum_size_requirement(out)
    out = apply_equity_universe_minimum_float_adjusted_mcap_requirement(out)
    out = apply_dm_em_minimum_liquidity_requirement(out)
    out = apply_non_constituent_high_price_liquidity_screen(out)
    out = apply_china_suspension_screen(out)
    out = apply_fm_minimum_liquidity_requirement(out)
    out = apply_global_minimum_fif_requirement(out)
    out = apply_minimum_foreign_room_requirement(out)
    out = apply_initial_foreign_room_adjustment_factor(out)
    out = apply_minimum_length_of_trading_requirement(out)
    out = apply_financial_reporting_requirement(out)
    out = apply_extreme_price_increase_standard_index_block(out)
    out = update_equity_universe_minimum_size_at_review(out)
    out = update_segment_number_of_companies_at_review(out)
    out = rank_standard_index_candidates_by_stability_weighted_ff_mcap(out)
    out = apply_standard_index_minimum_constituents(out)
    out = apply_foreign_room_adjustment_factor(out)
    out = apply_liquidity_adjustment_factor_em_fm(out)
    out = apply_micro_cap_maximum_size_requirement(out)
    out = apply_micro_cap_minimum_size_requirement(out)
    out = apply_micro_cap_minimum_liquidity_requirement(out)

    assert MIN_ROWS <= out.height <= MAX_ROWS, (
        f"unexpected row count after pipeline: {out.height} not in [{MIN_ROWS}, {MAX_ROWS}]"
    )
    weight_sum = float(out["weight"].sum())
    assert weight_sum > 0, f"all weights zeroed: sum={weight_sum}"
    assert not out["alert_board_flag"].any(), (
        f"security with alert_board_flag=True present: count={out['alert_board_flag'].sum()}"
    )
    bad_fol = out.filter(
        pl.col("has_fol_flag") & (pl.col("foreign_room_pct") < 15.0)
    )
    assert bad_fol.height == 0, (
        f"security with foreign_room_pct < 15 and has_fol_flag=True present:"
        f" {bad_fol.height} rows"
    )
    bad_price = out.filter(
        (~pl.col("is_existing_imi_constituent"))
        & (pl.col("stock_price_usd") > 10_000.0)
    )
    assert bad_price.height == 0, (
        f"non-constituent with stock_price_usd > 10000 present: {bad_price.height} rows"
    )
    bad_micro_max = out.filter(
        pl.col("full_market_cap_usd") > 1.5 * pl.col("imi_market_size_segment_cutoff_usd")
    )
    assert bad_micro_max.height == 0, (
        f"Micro Cap security with full_market_cap_usd > 1.5x imi cutoff present:"
        f" {bad_micro_max.height} rows"
    )
    return out
