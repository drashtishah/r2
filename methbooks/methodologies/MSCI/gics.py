"""MSCI GICS (Global Industry Classification Standard) methodology module."""
from __future__ import annotations

import random
from datetime import date
from pathlib import Path

import polars as pl

from methbooks.mock_universe import build_base_universe
from methbooks.rules.eligibility.gics_equity_issuer_eligibility import gics_equity_issuer_eligibility
from methbooks.rules.eligibility.classify_tracking_stock_by_underlying import classify_tracking_stock_by_underlying
from methbooks.rules.eligibility.classify_by_primary_revenue import classify_by_primary_revenue
from methbooks.rules.eligibility.classify_by_majority_revenue_and_earnings import classify_by_majority_revenue_and_earnings
from methbooks.rules.eligibility.classify_by_largest_combined_contribution import classify_by_largest_combined_contribution
from methbooks.rules.eligibility.classify_by_qualitative_research import classify_by_qualitative_research
from methbooks.rules.eligibility.classify_diversified_conglomerate import classify_diversified_conglomerate
from methbooks.rules.maintenance.restrict_reclassification_to_sustained_60pct import restrict_reclassification_to_sustained_60pct
from methbooks.rules.maintenance.minimize_reclassification_on_temporary_fluctuations import minimize_reclassification_on_temporary_fluctuations
from methbooks.rules.maintenance.propagate_gics_from_company_to_equity_securities import propagate_gics_from_company_to_equity_securities

SEED = 42
RNG_SEED = 44
MIN_ROWS = 1
MAX_ROWS = 2000

_CSV = Path(__file__).parent / "gics_data_dictionary.csv"

composition_order = [
    gics_equity_issuer_eligibility,
    classify_by_primary_revenue,
    classify_by_majority_revenue_and_earnings,
    classify_by_largest_combined_contribution,
    classify_by_qualitative_research,
    restrict_reclassification_to_sustained_60pct,
    minimize_reclassification_on_temporary_fluctuations,
    propagate_gics_from_company_to_equity_securities,
    classify_diversified_conglomerate,
    classify_tracking_stock_by_underlying,
]

GICS_SUB_INDUSTRY_CODES = [
    10101010, 10101020, 10102010, 10102020, 10102030, 10102040, 10102050,
    15101010, 15101020, 15101030, 15101040, 15101050, 15102010, 15103010,
    15103020, 15104010, 15104020, 15104025, 15104030, 15104040, 15104045,
    15104050, 15105010, 15105020,
    20101010, 20102010, 20103010, 20104010, 20104020, 20105010, 20106010,
    20106015, 20106020, 20107010, 20201010, 20201020, 20201030, 20201040,
    20201050, 20201060, 20201070, 20201080, 20202010, 20202020,
    25101010, 25101020, 25102010, 25102020, 25111010, 25111020, 25201010,
    25201020, 25201030, 25201040, 25201050, 25202010, 25202020,
    30101010, 30101020, 30101030, 30201010, 30201020, 30201030, 30202010,
    30202020, 30202030, 30301010, 30302010,
    35101010, 35101020, 35102010, 35102015, 35102020, 35102030, 35103010,
    35201010, 35202010,
    40101010, 40101015, 40102010, 40201010, 40201020, 40201030, 40201040,
    40202010, 40203010, 40203020, 40203030, 40204010, 40301010, 40301020,
    40401010, 40401020, 40402010, 40402020, 40402030, 40402035, 40402040,
    40402050, 40403010, 40403020, 40403030,
    45101010, 45101020, 45102010, 45102020, 45103010, 45103020, 45201010,
    45201020, 45202010, 45202020, 45202030, 45203010, 45203015, 45203020,
    45203030, 45204010,
    50101010, 50101020, 50102010, 50201010, 50201020, 50203010,
    55101010, 55102010, 55103010, 55104010, 55105010,
    60101010, 60101020, 60101030, 60101040, 60101050, 60101060, 60102010,
    60102020, 60102030, 60102040,
]


def build_mock_data() -> pl.DataFrame:
    """Return a 2000-row DataFrame with all GICS datapoints."""
    df = build_base_universe(seed=SEED)
    rng = random.Random(RNG_SEED)
    n = df.height
    n_companies = max(1, n // 5)
    company_pool = [f"CO{i:06d}" for i in range(n_companies)]

    entity_types = ["corporate"] * 80 + ["supranational"] * 3 + ["municipal"] * 3 + ["sovereign"] * 5 + ["shell_company"] * 3 + ["mutual_fund"] * 3 + ["etf"] * 3
    fi_entity_types = ["corporate"] * 90 + ["supranational"] * 4 + ["municipal"] * 3 + ["sovereign"] * 3

    return df.with_columns(
        pl.Series("entity_type", [rng.choice(entity_types) for _ in range(n)]),
        pl.Series("is_equity_issuer", [rng.random() < 0.80 for _ in range(n)]),
        pl.Series("is_subsidiary_with_separate_financials", [rng.random() < 0.10 for _ in range(n)]),
        pl.Series("primary_activity_revenue_pct", [rng.uniform(0.3, 1.0) for _ in range(n)]),
        pl.Series("primary_activity_earnings_pct", [rng.uniform(0.2, 1.0) for _ in range(n)]),
        pl.Series("num_distinct_active_sectors", [max(1, min(11, int(rng.gauss(1.5, 1.0)))) for _ in range(n)]),
        pl.Series("qualitative_classification_flag", [rng.random() < 0.05 for _ in range(n)]),
        pl.Series("is_tracking_stock", [rng.random() < 0.01 for _ in range(n)]),
        pl.Series("underlying_business_gics_sub_industry_code", [rng.choice(GICS_SUB_INDUSTRY_CODES) for _ in range(n)]),
        pl.Series("gics_sub_industry_code", [rng.choice(GICS_SUB_INDUSTRY_CODES) for _ in range(n)]),
        pl.Series("current_gics_sub_industry_code", [rng.choice(GICS_SUB_INDUSTRY_CODES) for _ in range(n)]),
        pl.Series("has_significant_restructuring", [rng.random() < 0.05 for _ in range(n)]),
        pl.Series("has_new_annual_report", [rng.random() < 0.90 for _ in range(n)]),
        pl.Series("revenue_trend_flag", [rng.random() < 0.15 for _ in range(n)]),
        pl.Series("company_id", [rng.choice(company_pool) for _ in range(n)]),
        pl.Series("fi_entity_type", [rng.choice(fi_entity_types) for _ in range(n)]),
        pl.Series("has_issued_corporate_bonds", [rng.random() < 0.60 for _ in range(n)]),
        pl.Series("has_issued_equity", [rng.random() < 0.75 for _ in range(n)]),
        pl.Series("parent_gics_sub_industry_code", [rng.choice(GICS_SUB_INDUSTRY_CODES) if rng.random() < 0.85 else None for _ in range(n)]),
        pl.Series("is_spv_or_captive_finance", [rng.random() < 0.10 for _ in range(n)]),
        pl.Series("related_entity_gics_sub_industry_code", [rng.choice(GICS_SUB_INDUSTRY_CODES) for _ in range(n)]),
        pl.Series("fi_last_review_date", [
            date(2025, rng.randint(5, 12), rng.randint(1, 28)) for _ in range(n)
        ]),
        pl.Series("structure_review_change_flag", [rng.random() < 0.05 for _ in range(n)]),
        pl.Series("client_review_requested", [rng.random() < 0.02 for _ in range(n)]),
    )


def get_data_dictionary() -> pl.DataFrame:
    """Return the GICS data dictionary as a polars DataFrame."""
    return pl.read_csv(_CSV)


def apply(df: pl.DataFrame) -> pl.DataFrame:
    """Apply the GICS composition pipeline and run final asserts."""
    out = df
    for rule in composition_order:
        out = rule(out)

    assert out["gics_sub_industry_code"].null_count() == 0, (
        f"null_count={out['gics_sub_industry_code'].null_count()}"
    )
    assert ((out["gics_sub_industry_code"] >= 10000000) & (out["gics_sub_industry_code"] <= 99999999)).all(), (
        f"first offending code="
        f"{out.filter((pl.col('gics_sub_industry_code') < 10000000) | (pl.col('gics_sub_industry_code') > 99999999))['gics_sub_industry_code'][0] if out.filter((pl.col('gics_sub_industry_code') < 10000000) | (pl.col('gics_sub_industry_code') > 99999999)).height > 0 else None}"
    )
    assert not out["entity_type"].is_in(["supranational", "municipal", "sovereign", "shell_company", "mutual_fund", "etf"]).any(), (
        f"offending entity_type="
        f"{out.filter(pl.col('entity_type').is_in(['supranational', 'municipal', 'sovereign', 'shell_company', 'mutual_fund', 'etf']))['entity_type'][0] if out.filter(pl.col('entity_type').is_in(['supranational', 'municipal', 'sovereign', 'shell_company', 'mutual_fund', 'etf'])).height > 0 else None}"
    )
    standard_rows = out.filter(
        ~pl.col("is_tracking_stock")
        & ~(
            (pl.col("num_distinct_active_sectors") >= 3)
            & (pl.col("primary_activity_revenue_pct") < 0.5)
        )
    )
    n_unique_per_company = standard_rows.group_by("company_id").agg(
        pl.col("gics_sub_industry_code").n_unique().alias("n_unique")
    )
    offenders = n_unique_per_company.filter(pl.col("n_unique") > 1)
    assert offenders.height == 0, (
        f"first offending company_id={offenders['company_id'][0] if offenders.height > 0 else None}"
    )
    tracking = out.filter(pl.col("is_tracking_stock"))
    assert (tracking["gics_sub_industry_code"] == tracking["underlying_business_gics_sub_industry_code"]).all(), (
        f"first offending security_id="
        f"{tracking.filter(pl.col('gics_sub_industry_code') != pl.col('underlying_business_gics_sub_industry_code'))['security_id'][0] if tracking.filter(pl.col('gics_sub_industry_code') != pl.col('underlying_business_gics_sub_industry_code')).height > 0 else None}"
    )
    conglomerate_rows = out.filter(
        (pl.col("num_distinct_active_sectors") >= 3) & (pl.col("primary_activity_revenue_pct") < 0.5)
    )
    assert conglomerate_rows["gics_sub_industry_code"].is_in([20105010, 40201030]).all(), (
        f"offending code="
        f"{conglomerate_rows.filter(~pl.col('gics_sub_industry_code').is_in([20105010, 40201030]))['gics_sub_industry_code'][0] if conglomerate_rows.filter(~pl.col('gics_sub_industry_code').is_in([20105010, 40201030])).height > 0 else None}"
    )
    return out
