"""MSCI Quality Indexes Methodology."""
from __future__ import annotations

import math
import random
from pathlib import Path

import polars as pl

from methbooks.mock_universe import build_base_universe
from methbooks.rules.eligibility.exclude_missing_roe_or_de import exclude_missing_roe_or_de
from methbooks.rules.scoring.winsorize_quality_variables import winsorize_quality_variables
from methbooks.rules.scoring.compute_quality_z_scores import compute_quality_z_scores
from methbooks.rules.scoring.compute_composite_quality_z_score import compute_composite_quality_z_score
from methbooks.rules.scoring.compute_quality_score_from_z import compute_quality_score_from_z
from methbooks.rules.ranking.rank_by_quality_score_then_parent_weight import rank_by_quality_score_then_parent_weight
from methbooks.rules.selection.apply_quality_buffer_rule import apply_quality_buffer_rule
from methbooks.rules.weighting.apply_quality_score_tilt_weight import apply_quality_score_tilt_weight
from methbooks.rules.weighting.cap_issuer_weight_broad_quality_index import cap_issuer_weight_broad_quality_index
from methbooks.rules.weighting.cap_issuer_weight_narrow_quality_index import cap_issuer_weight_narrow_quality_index
from methbooks.rules.weighting.compute_inclusion_factor import compute_inclusion_factor

SEED = 42
MIN_ROWS = 1
MAX_ROWS = 2000
FIXED_NUMBER_SECURITIES = 300  # default mock value
RAW_FIXED_NUMBER = 280  # pre-rounding value for mock


def build_mock_data() -> pl.DataFrame:
    df = build_base_universe(seed=SEED)
    rng = random.Random(SEED + 1)
    n = df.height

    def _col(name: str, vals: list) -> pl.Series:
        return pl.Series(name, vals)

    # roe: normal(0.15, 0.20), ~5% null
    roe_raw = [rng.gauss(0.15, 0.20) for _ in range(n)]
    roe = [None if rng.random() < 0.05 else v for v in roe_raw]

    # debt_to_equity: lognormal(0.5, 1.0), ~5% null
    de_raw = [math.exp(rng.gauss(0.5, 1.0)) for _ in range(n)]
    de = [None if rng.random() < 0.05 else v for v in de_raw]

    # earnings_variability: lognormal(0.3, 0.5), ~15% null
    ev_raw = [math.exp(rng.gauss(0.3, 0.5)) for _ in range(n)]
    ev = [None if rng.random() < 0.15 else v for v in ev_raw]

    # parent_index_weight: exponential distribution summing to 1
    raw_w = [rng.expovariate(1.0) for _ in range(n)]
    total_w = sum(raw_w)
    parent_index_weight = [w / total_w for w in raw_w]

    # issuer_id: ~80% of issuers have one security; some share
    issuer_ids = []
    shared_pool = [f"ISS{i:04d}" for i in range(int(n * 0.1))]
    for i in range(n):
        if rng.random() < 0.2:
            issuer_ids.append(rng.choice(shared_pool))
        else:
            issuer_ids.append(f"ISS{i+1000:04d}")

    # is_broad_parent_index: True for mock so the broad cap rule runs
    is_broad = [True] * n
    is_narrow = [False] * n

    # max_parent_issuer_weight: uniform(0.05, 0.35)
    max_parent_issuer_weight = [rng.uniform(0.05, 0.35)] * n

    # is_current_constituent: bernoulli(0.85)
    is_constituent = [rng.random() < 0.85 for _ in range(n)]

    # fixed_number_securities: constant FIXED_NUMBER_SECURITIES
    fixed_num = [FIXED_NUMBER_SECURITIES] * n

    # raw_fixed_number: constant RAW_FIXED_NUMBER
    raw_fixed = [RAW_FIXED_NUMBER] * n

    # characteristics_changed_flag: ~5% True
    char_changed = [rng.random() < 0.05 for _ in range(n)]

    # acquirer_is_index_constituent: None for most; False for ~2% (acquired by non-constituent)
    acquirer_col = []
    for _ in range(n):
        r = rng.random()
        if r < 0.02:
            acquirer_col.append(False)
        else:
            acquirer_col.append(None)

    # Derived columns added by apply(); listed here so the data dictionary check finds them.
    _derived = [
        "roe_winsorized", "debt_to_equity_winsorized", "earnings_variability_winsorized",
        "roe_z_score", "debt_to_equity_z_score", "earnings_variability_z_score",
        "composite_quality_z_score", "quality_score", "quality_rank", "inclusion_factor",
    ]

    return df.with_columns(
        _col("roe", roe),
        _col("debt_to_equity", de),
        _col("earnings_variability", ev),
        _col("parent_index_weight", parent_index_weight),
        _col("issuer_id", issuer_ids),
        _col("is_broad_parent_index", is_broad),
        _col("is_narrow_parent_index", is_narrow),
        _col("max_parent_issuer_weight", max_parent_issuer_weight),
        _col("is_current_constituent", is_constituent),
        _col("fixed_number_securities", fixed_num),
        _col("raw_fixed_number", raw_fixed),
        _col("characteristics_changed_flag", char_changed),
        _col("acquirer_is_index_constituent", acquirer_col),
    )


def get_data_dictionary() -> pl.DataFrame:
    csv = Path(__file__).with_name("quality_data_dictionary.csv")
    return pl.read_csv(csv)


def apply(df: pl.DataFrame) -> pl.DataFrame:
    # composition_order
    out = exclude_missing_roe_or_de(df)
    out = winsorize_quality_variables(out)
    out = compute_quality_z_scores(out)
    out = compute_composite_quality_z_score(out)
    out = compute_quality_score_from_z(out)
    out = rank_by_quality_score_then_parent_weight(out)
    out = apply_quality_buffer_rule(out)
    out = apply_quality_score_tilt_weight(out)
    out = cap_issuer_weight_broad_quality_index(out)
    out = cap_issuer_weight_narrow_quality_index(out)
    out = compute_inclusion_factor(out)

    # final_asserts
    assert abs(float(out["weight"].sum()) - 1.0) < 1e-9, (
        f"weights do not sum to 1.0: {out['weight'].sum()}"
    )
    # no individual security weight exceeds 0.05 in broad parent index variants
    if out["is_broad_parent_index"][0]:
        max_w = float(out["weight"].max())
        assert max_w <= 0.05 + 1e-9, f"security weight exceeds 0.05 in broad index: {max_w}"
    # all quality_score positive
    min_qs = float(out["quality_score"].min())
    assert min_qs > 0, f"non-positive quality_score in output: {min_qs}"
    # no null roe or debt_to_equity
    assert out["roe"].null_count() == 0, f"null roe in output: {out['roe'].null_count()} rows"
    assert out["debt_to_equity"].null_count() == 0, (
        f"null debt_to_equity in output: {out['debt_to_equity'].null_count()} rows"
    )
    # inclusion_factor = weight / parent_index_weight
    bad_if = out.filter(
        (pl.col("parent_index_weight") > 0)
        & ((pl.col("inclusion_factor") - pl.col("weight") / pl.col("parent_index_weight")).abs() > 1e-9)
    )
    assert bad_if.height == 0, (
        f"inclusion_factor != weight/parent_index_weight for {bad_if.height} rows"
    )
    return out
