"""MSCI Global ex Controversial Weapons Indexes Methodology."""
from __future__ import annotations

import random
from pathlib import Path

import polars as pl

from methbooks.mock_universe import build_base_universe
from methbooks.rules.eligibility.exclude_controversial_weapons_involvement import (
    exclude_controversial_weapons_involvement,
)
from methbooks.rules.eligibility.exclude_unrated_capital_goods_materials import (
    exclude_unrated_capital_goods_materials,
)

SEED = 42
MIN_ROWS = 1
MAX_ROWS = 2000

_GICS_INDUSTRY_GROUPS = [
    "Capital Goods",
    "Commercial & Professional Services",
    "Transportation",
    "Automobiles & Components",
    "Consumer Durables & Apparel",
    "Consumer Services",
    "Retailing",
    "Food & Staples Retailing",
    "Food Beverage & Tobacco",
    "Household & Personal Products",
    "Health Care Equipment & Services",
    "Pharmaceuticals Biotechnology & Life Sciences",
    "Banks",
    "Diversified Financials",
    "Insurance",
    "Real Estate",
    "Software & Services",
    "Technology Hardware & Equipment",
    "Semiconductors & Semiconductor Equipment",
    "Telecommunication Services",
    "Media & Entertainment",
    "Utilities",
    "Energy",
    "Materials",
]


def build_mock_data() -> pl.DataFrame:
    df = build_base_universe(seed=SEED)
    rng = random.Random(SEED + 1)
    n = df.height
    return df.with_columns(
        pl.Series(
            "bisr_controversial_weapons_flag",
            [rng.random() < 0.02 for _ in range(n)],
        ),
        pl.Series(
            "bisr_assessed",
            [rng.random() < 0.85 for _ in range(n)],
        ),
        pl.Series(
            "gics_industry_group",
            [rng.choice(_GICS_INDUSTRY_GROUPS) for _ in range(n)],
        ),
        pl.Series(
            "is_new_parent_addition",
            [rng.random() < 0.005 for _ in range(n)],
        ),
        pl.Series(
            "is_spinoff_from_constituent",
            [rng.random() < 0.003 for _ in range(n)],
        ),
        pl.Series(
            "is_acquisition_target",
            [rng.random() < 0.002 for _ in range(n)],
        ),
        pl.Series(
            "acquirer_is_index_constituent",
            [rng.random() < 0.5 for _ in range(n)],
        ),
        pl.Series(
            "has_characteristics_change",
            [rng.random() < 0.01 for _ in range(n)],
        ),
        pl.Series(
            "is_parent_deletion_event",
            [rng.random() < 0.005 for _ in range(n)],
        ),
    )


def get_data_dictionary() -> pl.DataFrame:
    csv = Path(__file__).with_name("global_ex_controversial_weapons_data_dictionary.csv")
    return pl.read_csv(csv)


def apply(df: pl.DataFrame) -> pl.DataFrame:
    out = exclude_controversial_weapons_involvement(df)
    out = exclude_unrated_capital_goods_materials(out)

    # Renormalise weights to sum to 1.0 after eligibility filtering.
    total = out["weight"].sum()
    out = out.with_columns((pl.col("weight") / total).alias("weight"))

    assert MIN_ROWS <= out.height <= MAX_ROWS, (
        f"unexpected row count: {out.height} not in [{MIN_ROWS}, {MAX_ROWS}]"
    )
    assert out["bisr_controversial_weapons_flag"].sum() == 0, (
        f"constituent with bisr_controversial_weapons_flag == True present: "
        f"{out['bisr_controversial_weapons_flag'].sum()} rows"
    )
    unrated_cg_mat = out.filter(
        (pl.col("bisr_assessed") == False)  # noqa: E712
        & pl.col("gics_industry_group").is_in(["Capital Goods", "Materials"])
    )
    assert unrated_cg_mat.height == 0, (
        f"constituent with bisr_assessed == False and gics_industry_group in "
        f"['Capital Goods', 'Materials'] present: {unrated_cg_mat.height} rows"
    )
    weight_sum = float(out["weight"].sum())
    assert abs(weight_sum - 1.0) < 1e-6, (
        f"constituent weights do not sum to 1.0: sum={weight_sum}"
    )
    assert float(out["weight"].min()) >= 0.0, (
        f"negative constituent weight found: min={out['weight'].min()}"
    )
    return out
