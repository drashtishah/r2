"""
Purpose: Annualize DPS as trailing 12m declared regular cash distributions; on distribution frequency change, use latest 6m only if it yields a higher annualized amount than prior; for US and Canada, multiply latest payment by distribution frequency.
Datapoints: cash_distributions_last_12m, cash_distributions_last_6m, prior_annualized_dps, country_of_classification, payment_frequency.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "1.2.4 Dividends Per Share (DPS)" near line 821.
See also: methbooks/rules/event_handling/suppress_small_event_dps_adjustment.py (DPS adjustment suppressed for small PAF events).
"""
from __future__ import annotations

import polars as pl

_USCA_COUNTRIES = {"US", "CA"}


def compute_dps_annualized(df: pl.DataFrame) -> pl.DataFrame:
    for col in ("cash_distributions_last_12m", "cash_distributions_last_6m",
                "prior_annualized_dps", "country_of_classification", "payment_frequency"):
        assert col in df.columns, f"{col} column missing: {df.columns}"

    have_distributions = df.filter(
        pl.col("cash_distributions_last_12m").is_not_null()
        | pl.col("cash_distributions_last_6m").is_not_null()
    ).height
    assert have_distributions > 0, (
        f"no rows have cash_distributions_last_12m or cash_distributions_last_6m: "
        f"have_distributions={have_distributions}"
    )

    out = df.with_columns(
        pl.when(pl.col("country_of_classification").is_in(list(_USCA_COUNTRIES)))
        .then(pl.col("cash_distributions_last_6m") * pl.col("payment_frequency"))
        .when(
            pl.col("cash_distributions_last_6m").is_not_null()
            & pl.col("prior_annualized_dps").is_not_null()
            & (pl.col("cash_distributions_last_6m") * 2 > pl.col("prior_annualized_dps"))
        )
        .then(pl.col("cash_distributions_last_6m") * 2)
        .when(
            pl.col("cash_distributions_last_6m").is_not_null()
            & pl.col("prior_annualized_dps").is_not_null()
        )
        .then(pl.col("prior_annualized_dps"))
        .otherwise(pl.col("cash_distributions_last_12m"))
        .alias("annualized_dps")
    )
    assert "annualized_dps" in out.columns, (
        f"annualized_dps column missing after compute: {out.columns}"
    )
    usca = out.filter(pl.col("country_of_classification").is_in(list(_USCA_COUNTRIES)))
    if usca.height > 0:
        bad = usca.filter(
            (pl.col("annualized_dps") - pl.col("cash_distributions_last_6m") * pl.col("payment_frequency")).abs() > 1e-9
        ).height
        assert bad == 0, (
            f"annualized_dps != latest_distribution * frequency for {bad} US/CA rows"
        )
    return out
