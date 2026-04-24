"""
Purpose: Use estimated pro forma combined fundamental data for an acquirer when it acquires a loss-making Standard Index constituent whose trailing 12-month losses exceeded USD 1 billion at month-end before the ex-date.
Datapoints: acquired_company_trailing_12m_losses_usd, acquired_company_was_standard_constituent.
Thresholds: LOSS_THRESHOLD_USD_BN = 1.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "4.4 Changes in Business Structure" near line 3710.
See also: methbooks/rules/event_handling/apply_proforma_fundamental_business_structure_change.py (broader pro forma trigger).
"""
from __future__ import annotations

import polars as pl

LOSS_THRESHOLD_USD_BN = 1
_LOSS_THRESHOLD_USD = LOSS_THRESHOLD_USD_BN * 1_000_000_000


def apply_proforma_for_loss_making_acquisition(df: pl.DataFrame) -> pl.DataFrame:
    for col in ("acquired_company_trailing_12m_losses_usd", "acquired_company_was_standard_constituent"):
        assert col in df.columns, f"{col} column missing: {df.columns}"

    out = df.with_columns(
        pl.when(
            pl.col("acquired_company_was_standard_constituent")
            & (pl.col("acquired_company_trailing_12m_losses_usd") > _LOSS_THRESHOLD_USD)
        )
        .then(pl.lit(True))
        .otherwise(pl.lit(False))
        .alias("proforma_loss_acquisition_flag")
    )
    assert "proforma_loss_acquisition_flag" in out.columns, (
        f"proforma_loss_acquisition_flag column missing after transform: {out.columns}"
    )
    flagged = out.filter(pl.col("proforma_loss_acquisition_flag"))
    if flagged.height > 0:
        not_standard = flagged.filter(~pl.col("acquired_company_was_standard_constituent")).height
        assert not_standard == 0, (
            f"proforma_loss_acquisition_flag set for {not_standard} rows where acquired company "
            "was not a Standard constituent"
        )
        below_threshold = flagged.filter(
            pl.col("acquired_company_trailing_12m_losses_usd") <= _LOSS_THRESHOLD_USD
        ).height
        assert below_threshold == 0, (
            f"proforma_loss_acquisition_flag set for {below_threshold} rows where trailing losses "
            f"<= USD {LOSS_THRESHOLD_USD_BN}bn"
        )
    return out
