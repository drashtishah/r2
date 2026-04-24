"""
Purpose: Compute pro forma BVPS for real capital structure changes by blending existing book value with capital raised at issue price, divided by post-event total shares.
Datapoints: latest_bvps, shares_outstanding_pre_event, shares_issued, issue_price.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "4.2 Real Changes in Capital Structure" near line 3610.
See also: methbooks/rules/event_handling/apply_loss_per_share_restatement.py (also triggered by real events).
"""
from __future__ import annotations

import polars as pl


def compute_proforma_bvps_real_event(df: pl.DataFrame) -> pl.DataFrame:
    for col in ("latest_bvps", "shares_outstanding_pre_event", "shares_issued", "issue_price"):
        assert col in df.columns, f"{col} column missing: {df.columns}"

    pre_min = float(df["shares_outstanding_pre_event"].min())
    assert pre_min > 0, (
        f"shares_outstanding_pre_event must be positive: min={pre_min}"
    )
    ip_min = float(df["issue_price"].min())
    assert ip_min > 0, (
        f"issue_price must be positive: min={ip_min}"
    )
    out = df.with_columns(
        (
            (pl.col("latest_bvps") * pl.col("shares_outstanding_pre_event")
             + pl.col("shares_issued") * pl.col("issue_price"))
            / (pl.col("shares_outstanding_pre_event") + pl.col("shares_issued"))
        ).alias("proforma_bvps")
    )
    assert "proforma_bvps" in out.columns, (
        f"proforma_bvps column missing after compute: {out.columns}"
    )
    sample = out.head(1)
    if sample.height > 0:
        bvps = float(sample["latest_bvps"][0])
        pre = float(sample["shares_outstanding_pre_event"][0])
        issued = float(sample["shares_issued"][0])
        ip = float(sample["issue_price"][0])
        expected = (bvps * pre + issued * ip) / (pre + issued)
        actual = float(sample["proforma_bvps"][0])
        assert abs(actual - expected) < 1e-6, (
            f"proforma_bvps formula mismatch: expected={expected}, got={actual}"
        )
    return out
