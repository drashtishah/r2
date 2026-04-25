"""
Purpose: Reduce turnover at semi-annual review by giving existing constituents within
    buffer zone priority retention.
Datapoints: quality_rank, is_current_constituent, fixed_number_securities.
Thresholds: BUFFER_PCT = 0.2.
Source: methbooks/data/markdown/msci_quality_indexes_methodology_20250520.md section
    "3.1.1 Buffer Rules" near line 283.
"""
from __future__ import annotations

import polars as pl

BUFFER_PCT = 0.2


def apply_quality_buffer_rule(df: pl.DataFrame) -> pl.DataFrame:
    assert "quality_rank" in df.columns, f"quality_rank missing: {df.columns}"
    assert "is_current_constituent" in df.columns, (
        f"is_current_constituent missing: {df.columns}"
    )
    assert "fixed_number_securities" in df.columns, (
        f"fixed_number_securities missing: {df.columns}"
    )

    fixed_n = int(df["fixed_number_securities"][0])
    lower_bound = int(fixed_n * (1 - BUFFER_PCT))
    upper_bound = int(fixed_n * (1 + BUFFER_PCT))

    # Step 1: unconditional inclusion for rank <= lower_bound.
    unconditional = df.filter(pl.col("quality_rank") <= lower_bound)
    selected_ids = set(unconditional["quality_rank"].to_list())

    # Step 2: buffer zone (lower_bound < rank <= upper_bound).
    buffer_zone = df.filter(
        (pl.col("quality_rank") > lower_bound) & (pl.col("quality_rank") <= upper_bound)
    )
    # Existing constituents first, then non-constituents.
    buffer_sorted = buffer_zone.sort(
        ["is_current_constituent", "quality_rank"], descending=[True, False]
    )
    buffer_ranks = buffer_sorted["quality_rank"].to_list()
    buffer_constituent = buffer_sorted["is_current_constituent"].to_list()

    needed = fixed_n - len(selected_ids)
    for i, rank in enumerate(buffer_ranks):
        if needed <= 0:
            break
        selected_ids.add(rank)
        needed -= 1

    # Step 3: if still short, fill from rank > upper_bound ascending.
    if needed > 0:
        beyond = df.filter(pl.col("quality_rank") > upper_bound).sort("quality_rank")
        for rank in beyond["quality_rank"].to_list():
            if needed <= 0:
                break
            selected_ids.add(rank)
            needed -= 1

    out = df.filter(pl.col("quality_rank").is_in(list(selected_ids)))

    # Invariants.
    uncond_ranks = set(unconditional["quality_rank"].to_list())
    missing_uncond = uncond_ranks - set(out["quality_rank"].to_list())
    assert len(missing_uncond) == 0, (
        f"unconditional inclusions missing from output: {missing_uncond}"
    )

    # Existing constituents in buffer zone must be included before non-constituents.
    buffer_existing = buffer_zone.filter(pl.col("is_current_constituent") == True)  # noqa: E712
    missing_existing = set(buffer_existing["quality_rank"].to_list()) - set(out["quality_rank"].to_list())
    # They should be missing only if we exceeded fixed_n even for existing constituents.
    output_count = out.height
    assert len(missing_existing) == 0 or output_count >= fixed_n, (
        f"existing constituents in buffer zone excluded before reaching fixed_n: "
        f"missing={missing_existing}, output={output_count}, fixed_n={fixed_n}"
    )

    return out
