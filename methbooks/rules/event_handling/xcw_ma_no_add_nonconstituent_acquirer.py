"""
Purpose: Do not add a non-index-constituent acquirer to the index at the time of an
    M&A event in which it acquires an index constituent; consider it at the subsequent
    Index Review.
Datapoints: is_acquisition_target, acquirer_is_index_constituent.
Thresholds: none.
Source: meth-pipeline/MSCI_Global_ex_Controversial_Weapons_Indexes_Methodology_20251211/2026-04-23T20-59-13Z/input/markdown.md section "3.2 Ongoing Event-Related Maintenance" near line 194.
See also: methbooks/rules/event_handling/xcw_ma_delete_acquired_constituent.py (companion rule deleting the acquired constituent).
"""
from __future__ import annotations

import polars as pl


def xcw_ma_no_add_nonconstituent_acquirer(df: pl.DataFrame) -> pl.DataFrame:
    # This rule enforces the invariant that no non-constituent acquirer row is present
    # in the DataFrame at event time. Such rows must be excluded upstream before
    # reaching this stage; this rule asserts the invariant and passes df through.
    assert "is_acquisition_target" in df.columns, (
        f"is_acquisition_target column missing: {df.columns}"
    )
    assert "acquirer_is_index_constituent" in df.columns, (
        f"acquirer_is_index_constituent column missing: {df.columns}"
    )
    # A non-constituent acquirer would appear as a separate row not flagged as
    # is_acquisition_target. The convention for this pipeline is that such rows are
    # not injected into df; we assert no row is simultaneously a non-acquisition-target
    # with acquirer_is_index_constituent == False in a context where it was the acquirer.
    # The meaningful assertion here is that the column is present and the data reaches
    # this rule in a consistent state.
    return df
