"""Pydantic methbook schema fed to the Agent SDK as JSON Schema structured output.

Single `Methbook` shape covers every MSCI methodology doc: index families
(Selection, Quality, Climate Action, ...) and foundation docs (Corporate
Events, GIMI, GICS, ...) alike. All slots except `identification` are
optional; silence in the source = absent slot. Variants are separate
methbooks with `identification.base_methbook` pointing to the parent
slug. Rules from another methbook are referenced via `reused_rules`
rather than copied.

`Methbook.model_json_schema()` goes into `ClaudeAgentOptions.output_format`.
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

RuleCategory = Literal[
    "eligibility",
    "ranking",
    "selection",
    "weighting",
    "maintenance",
    "event_handling",
    "scoring",
]

IndexType = Literal[
    "esg_selection",
    "esg_screen",
    "climate",
    "factor",
    "overlay",
    "reference",
    "other",
]


class Source(BaseModel):
    markdown: str
    section: str
    line: int
    page: int | None = None


class Asserts(BaseModel):
    technical: list[str] = Field(default_factory=list)
    business: list[str] = Field(default_factory=list)


class NewRule(BaseModel):
    category: RuleCategory
    name: str
    purpose: str
    datapoints: list[str] = Field(default_factory=list)
    thresholds: dict[str, Any] = Field(default_factory=dict)
    source: Source
    asserts: Asserts = Field(default_factory=Asserts)
    canonical_concept: str | None = None  # stable cross-methbook concept ID


class ReusedRule(BaseModel):
    methbook: str
    name: str
    justification: str


class MockDatapoint(BaseModel):
    name: str
    type: str
    distribution: str
    description: str
    source_section: str | None = None
    semantic_threshold: str | None = None
    taxonomy: str | None = None
    definition_notes: list[str] = Field(default_factory=list)
    canonical_concept: str | None = None  # stable cross-methbook concept ID


class Clarification(BaseModel):
    quote: str
    concern: str


class RegulatoryFramework(BaseModel):
    name: str
    reference: str | None = None
    key_requirements: list[str] = Field(default_factory=list)


class Identification(BaseModel):
    provider: str
    family: str
    slug: str
    version_date: str | None = None
    index_type: IndexType | None = None
    base_methbook: str | None = None
    regulatory_framework: RegulatoryFramework | None = None


class DocRef(BaseModel):
    """Typed pointer to an item in another methbook."""

    methbook: str
    item_type: str | None = None
    item: str | None = None
    section: str | None = None


class Universe(BaseModel):
    applicable_universe: DocRef | str | None = None
    reference_index: DocRef | str | None = None
    eligibility_scope: str | None = None


class DataRequirements(BaseModel):
    mandatory_products: list[DocRef | str] = Field(default_factory=list)
    preprocessing: list[dict[str, Any]] = Field(default_factory=list)
    mandatory_fields: list[dict[str, Any]] = Field(default_factory=list)


class Eligibility(BaseModel):
    rule_names: list[str] = Field(default_factory=list)


class Scoring(BaseModel):
    variables: list[dict[str, Any]] = Field(default_factory=list)
    normalization: str | None = None
    aggregation: str | None = None
    transformation: dict[str, Any] | None = None
    conditional_adjustments: list[dict[str, Any]] = Field(default_factory=list)


class Selection(BaseModel):
    method: str
    content: dict[str, Any] = Field(default_factory=dict)


class Weighting(BaseModel):
    method: str
    caps: list[dict[str, Any]] = Field(default_factory=list)
    floors: list[dict[str, Any]] = Field(default_factory=list)
    buffers: list[dict[str, Any]] = Field(default_factory=list)


class UnratedTreatment(BaseModel):
    policy: str
    details: dict[str, Any] = Field(default_factory=dict)


class Cadence(BaseModel):
    name: str
    frequency: str
    months: list[str] = Field(default_factory=list)
    data_cutoff: str | None = None
    announcement_lead: str | None = None


class ReviewCadence(BaseModel):
    schedules: list[Cadence] = Field(default_factory=list)
    base_date_reset: dict[str, Any] | None = None


class EventHandling(BaseModel):
    defers_to: DocRef | None = None
    rule_names: list[str] = Field(default_factory=list)
    intra_review_invariants: list[Clarification] = Field(default_factory=list)


class Methbook(BaseModel):
    identification: Identification
    universe: Universe | None = None
    data_requirements: DataRequirements | None = None
    eligibility: Eligibility | None = None
    scoring: Scoring | None = None
    selection: Selection | None = None
    weighting: Weighting | None = None
    unrated_treatment: UnratedTreatment | None = None
    review_cadence: ReviewCadence | None = None
    event_handling: EventHandling | None = None
    new_rules: list[NewRule] = Field(default_factory=list)
    reused_rules: list[ReusedRule] = Field(default_factory=list)
    composition_order: list[str] = Field(default_factory=list)
    final_asserts: list[str] = Field(default_factory=list)
    mock_datapoints: list[MockDatapoint] = Field(default_factory=list)
    external_references: list[DocRef] = Field(default_factory=list)
    clarifications_needed: list[Clarification] = Field(default_factory=list)
    scoped_out: list[Clarification] = Field(default_factory=list)
    document_metadata: dict[str, Any] = Field(default_factory=dict)
