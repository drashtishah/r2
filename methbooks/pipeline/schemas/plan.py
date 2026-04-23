"""Pydantic plan schema fed to the Agent SDK as JSON Schema structured output.

Mirrors the plan JSON shape exchanged between planner and critique agents.
PlanModel.model_json_schema() goes into ClaudeAgentOptions.output_format.
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class Source(BaseModel):
    markdown: str
    section: str
    line: int


class Asserts(BaseModel):
    technical: list[str]
    business: list[str]


class NewRule(BaseModel):
    category: str
    name: str
    purpose: str
    datapoints: list[str]
    thresholds: dict[str, Any]
    source: Source
    asserts: Asserts


class ReusedRule(BaseModel):
    path: str
    justification: str


class MockDatapoint(BaseModel):
    name: str
    type: str
    distribution: str
    description: str


class Methodology(BaseModel):
    provider: str
    slug: str
    composition_order: list[str]
    final_asserts: list[str]


class Clarification(BaseModel):
    quote: str
    concern: str


class PlanModel(BaseModel):
    new_rules: list[NewRule]
    reused_rules: list[ReusedRule]
    mock_datapoints: list[MockDatapoint]
    methodology: Methodology
    clarifications_needed: list[Clarification]
