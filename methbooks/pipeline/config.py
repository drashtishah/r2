"""Central pipeline config.

Changing which model drives which role is a one-line edit here.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AgentConfig:
    model: str
    effort: str         # "low" | "medium" | "high" | "xhigh" | "max"
    max_tokens: int


PLANNER           = AgentConfig("claude-sonnet-4-6", "medium", 8000)
CRITIQUE          = AgentConfig("claude-opus-4-7",   "high",   8000)
IMPLEMENTER       = AgentConfig("claude-sonnet-4-6", "low",    8000)
SEMANTIC_VERIFIER = AgentConfig("claude-opus-4-7",   "high",   4000)

ALLOWED_PATH_PREFIXES = (
    "methbooks/rules/",
    "methbooks/methodologies/",
    "methbooks/mock_universe.py",
)
