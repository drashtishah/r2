"""Agent wrappers for the methbook pipeline.

One thin wrapper per role over claude_agent_sdk.query(). Each function
builds ClaudeAgentOptions from its AgentConfig constant, composes a
single-block system prompt from CONVENTIONS + AGENT_GUIDELINES (single
block side-steps SDK cache_control bug #626), calls query() with
role-appropriate tools, logs tool_call / tool_result via SDK hooks and
agent_start / agent_end via log_event, and writes structured output for
planner and critique to disk.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from claude_agent_sdk import (
    ClaudeAgentOptions,
    HookContext,
    HookMatcher,
    ResultMessage,
    create_sdk_mcp_server,
    query,
)

from methbooks.pipeline.config import (
    AgentConfig,
    CRITIQUE,
    IMPLEMENTER,
    PLANNER,
    SEMANTIC_VERIFIER,
)
from methbooks.pipeline.logging import log_event
from methbooks.pipeline.schemas.plan import PlanModel
from methbooks.pipeline.tools import list_existing_rules

PROMPT_DIR = Path(__file__).parent / "prompts"
METHBOOKS_DIR = Path("methbooks")


def _system_prompt() -> str:
    conventions = (METHBOOKS_DIR / "CONVENTIONS.md").read_text()
    guidelines = (METHBOOKS_DIR / "AGENT_GUIDELINES.md").read_text()
    return conventions + "\n\n---\n\n" + guidelines


def _load_role_prompt(role: str, slug: str, ts: str) -> str:
    template = (PROMPT_DIR / f"{role}.md").read_text()
    return template.replace("<slug>", slug).replace("<ts>", ts)


def _hooks_for(role: str) -> dict[Any, list[HookMatcher]]:
    async def pre(
        hook_input: Any, tool_use_id: str | None, context: HookContext,
    ) -> dict[str, Any]:
        log_event(role, "tool_call", tool=str(hook_input.get("tool_name", "?")))
        return {}

    async def post(
        hook_input: Any, tool_use_id: str | None, context: HookContext,
    ) -> dict[str, Any]:
        log_event(role, "tool_result", tool=str(hook_input.get("tool_name", "?")))
        return {}

    return {
        "PreToolUse": [HookMatcher(hooks=[pre])],  # type: ignore[list-item]
        "PostToolUse": [HookMatcher(hooks=[post])],  # type: ignore[list-item]
    }


def _make_options(
    config: AgentConfig,
    role: str,
    allowed_tools: list[str],
    output_format: dict[str, Any] | None,
    include_mcp: bool,
) -> ClaudeAgentOptions:
    mcp_servers: dict[str, Any] = {}
    if include_mcp:
        mcp_servers["methbooks"] = create_sdk_mcp_server(
            name="methbooks", tools=[list_existing_rules],
        )
    return ClaudeAgentOptions(
        model=config.model,
        effort=config.effort,  # type: ignore[arg-type]
        system_prompt=_system_prompt(),
        allowed_tools=allowed_tools,
        mcp_servers=mcp_servers,
        output_format=output_format,
        hooks=_hooks_for(role),
    )


async def _drain(prompt: str, options: ClaudeAgentOptions, role: str) -> ResultMessage:
    final: ResultMessage | None = None
    async for msg in query(prompt=prompt, options=options):
        if isinstance(msg, ResultMessage):
            final = msg
    if final is None:
        log_event(role, "error", note="no ResultMessage")
        raise RuntimeError(f"{role}: no ResultMessage from query()")
    if final.is_error:
        log_event(role, "error", note=f"is_error stop_reason={final.stop_reason}")
        raise RuntimeError(f"{role}: agent returned error stop_reason={final.stop_reason}")
    return final


async def run_planner(run_dir: Path, slug: str, ts: str) -> PlanModel:
    prompt = _load_role_prompt("planner", slug, ts)
    options = _make_options(
        PLANNER, "planner",
        allowed_tools=["Read", "Glob", "Grep", "mcp__methbooks__list_existing_rules"],
        output_format={"type": "json_schema", "schema": PlanModel.model_json_schema()},
        include_mcp=True,
    )
    log_event("planner", "agent_start", model=PLANNER.model)
    t0 = time.time()
    result = await _drain(prompt, options, "planner")
    duration = int((time.time() - t0) * 1000)
    structured = result.structured_output
    if structured is None:
        log_event("planner", "error", note="no structured_output")
        raise RuntimeError("planner: structured_output missing")
    plan = PlanModel.model_validate(structured)
    (run_dir / "plan_v1.json").write_text(json.dumps(structured, indent=2))
    log_event(
        "planner", "agent_end",
        duration_ms=duration, stop_reason=result.stop_reason,
    )
    return plan


async def run_critique(run_dir: Path, slug: str, ts: str) -> PlanModel:
    prompt = _load_role_prompt("critique", slug, ts)
    options = _make_options(
        CRITIQUE, "critique",
        allowed_tools=["Read", "Glob", "Grep", "mcp__methbooks__list_existing_rules"],
        output_format={"type": "json_schema", "schema": PlanModel.model_json_schema()},
        include_mcp=True,
    )
    log_event("critique", "agent_start", model=CRITIQUE.model)
    t0 = time.time()
    result = await _drain(prompt, options, "critique")
    duration = int((time.time() - t0) * 1000)
    structured = result.structured_output
    if structured is None:
        log_event("critique", "error", note="no structured_output")
        raise RuntimeError("critique: structured_output missing")
    plan = PlanModel.model_validate(structured)
    (run_dir / "plan_v2.json").write_text(json.dumps(structured, indent=2))
    log_event(
        "critique", "agent_end",
        duration_ms=duration, stop_reason=result.stop_reason,
    )
    return plan


async def run_implementer(run_dir: Path, slug: str, ts: str) -> None:
    prompt = _load_role_prompt("implementer", slug, ts)
    options = _make_options(
        IMPLEMENTER, "implementer",
        allowed_tools=[
            "Read", "Glob", "Grep", "Write", "Edit", "Bash",
            "mcp__methbooks__list_existing_rules",
        ],
        output_format=None,
        include_mcp=True,
    )
    log_event("implementer", "agent_start", model=IMPLEMENTER.model)
    t0 = time.time()
    result = await _drain(prompt, options, "implementer")
    duration = int((time.time() - t0) * 1000)
    log_event(
        "implementer", "agent_end",
        duration_ms=duration, stop_reason=result.stop_reason,
    )


async def run_semantic_verifier(
    run_dir: Path, slug: str, ts: str,
) -> dict[str, Any]:
    prompt = _load_role_prompt("semantic_verifier", slug, ts)
    options = _make_options(
        SEMANTIC_VERIFIER, "semantic_verifier",
        allowed_tools=["Read", "Glob", "Grep"],
        output_format=None,
        include_mcp=False,
    )
    log_event("semantic_verifier", "agent_start", model=SEMANTIC_VERIFIER.model)
    t0 = time.time()
    result = await _drain(prompt, options, "semantic_verifier")
    duration = int((time.time() - t0) * 1000)
    text = result.result or ""
    try:
        report: dict[str, Any] = json.loads(text)
    except json.JSONDecodeError:
        report = {"overall": "fail", "items": [], "raw": text}
    log_event(
        "semantic_verifier", "agent_end",
        duration_ms=duration, stop_reason=result.stop_reason,
    )
    return report
