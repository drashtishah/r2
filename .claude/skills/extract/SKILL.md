---
name: extract
description: Turn raw input (pasted text, URLs, notes) into ontology-shaped notes in the `things` Obsidian vault at ~/experiments/r2/things/. Drafts notes, runs a verifier sub-agent, presents a plan via ExitPlanMode for the user's approval, then writes approved files. Trigger on `/extract` or when the user asks to add content to the `things` vault.
---

# extract

Turns raw input into notes in the `things` vault at `~/experiments/r2/things/`, following a strict four-type ontology. Every run ends with a plan the human approves before anything is written to disk.

## At the start of every run

1. Read `references/learnings.md` and apply its guidance.
2. Announce that you're using the `extract` skill.

## The ontology (summary)

Four note types. Every note declares its type in frontmatter.

| Type | Purpose | Required `## Related` links |
|---|---|---|
| `event` | Dated, verifiable happening | ≥1 `type: specific` |
| `specific` | Domain concept, fact, or construct | ≥1 `type: abstract` |
| `abstract` | Cross-discipline concept, plain English | none |
| `mechanism` | Causal claim between two notes | exactly 2, both non-event |

Relationships are undirected. The target's `type:` carries the role; never add `from`/`to` or parent/child labels. Each note expresses one idea.

Full type rules (title style, jargon policy, per-type checks) live in `references/checklists.md`. Read it during drafting and verification.

## Obsidian formatting conventions (inlined — skill is self-contained)

- **Frontmatter**: YAML block delimited by `---` at the top. Keys: `type` (required); `date` (events, YYYY-MM-DD or YYYY); `domain` (specifics and events; single domain; kebab-case); `source` (events; URL or citation); `tags` (list; always include `type/<x>` mirroring `type:`, and `domain/<x>` when a domain is set).
- **Wiki-links**: `[[filename-without-extension]]` or `[[filename|display text]]`. Kebab-case targets, no `.md` extension in the link.
- **Tags**: lowercase, slash-hierarchical (`type/specific`, `domain/molecular-biology`). In frontmatter: list entries without leading `#`. In body prose: with leading `#`.
- **Body**: `# Title` H1 → 1–3 short prose paragraphs → `## Related` with wiki-links as a bulleted list. No arrows, no parent/child labels.
- **Filenames**: kebab-case, no underscores, no punctuation. H1 is readable English matching filename semantics.

### Frontmatter templates

```yaml
# event
---
type: event
date: 2026-04-18
domain: molecular-biology
source: <url or citation>
tags: [type/event, domain/molecular-biology]
---

# specific
---
type: specific
domain: molecular-biology
tags: [type/specific, domain/molecular-biology]
---

# abstract
---
type: abstract
tags: [type/abstract]
---

# mechanism
---
type: mechanism
tags: [type/mechanism]
---
```

## Pipeline

1. **Parse** the raw input. Identify candidate ideas without assigning types yet.
2. **Decompose** compound ideas into atomic pieces using the decomposition checklist in `references/checklists.md`.
3. **Draft notes as plan content** (session only; not saved to disk, not written to any memory system). For each atom pick type, title, body prose, and `## Related` wiki-links. Glob `~/experiments/r2/things/*.md` to find existing notes; draft stubs for any missing link targets in the same pass.
4. **Dispatch a verifier sub-agent** (Agent tool, subagent_type `general-purpose`). Pass `references/verifier-prompt.md` verbatim as its instructions, the drafted note contents inline, and the vault path `~/experiments/r2/things/` for backlink lookups. It returns structured pass/fail per note.
5. **Present the plan via `ExitPlanMode`**, formatted per `references/plan-template.md`. If the session isn't already in plan mode, call `EnterPlanMode` first. The plan must list, per proposed note: filename, title, type + one-sentence rationale, `## Related` links (mark `(stub)` where applicable), writer uncertainty, and verifier results.
6. **Wait for approval.** The user may approve, reject, or edit (change a type, rename, split, merge, drop, overrule a verifier flag). Write nothing until approval.
7. **Write files** to `~/experiments/r2/things/` after approval. If the user overruled a verifier flag, record the disagreement in the note body under `## Related` (e.g. `Verifier flagged: <item> — user overruled because <reason>.`).
8. **Brief summary** of what was written.

## Memory-system boundary

This skill must not write anything to Claude's auto-memory system (`~/.claude/projects/.../memory/`) during a run. Drafts are session-scoped plan content only. Files this skill may write:

- Approved notes in `~/experiments/r2/things/` (after step 7).
- Its own `references/learnings.md` (and, on user approval, any of its own files — see below).

## Self-improvement

**Per-run learning (routine).** When the user corrects the skill's judgment during plan review, offer to append a short bullet to `references/learnings.md` (rule + one-line reason). Ask first; do not append silently.

**Structural self-edits (periodic).** When the skill notices a repeated pattern across runs — the same correction landing in `learnings.md` several times, a checklist item that keeps getting overruled, a verifier question that never finds real failures — propose a structural edit. May edit any file in this skill, but must first describe the pattern, the proposed edit, and the files affected, and get user approval. Never edit silently.

## Stop conditions

- Raw input too ambiguous to decompose → ask.
- A draft seems to duplicate an existing vault note → user decides merge vs. keep separate.
- Verifier returns failures you don't know how to address → ask.

## Files

| Path | Purpose |
|---|---|
| `SKILL.md` | This file — entry point, conventions, pipeline. |
| `references/checklists.md` | Decomposition + four type-specific checklists. |
| `references/verifier-prompt.md` | Instructions passed verbatim to the verifier sub-agent. |
| `references/plan-template.md` | Shape for the plan presented via `ExitPlanMode`. |
| `references/learnings.md` | Self-improving scratchpad; read at start of every run. |
