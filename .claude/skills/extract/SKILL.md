---
name: extract
description: Turn a topic into ontology-shaped notes in the `things` Obsidian vault at things/ by researching the web (and NotebookLM for complex topics). User provides a topic; skill researches, drafts notes, runs a verifier sub-agent, presents a plan via ExitPlanMode for approval, then writes approved files. Trigger on `/extract <topic>` or when the user asks to research a topic for the `things` vault.
---

# extract

Turns a user-supplied topic into notes in the `things` vault at `things/` by researching the web (and NotebookLM for complex topics), following a strict four-type ontology. Every run ends with a plan the human approves before anything is written to disk.

## At the start of every run

1. Read `.claude/skills/extract/references/learnings.md` and apply its guidance.
2. Announce that you're using the `extract` skill.

## The ontology (summary)

Four note types. Every note declares its type in frontmatter.

| Type | Purpose | Required links (woven into body prose) |
|---|---|---|
| `event` | Dated, verifiable happening | ≥1 `type: specific` |
| `specific` | Domain concept, fact, or construct | ≥1 `type: abstract`, unless the note is a hub (see below) |
| `abstract` | Cross-discipline concept, plain English | none |
| `mechanism` | Causal claim between two notes | exactly 2, both non-event |

All links live in body prose. Notes do not use a `## Related` section; the Obsidian graph and backlinks pane surface connections automatically.

Relationships are undirected. The target's `type:` carries the role; never add `from`/`to` or parent/child labels. Each note expresses one idea.

### Hub-and-spoke decomposition for big concepts

When a topic is broad enough to cover several independent sub-ideas, split it: keep the broad concept as a **hub specific** whose body enumerates its moving parts, and give each sub-idea its own **child specific**. Each child specific pairs with its own abstract. The hub note may skip the abstract link; its role is to federate the children. Children link back to the hub in their `## Related`.

Signs a topic wants this shape: the one-sentence definition keeps spawning "...which depends on X, Y, and Z," or the draft for one note tries to explain three unrelated sub-concepts inline. Do not force the shape on narrow topics; most runs produce flat sets of notes.

Full type rules (title style, jargon policy, per-type checks) live in `.claude/skills/extract/references/checklists.md`. Read it during drafting and verification.

## Prose style

Write note body prose in Hemingway style. Notes are reference material, not essays: readers scan them.

- Extreme concision. Cut every word that does not carry meaning.
- Short sentences. Subject, verb, object. Break long sentences.
- Active verbs. Prefer "the provider adds" over "are added by the provider."
- Few adjectives and adverbs. Lean on nouns and verbs.
- Objective tone. Journalistic and detached. No hedging, no emotion.
- Iceberg theory. State the surface fact. Let the reader infer the rest.

This applies to body prose only. Frontmatter, `## Related` lists, and wiki-link targets are structural and unchanged by style.

## Obsidian formatting conventions (inlined — skill is self-contained)

- **Frontmatter**: YAML block delimited by `---` at the top. Keys: `type` (required); `date` (events, YYYY-MM-DD or YYYY); `domain` (specifics and events; single domain; kebab-case); `source` (events; URL or citation). Do not add a `tags:` list that merely duplicates `type:` or `domain:` — those already exist as structured fields. Use `tags:` only for genuinely orthogonal labels, which are rare.
- **Wiki-links**: `[[filename-without-extension]]`. Kebab-case targets, no `.md` extension. Do not use aliased links (`[[target|display text]]`); pick one surface name per concept and use it consistently across notes.
- **Body**: `# Title` H1 → 1–3 short prose paragraphs. All wiki-links live inside the prose. No `## Related` section, no arrows, no parent/child labels.
- **Filenames**: kebab-case, no underscores, no punctuation. H1 is readable English matching filename semantics.

### Frontmatter templates

```yaml
# event
---
type: event
date: 2026-04-18
domain: molecular-biology
source: <url or citation>
---

# specific
---
type: specific
domain: molecular-biology
---

# abstract
---
type: abstract
---

# mechanism
---
type: mechanism
---
```

## Pipeline

1. **Research** the topic. Pick the tier based on what the topic needs:
   - **Simple topics** (well-known concepts, widely-documented events, common knowledge domains): WebSearch + WebFetch on 3–6 authoritative sources. Keep URLs for event `source:` fields.
   - **Complex topics** (research papers, technical depth, contested or specialized domains): use NotebookLM via `nlm` CLI (pre-authenticated).
     - `nlm notebook create "<topic>"`, capture the notebook ID.
     - `nlm research start <id> --query "<topic>"`, poll `nlm research status <task-id>` until done, then `nlm research import <task-id>` to pull in discovered sources.
     - `nlm notebook query <id> "<question>"` per fact, date, or claim. Capture citation URLs for event `source:` fields.
     - Do not generate audio overviews, reports, flashcards, or other studio artifacts.
   - If unsure which tier, start with WebSearch; escalate to NotebookLM if sources are shallow or you need to triangulate across papers. WebSearch is also fine for looking up how to use NotebookLM well.

   Then identify candidate ideas without assigning types yet.
2. **Decompose** compound ideas into atomic pieces using the decomposition checklist in `.claude/skills/extract/references/checklists.md`.
3. **Invoke the `superpowers:brainstorming` skill before drafting.** Use it to pressure-test scope, surface assumptions, and align on what to include before committing to note text. Skip only for trivial single-note topics.
4. **Draft notes as plan content** (session only; not saved to disk, not written to any memory system). For each atom pick type, title, and body prose with all required wiki-links woven in. Glob `things/*.md` to find existing notes; draft stubs for any missing link targets in the same pass.
5. **Dispatch a verifier sub-agent** (Agent tool, subagent_type `general-purpose`). Pass `.claude/skills/extract/references/verifier-prompt.md` verbatim as its instructions, the drafted note contents inline, and the vault path `things/` for backlink lookups. It returns structured pass/fail per note.
6. **Enter plan mode, then present the plan.** Always call `EnterPlanMode` explicitly before `ExitPlanMode`, even if you believe the session is already in plan mode; the skill is responsible for driving this transition and should not assume the harness has done it. Then call `ExitPlanMode`, formatted per `.claude/skills/extract/references/plan-template.md`. The plan must list, per proposed note: filename, title, type + one-sentence rationale, `## Related` links (mark `(stub)` where applicable), writer uncertainty, and verifier results.
7. **Wait for approval.** The user may approve, reject, or edit (change a type, rename, split, merge, drop, overrule a verifier flag). Write nothing until approval.
8. **Write files** to `things/` after approval. If the user overruled a verifier flag, record the disagreement in the note body inline (e.g. `Verifier flagged: <item> — user overruled because <reason>.`).
9. **Brief summary** of what was written.

## Memory-system boundary

This skill must not write anything to Claude's auto-memory system (`~/.claude/projects/.../memory/`) during a run. Drafts are session-scoped plan content only. Files this skill may write:

- Approved notes in `things/` (after step 7).
- Its own `.claude/skills/extract/references/learnings.md` (and, on user approval, any of its own files — see below).

## Self-improvement

**Per-run learning (routine).** When the user corrects the skill's judgment during plan review, offer to append a short bullet to `.claude/skills/extract/references/learnings.md` (rule + one-line reason). Ask first; do not append silently.

**Structural self-edits (periodic).** When the skill notices a repeated pattern across runs — the same correction landing in `learnings.md` several times, a checklist item that keeps getting overruled, a verifier question that never finds real failures — propose a structural edit. May edit any file in this skill, but must first describe the pattern, the proposed edit, and the files affected, and get user approval. Never edit silently.

## Stop conditions

- Topic too broad or ambiguous to scope → ask for narrower framing.
- Research (either tier) returns no useful sources → report back rather than drafting from thin material.
- A draft seems to duplicate an existing vault note → user decides merge vs. keep separate.
- Verifier returns failures you don't know how to address → ask.

## Files

| Path | Purpose |
|---|---|
| `.claude/skills/extract/SKILL.md` | This file — entry point, conventions, pipeline. |
| `.claude/skills/extract/references/checklists.md` | Decomposition + four type-specific checklists. |
| `.claude/skills/extract/references/verifier-prompt.md` | Instructions passed verbatim to the verifier sub-agent. |
| `.claude/skills/extract/references/plan-template.md` | Shape for the plan presented via `ExitPlanMode`. |
| `.claude/skills/extract/references/learnings.md` | Self-improving scratchpad; read at start of every run. |
