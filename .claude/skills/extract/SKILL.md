---
name: extract
description: Turn a topic into ontology-shaped notes in the `things` Obsidian vault at things/ by researching the web (and NotebookLM for complex topics). User provides a topic; skill researches, drafts notes, walks the user through each one interactively, then writes approved files. Trigger on `/extract <topic>` or when the user asks to research a topic for the `things` vault.
---

# extract

Turns a user-supplied topic into notes in the `things` vault at `things/` by researching the web (and NotebookLM for complex topics), following a strict four-type ontology. The human is the sole verifier; every draft is reviewed one note at a time before anything is written to disk.

## At the start of every run

Announce that you are using the `extract` skill.

## The ontology

Four note types. Every note declares its type in frontmatter. All links live in body prose; notes do not use a `## Related` section. Relationships are undirected; the target's `type:` carries the role.

| Type | Purpose | Required links (in body prose) |
|---|---|---|
| `event` | Dated, verifiable happening | ≥1 `type: specific` |
| `specific` | Domain concept, fact, or construct | ≥1 `type: abstract`, unless hub (see below) |
| `abstract` | Cross-discipline concept, plain English | none |
| `mechanism` | Causal claim between two notes | exactly 2, both non-event |

### Hub-and-spoke for big concepts

When a topic spans several independent sub-ideas, split it: keep the broad concept as a **hub specific** that enumerates its moving parts, and give each sub-idea its own **child specific** with its own abstract. The hub may skip the abstract link; declare hub status during review. Do not force this shape on narrow topics.

Full type rules live in `.claude/skills/extract/references/checklists.md`. Full body templates live in `.claude/skills/extract/references/note-templates.md`. Read both during drafting.

## Prose style

Write bodies in Hemingway style. Notes are reference material; readers scan.

- Extreme concision. Cut words that do not carry meaning.
- Short sentences. Subject, verb, object.
- Active verbs.
- Few adjectives and adverbs.
- Objective, journalistic tone. No hedging.
- State the surface fact. Let the reader infer.

Applies to body prose only.

## Obsidian formatting

- **Frontmatter**: YAML block delimited by `---`. Keys: `type` (required); `date` (events, YYYY-MM-DD or YYYY); `domain` (specifics and events; single domain; kebab-case); `source` (events; URL or citation). No `tags:` that merely duplicate `type:` or `domain:`.
- **Wiki-links**: `[[filename-without-extension]]`, kebab-case targets, no `.md`. No aliased links (`[[target|display]]`); pick one surface name per concept and use it everywhere.
- **Body**: `# Title` H1, then 1–3 short prose paragraphs. All wiki-links live inside the prose.
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

Body templates per type are in `.claude/skills/extract/references/note-templates.md`.

## ASCII diagrams (optional)

When a concept is easier to see than read (short causal chain, ordered steps), include one spare ASCII diagram inside a fenced code block after the prose. Prose first, diagram second. Keep it under ~6 lines. Wiki-link targets inside a diagram count as real links. See `.claude/skills/extract/references/note-templates.md` for an example. Skip the diagram if the prose is already clear.

## Pipeline

1. **Research.** Pick the tier:
   - **Simple topics** (well-known concepts, widely-documented events): WebSearch + WebFetch on 3–6 authoritative sources. Keep URLs for event `source:` fields.
   - **Complex topics** (research papers, technical depth, contested domains): use NotebookLM via the `nlm` CLI (pre-authenticated).
     - `nlm notebook create "<topic>"`, capture the notebook ID.
     - `nlm research start <id> --query "<topic>"`, poll `nlm research status <task-id>` until done, then `nlm research import <task-id>`.
     - `nlm notebook query <id> "<question>"` per fact, date, or claim. Capture citation URLs.
     - Do not generate audio overviews, reports, or flashcards.
   - If unsure, start with WebSearch; escalate if sources are shallow.

   Identify candidate atomic ideas; do not yet assign types.
2. **Decompose** compound ideas into atomic pieces using the decomposition checklist in `.claude/skills/extract/references/checklists.md`.
3. **Invoke `superpowers:brainstorming` before drafting.** Use it to pressure-test scope and surface assumptions. Skip only for trivial single-note topics.
4. **Draft notes in-memory** (session only; not written to disk, not written to any memory system). For each atom, pick type, title, and body prose with required wiki-links woven in. `Glob things/*.md` to map drafts to real link targets; mark missing targets as stubs.
5. **Send an overview message** per `.claude/skills/extract/references/plan-template.md`:
   - A mermaid flowchart of every proposed note plus every existing vault note reachable in one hop. Shape per type (event = `([name])`, specific = `[name]`, abstract = `(name)`, mechanism = `{name}`). Box contents = filename only.
   - A numbered summary list: filename, type, one-sentence rationale.
   - Self-check: before sending, scan the mermaid for orphans and dangling links. Fix silently when possible; flag when a human decision is needed. This diagram doubles as your type-check and orphan-check.
6. **Walk through each note, one per message.** For each note:
   - Run the matching type checklist in `.claude/skills/extract/references/checklists.md` against the draft.
   - Show frontmatter + H1 + body + optional ASCII diagram.
   - Report checklist results ("all passed" or "failed: X — reason") and any writer uncertainty.
   - Ask for feedback: approve, edit, split, merge, drop, rename. The user may also ask understanding questions; answer them without mutating the draft unless the user explicitly asks for a change. The user may overrule a checklist failure; if so, record it inline in the note body at write time (e.g. "Checklist flagged: <item>, user overruled because <reason>.").
   - Only move to the next note once the current one is approved.
7. **Final confirm.** One line restating the write set; ask for the final green light.
8. **Write files** to `things/` after confirmation.
9. **Brief summary** of what was written.

## Memory-system boundary

This skill does not write to Claude's auto-memory system. Drafts are session-scoped until approved.

## Stop conditions

- Topic too broad or ambiguous → ask for narrower framing.
- Research returns no useful sources → report back rather than drafting from thin material.
- A draft seems to duplicate an existing vault note → user decides merge vs. keep separate.

## Files

| Path | Purpose |
|---|---|
| `.claude/skills/extract/SKILL.md` | This file. |
| `.claude/skills/extract/references/checklists.md` | Decomposition + four type-specific checklists. |
| `.claude/skills/extract/references/note-templates.md` | Body templates per note type, with ASCII diagram example. |
| `.claude/skills/extract/references/plan-template.md` | Shape for the overview, per-note, and final-confirm messages. |
