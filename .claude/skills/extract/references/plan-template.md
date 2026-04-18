# Plan template

Use this shape when formatting the plan passed to `ExitPlanMode`. Terse is better than verbose — the human is reading to catch type mistakes and missing links, not to enjoy prose.

```
## Proposed notes for the `things` vault

### 1. <filename.md> — <H1 title>

- **Type:** <event | specific | abstract | mechanism>
- **Rationale:** <one sentence — why this type>
- **Related:**
  - [[link-a]]
  - [[link-b]] (stub)
- **Writer uncertainty:** <short note, or "none">
- **Verifier:**
  - ✅ <check that passed>
  - ❌ <check that failed> — <one-sentence reason>

### 2. <filename.md> — <H1 title>

...

## Stubs to create in the same pass

- `link-b.md` — type: abstract, one-line gloss: "<...>"

## Notes flagged but keeping

- <filename> — <which verifier item> — <why we're keeping it anyway; will record disagreement in body>

## Write plan

Write the above notes to `~/experiments/r2/things/`. Nothing else changes.
```

Rules:

- Stubs listed separately at the end, with their intended type and a one-line gloss. They will be written as minimal notes (frontmatter + H1 + one sentence + empty `## Related`) so backlinks resolve immediately.
- Mark `(stub)` next to any `## Related` link whose target is being created as a stub in this same plan.
- If every verifier item passed, collapse to one line: `**Verifier:** all checks passed.`
- If the user pre-answered any verifier flag (e.g. "ignore the jargon one for this note"), put it under "Notes flagged but keeping" and note that the disagreement will be recorded in the note body.
