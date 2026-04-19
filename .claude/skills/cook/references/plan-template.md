# Interactive review template

The skill does not use `EnterPlanMode`/`ExitPlanMode`. Review is conversational: one overview message, then one message per note, then a one-line final confirm.

## 1. Overview message

Format:

```
## Proposed notes for `brain`

<mermaid flowchart: every proposed note + existing vault notes reached in one hop>

### Summary

1. `<filename.md>` — <type> — <one-sentence rationale>
2. ...

### Self-check

- Orphans: <none | list>
- Dangling links: <none | list>
```

Before sending, scan the mermaid for orphans (no edges) and dangling links. Fix silently if possible; call them out if they need a human decision.

### Mermaid shape per type

- `event`: stadium, e.g. `a([1859-solar-flare])`
- `specific`: rectangle, e.g. `b[spectral-line]`
- `abstract`: rounded, e.g. `c(observation)`
- `mechanism`: rhombus, e.g. `d{thomson-scattering}`
- Stubs: same shape, label ends with ` (stub)`.
- Edges: undirected-looking `---`, no labels. Box contents = filename only.

Minimal example:

```
flowchart LR
  a(observation) --- b[spectral-line]
  b --- c([1859-solar-flare])
  b --- d{thomson-scattering}
  d --- e(light-matter-interaction)
```

## 2. Per-note message (one per note)

Format:

```
### Note <n>/<total>: `<filename.md>`

<full draft: frontmatter block + H1 + body + optional ASCII diagram>

**Checklist:** all passed | failed: <item> — <one-line reason>; ...
**Uncertainty:** <short note, or "none">

Approve, edit, split, merge, drop, or rename? Questions about the concept are fine too; they will not change the draft unless you ask.
```

Run the matching type checklist from `checklists.md` against the draft before sending. Only send the message once you have either passed the checks or have a clear description of what failed.

## 3. Final confirm message

One line after every note is approved:

```
Ready to write: `a.md`, `b.md`, `c.md` to `brain/`. Green light?
```

Write on confirmation. If the user overruled a checklist failure for a note, record that inline in the note body at write time, e.g. `Checklist flagged: <item>, user overruled because <reason>.`
