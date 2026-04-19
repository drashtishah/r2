## Conventions

- No emojis.
- No `--` as punctuation. Use commas, periods, or colons instead.
- All file paths in markdown and JSON must be root-relative.
- Backticks in markdown are for file paths and code only. Never
  backtick-wrap YAML tags, labels, or other slash-separated values that
  are not filesystem paths.
- Be concise in ALL output, workspace-wide: terminal conversation,
  commit message bodies, PR descriptions, code comments, subagent
  dispatch prompts, and any authored markdown. Remove filler words,
  preambles, and trailing summaries. Code explanations include only
  what is necessary; skip restating what is visible in the diff.
  Exemption: reasoning steps (debugging, root-cause analysis, design
  trade-offs) are NOT capped; padding around them is.
