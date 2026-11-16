# deslop

Markdown-aware AI-slop detector. Same rules as the Slop Detector web page.

## Install as a Claude Code skill

Global (every project):

```bash
mkdir -p ~/.claude/skills/deslop
cp SKILL.md slop.mjs ~/.claude/skills/deslop/
```

Per repo: copy the same two files to `.claude/skills/deslop/` and commit them.

Then in Claude Code: "deslop content/articles" or `/deslop content/articles/my-post.md`.

## Use directly

```bash
node slop.mjs content/                 # report
node slop.mjs content/ --summary       # scores only
node slop.mjs post.md --fix            # safe swaps, in place
node slop.mjs content/ --json > slop.json
node slop.mjs content/ --min 35        # exit 1 if anything >= 35 (QA gate / CI / pre-commit)
```

## Article Factory

Call it as a deterministic gate before the LLM QA gates:

```bash
node slop.mjs "$ARTICLE" --json --min 35
```

Exit code 1 means fail. The JSON gives line/col findings to hand to the rewrite step.

## Editing the rules

Rules live near the top of `slop.mjs` (`words`, `P`, `G` arrays and `rule(...)` calls). Each is a regex, a category, a weight, a message and an optional fix (a string, or a map from matched form to replacement).
