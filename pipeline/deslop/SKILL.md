---
name: deslop
description: Detect and remove AI-slop in markdown articles (word choice, stock phrases, sentence constructions, filler, wordiness, rhythm). Use when asked to deslop, de-AI, clean up, or slop-check a .md file or a folder of articles.
---

# Deslop

`slop.mjs` (in this skill's folder) is a deterministic, zero-dependency scanner. It ignores frontmatter, code, URLs, link targets and HTML, and scores each file 0–100 (under 15 clean, 15–34 light, 35–59 noticeable, 60+ heavy).

## Commands

```bash
S=~/.claude/skills/deslop/slop.mjs      # or .claude/skills/deslop/slop.mjs inside a repo
node $S <file-or-folder>                # human report, worst files first
node $S <path> --summary                # one line per file
node $S <path> --json                   # findings with line, col, rule, suggested fix
node $S <path> --fix                    # apply safe word/phrase swaps IN PLACE
node $S <path> --min 35                 # exit 1 if any file scores >= 35
```

## Workflow when asked to deslop

1. Check the target is under git (`git status`). If it isn't, copy the files to a `deslopped/` folder and work on the copies.
2. Run `--summary` to see scores. Work only on files scoring 15 or higher unless told otherwise.
3. Run `--fix` on those files. This handles word swaps, wordiness and cut-able filler.
4. Run `--json` on each file and rewrite every remaining finding by hand, by category:
   - **construct**: rewrite the sentence. "Not just X, it's Y" becomes a direct claim. A rhetorical question + reveal becomes a statement. A tacked-on ", ensuring/highlighting…" clause is cut or becomes its own sentence with a concrete subject. "This ensures…" names what "this" is. Em-dash asides become commas, parentheses or two sentences.
   - **phrase**: replace the stock phrase with the specific thing it gestures at, or cut it.
   - **word** with no suggested fix (tapestry, landscape, journey, unlock, ecosystem…): name the concrete thing.
   - **rhythm**: split sentences over 35 words; if rhythm is uniform, vary sentence length.
   - **grammar / passive**: fix only when it reads better; passive is a soft flag.
5. Rules for the rewrite: keep every fact, number, name, citation, link and heading. Don't change frontmatter, code or markdown structure. Keep the author's voice and the article's length within about 10%. Don't introduce new tells while fixing old ones.
6. Re-run the scanner. Aim for under 15, or under 35 for long technical pieces. Stop after two passes per file.
7. Report a short table: file, score before → after, and anything left that needed a judgment call.

Never batch-rewrite a whole folder without showing the user the first file's diff and getting a go-ahead.
