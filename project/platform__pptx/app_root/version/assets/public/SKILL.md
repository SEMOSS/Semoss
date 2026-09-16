---
name: pptx
description: "Create, inspect, or edit PowerPoint presentations and templates (.pptx and .potx), including slide design, charts, notes, and existing deck content."
license: Proprietary. LICENSE.txt has complete terms
---

# PowerPoint in SEMOSS

Create a deck suited to the user's audience, content and visual direction. Preserve
the requested filename, total slide count, source data and branding. Components and
native PptxGenJS calls may share a slide; every slide may also be entirely custom.

## Load only what the task needs

`LoadSkill(skill_name="pptx")` reads this complete entry document with the default
8192-byte limit. For a new deck, read one relevant example from the table below.
Use supplied templates or design references when they provide the direction instead.
Read API references only when needed; do not load every file in the tool's listing.

| Example | Useful for | What to borrow |
|---|---|---|
| [examples/image-led.js](examples/image-led.js) | A story built around a place, product or photograph | Large imagery, deliberate crops, quiet captions and text beside images |
| [examples/analytical.js](examples/analytical.js) | Results, comparisons and decisions supported by data | Takeaway headlines, editable charts, clear units and selective emphasis |
| [examples/editorial.js](examples/editorial.js) | A proposal, strategy or idea with little relevant imagery | Strong typography, asymmetry, open space and a coherent argument |

For example, call `LoadSkill(skill_name="pptx/examples/analytical.js")`. Each example
fits in one default read and is a complete `ExecuteNodeCode` call. Adapt its content,
palette, fonts, geometry and compositions to the brief. The three example slides are
demonstrations, not a required count or sequence. Replace their demo data and output
filename. The bundled Earthrise photograph is only for relevant imagery; its source
is recorded in [assets/CREDITS.md](assets/CREDITS.md).

Read another example only when its techniques are useful. You can combine examples,
customize the helpers, or write native code throughout. The fixed JSON layouts are
optional for simple decks, not the default design boundary.

Supporting references load the same way, e.g.
`LoadSkill(skill_name="pptx/references/components.md")`:

- [references/components.md](references/components.md): helper parameters, theme controls, grid geometry and saving.
- [references/native-api.md](references/native-api.md): native API details and known library mistakes.
- [references/editing.md](references/editing.md): read this for edits to an existing deck; preserve its design and unrelated content.
- [references/json-layouts.md](references/json-layouts.md): the optional JSON schema and render call.
- [references/upstream.md](references/upstream.md): preserved upstream guidance for advanced details. Its Python/rendering workflows are unavailable here. This long reference needs `max_bytes: 65536` or continuation reads.

If ANY requested file returns a continuation marker, use the exact next `offset`
reported by the tool until that file is complete. Loading the entry once does not
prohibit reading references or continuing a truncated response.

## Design before authoring

Choose the deck's visual direction and each slide's takeaway. Match composition to
the content: an evidence-led chart, a useful photograph, a diagram, a comparison or
a deliberate typographic statement. A text-only slide can work when the typography
and message carry it. Decoration is optional; the user's brief governs the style.

Use a consistent type hierarchy, palette and spacing rhythm, with enough variation
to distinguish the slide purposes. Give each slide a clear focal point. Prefer
shorter copy and more space to tiny body text. Keep meaningful charts, diagrams and
labels editable. Never invent source values to fill an example chart.

## Execution contract

- Run JavaScript through `ExecuteNodeCode`. Every call must be ONE
  `(async () => { ... })()` with EVERY `require`, `const`, `let`, class and function
  declaration inside it. Await all asynchronous work. Use `globalThis` only for
  intentional durable state between calls.
- `ROOT` is the room's working directory. Put the exact output filename, including
  its extension, in `path.join(ROOT, "requested-filename.pptx")`. `APP_ROOT` and
  `USER_ROOT` identify project and user assets when available.
- Inside the IIFE: `const path = require("path");`,
  `const PptxGenJS = require("pptxgenjs");`, and
  `const deck = require(path.join(ROOT, ".claude/skills/pptx/scripts/deck.js"));`.
  Inject `PptxGenJS` into `deck.create({ PptxGenJS, ... })`. The skill folder cannot
  resolve curated packages itself. All examples show these declarations in place.
- `deck.create` sets the layout before slides and returns a native presentation.
  `pres.addSlide()` returns a native slide. Use instance `pres.ShapeType` and
  `pres.ChartType`. Guarded native calls clone options, normalize colors and signed
  geometry, and correct illegal stacked-bar labels; other native options pass through.
- After authoring, `await deck.save(pres, outPath, { slides: requestedCount });`
  then `return deck.validate(outPath, { slides: requestedCount, strictCanvas: false });`.
  Both variables must reflect the user's request. Direct `new PptxGenJS()` authoring
  may use `writeFile` followed by the same validator; set its layout before slides.
- Bash allows `awk`, `cat`, `cp`, `curl`, `cut`, `diff`, `dir`, `find`, `grep`, `head`,
  `jq`, `ls`, `mkdir`, `mv`, `pwd`, `python`, `python3`, `rg`, `sed`, `sort`, `stat`,
  `tail`, `touch`, `tr`, `uniq`, `unzip`, `wc`, `wget`, `which`, `zip`. One command;
  working-directory-relative paths; no pipes, chaining, redirects, `$()`, backticks,
  absolute paths, `~` or `..`. Read output from the tool result.
- Use `ExecuteNodeCode` for JavaScript. No Bash `node`/`npm`/`npx`, package installs,
  `markitdown`, `soffice`, `pdftoppm`, or upstream Python validation workflow here.

## Review and completion

Fix structural validation errors and verify the requested content, order and count.
Review the slide/object diagnostics for likely text overflow, overlap, small text
and low contrast. These are advisory estimates. With `strictCanvas: false`, intended
decorative bleed may remain; unintended clipping needs correction.

Review each slide's hierarchy, spacing, contrast and accuracy. Make a correction
pass when useful. `ok: true` alone does not complete the design review. No rendered
slide preview tool is currently exposed; do not claim rendered inspection or measured
text fit. Finish when content, structural checks and design review are satisfactory.
Available turns are a ceiling, not a target. Reply with the filename and slide count,
plus any unresolved issue that affects use of the deck.
