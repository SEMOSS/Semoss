# Editing an existing PowerPoint

The current PPTX is authoritative, including manual edits. PptxGenJS creates new
presentations; it does not import the original. Never reconstruct existing slides,
rerun an old creation generator, or use creation examples for an editing task.

## Managed editing

1. Call `PreparePptxEdit` alone with the exact existing `filePath` and only the
   requested original 1-based `slides`, for example `[1]`. Omit `outputFilePath`
   for an in-place edit, or pass the exact separate output the user requested.
   For wording changes use `editType: "text"` (the default).
2. It returns a protected `inputSnapshot`, true presentation order, selected slide
   package `part` names, and existing `texts` with zero-based indexes. Use those
   exact values. A slide filename alone does not establish display order.
3. Save the editing program as `build-deck.js` with `WriteFile`. Read from that
   snapshot every time so repairs are reproducible. Originals are captured before
   author tools run; `.semoss/pptx-workflow` is read-only.
4. Call `BuildPptx` alone with the program, exact output filename, original slide
   count, and review instructions limited to the requested edits. It executes,
   validates, checks preservation and invokes review. ExecuteNodeCode is unavailable
   in managed runs. Do not install packages or run local rendering commands.

## Text editing example

Substitute `inputSnapshot`, `part`, `index`, and `oldText` from PreparePptxEdit,
and the user's output filename and replacement. For a title change, change only
that title; do not rewrite the subtitle or other content unless requested.

```javascript
(async () => {
  const path = require("path");
  const JSZip = require("jszip");
  const edit = require(path.join(ROOT, ".claude/skills/pptx/scripts/edit.js"));
  const deck = require(path.join(ROOT, ".claude/skills/pptx/scripts/deck.js"));
  const output = path.join(ROOT, "requested-filename.pptx");
  await edit.replaceText({
    JSZip,
    input: path.join(ROOT, "<inputSnapshot returned by PreparePptxEdit>"),
    output,
    replacements: [{ part: "ppt/slides/slide1.xml", index: 0,
      oldText: "DOGS", newText: "CATS" }]
  });
  return deck.validate(output, { slides: 5, strictCanvas: false });
})()
```

The helper verifies exact old text at each inspected node, escapes replacements,
and patches only those XML spans. It preserves existing runs, paragraphs, bullets,
font properties, geometry and all other package entries. Edit several runs in one
batch when necessary, retaining their boundaries and formatting. A stale or
ambiguous match fails before writing. Do not replace a whole text frame to bypass
that error; use the inspected node indexes. Do not read the full authoring helper.

## Layout, notes, chart and image edits

Use `editType: "slides"` only for requested layout or object changes. Selected
slide XML may then change. Use JSZip to modify the snapshot's existing XML/parts
inside the saved build program. Keep unrelated content intact.

Declare exact existing `additionalParts` only when the request requires notes,
image, chart, embedded workbook or relationship edits. A shared part requires all
affected slides to be in the requested scope. For chart data update both the cache
and embedded workbook, retaining units, labels and axes. Keep image crop and frame
unless requested. Inspect relationships with file tools when needed.

The preservation contract keeps slide count, order and package entry names fixed.
Adding/removing slides or package parts needs a broader authoring operation; do not
silently regenerate the deck or broaden scope to force it. Scope is immutable once
prepared, including during repairs. Do not fix unrelated pre-existing warnings.

BuildPptx compares against the run's original package. Unrelated parts must be
byte-identical after decompression. In text mode, formatting and object structure
on edited slides must remain identical too. A violation restores the previous
validated output or original and returns an actionable error. The original is not
reported as a successful edit. Original advisory warnings are supplied to review
separately; review concerns the requested changes, not a redesign.

Without managed tools, use the same helper with JSZip in one async ExecuteNodeCode
IIFE, reading from a separate preserved source. Validate the slide count and report
actual inspection coverage. The preserved [upstream guidance](upstream.md) covers
advanced OOXML; its Python/rendering commands are unavailable through agent tools.
