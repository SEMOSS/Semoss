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

## Optional structured edits

PreparePptxEdit now returns selected-slide `objects` (objectId, name, type, local
geometry/fill, textIndexes) and `texts` (index, exact text, objectId and explicit
run formatting). Missing styles are inherited; do not assume a color or font.
Geometry is in EMU, and grouped objects use group-local coordinates. This is
slide-focused inspection of the original package, not an exported single-slide deck.

For wording changes prepare `editType: "text"`; for colors prepare `editType:
"slides"`. Then call `ApplyPptxEdits` alone; it builds, checks preservation, and
reviews the selected original slides without model-written generator code:

```json
{"operations":[
  {"type":"setBackground","part":"ppt/slides/slide3.xml","color":"000000"},
  {"type":"setTextColor","part":"ppt/slides/slide3.xml","objectId":"5","color":"FFFFFF"}
],"instructions":"Check the requested background and body-text colors and readability."}
```

Use actual part/object IDs from inspection. `setTextColor` changes every existing
text run in that text shape, preserving the other font properties and geometry.
It supports ordinary text shapes, including shapes within groups; tables/charts
use the advanced path below. For wording changes use:

```json
{"operations":[{"type":"replaceText","part":"ppt/slides/slide1.xml","objectId":"2","index":0,"oldText":"DOGS","newText":"CATS"}]}
```

Each call starts from the protected original. During repair submit the COMPLETE
operation list, including edits already requested. A bad ID, stale oldText,
invalid color, duplicate target or out-of-scope operation fails without publishing
partial changes. Preparation can be corrected before the first build; keep the selected slides faithful to the user request. Check affected foreground
readability when changing a background and include explicit text-color operations
where needed; do not recolor unrelated objects. Do not change text-box size or
font properties in text mode to fix a longer replacement.

The Office app opts into a bounded edit context: original/recent user requests
and the authoritative current deck. Historical generator/tool output is omitted
from model input while the full chat remains stored. Earlier excerpts may be
truncated; inspect the current deck and request clarification when a necessary
reference cannot be resolved.

## Advanced text editing example

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

Linked chart, workbook, image, notes and relationship parts are discovered from
both the original and edited package. `PreparePptxEdit` returns `linkedParts` and
`usedBySlides` so you can inspect shared resources. No `additionalParts` whitelist
is required. For chart data update both the cache and embedded workbook, retaining
units, labels and axes. Keep image crop and frame unless requested. New or removed
parts are supported when the package remains valid.

BuildPptx validates the saved package and records changed parts and affected slides.
XML serialization differences alone are not content edits. Actual changes outside
the requested slides, text-mode formatting changes, slide order/count changes, or
changes whose impact cannot be determined are saved as a separate proposed revision
for inspection. They do not cause a file-preservation failure or replace the accepted
Office library version. Broken output and unchanged content are still unsuccessful
edits; the last validated output or original is recoverable. Missing visual review
is reported separately from saving. Pre-existing warnings do not authorize redesign.

The selected slide context is for inspection; the editing program still uses the
whole original package so linked assets, themes, notes and manual edits stay intact.

Without managed tools, use the same helper with JSZip in one async ExecuteNodeCode
IIFE, reading from a separate preserved source. Validate the slide count and report
actual inspection coverage. The preserved [upstream guidance](upstream.md) covers
advanced OOXML; its Python/rendering commands are unavailable through agent tools.
