# Save and build a deck generator

Save the complete `(async () => { ... })()` program with `WriteFile` as `build-deck.js`
in the working directory. Adapt a relevant example to the user's brief and save the
exact requested output filename. The generator can also modify an existing deck's
OOXML; it must save the requested output file before returning.

When `BuildPptx` is available, call it alone:

```json
{
  "generator": "build-deck.js",
  "filePath": "requested-filename.pptx",
  "expectedSlides": 5,
  "instructions": "Check clipping, overlap, readability and consistency. Preserve the requested design."
}
```

Use the actual filename and requested slide count. Include `engine` only when the
caller supplied a vision engine ID. BuildPptx reads the generator afresh, executes it,
independently validates the saved PPTX, and invokes the regular reviewer automatically.
A successful check ends the run with a delivery response produced by SEMOSS.

If the result requests repairs, edit the generator to address the reported issues,
then call BuildPptx with the same generator, filename and count. There is one bounded
repair pass; prioritize rebuilding over more edits. Advisory warnings are already
sent to the reviewer. Do not invoke another reviewer or use ExecuteNodeCode for an
alternative build in the managed workflow.

At the repair or tool-turn limit, SEMOSS attempts one final build if the generator
has changed since the last attempt. This does not open another author or repair cycle.
If refinement or review fails, SEMOSS delivers the last structurally validated deck
with warnings; later edits that could not be built are explicitly excluded. A run
without a verifiable saved deck still reports failure.

Packaged scripts are read-only and can be inspected with ReadFile. Use component parameters or native editable objects
for layout changes. After a failed exact-text edit, read the affected lines and make
one corrected attempt; change approach if it still fails.

## Environments without BuildPptx

Run the saved generator through the existing ExecuteNodeCode tool:

```js
(async () => {
  const fs = require("fs");
  const path = require("path");
  return await eval(fs.readFileSync(path.join(ROOT, "build-deck.js"), "utf8"));
})()
```

The source is read on every execution, so generator edits take effect despite Node's
persistent module cache. Follow the manual review contract in visual-review.md.
