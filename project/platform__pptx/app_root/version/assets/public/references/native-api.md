# Native PptxGenJS details

Use this reference for native authoring beyond a component's parameters. The examples
show complete IIFEs and native calls. The following details apply to the curated
PptxGenJS environment; they do not impose a visual style.

`deck.create({ PptxGenJS, title, theme, width?, height? })` returns the actual native
presentation. Custom `width` and `height` are positive inches and must both be supplied.
Native `addText`, `addShape`, `addImage`, `addChart`, `addTable`, `addNotes`, rich text,
hyperlinks and slide masters remain available. `deck.save` checks the requested count
before replacing a file. Always validate afterward.

- Use `pres.ShapeType.rect`, `pres.ShapeType.line`, and `pres.ChartType.bar` from the
  instance. Set the layout before the first slide when using `new PptxGenJS()` directly.
- Geometry is in inches; native calls also accept percentages. The wide canvas is
  approximately 13.333 x 7.5 inches. Keep required content on it. Use positive widths
  and heights with `flipH`/`flipV` for direction. The guarded path normalizes signed
  extents; direct native calls require correct values.
- Use six-digit hex colors. The guards remove a leading `#` and reject alpha in hex.
  Set native `transparency` on fills/images or `opacity` on shadows for translucency.
  Shadow offsets must be nonnegative. A fresh options object per native call avoids
  PptxGenJS mutation; guarded slides clone options for you.
- Set `margin: 0` for aligned text. `charSpacing` is the supported character-spacing
  option. Typography, alignment and rich-text run options can be freely overridden.
  Use real bullet options, not a literal bullet glyph in the text; use paragraph
  spacing rather than oversized line spacing to separate list items.
- Notes use `slide.addNotes("...")`. Keep notes out of visible text boxes.
- `deck.image(slide, { path, x, y, w, h, fit: "cover" })` crops without stretching.
  `fit: "contain"` shows the complete image; `"stretch"` deliberately changes aspect.
  PNG/JPEG/GIF/SVG dimensions are detected. For other formats use curated `sharp` or
  supply `aspectRatio: width / height`. Base64 images need an image MIME prefix such
  as `image/png;base64,` before the data.
- Use native charts for supported chart types. Preserve the source categories,
  values, units and denominators. Style labels, axes and series explicitly. In the
  chart component, pass native overrides in `options`.
- Native charts accept `slide.addChart(type, data, options)`. Combo charts use
  `slide.addChart([{ type, data, options }, ...], options)`; the second argument is
  the shared options object. The guards preserve both signatures.
- On stacked/percent-stacked bar charts, `dataLabelPosition` must be `ctr`, `inEnd`
  or `inBase`. The guards correct illegal positions. Secondary-axis combos need
  both `valAxes` and `catAxes`, with two entries in each, so every referenced axis
  ID is declared. Validate the resulting package.
- PptxGenJS may ignore an option it does not implement. For example, the installed
  4.0.1 generator omits `dataLabelPosition` for doughnut charts. If exact placement
  matters, use an appropriate supported chart or an explicit native label, and
  inspect the chart XML. Do not assume setting an option proves it was applied.
- PptxGenJS does not support gradient fills. A background image can supply that
  effect while foreground text and chart data remain editable.
- Do not reorder `ppt/presentation.xml` children or manually repair generated XML
  just to suppress a generator error; correct the authoring code and regenerate.

`deck.validate(outPath, { slides: requestedCount, strictCanvas: false })` reports
structural errors and advisory layout diagnostics. It does not render slides or
measure the actual font. No font is guaranteed to render identically on every
recipient's system; match requested fonts and leave sensible room for text.
