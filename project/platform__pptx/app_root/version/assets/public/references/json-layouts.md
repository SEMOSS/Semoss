# Optional JSON layouts

Load this reference when a simple fixed layout suits the request. Native authoring and design review remain available.

### Optional JSON workflow

`await deck.render({ PptxGenJS, spec, outPath })` remains available for straightforward
decks or when the user prefers a simple template. It uses fixed wide layouts. Choose
native authoring or components whenever the design would benefit from more control.
Both workflows require the same content and design review after validation.

Every `spec.slides` entry has a `kind`, `title`, and optional `notes`:

| kind | Content fields |
|---|---|
| `title`, `section` | Optional `subtitle`; title also accepts `footer` |
| `bullets` | `bullets: [string or { text, level, bold }]`, optional `lead` |
| `two-col` | `left`, `right`: `{ heading, bullets }` or `{ heading, text }` |
| `stat-row` | `stats: [{ value, label, caption? }]` (up to 6); optional `bullets` |
| `quote` | `quote`, optional `attribution` |
| `table` | `columns: [string]`, `rows: [[cell, ...]]`; equal lengths, up to 10 rows including the header |
| `chart` | `chartType`, `categories`, `series: [{ name, values }]` |
| `image` | `imagePath` or `imageData`; optional `caption`, `flipH`, `flipV` |

Top-level spec fields include `title`, `author`, `company`, `footer`, `theme`, and
`slides`. JSON chart types: `bar`, `column`, `line`, `area`, `pie`, `doughnut`, `radar`.
Optional chart fields: `chartTitle`, `barDir`, `barGrouping`, `dataLabelPosition`.
Each series must have one finite value per category.

