# Native deck components

Load this reference when you need helper parameters. Use native PptxGenJS calls for additional control. The examples demonstrate complete execution calls.

### Component reference

All component geometry is in inches; native calls also accept percentages. Use
`x`, `y`, `w`, `h` to place and combine components. Defaults use a wide 13.333 x 7.5
canvas. Components also accept `geometry: { x, y, w, h }`; explicit top-level
coordinates take precedence. Nested geometry accepts only these four keys.

```js
deck.callout(slide, { value: "2.0", label: "Million years", caption: "Room for a caption",
  x: 0.95, y: 2, w: 11.4, h: 5, captionSize: 16 });
```

`deck.create({ PptxGenJS, width, height, ... })` supports a custom canvas.
Common text controls include `fontSize`, `color`, and the options listed below.
For details beyond a component's parameters, use the native slide API directly.

| Call | Parameters and purpose |
|---|---|
| `deck.text(slide, textOrRuns, options)` | Native text options plus `role`: `title`, `heading`, `body`, `caption`, `value`. Role sets defaults; explicit font, size, color, alignment and rich-text styling win. |
| `deck.heading(slide, options)` | `title`, optional `kicker`, `fontSize`, `fontFace`, `color`, `accent`, and geometry. |
| `deck.cover(slide, options)` | `title`, optional `subtitle`, `kicker`, `footer`, `image: { path or data, fit? }`, `background` as a color string (`"1A7F5A"`) or native object (`{ color: "1A7F5A" }`), `color`, `accent`, `fontFace`, `fontSize`, `subtitleColor`, `subtitleSize`, and title geometry. |
| `deck.callout(slide, options)` | `value`, `label`, optional `caption`, `fontSize`, `labelSize`, `captionSize`, `color`, `labelColor`, `captionColor`, `align`, and geometry. |
| `deck.comparison(slide, options)` | `left` and `right`: `{ heading, text }` or `{ heading, bullets }`. Optional column `color`, `headingSize`; overall `gap`, `headingSize`, `fontSize`, geometry. |
| `deck.timeline(slide, options)` | `steps: [{ title, text?, label? }]`, `direction: "horizontal"` or `"vertical"`, `fontSize`, `bodySize`, `color` (marker fill), `markerTextColor`, `labelColor`, `titleColor`, `bodyColor`, `gap`, geometry. Marker ink defaults to readable black/white; label/title ink defaults to the theme text color. Native editable connectors and text. |
| `deck.bullets(slide, items, options)` | Items are strings or `{ text, level, bold }`. Uses real bullets; accepts native text options. |
| `deck.image(slide, options)` | A local `path` or base64 `data`, geometry, `fit: "cover"` (default), `"contain"`, or `"stretch"`, plus native image options. PNG/JPEG/GIF/SVG dimensions are detected. For other formats use curated `sharp` or supply `aspectRatio: width / height`. |
| `deck.chart(slide, options)` | `type`, `categories`, `series: [{ name, values }]`, geometry. `options` accepts native chart styling and axis settings. Keep source values, units and categories intact. Native chart data remains editable. |
| `deck.grid(options)` | `{ x, y, w, h, columns, rows, gap }` returns rectangles in reading order for any native calls or components. It adds no content. |
| `deck.addLayout(pres, spec)` | Add one original JSON layout to a wide native deck and return the slide for customization. |
| `deck.save(pres, outPath, { slides })` | Save after authoring; checks the requested count before writing. Run `validate` afterward. |

Theme presets `navy`, `slate`, `forest`, `plum`, and `mono` are optional. Use
`theme: { preset, palette, fonts: { heading, body } }` to customize them. Palette
keys: `bg`, `panel`, `ink`, `muted`, `accent`, `accentSoft`, `invertInk`, `bandBg`,
`series` (color array). Native slide backgrounds and object styling remain free.
Use 6-digit hex; the helpers strip a leading `#` but reject alpha in hex strings.
Use native transparency/opacity properties for translucent objects.
