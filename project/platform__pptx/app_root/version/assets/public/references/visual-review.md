# Rendered visual review

Save the PowerPoint and run structural validation before visual inspection. The
remote UnoServer service handles conversion; local soffice, pdftoppm and Python
rendering commands are unnecessary.

When `BuildPptx` is available, it invokes the attached reviewer and waits for the
report automatically. Pass your review criteria and optional caller-selected engine
to BuildPptx. Follow only its bounded repair requests. The manual delegation steps
below apply when BuildPptx is absent. For prepared edits, BuildPptx reviews only the
requested slides, verifies preservation against the original package and supplies
original advisory warnings separately. Do not broaden the edit scope or repair
unrelated pre-existing issues. This selected-slide review is not a whole-deck pass.

Use the attached reviewer (normally `agent_pptx_reviewer`) and delegate with `inherit_parent_workdir: true`.
Its `prompt` should specify the relative filename, optional original slide numbers,
what to inspect, and audience/design constraints. When the caller selects a model, include the exact `engine` ID in the task; the reviewer forwards it unchanged. Wait for its result using the
existing subagent wait tool. The reviewer reports findings; the author applies edits.

If no reviewer is attached, call `InspectPptx` directly when available:

```json
{
  "filePath": "proposal.pptx",
  "instructions": "Check clipped or overlapping text, chart-label readability, spacing and consistency. Preserve the supplied template.",
  "context": "Executive audience; intentional decorative bleed is allowed."
}
```

Omit `slides` for the whole deck, including hidden slides. Use `"slides": [3]` to
review only original slide 3. `context` is an optional string. `engine` optionally
selects any accessible SEMOSS image-capable text-generation model; otherwise the deployment default or reviewer's
model is used. The authoring model need not accept images: the tool makes the vision
request internally. `checkConsistency` defaults to true for comparing selected slides.

The report records the source hash, requested/reviewed/unreviewed/inconclusive
slides, issues with severity/location/evidence/suggestedFix, limitations, errors, failures, retry attempts,
and paths to slide images and the report. `status` describes execution; `verdict`
describes the findings. Passing requires `status: complete`, `verdict: pass` and
`sourceChanged: false`. A selected-slide pass never proves whole-deck coverage.

Apply fixes only when the reported evidence matches visible content or the deck structure. A format/provider failure, an inconclusive assessment, or missing coverage is not a slide defect and must not trigger edits or a deck rebuild. Do not redesign to satisfy a
reviewer's taste. Send advisory structural warnings with the initial review brief. For new decks use one whole-deck review and at most one repair pass followed by one targeted recheck. For prepared edits keep both reviews inside the original edit scope. After that, deliver the saved deck and disclose unresolved issues or missing coverage. A targeted recheck does not establish a whole-deck pass for an edited file. Any edit invalidates the prior
whole-deck review even when the filename stays the same.

A partial/failed/inconclusive review is not a pass. Report missing coverage or the
service/model error. The tool retries a failed image locally, at most once, with at most three retry calls per inspection. Do not restart the review or split it into new calls to compensate for failures. Use the returned report as the authority for coverage and consistency, including when a reviewer summary claims otherwise. The tool accepts at
most 100 slides per call. For larger decks, use explicit batches and disclose that
consistency comparisons cover each batch. If the tool is unavailable, complete
available structural checks and state that rendered inspection could not run.

This is static visual inspection using LibreOffice rendering, not native PowerPoint
verification, exact font measurement, editability checking or factual verification.
Preserve the requested content, branding and native editable objects independently.
