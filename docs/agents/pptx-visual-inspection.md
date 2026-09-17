# PPTX visual inspection

`InspectPptx` is a default SEMOSS agent tool and a Pixel reactor. It accepts a PPTX and a review brief, renders through the existing UnoServer service, and calls a SEMOSS image-capable text model. The authoring model does not need vision support. `PptxRenderService` and `PptxInspectionService` can also be tested independently with injected conversion and vision clients.

## Configuration

Keep the existing `UNOSERVER` property. The deployed service needs `GET /health` and multipart `POST /convert?to=pdf`; no server-side slide-selection extension is needed. `UNOSERVER_TIMEOUT_SECONDS` bounds health/conversion response waits (default 180 seconds, allowed 1–1800); connection waits are bounded separately.

Set the reviewer agent's `model_id` to an accessible model whose endpoint supports tool calling. The model used *inside* `InspectPptx` requires image input, text output, and strict JSON Schema generation through the SEMOSS `schema` parameter, but does not need tool calling. The OpenAI-compatible client maps this to `response_format.type=json_schema` with `strict=true`; schema support must be tested on the deployed endpoint. These can be different engines: for example, use a tool-capable model for the reviewer and configure a separate default inspection engine. Selecting an image-capable model as the agent model alone does not establish tool-calling support.

`InspectPptx.engine` accepts any accessible image-capable text-model engine ID. An explicit argument overrides `tool_policy.parameter_defaults.InspectPptx.engine` on the reviewer; that configured tool default overrides the deployment property `PPTX_VISION_MODEL_ID`, which overrides the calling agent's model. When relying on that last fallback, the agent model must support both tools and images. The Pixel reactor has no calling agent model, so it requires `engine` or `PPTX_VISION_MODEL_ID`. Unknown legacy model capabilities are validated by the provider; known incompatible vision engines fail before rendering or inference.

Create an agent named **PPTX Reviewer**, using [pptx-reviewer-prompt.txt](pptx-reviewer-prompt.txt) as its system prompt. Enable default agent tools. Disable WriteFile, EditFile, MultiEdit, MoveFile, DeleteFile, BashCommand, ExecuteNodeCode and TodoWrite in `tool_policy.default_tools.disabled`. Set `tool_policy.result_tool` to `"InspectPptx"`. The Semoss harness persists that tool result verbatim as the final response and ends the run without another reviewer-model call or reflection. The result tool must be invoked alone in a batch. This prevents model-generated coverage summaries and reviewer retry loops. Give it no MCP projects or skills; InspectPptx and read-only file tools are sufficient. Suggested budgets: 3 tool rounds, 0 reflections and 900 seconds. Prevent further delegation with `spawn_policy.max_subagents_per_run: 0`.

Attach the reviewer workspace to the authoring agent's `CONFIG_JSON.subagents` array as `{"workspaceId":"<reviewer-workspace-id>"}`. The named tool alias is derived from the agent name (`agent_pptx_reviewer`). Named subagent calls currently take a `prompt` string and `inherit_parent_workdir`; send the task details in the prompt and set `inherit_parent_workdir: true` so the tool reads the author's saved file.

For the dedicated-reviewer workflow, add `InspectPptx` to the author's `tool_policy.default_tools.disabled` list, while leaving it enabled on the reviewer. This prevents the author from first calling the inspection service with its own text-only model. The model selector in the reviewer's agent settings controls its tool-calling model; a separate vision engine is selected by the explicit `engine` argument, the reviewer's configured tool default, or the deployment default. Include an exact requested vision ID in the delegation prompt; the reviewer forwards it unchanged.

The compiled Java changes require an application reload. If deployment replaces default tools using `AGENT_DEFAULT_TOOLS_MCP_ID`, expose the InspectPptx reactor through that MCP instead of relying on the built-in handler.

## Calls and results

Agent tool arguments:

```json
{
  "filePath": "proposal.pptx",
  "slides": [3],
  "instructions": "Check chart-label readability and clipped text. Preserve the existing design.",
  "context": "Executive audience; use the supplied corporate template.",
  "checkConsistency": false
}
```

Omit `slides` to inspect all slides, including hidden slides. `slides` must be unique, one-based integers; original order is retained. Detail inspection sends one full-resolution slide image per request. `checkConsistency` defaults to true and adds a comparison using one labelled contact sheet containing all selected slides. Every model request contains just one image, including for decks larger than 16 slides, so endpoints with a one-image limit are supported. The overview cannot substitute for detail inspection. A deck with N selected slides normally uses N detail requests plus one comparison request.

Equivalent Pixel:

```text
InspectPptx(filePath="proposal.pptx", instructions="Check chart labels and clipping", slides=[3], engine="<vision-engine-id>");
```

Pixel resolves `filePath` inside the requested `space` (current insight/room by default) and requires edit access because it writes review artifacts there. The built-in agent tool resolves it inside the actual agent working directory, including inherited parent workspaces. Both reject paths outside the authorized root.

The report includes `status` (`complete`, `partial`, `failed`), `verdict` (`pass`, `needs_changes`, `inconclusive`), `sourceHash`, requested/reviewed/unreviewed/inconclusive slide numbers, consistency-review status, issues, limitations, errors, model token usage, inference-room IDs, and artifact paths. Each issue has slide, severity (major/minor), category, location, evidence and suggestedFix. In report schema version 2, coverage is computed from completed detail calls. Per-image model responses contain `observations` describing visible content, `assessment` (`reviewed` or `inconclusive`), `issues`, and `limitations`; the application attaches original slide IDs. Comparison issues use a separate schema with slide IDs constrained to the selected set. The same schema is included in the prompt and enforced during generation. Strict parsing rejects duplicate keys, extra fields and invalid types. Comparison failure or uncertainty cannot yield a pass. Invalid model JSON, missing coverage, provider failure, or changes to the source file cannot yield a pass.

Invalid responses and transient provider errors get at most one local retry, with at most three extra calls per inspection. Transient retries wait one second. Exhausted slide failures preserve completed results and continue unaffected slides. Authentication, invalid/unsupported-schema and other configuration errors stop immediately; there is no unconstrained fallback. `attempts` records stage, slide, outcome, failure kind and duration; `failures` and `errors` contain unrecovered failures, separate from visual `issues`. `usage.retryCalls` counts retries. A clean five-slide deck uses six model calls.

Reports and images live below `.pptx-review/review-<uuid>/` in the working directory. Model calls use separate stateless rooms linked to the calling room, with the images copied into those rooms as normal SEMOSS media attachments. Their requests and usage follow the standard Room/model logging path. Tool-internal model usage is also returned in the report; it is not counted as additional authoring-agent turns.

## Rendering invariants and limits

The source is snapshotted and hashed before conversion. Hidden slides are made visible only in a temporary rendering copy by changing their OOXML visibility attribute; all other package entries are retained. The source file is never rewritten. The PDF must contain exactly one page per original slide or inspection fails. PDFBox renders only selected pages, with a maximum image edge of 1600 pixels.

PDFs are cached by source hash, service address, rendering profile and `PPTX_RENDER_CACHE_VERSION`. Cache entries expire after 24 hours and their digest is checked before reuse. PNGs are rendered fresh for each review. Bump `PPTX_RENDER_CACHE_VERSION` after changing LibreOffice or fonts. Review artifacts remain with their owning room; normal room retention applies. They are not automatically deleted on a passing review.

Limits: 50 MiB compressed source, 500 MiB expanded package, 10,000 ZIP entries, 100 selected slides per call, 2,000 output tokens per vision request, 200,000 reported vision tokens per inspection, and a cooperative 10-minute inspection deadline. The token limit is checked between calls; the final call can cross it. The deadline is checked between operations; provider transport timeouts also govern in-flight model calls. Cancellation propagates rather than producing a successful report.

This inspects a static LibreOffice rendering. Install the intended fonts on the rendering service. PowerPoint-specific rendering, animation, editability and exact font metrics require other checks. A screenshot cannot verify factual claims without supporting source material.

## Authoring workflow and verification

The opt-in managed authoring workflow below makes structural validation, review and bounded repair code-owned transitions. Agents without that configuration can still use InspectPptx or delegate manually. A review of selected slides cannot establish a whole-deck pass for an edited file. Provider failures and incomplete reviews must be disclosed separately from slide defects.

`PptxInspectionUnitTests` covers original slide numbering and hidden slides, source preservation, cached PDF reuse and invalidation, corrupted cache recovery, PDF page mismatches, invalid selections, path escapes, actual image delivery to the vision boundary, instruction propagation, malformed coverage, provider failure, uncertain results, cross-slide comparison, single-image endpoints including decks larger than 16 slides, concurrent edits, token limits, cancellation and model capability validation. Nine captured invalid responses from run `01a0abd7-fdde-792c-9396-61dff61f3e3b` are replayed as regression fixtures. Tests also cover bounded retries and direct final report delivery. The offline `test/prerna/util/pptx/fixtures/check_provider_wire.py` test verifies the SEMOSS Python client forwards strict JSON Schema and exact image bytes. A live integration check must verify strict schema and image bytes on the provider wire, then distinguish a clean deck from a deliberately clipped slide; valid JSON alone is not evidence of accurate visual review.

## Tool-choice errors from vLLM

An error saying `"auto" tool choice requires --enable-auto-tool-choice and --tool-call-parser to be set` means the agent's model endpoint has not enabled automatic tool calling. It can occur before InspectPptx executes. Use a tool-capable model for the reviewer and select the image-only model with `engine`, or configure the serving endpoint with the required flags and an appropriate parser for that model. See [vLLM tool-calling configuration](https://docs.vllm.ai/en/latest/features/tool_calling/#automatic-function-calling). Setting `tool_choice=none` on the reviewer would prevent it from invoking the inspection tool. The internal vision requests contain images and instructions, with no tool definitions or tool choice.


## Managed authoring workflow

Merge these fields into the PPTX author's existing configuration, preserving its model and other settings:

```json
{
  "pptx_workflow": {
    "enabled": true,
    "reviewer_alias": "agent_pptx_reviewer",
    "repair_turns": 6,
    "review_timeout_seconds": 600
  },
  "tool_policy": {
    "read_only_paths": [".claude/skills/pptx", ".semoss/pptx-workflow"],
    "default_tools": { "disabled": ["InspectPptx", "ExecuteNodeCode"] }
  },
  "spawn_policy": { "max_subagents_per_run": 2, "max_spawns_per_turn": 1 }
}
```

Attach the regular reviewer workspace in `subagents`. Use [pptx-author-workflow-prompt.txt](pptx-author-workflow-prompt.txt) as the managed author prompt. This configuration applies only to the Semoss harness and is disabled by default. The regular reviewer remains independently usable with any accessible compatible vision engine; its model and configured default are unchanged.

The managed harness exposes a run-owned tool:

```json
{
  "generator": "build-deck.js",
  "filePath": "proposal.pptx",
  "expectedSlides": 5,
  "instructions": "Check readability, clipping and consistency; preserve the user's template.",
  "engine": "optional-exact-caller-supplied-vision-engine-id"
}
```

Call `BuildPptx` alone. Omit `engine` to retain the reviewer's configured default. The tool reads and executes the generator afresh, requires an actual save to the requested file, and independently runs the packaged structural validator. A returned generator value or model-written completion claim cannot substitute for validation. The output filename, generator and slide count remain fixed during repairs. Managed builds support 1–100 slides.

`PptxWorkflow` enforces these transitions:

1. **Save and validate.** Hash the saved PPTX and generator; verify requested count and structural validity. A structural failure allows one bounded repair attempt. A successful save sends advisory warnings directly to review before another author request.
2. **Review.** Spawn the configured named reviewer with the saved file, original brief, explicit criteria, selected original slide numbers and optional exact engine. Wait in code and verify report status, source hash, filename, count and coverage. A provider/setup error, changed source, inconclusive result or missing coverage leaves the review incomplete. It never triggers layout repair. An inconclusive overview gets one retry with the same image, without repeating individual slide checks. That retry shares the existing per-image two-attempt limit, deck-wide three-retry limit and token/time budgets. Both assessments are retained in the attempt history; continued uncertainty remains inconclusive.
3. **Repair.** Significant findings permit one visual repair pass, capped at `repair_turns` author tool rounds (default six, configurable 2–12). The author edits its generator and calls BuildPptx again. At the repair/tool-turn limit or when the author stops, the controller reserves one final build of generator code changed since the last attempt. This uses the existing build/review timeouts, consumes no additional author model turn, never retries unchanged broken code, and never opens another repair cycle. There are at most two reviews. Rechecks cover the reported slides plus any other changed slide XML; changes to shared images, charts, layouts, themes or other presentation resources conservatively expand coverage to the whole deck.
4. **Deliver.** The harness writes a final response from verified artifact and review evidence without requesting another model response. A structurally validated saved deck finishes as COMPLETED even if later refinement, review, a model call, or a runtime/turn limit prevents further work. The controller verifies or restores that file before delivery. Outstanding problems carry workflow status `complete_with_warnings`, phase `delivered_with_warnings`, an explicit final-text warning, and a separate `reviewOutcome`. Failed or incomplete review never claims a visual pass, and unbuilt generator edits are explicitly excluded. This preserves the existing app's COMPLETED-only room-to-user import without an app change. Minor findings remain disclosed on ordinary delivery. If neither the requested file nor its recovery copy can be verified, the run remains FAILED. Cancellation and input-required states retain their existing semantics.

A failed rebuild or source mutation restores the last validated saved copy when one exists. Generator edits after that build are explicitly excluded from delivery. State, hashes, review history and recovery copy are stored under `.semoss/pptx-workflow/<run-id>/`. A resume retains workflow budgets and evidence; an interrupted build/review is not silently accepted or retried. Cancellation propagates to the child reviewer. Waiting has a configured deadline and does not consume extra author model turns.

Workflow state and progress include `artifact` (availability, relative file path, hash, slide count and structural validation), `reviewOutcome` (verified completion/verdict plus reported status and coverage), `lastBuildError`, `finalBuildAttempted`, and `finalBuildTimeMs`. Final-build time includes its review and is separate from ordinary tool-call timings. Run records and polling snapshots expose available files in `artifacts`, the separate `reviewOutcome`, and outstanding `warnings`. These describe evidence captured at delivery, not an ongoing filesystem guarantee. The final message includes the actual failure or inconclusive reason and puts significant findings first. Missing native PowerPoint/font/editability verification must not prevent a static visual assessment. Ordinary observations and optional embellishments are not defects; review suggestions must not invent supporting statistics.

File-edit tools protect packaged helpers and workflow evidence, including ancestor moves and symlink aliases; ReadFile can still inspect them. This is an authoring guard, not an OS sandbox for arbitrary generator code. The helper accepts flat component coordinates and `geometry: {x,y,w,h}`; explicit flat values win, and malformed nested geometry fails clearly. The cover background accepts either a color string or the native `{color: "1A7F5A"}` form; malformed values fail with an explicit helper error.

`PptxWorkflowTest` covers immediate review despite warnings, direct delivery without another author request, minor/major outcomes, final-turn correction builds, failed-refinement recovery, model-continuation failure through the actual harness, cancellation, one recheck, expansion of changed-slide coverage, invalid report identity/coverage, unavailable providers, structural failures, unsaved edits, literal tool markup and path containment. `WorkflowHardeningTest` exercises the real ReadFile and mutation handlers on protected resources. JavaScript tests inspect saved caption coordinates and cover background XML.

Generic agents retain the existing `finishing_turns` guidance behavior. Managed PPTX runs use the controller's completion evidence instead of relying on the generic last-turn text response.

### Runtime reminders and prefix caching

The Semoss harness captures the composed system prompt once per run. Changing turn counts, phases, repeated-failure reminders and PPTX repair budgets are appended as `[SEMOSS runtime status]` text in the new input, rather than interpolated into the system prompt. Initial and reflection inputs retain their original UI text. After a completed tool batch, the note is a trailing TEXT part after every TOOL_RESULT in the hidden tool-result input; the provider adapters keep it at the conversation tail. Resumed tool continuations use the same path. Earlier messages and earlier reminders remain unchanged, and retrying the same pending continuation does not duplicate an identical reminder.

This preserves the shared request prefix across ordinary tool rounds while retaining the existing backend limits. It removes the countdown's cache invalidation; actual cache hits still depend on the serving backend, routing, eviction, and other prompt changes such as compaction or model/tool configuration changes. It does not enable provider caching or guarantee a particular latency improvement.

`RoomRuntimeContextTest` verifies the real Room continuation preserves prior parts and tool results, includes status only after a complete batch, and avoids duplicate status on retry. It can export Java provider-history JSON using `-Dsemoss.runtimeContextFixture=<path>`. Run `test/prerna/reactor/agent/runtime/check_runtime_context_wire.py` with `SEMOSS_RUNTIME_CONTEXT_FIXTURE=<path>` to verify stable system content, tool definitions and earlier conversation prefixes through the Chat Completions, Responses, Anthropic, Bedrock and Google adapters. Without that variable, it uses an equivalent built-in fixture. A negative-control test confirms that putting the countdown back into SYSTEM changes the request prefix.

## Run progress

`GetAgentRun` and streaming snapshots include optional `progress` for instrumented Semoss-harness runs. A `kind: "progress"` stream item replaces the previous progress item for that run. Fields include phase, current activity/tool, tool rounds remaining, model-call count/time, summed tool time, tool wall time (overlapping tools counted once), active elapsed time, tool failures, and repeated failures. Measurements persist in nullable `AGENT_RUN.PROGRESS_JSON` on completion, failure, cancellation and pause. Existing runs have no backfilled measurements.

Managed runs also include `progress.workflow` with phase, artifact identity, build/review counts and an incomplete reason. The BuildPptx parent tool time includes building and waiting for its reviewer; the child retains its own timing and model logs.

Model time excludes tool execution. InspectPptx's internal vision requests count within the reviewer's tool time; the report contains their separate usage. Parent and child measurements are separate. Human-input pause time is excluded from active execution time.

The standard playground chat displays progress and keeps partial results and errors when a submitted run fails. The Microsoft SEMOSS application is unchanged. File-based deployments must apply the additive column migration (or their standard schema updater) before loading the new backend classes.
