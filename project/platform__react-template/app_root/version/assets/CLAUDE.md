# SEMOSS React App Template

This file is deliberately kept byte-for-byte identical to its companion
agent-instruction file. It is the working contract for an App Builder coding
agent operating on a cloned app.

## Scope

- You are operating on the current cloned project. It originated from the
  `react-template` platform template; this project is the editable target for
  the user's requested feature.
- Your working directory is the cloned project's `assets/` folder. Edit React
  source in `client/`; generated browser files are written to `portals/`.
- Do not hand-edit generated files in `portals/` unless the request explicitly
  asks for a no-build static app.

## SEMOSS runtime

- A SEMOSS app is a project. Its project ID identifies the cloned app and
  scopes Pixel calls, permissions, and published portal files.
- Do not write or hardcode the project ID in client source or `client/.env`.
  Publishing injects the project ID through `semoss-env`. For direct local
  development only, a developer may provide `APP` in `client/.env.local`.
- Use `@semoss/sdk` and `@semoss/sdk/react` for server calls. Reuse the
  `InsightProvider` and the app context instead of creating an insight per
  component.
- Pixel is the server command language. Treat engine IDs and Pixel result
  shapes as server-authoritative; inspect available engines/tools before
  adding an integration. Do not invent MCP tools or placeholder tool results.

## Build and publish

- After editing `client/`, invoke
  `BuildAndPublishApp(project="<current-project-id>")` through the App Builder
  workflow.
- That is the supported build path for this agent: it builds the React source,
  replaces `portals/`, publishes it, and releases the project. Do not run
  Node, npm, or pnpm through Bash from the agent sandbox.
- Report the build/publish result. Success proves that assets were built and
  published; do not claim the UI behavior was verified unless it was actually
  opened and tested.

## Safe implementation defaults

- Keep user-visible errors actionable. Check Pixel errors before reading
  `pixelReturn` and never silently discard failures.
- Keep secrets out of source, `.env`, and generated portal assets. Do not add
  access keys to client-side variables.
- Prefer a small, task-specific UI and only add dependencies that it imports.
- If the request needs a model, database, vector engine, storage, or a new
  backend tool, first inspect the app's configured resources and ask for
  direction when no approved resource exists.

## User-facing UI

- Before editing, identify the primary user task, intended audience, and the
  one flow that must work. Reuse the template's existing components and keep a
  consistent type, spacing, color, and radius system; do not imitate a named
  product or brand unless the user asks for it.
- Deliver a complete, polished, professional interface appropriate to the
  request, not a wireframe or a generic component showcase. Make ordinary
  visual and interaction decisions yourself and build the app on the user's
  behalf; do not interrupt for routine choices such as layout, spacing, color,
  labels, or component arrangement.
- Every prominent control must work, be disabled with an explanation, or be
  visibly labelled as a demo or coming soon. Do not present local parsing,
  in-memory state, file selection, or mocked data as a model call, upload, or
  durable submission. State that limitation in the UI, not only in the final
  response.
- Derive dashboard metrics, counts, and filters from the same state shown in
  the UI. Use the current date or visibly label a fixed demo date; never let a
  successful user action leave summary values stale or contradictory.
- Use semantic interactive elements, labelled inputs, keyboard-visible focus,
  actionable empty/loading/error states, and responsive layouts. Keep normal
  reading text comfortably legible; reserve very small text for secondary
  metadata only.
