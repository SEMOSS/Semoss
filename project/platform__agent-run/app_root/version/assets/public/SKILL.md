---
name: agent-run
description: Use when writing code in an app that starts, streams, or resumes a durable autonomous agent run. Covers runAgent (RunAgent), subscribeRunAgent/pollAgentRun for live streaming, getAgentRun for reconciliation after a page reload, decideAgentRunAction/submitAgentToolDecision for resuming a run paused on a human tool decision, and getSubagentRuns for subagents the run spawned, all via @semoss/sdk. Do not use for a single one-shot chat turn (see room/model).
---

# Agent Run

An **agent run** is a server-driven, multi-turn agentic loop (`RunAgent`) — the model calls tools, gets results, and keeps going on its own until it produces a final answer, hits a limit, or pauses for a human decision. This is different from a plain `LLM()` turn (see the `room` skill), which is a single request/response with no autonomous looping. All agent-run calls go through `@semoss/sdk`'s dedicated functions, not raw `runPixel` — they wrap `RunAgent`, `GetAgentRun`, `GetSubagentRuns`, and `RunMCPTool`.

A run is identified by its own `runId` (also its `jobId`, the model-facing handle) and writes its messages into a normal room (`roomId`), so `GetPlaygroundMessages` on that room shows the same message history a plain chat turn would.

## Usage — start and stream a run

```typescript
import { runAgent, subscribeRunAgent } from "@semoss/sdk";

const { runId } = await runAgent(
  {
    roomId: "<room-uuid>",
    command: "Research the top 3 competitors and summarize their pricing.",
    harnessType: "semoss",
  },
  insightId,
);

const subscription = subscribeRunAgent(runId, {
  onEvent: (event, items) => {
    // event.type: "item.started" | "item.updated" | "item.completed"
    // items.itemsById / items.itemOrder hold the accumulated view — see "Item events" below.
  },
  onSnapshot: (snapshot) => {
    // snapshot.status: SUBMITTED | RUNNING | INPUT_REQUIRED | COMPLETED | FAILED | CANCELLED
  },
  onReconcile: (full) => {
    // Fires once per transition into INPUT_REQUIRED or a terminal status,
    // with full.messages (persisted room messages) included.
  },
});

// later, e.g. on unmount
subscription.stop();
```

`runAgent` always returns immediately with `status: "SUBMITTED"` — it never waits for the run to finish. Stream progress with `subscribeRunAgent`.

## Run lifecycle

### Start — `runAgent` (`RunAgent`)

```typescript
runAgent(
  {
    roomId: string,
    command: string,
    engine?: string,        // defaults to the room's configured model
    harnessType?: string,   // which agent harness runs the loop, e.g. "semoss"
    agentId?: string,       // the agent whose tools/config the run should use
    maxTurns?: number,      // cap on model round-trips before the run stops itself
    maxReflections?: number,
    media?: string[],       // file locations or base64 image/PDF data URIs; any file type the model accepts (image, pdf, document, spreadsheet, audio, video)
    urls?: string[],
  },
  insightId,
);
```

Returns `{ runId, roomId, status }` — a thin handle, not a full snapshot. Fetch `getAgentRun(runId)` if you need the rest immediately.

> `agentId` is sent to the backend as `workspaceId` (a workspace record IS the backend's agent) — use `agentId` here, it's the term callers should reach for.

### Stream progress — `subscribeRunAgent`

Polls a run to completion, handling dedup, ordering, and retry/backoff internally — a transport failure never concludes the run failed.

```typescript
subscribeRunAgent(
  runId,
  {
    onEvent: (event, items) => { ... },     // fires per new item event, deduped, in order
    onSnapshot: (snapshot) => { ... },      // fires on every successful poll
    onReconcile: (full) => { ... },         // once per INPUT_REQUIRED/terminal transition, with persisted messages
    onError: (error) => { ... },            // non-fatal transport error; polling keeps retrying
  },
  {
    pollIntervalMs: 500,                     // default
    inputRequiredIntervalMultiplier: 3,      // slower interval while paused
    signal: abortController.signal,          // stops polling without affecting the run itself
  },
);
```

Returns an `AgentRunSubscription`: `stop()` (stop polling locally, does not cancel the run), `getItems()` (current items-state, for seeding a late-joining renderer), `pokeNow()` (poll immediately instead of waiting out the current interval — use right after an action you know changed the run, e.g. deciding a paused tool call).

If you'd rather drive polling yourself instead of using `subscribeRunAgent`, the lower-level pieces are exposed too: `pollAgentRun(runId)` drains one batch of events plus the current snapshot, and `applyAgentRunItemEvent`/`createAgentRunItemsState` do the same event-accumulation `subscribeRunAgent` does internally, for a custom reducer.

### Reconnect after a page reload

`subscribeRunAgent` only starts from your own `runAgent` call in memory — it does not survive a reload. Persist `runId` alongside whatever message it's tied to, and on reload:

```typescript
import { getAgentRun, subscribeRunAgent } from "@semoss/sdk";

const snapshot = await getAgentRun(runId, { includeMessages: true }, insightId);

if (snapshot.status === "RUNNING" || snapshot.status === "INPUT_REQUIRED") {
  // still going (or paused) — resume streaming
  subscribeRunAgent(runId, handlers);
} else {
  // already terminal — snapshot.finalText / snapshot.errorMessage has the answer
}
```

### Item events

Each `AgentRunItem` is one of four kinds, discriminated by `kind`:

- `"message"` — assistant text. `text` accumulates via `item.updated`'s `delta`, or arrives whole on `item.started`.
- `"reasoning"` — a reasoning/thinking summary, same accumulation pattern as `message`.
- `"tool"` — a tool call. `name`, `arguments`, `status` (`QUEUED` → `RUNNING` → a terminal status), `output`/`error` once it finishes.
- `"subagent"` — a subagent the run spawned (see below). `childRunId`, `alias?` (only for named subagents), `status` (an `AgentRunStatusValue`), `resultPreview?`/`error?` once terminal.

`item.updated` events carry either `delta` (message/reasoning) or `patch` (tool/subagent) — never both.

### Resuming a paused run (human-in-the-loop tool decisions)

When `status` is `INPUT_REQUIRED`, `pendingActions` (from `onReconcile` or `getAgentRun`) lists the paused tool calls awaiting a decision. Resolve one with `submitAgentToolDecision` — it auto-resolves to `"approve"` or `"edit"` depending on whether you pass changed arguments — or `decideAgentRunAction` for the raw `approve`/`edit`/`reject`/`respond` decision:

```typescript
import { submitAgentToolDecision } from "@semoss/sdk";

// unchanged args -> resolves to "approve"; changed args -> "edit"
await submitAgentToolDecision(pendingAction, "submit", editedArgs, insightId);

// or explicitly decline
await submitAgentToolDecision(pendingAction, "reject", undefined, insightId);
```

Either call pokes the run's live `subscribeRunAgent` subscription (if one is active in this session) so it notices the resumed run immediately instead of waiting out `INPUT_REQUIRED`'s slower interval.

### Subagents

A run can spawn subagents — independent agent runs with their own room, delegated a subtask. Live subagent activity arrives as ordinary `kind: "subagent"` items on the **parent's own** stream; no extra subscription is needed while both are active in the same session.

To reconstruct subagent state after a reload (the live stream is ephemeral and doesn't replay), or to inspect a specific subagent directly, use `getSubagentRuns` — durable and DB-backed:

```typescript
import { getSubagentRuns } from "@semoss/sdk";

const subagents = await getSubagentRuns(parentRunId, insightId);
// subagents[i]: { runId, status, input, finalText, errorMessage, alias is NOT included — see note below }
```

`SubagentRunSummary` does not carry an `alias` field — named-subagent aliases are only known live, off the stream item, never persisted. If you need to show a subagent's alias after a reload, capture it from the `kind: "subagent"` item while it's live and store it alongside your own message data.

### Known limitation — cancellation is not reliable yet

The backend exposes a `StopAgentRun(runId=[...])` pixel that requests cancellation, but the underlying interrupt can be silently swallowed while the harness is blocked on a model call, so the run may keep going past the cancel request. Don't build a user-facing "Stop" control on top of it without confirming this has been fixed — the platform Playground app itself currently shows a plain spinner instead of a working Stop button during an agent run, for this reason.

## Response shapes

| Function | Returns |
| --- | --- |
| `runAgent` | `{ runId, roomId, status }` |
| `pollAgentRun` | `{ run: AgentRunSnapshot, events: AgentRunItemEvent[], droppedEvents }` |
| `getAgentRun` | `AgentRunSnapshot & { messages? }` (messages only when `includeMessages: true`) |
| `getSubagentRuns` | `SubagentRunSummary[]` |
| `decideAgentRunAction` | the tool-result string the decision produced |
| `submitAgentToolDecision` | the tool-result string the decision produced |

`AgentRunSnapshot.status` and `SubagentRunSummary.status` are both `AgentRunStatusValue`: `SUBMITTED | RUNNING | INPUT_REQUIRED | COMPLETED | FAILED | CANCELLED`.

## Related

- `room` — plain one-shot `LLM()` turns; use `agent-run` instead when the model needs to autonomously call tools across multiple turns without the caller driving each step.
- `model` — `LLM()` reference for single-turn completions.
