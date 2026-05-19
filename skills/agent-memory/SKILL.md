---
name: agent-memory
description: Use BEFORE starting a non-trivial task to recall prior lessons, and IMMEDIATELY when the user corrects you, accepts a non-obvious approach, or you hit a subtle gotcha — to persist the lesson for future sessions. Covers SearchMemories, GetMemory, SaveMemoryCandidate, and MarkVerified from the Agent_Memory MCP. Skip entirely if no Agent_Memory MCP is attached to the current app.
---

# Agent Memory

A curated, cross-session lessons store for SEMOSS agent harnesses. You READ from it at the start of a task to avoid repeating past mistakes, and you WRITE to it in the same turn you learn something reusable. Writes go to a human-reviewed queue — they are not searchable until promoted.

**Availability:** this skill is only useful when the current app has the `Agent_Memory` MCP attached (tag `MCP`, tools `SearchMemories` / `GetMemory` / `SaveMemoryCandidate` / `MarkVerified`). If those tools are not available, skip this skill.

---

## When to call each tool

### SearchMemories — CALL FIRST

At the **start** of any non-trivial task (bug fix, new feature, refactor). Query with the problem description plus exact facets for boosted matches.

```
SearchMemories(
  query="vite build fails with ESM resolver on dynamic imports",
  project_id="<current-app-uuid>",
  file_paths=["vite.config.ts"],
  framework_tags=["vite", "pnpm"]
);
```

Rules:

- Pass `project_id` so project-scoped lessons outrank generic builder lessons.
- Pass `file_paths` / `framework_tags` for exact-match boosts — these are deterministic rerank signals, not just filters.
- Results are compact summaries. Only call `GetMemory` for ones that look directly relevant to the current task.
- **Verify before acting.** If a recalled memory names a file, function, or flag, check it still exists before relying on it. Code rots; memories don't auto-expire.

### GetMemory — only when a summary looks relevant

```
GetMemory(memory_id="mem_...");
```

Pulls `detail`, `how_to_apply`, and `evidence`. Not for browsing — only when the summary genuinely applies to what you're about to do.

### SaveMemoryCandidate — CALL IMMEDIATELY on a lesson

This is the behavior that breaks most often. Save in the **same turn** you learn the lesson. Do not defer. Do not say "I can save this if you want" — just save it.

**Triggers:**

1. User **corrects you**: "no, don't do X" / "use Y instead" / "that's wrong because..."
   → `kind="gotcha"` (a trap) or `kind="preference"` (user/team style).
2. User **accepts a non-obvious approach** without pushback.
   → `kind="pattern"` (validated approach worth reusing).
3. You **hit a subtle trap** worth warning future-you about.
   → `kind="gotcha"`.

```
SaveMemoryCandidate(
  title=["runPixelAsync streams require partial() polling, not runPixel()"],
  summary=["To stream LLM output from a Pixel call, use runPixelAsync() and poll partial(insightId) on a ~1s interval. Calling runPixel() on the same pixel returns only the final value with no progress events."],
  why_it_matters=["Spent a full session debugging a 'frozen' chat UI before realizing runPixel buffers the whole stream — the Pixel itself was fine."],
  kind=["gotcha"],
  scope_type=["builder"],
  file_paths=["src/hooks/useStreamingPixel.ts"],
  framework_tags=["pixel", "runpixelasync", "streaming"],
  how_to_apply=["For any LLM-streaming surface, wire runPixelAsync + partial() polling. Reserve runPixel() for one-shot calls where you only need the final result."],
  confidence=["0.9"]
);
```

Use plain string values in each field above. Do not URL-encode them before passing them into the Pixel call.

Required: `title`, `summary`, `why_it_matters`. Always include `why_it_matters` — the _reason_ lets future-you judge edge cases. The rule alone rots.

**Do NOT save:**

- Ephemeral state — current task, in-progress paths, "what I just built."
- Information already documented in the codebase (CLAUDE.md, AGENTS.md, obvious function names).
- Raw transcripts or secrets in `evidence`. Pointers + short excerpts only.

### Capture the PLATFORM, not the DOMAIN

The single biggest failure mode: saving memories that describe **what you built for this customer** instead of **how the SEMOSS platform behaves**.

**Litmus test:** _Would this memory help someone building a completely different app on SEMOSS?_ If no, it's a task note — do not save.

Save the **mechanism** (the Pixel call shape, the encoding rule, the framework quirk, the tool to run). Drop the **subject** (this app's domain, its specific tables, its KPI layout, its customer).

| Bad (domain)                                                 | Good (platform)                                                                                                                                                                                                                               |
| ------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| "Finance app dashboard uses 4 KPI cards at the top"          | — not a memory, delete                                                                                                                                                                                                                        |
| "Revenue chart filtered by region in the CRM app"            | — not a memory, delete                                                                                                                                                                                                                        |
| "To build the frontend, run `pnpm build`"                    | "To build the frontend, call the `BuildAndPublishApp` tool — do NOT use bash/pnpm/npm directly"                                                                                                                                               |
| "In this app the customers table needs an index on email"    | — not a memory, delete                                                                                                                                                                                                                        |
| "Had to wrap the finance SQL before passing it"              | "Dynamic SQL interpolated into a `SqlQuery()` Pixel call should be passed plainly. Do **not** also URL-encode the SQL; doing both produces literal escape sequences in the executed pixel and the database rejects it as a SQL syntax error." |
| "Loaded the model list into a dropdown on the settings page" | "List models with `MyEngines(engineTypes=[\"MODEL\"])` — returns `engine_id`, `engine_name`, `engine_type`, `engine_subtype`"                                                                                                                 |
| "Streaming LLM output into the chat window"                  | "Stream LLM output via `runPixelAsync(...)` + polling `partial(insightId)` every ~1s — do not call `runPixel` for streams"                                                                                                                    |

**Rule of thumb for the `title` field:** start it with a Pixel call name, a file/framework name, or a tool name — not a domain noun. `"SqlQuery raw-string handling for dynamic SQL"` ✅. `"Finance dashboard filter logic"` ❌.

**Rule of thumb for `framework_tags`:** these should be platform primitives (`pixel`, `sqlquery`, `llm`, `vite`, `runpixelasync`, `mcp`, `insight`), not business domains (`finance`, `crm`, `customer`).

If you catch yourself writing "in this app..." or "for the customer..." in the body, stop and either rewrite it platform-first or drop it.

**Scope defaults:**

- `scope_type="project"` + `scope_id=<app-uuid>` — this app only. Use for app-specific gotchas.
- `scope_type="builder"` (omit `scope_id`) — generic lesson useful across SEMOSS apps.
- `scope_type="user"` + `scope_id=<user-id>` — per-user preference.

### MarkVerified — only after actually using a memory

```
MarkVerified(memory_id="mem_...");
```

Call after you **just used** a recalled memory this turn and confirmed the files / functions it names still exist and the guidance still held. Bumps `last_verified_at`, which boosts the memory in future rerank. Do **not** call speculatively, in bulk, or on memories you merely looked at.

---

## Quick decision flow

```
Starting a task?
  → SearchMemories(query=..., project_id=..., file_paths=..., framework_tags=...)

  Relevant summary?
    → GetMemory(memory_id=...)

  Used the memory successfully this turn?
    → MarkVerified(memory_id=...)

Learned a reusable lesson this turn?
  → SaveMemoryCandidate(...) NOW, same turn, before responding to the user.
```

---

## Discipline

- **Save in the same turn you learn it.** Deferring = forgetting.
- **`why_it_matters` is not optional.** Future-you needs the reason to judge edge cases.
- **Verify before acting on a recalled memory.** Code moves; memories don't.
- **Never save ephemeral state.** Memory is for durable, reusable lessons — not activity logs.
- **Scope project-first.** If the lesson is app-specific, use `scope_type="project"` so it doesn't pollute the global builder scope.
- **Evidence is a pointer, not a dump.** No transcripts. No secrets.

---

## Related

- Full project docs: `project/Agent_Memory__08621bde-8f32-4f75-aeae-2c2fcdb6a8ae/app_root/version/assets/CLAUDE.md`
- Review UI: open the `Agent_Memory` app to promote / reject pending candidates.
