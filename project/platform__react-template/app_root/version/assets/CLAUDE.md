---
name: semoss-sdk
description: Use the @semoss/sdk JavaScript/TypeScript package to talk to a SEMOSS app server — manage the Insight workspace, execute Pixel commands, stream model output, upload/download files, run Python, and log in/out. Use whenever a task involves `Insight`, `runPixel`, a Pixel expression, or the SEMOSS app server.
---

Use when compiling, building, or publishing the React app after making changes to source files — at the end of any turn that edited client code, or when the user asks to build, rebuild, or deploy. Invoke the BuildAndPublishApp tool with the project id. Do not attempt to run node, npm, pnpm, or any JavaScript build command via Bash — the sandbox blocks node execution, and BuildAndPublishApp is the only supported build path.

# @semoss/sdk

`@semoss/sdk` is the client SDK for a SEMOSS app server. Every interaction ultimately runs through **Pixel**, the server's command language, sent via either:

- **`runPixel(pixel, insightId?)`** — top-level function from `@semoss/sdk`.
- **`insight.actions.run(pixel)`** — method on an `Insight` instance. Reuses the insight's `insightId` automatically.

Both are equivalent. Use whichever is more ergonomic — `actions.run` inside a React component that already has `useInsight()`, `runPixel` from standalone code or utilities.

For React apps, `@semoss/sdk/react` adds `<InsightProvider>`, `useInsight()`, and `usePixel()` on top of the same core.

---

## Where to find what

ALWAYS REFER TO THE SKILL FILES WHEN IMPLEMENTING A TASK THAT REQUIRES A MODEL, DATABASE OR VECTOR. THE SPECIFIC OUTPUT SCHEMA AND PARAMETERS ARE LISTED IN EACH SKILL FILE. DO NOT GUESS THE SCHEMA OR PARAMETERS, ALWAYS USE THE SKILL FILES.

Task-specific patterns live in dedicated skills. Use them instead of reimplementing from scratch:

| Task                                                                          | Skill               |
| ----------------------------------------------------------------------------- | ------------------- |
| Pick which engine ID to use for a _new_ engine call (MODEL, DATABASE, VECTOR) | `selected-engines`  |
| Query a relational or graph database (SELECT, INSERT, schema)                 | `database`          |
| Call an LLM, pass images, manage conversation history                         | `model`             |
| Semantic search, RAG, embed/remove documents                                  | `vector`            |
| Build and publish the React app                                               | `build-and-publish` |

This file covers what those skills don't: the Insight lifecycle, the Pixel response envelope, streaming, files, Python, MCP, auth, and React plumbing.

---

## Core concepts

- **Insight** — temporal workspace on the server. You need one to run any command. Its ID lives at `insight._store.insightId`.
- **Pixel** — the server's command language. A Pixel expression is a string like `LLM(engine="<id>", command=["hi"]);` or `MyEngines(engineTypes=["MODEL"]);`. Multiple statements are separated by `;`.
- **Engine** — a configured resource on the server: a model, database, storage, or vector store. Every engine has a UUID (`engine_id`) that you pass into Pixel calls. **When introducing a new engine call, consult the `selected-engines` skill for the project's selected engine IDs — do not guess or hardcode.**

---

## Initialize

### Plain JS/TS

```ts
import { Insight } from "@semoss/sdk";

const insight = new Insight();
await insight.initialize();

const insightId = insight._store.insightId;
```

`initialize()` optionally takes `{ app?, python?, disableRoom?, insightId? }`. Pass `app` to load a specific app's reactors, `python` to preload Python code, or `insightId` to reconnect to an existing session.

### React

```tsx
import { InsightProvider, useInsight } from "@semoss/sdk/react";

function App() {
  return (
    <InsightProvider>
      <Chat />
    </InsightProvider>
  );
}

function Chat() {
  const { isReady, actions } = useInsight();
  if (!isReady) return <p>Loading…</p>;
  // call actions.run(...), etc.
}
```

---

## The Pixel response envelope

Every `runPixel` / `actions.run` call returns the same shape:

```ts
{
  errors: string[];          // non-empty ⇒ something failed
  insightId: string;
  pixelReturn: Array<{
    pixelId: string;
    pixelExpression: string;   // the parsed pixel the server actually ran
    isMeta: boolean;
    timeToRun: number;
    operationType: string[];   // e.g. ["OPERATION"], ["LLM_EXECUTION"], ["ERROR"]
    output: unknown;           // shape depends on the Pixel — type it via generics
  }>;
}
```

One entry in `pixelReturn` per `;`-separated statement in your Pixel string. The shape of `output` depends on which pixel ran — see the relevant skill.

### Type-safe outputs

```ts
const { pixelReturn } = await runPixel<[{ response: string }]>(
  `LLM(engine="${MODEL_ID}", command=["${prompt}"]);`,
);
const text: string = pixelReturn[0].output.response;
```

For multiple statements, the generic is a tuple with one entry per `;`:

```ts
runPixel<[{ response: string }, Array<{ engine_id: string }>]>(
  `LLM(engine="${MODEL_ID}", command=["hi"]);
   MyEngines(engineTypes=["MODEL"]);`,
);
```

---

## Error handling

Always check `errors` first, then `operationType`:

```ts
const { errors, pixelReturn } = await runPixel(pixel);

if (errors.length) throw new Error(errors[0]);

if (pixelReturn[0].operationType.includes("ERROR")) {
  throw new Error(`Pixel failed: ${pixelReturn[0].output}`);
}
```

**Do not swallow errors in empty `catch` blocks.** Surface them with `console.error` at minimum — silent failures are the single most common cause of dashboards that "just show `--`".

---

## Streaming LLM output

For token-by-token streaming, pair `runPixelAsync` with polling `partial`:

```ts
import { runPixelAsync, partial } from "@semoss/sdk";

async function askWithStream(
  question: string,
  onUpdate: (text: string) => void,
) {
  let collecting = true;

  const poll = async () => {
    if (!collecting) return;
    try {
      const { message } = await partial(insight._store.insightId);
      if (message?.total) onUpdate(message.total);
    } catch {
      /* noop */
    }
    setTimeout(poll, 1000);
  };

  setTimeout(poll, 500);

  const { errors } = await runPixelAsync(
    `LLM(engine="${MODEL_ID}", command=["${question}"]);`,
    insight._store.insightId,
  );

  collecting = false;
  if (errors.length) throw new Error(errors[0]);
}
```

See the `model-engine` skill for non-streaming LLM calls, conversation history, and image inputs.

---

## React hooks

### `useInsight` — access the insight inside components

```tsx
const { isReady, isAuthorized, actions, insightId } = useInsight();

// run any pixel via actions.run — same envelope as top-level runPixel
const { pixelReturn } = await actions.run(`MyEngines(engineTypes=["MODEL"]);`);
```

### `usePixel` — declarative one-shot query

```tsx
import { usePixel } from "@semoss/sdk/react";

function Models() {
  const { status, data, error, refresh } = usePixel
    Array<{ engine_id: string; engine_name: string }>
  >(`MyEngines(engineTypes=["MODEL"]);`);

  if (status === "LOADING") return <p>Loading…</p>;
  if (status === "ERROR") return <p>{error?.message}</p>;

  return <ul>{data.map((m) => <li key={m.engine_id}>{m.engine_name}</li>)}</ul>;
}
```

`usePixel` unwraps the envelope for you — `data` is `pixelReturn[0].output` directly.

---

## Files, Python, MCP

```ts
// Upload into the current insight
await insight.actions.uploadInsight(path, file);

// Download from the current insight
await insight.actions.download(path);

// Run Python in the insight's Python process
await insight.actions.runPy(code);

// Call an MCP tool
const { output } = await insight.actions.runMCPTool("tool_name", {
  param1: "value",
});
```

---

## Auth

```ts
await insight.actions.login({
  /* provider-specific payload */
});
await insight.actions.logout();
```

In production, the server injects config — do not call `Env.update`. Only use `Env.update` for local dev with access/secret keys.

---

## Cleanup

```ts
await insight.destroy();
```

---

## Quick reference

Only what this file covers. For engine-specific pixels, see the relevant skill.

| Task                                     | Call                                                         |
| ---------------------------------------- | ------------------------------------------------------------ |
| Create workspace                         | `const insight = new Insight(); await insight.initialize();` |
| Run any pixel (standalone)               | `runPixel(\`...\`)`                                          |
| Run any pixel (inside an Insight)        | `insight.actions.run(\`...\`)`                               |
| Run any pixel (inside a React component) | `const { actions } = useInsight(); actions.run(\`...\`)`     |
| Stream an LLM                            | `runPixelAsync(...)` + poll `partial(insightId)`             |
| Upload file                              | `insight.actions.uploadInsight(path, file)`                  |
| Download file                            | `insight.actions.download(path)`                             |
| Run Python                               | `insight.actions.runPy(code)`                                |
| Run MCP tool                             | `insight.actions.runMCPTool(name, params)`                   |
| Log in / out                             | `insight.actions.login({...})` / `insight.actions.logout()`  |
| Destroy                                  | `insight.destroy()`                                          |

---

## Rules of thumb

1. **One Insight per session.** Reuse it; don't spin up a new one per request.
2. **Check `errors` before `pixelReturn`.** A non-empty `errors` array means the pixel didn't execute.
3. **Never swallow errors.** Empty `catch` blocks hide the real failure cause.
4. **`pixelReturn[i].output` shape is pixel-specific.** See the relevant skill.
5. **Don't call `Env.update` in production.** The server injects config.
6. **Before introducing a new engine call (MODEL, DATABSE, VECTOR), consult `selected-engines`.** It lists the engine IDs the project is configured for. Skip this when editing existing calls — the engine ID is already in the code; preserve it.
