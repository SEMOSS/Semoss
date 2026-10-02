# @semoss/sdk API surface

Every export, and which ones need React. Two entry points:

| Import | Needs React | Contents |
| --- | --- | --- |
| `@semoss/sdk` | no | the entire core: insight, pixel, env, auth, files, agents, permissions, websocket, utilities |
| `@semoss/sdk/react` | yes | re-exports all of the core, plus one provider and eight hooks |

**Everything below is on the core entry unless the section says otherwise.** React appears in
exactly one section, at the bottom. `react` is an optional peer dependency; nothing on the core
entry imports it.

---

## Insight

`Insight` is the `InsightStore` class.

### Getters

| Getter | Type | Meaning |
| --- | --- | --- |
| `insightId` | `string` | id of the insight; empty until created |
| `isInitialized` | `boolean` | system config loaded |
| `isAuthorized` | `boolean` | session exists, or dev keys are set |
| `isReady` | `boolean` | insight created and bound |
| `error` | `Error \| null` | last initialization error |
| `system` | `{ config } \| null` | `logins`, `availableProviders`, `theme`, `systemDate`, plus whatever else `/api/config` returned |
| `defaultTextGenerationModel` | `string` | user meta `text-generation-model`, or `""` |
| `defaultCodeGenerationModel` | `string` | user meta `code-generation-model`, or `""` |

`_store` is private. Use the getters.

### Methods

| Method | Signature |
| --- | --- |
| `initialize` | `(options?: { app?, python?, disableRoom?, insightId? }) => Promise<{ tool } \| null>` |
| `destroy` | `() => Promise<void>` (runs `DropInsight()`) |
| `updateUserDefaultModel` | `(modelName: string, modelId: string) => void` |
| `setUserDefaultModel` | `(meta: Record<string, unknown>) => void` |

`initialize` resolves to `{ tool }` only when it reached the end; it resolves to `null` when the
user is unauthorized **and** when it caught an error, so check `insight.error` rather than
treating `null` as "not logged in".

`initialize` options:

- `app` - project id to bind with `SetContext`. Overridden by `Env.APP` when the `semoss-env`
  tag is present, so in a published build this is redundant.
- `python` - `{ type: "script", script, alias }` or `{ type: "file", path, alias }`, preloaded
  into the insight. Alias defaults to `smss` with a console warning if omitted.
- `disableRoom` - skip `SetRoomForInsight` when running as an MCP tool in a room.
- `insightId` - attach to an existing insight instead of creating one. With no other option set,
  the setup pixel is skipped entirely so the existing frame is not reset.

### actions

All of these run against the insight's own `insightId`, which is the main reason to prefer them
over the bare functions.

| Action | Signature | Notes |
| --- | --- | --- |
| `run` | `<O>(pixel) => Promise<{ pixelReturn }>` | throws on `errors`; throws `"No response"` when the session died |
| `runAsync` | `(pixel) => Promise<{ jobId }>` | pair with `getPixelJobStreaming` |
| `runPy` | `<O>(python) => Promise<{ output }>` | wraps `Py("<encode>...</encode>")` |
| `askModel` | `(engineId, command) => Promise<{ output }>` | one-shot `LLM()`; see the `model` skill for real usage |
| `queryDatabase` | `(databaseId, query, { collect }) => Promise<{ output }>` | `Database \| Query \| Collect`; `collect` defaults to `-1`. See the `database` skill |
| `login` | `(credentials) => Promise<boolean>` | native, ldap, linotp, oauth; rebuilds the insight on success |
| `requestOTP` | `(username, pin) => Promise<boolean>` | linotp step one |
| `logout` | `() => Promise<boolean>` | clears `insightId` and the CSRF token |
| `upload` | `(files, path) => Promise<{fileName, fileLocation}[]>` | **deprecated**, and the only upload that returns the array directly |
| `uploadInsight` | `(path, files) => Promise<{ response, data }>` | array is on `.data` |
| `uploadApp` | `(appId, path, files) => Promise<{ response, data }>` | array is on `.data` |
| `uploadEngine` | `(engineId, path, files) => Promise<{ response, data }>` | array is on `.data` |
| `uploadUser` | `(path, files) => Promise<{ response, data }>` | array is on `.data` |
| `download` | `(path) => Promise<boolean>` | `DownloadAsset` then a browser download |
| `runMCPTool` | `(name, parameters) => Promise<{ output }>` | posts the result to the playground when in tool mode |
| `sendMCPResponseToPlayground` | `(response, status?, executedParameters?) => void` | throws outside an embedded browser |

Two traps in that table:

1. **Argument order.** `upload` is `(files, path)`; every other upload is path-first. A request
   URL containing `path=[object File]` means they were swapped.
2. **Return shape.** `upload` returns the array. `uploadInsight`, `uploadApp`, `uploadEngine`,
   and `uploadUser` return the raw fetch envelope, so the array is `result.data`.

`run` catches `UnauthorizedError` internally, sets `isAuthorized` to false, and then throws
`new Error("No response")`. Treat that message as "session died", not as a transport failure.

---

## Pixel

| Export | Purpose |
| --- | --- |
| `runPixel<O>(pixel, insightId?)` | run a pixel; returns `{ errors, insightId, pixelReturn }` |
| `runPixelAsync(pixel, insightId?)` | submit a pixel; returns `{ jobId }` |
| `getPixelAsyncResult<O>(jobId)` | fetch a finished async result; returns `{ errors, insightId, results }` |
| `getPixelJobStreaming(jobId)` | poll an async job for `content`, `tool`, and `thinking` chunks |
| `partial(insightId)` | **deprecated**; use `getPixelJobStreaming` |
| `console(insightId)` | console output from an insight |
| `getSystemConfig()` | `GET /api/config`; also captures the CSRF token |

`runPixel` throws `Error("Missing Pixel")` on an empty string, and sends the browser timezone as
`tz` alongside the expression. Omitting `insightId` makes the server create a throwaway
contextless insight rather than failing - see SKILL.md.

`getPixelJobStreaming` returns `{ message[], status }`. Each message is one of three shapes,
discriminated by `stream_type`:

| `stream_type` | payload |
| --- | --- |
| `"content"` | `{ content?, finish_reason? }` |
| `"thinking"` | `{ thinking?, finish_reason? }` |
| `"tool"` | `{ index?, id?, type?, function?: { name?, arguments? }, finish_reason? }` - `arguments` arrives as JSON string chunks to concatenate |

`status` is `Created`, `Submitted`, `Canceled`, `InProgress`, `ProgressComplete`, `Streaming`,
`Complete`, `Paused`, `Error`, or `UnknownJob`.

---

## Env

Getters for `APP`, `MODULE`, `ACCESS_KEY`, `SECRET_KEY`, `BEARER_TOKEN`, `BEARER_PROVIDER`,
`CSRF`, `TOOL`, plus `Env.update(partial)`. It is a module singleton, so `Env.update` at module
scope in one file is visible everywhere.

`Env.TOOL` is populated by a window `message` listener the SDK installs on import, in response to
a `SMSS_INIT_TOOL` post from the playground host. Typed as `MCPToolRequest`.

---

## Auth

`login`, `loginLDAP`, `loginOTP`, `confirmOTP`, `oauth`, `logout`.

These authenticate and stop there. Prefer `insight.actions.login`, which also rebuilds the
insight - logging in with the bare function leaves you authorized with no insight, which presents
as a login page that never clears.

`loginOTP(username, pin)` only requests the linotp challenge; `confirmOTP(otp)` completes it.
`oauth(provider)` first checks `/api/auth/userinfo/{provider}` and returns true if already signed
in, otherwise opens a popup on `window.top` and polls it once a second, so it must run inside a
user gesture. `logout` clears `CSRF.token`.

---

## Files

| Export | Signature |
| --- | --- |
| `upload` | `(files, insightId, projectId, path)` -> `{fileName, fileLocation}[]` |
| `uploadInsight` | `(insightId, path, files)` -> `{ response, data }` |
| `uploadApp` | `(appId, path, files, insightId)` -> `{ response, data }` |
| `uploadEngine` | `(engineId, path, files, insightId)` -> `{ response, data }` |
| `uploadUser` | `(path, files, insightId)` -> `{ response, data }` |
| `download` | `(insightId, fileKey)` |

All of them post `FormData` with every file under the field name `file`, to a
`/api/uploadFile/*` endpoint. Do not set `Content-Type` by hand; the SDK leaves it off for
`FormData` so the browser can set the multipart boundary.

`download` behaves differently by environment: in a browser it creates a hidden anchor, clicks
it, and resolves immediately without waiting for the bytes; outside a browser it returns an
`ArrayBuffer`.

For attaching files to a model prompt, use the `file-uploads` skill - it covers the two-step
upload and the MEDIA part shape.

---

## Agent runs

`runAgent`, `subscribeRunAgent`, `pollAgentRun`, `getAgentRun`, `decideAgentRunAction`,
`submitAgentToolDecision`, `getSubagentRuns`, the state helpers `createAgentRunItemsState` and
`applyAgentRunItemEvent`, and the types `AgentRunSnapshot`, `AgentRunItem`, `AgentRunItemEvent`,
`AgentRunItemsState`, `AgentRunStatusValue`, `AgentToolDecision`, `PendingAgentAction`,
`SubagentRunSummary`, `AgentRunSubscription`.

All framework-agnostic. See the `agent-run` skill for how to drive them.

---

## Permissions

Every one of these is a plain async function on the core entry.

| Export | Signature |
| --- | --- |
| `getUserProjectPermission` | `(projectId) => Promise<Role>` |
| `getUserEnginePermission` | `(engineId) => Promise<Role>` |
| `getProjectUsers` | `(projectId, userId?, permission?, limit?, offset?)` |
| `getEngineUsers` | `(engineId, userId?, permission?, limit?, offset?)` |
| `getProjectUsersNoCredentials` | `(projectId, searchTerm?, limit?, offset?) => Promise<User[]>` |
| `getEngineUsersNoCredentials` | `(engineId, searchTerm?, limit?, offset?) => Promise<User[]>` |
| `addProjectUserPermissions` | `(projectId, users: PostUser[]) => Promise<boolean>` |
| `addEngineUserPermissions` | `(engineId, users: PostUser[]) => Promise<boolean>` |
| `editProjectUserPermissions` | `(projectId, users: PostUser[]) => Promise<boolean>` |
| `editEngineUserPermissions` | `(engineId, users: PostUser[]) => Promise<boolean>` |
| `removeProjectUserPermissions` | `(projectId, userIds: string[]) => Promise<boolean>` |
| `removeEngineUserPermissions` | `(engineId, userIds: string[]) => Promise<boolean>` |
| `approveProjectUserAccessRequest` | `(projectId, requests: UserAccessRequest[]) => Promise<boolean>` |
| `denyProjectUserAccessRequest` | `(projectId, userIds: string[]) => Promise<boolean>` |
| `propagateUserPermissions` | `(projectId, users: PostUser[]) => Promise<boolean>` |

`Role` is `"OWNER" | "EDIT" | "READ_ONLY" | "DISCOVERABLE"`.

---

## Websocket

`InsightWebSocket` is a class, not a hook - usable from any app.

```ts
const ws = new InsightWebSocket(insightId, {
  onMessage, onStatusChange, onError, reconnect, maxReconnectAttempts,
});
```

| Member | Purpose |
| --- | --- |
| `connect()` | open the connection |
| `send(pixel)` | run a pixel over the socket |
| `watch(type, params?)` | start a backend streamer (e.g. `"claude_code"`) |
| `unwatch(type, params?)` | stop it; params must match `watch` |
| `sendRaw(data)` | send arbitrary JSON |
| `close()` | close and stop reconnecting |
| `isConnected` | getter, true while the socket is open |

Connects to `{ws|wss}://{host}{Env.MODULE}/insightSocket?insightId=...`, derived from
`window.location`. Messages are JSON-parsed before reaching `onMessage`, falling back to the raw
string. Reconnect is on by default with exponential backoff capped at 30 seconds and 5 attempts.
`send`, `watch`, `unwatch`, and `sendRaw` throw if the socket is not open.

---

## Utilities and types

`get`, `post`, `CSRF`, `UnauthorizedError`, `waitForEmbedAuth`, `DATA_FRAME_TYPES`, and the types
`Script`, `Role`, `User`, `PostUser`, `UserAccessRequest`, `ColumnInterface`, `TableInterface`,
`MCPToolRequest`, `MCPToolResponse`, `WebSocketStatus`, `InsightWebSocketOptions`.

`post` form-encodes plain objects as `application/x-www-form-urlencoded` (non-string values are
JSON-stringified first) and leaves `FormData` alone. Both `get` and `post` run interceptors that
attach basic auth from `ACCESS_KEY`/`SECRET_KEY`, a bearer token from `BEARER_TOKEN`, the CSRF
token on POSTs, and that follow a `redirect`/`location` header by navigating the window and
throwing `UnauthorizedError`.

`CSRF` is `{ isEnabled, token }`. `getSystemConfig` turns it on when the server asks for it; the
first POST afterwards fetches a token from `/api/config/fetchCsrf` if one is not cached. Basic
auth skips the whole handshake.

`waitForEmbedAuth()` resolves immediately unless the page is in an iframe **and**
`SMSS_EMBED_AUTH=true` is in the query string or hash query. When it does apply, it posts
`SMSS_EMBED_AUTH_READY` to the parent, waits up to 4 seconds for a `SMSS_EMBED_AUTH` message,
writes `BEARER_TOKEN`/`BEARER_PROVIDER` onto `Env`, and resolves either way. Nothing calls it for
you - await it before `initialize()`.

---

## React entry

Only this section requires React. Everything here wraps the core above; none of it adds
capability.

| Export | Wraps |
| --- | --- |
| `InsightProvider` | `new Insight()` + `initialize()` + state sync |
| `InsightContext` | the raw context, for class components |
| `useInsight()` | the `Insight` getters + `actions` + `tool` |
| `usePixel<D>(pixel, config?, insightId?)` | `runPixel`, unwrapped to `pixelReturn[0].output` |
| `useWebSocket(insightId, options?)` | `InsightWebSocket` |
| `useIteratorPixel(query, getTotal, getData, options?, deps?)` | paged `runPixel` with accumulation |
| `useIteratorApi(fetchPage, options?, deps?)` | the same for a REST `fetchPage(limit, offset)` |
| `useDebouncedCallback`, `useDebouncedValue` | **deprecated**; use `@semoss/ui/next` |

`InsightProvider` props: `options` (passed to `initialize`, compared by `JSON.stringify`, so an
inline literal is safe) and `destroyOnUnmount` (default `true`).

`useInsight()` returns `{ isInitialized, isAuthorized, isReady, error, system, actions, insightId, tool }`.
It throws if called outside a provider. Its `actions` are wrapped so every call re-syncs React
state afterwards.

`usePixel` returns `{ status, data, error, refresh, update }` with `status` of `INITIAL`,
`LOADING`, `SUCCESS`, or `ERROR`. It is for a single statement, not a `;`-separated chain. It
resolves its insight id from the context, so it must not mount before the insight is ready. Its
`config` accepts `{ data, onSuccess, onError, onFinal }`; an empty `pixel` string resets it to
`INITIAL` instead of firing.

`useIteratorPixel` accumulates pages and stops when `data.length >= totalCount`. `useIteratorApi`
stops when a page returns fewer rows than `limit`; its `limit` must be stable for the hook's
lifetime, and anything that should re-page from scratch belongs in `deps`.
