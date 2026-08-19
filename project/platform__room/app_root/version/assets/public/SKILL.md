---
name: room
description: Use when writing code in an app that creates, lists, renames, pins, or deletes playground rooms, reads/updates room options (model, system prompt, MCPs, temperature), or reads chat history from a room. Covers CreatePlaygroundRoom, GetPlaygroundRooms, GetWorkspaceRooms, RenameRoom, PinRoom, RemoveUserRoom, SetRoomForInsight, GetRoomOptions, UpdateRoomOptions, and GetPlaygroundMessages via @semoss/sdk's runPixel. Do not use for LLM completions (see model), document/vector ingestion (see vector), or an autonomous multi-turn agent loop (see agent-run).
---

# Room

A **room** is a persistent, named conversation on the platform — it carries chat history, a selected model, a system prompt, MCP tool configuration, and optional workspace association. All room calls go through `runPixel` from `@semoss/sdk`. A room has its own `roomId` (the durable chat identifier) and runs inside an `insightId` (the per-session execution scope).

> **Bind the insight to the room before asking.** Pass `SetRoomForInsight(roomId=...)` once per session — typically alongside your first `GetPlaygroundMessages` / `GetRoomOptions` call — so subsequent `LLM(...)` turns in this insight thread into the room's history.

## Usage — create a room, then ask a message

```typescript
import { runPixel } from "@semoss/sdk";

// 1. Create the room. Pass workspaceId to attach it to a workspace (optional).
const { errors, pixelReturn, insightId } = await runPixel<[{ roomId: string }]>(
  `CreatePlaygroundRoom();`,
  "new",
);

if (errors.length) throw new Error(errors[0]);

const roomId = pixelReturn[0].output.roomId;

// 2. Bind this insight session to the room so future LLM calls are threaded.
await runPixel(
  `SetRoomForInsight(roomId=${JSON.stringify(roomId)});`,
  insightId,
);

// 3. Ask the model — pass roomId so the turn is recorded against the room.
const MODEL_ID = "6dd0bbfd-cd3b-4f2c-b13a-fe4545872e3d";
await runPixel(
  `LLM(engine="${MODEL_ID}", roomId="${roomId}", command=["Hello"]);`,
  insightId,
);
```

The variations below show only the pixel string. The surrounding `runPixel(...)` call, the `errors` check, and response parsing follow the same shape.

## Room lifecycle

### Create — `CreatePlaygroundRoom`

```
CreatePlaygroundRoom();
CreatePlaygroundRoom(workspaceId="<workspace-uuid>");
```

Always call against a **new** insight (`runPixel(pixel, "new")`). The pixel returns `{ roomId }`; capture the surrounding `insightId` too — both are needed for subsequent calls.

### List user rooms — `GetPlaygroundRooms`

Prefix with `META |` to bypass insight execution. Supports search, pinned filter, paging, sort.

```
META | GetPlaygroundRooms(limit=25, offset=0, sort=["DESC"]);
META | GetPlaygroundRooms(pinned=[true], offset=0, sort=["DESC"]);
META | GetPlaygroundRooms(search="<encode>quarterly review</encode>", limit=25, offset=0, sort=["DESC"]);
```

Each row contains `ROOM_ID`, `ROOM_NAME`, `DATE_CREATED`, optional `WORKSPACE_ID`, and `PINNED`.

### List workspace rooms — `GetWorkspaceRooms`

Scoped to a single workspace. Output is `{ total_count, rooms: [{ room_id, room_name, date_updated }] }`.

```
GetWorkspaceRooms(workspaceId=["<workspace-uuid>"], limit=[25], offset=[0]);
GetWorkspaceRooms(workspaceId=["<workspace-uuid>"], filters=[Filter(room_name ?like "review")], limit=[25], offset=[0]);
```

### Rename — `RenameRoom`

```
RenameRoom(roomId=["${roomId}"], name=["My renamed room"]);
```

### Pin / unpin — `PinRoom`

`pinned=true` to favorite, `pinned=false` to remove from favorites.

```
PinRoom(roomId=["${roomId}"], pinned=[true]);
```

### Delete — `RemoveUserRoom`

Removes the room from the current user. If you have its `insightId` cached, also call `DropInsight()` against that insight to release server-side resources.

```
RemoveUserRoom(roomId=["${roomId}"]);
```

## Room options

`GetRoomOptions` / `UpdateRoomOptions` read and write the per-room configuration: model, instructions (system prompt), MCPs, temperature, token length, workspace, predefined prompts.

### Read — `GetRoomOptions`

```
GetRoomOptions(roomId="${roomId}");
```

The output is `{ OPTIONS?: { instructions, mcp, tokenLength, temperature, workspace?, predefinedPrompts } }`. `OPTIONS` is missing on rooms created before options existed — treat it as optional.

### Bind insight to room — `SetRoomForInsight`

Call once when opening a room so the insight's subsequent `LLM(...)` calls are threaded into the room's history. Typically chained with `GetPlaygroundMessages` and `GetRoomOptions` in a single `runPixel` to initialize:

```
GetPlaygroundMessages(roomId=["${roomId}"]); GetRoomOptions(roomId="${roomId}"); SetRoomForInsight(roomId="${roomId}");
```

### Update — `UpdateRoomOptions`

Pass the **full** options object. Include `modelId` to set the room's default model.

```
UpdateRoomOptions(roomId="${roomId}", roomOptions=[${JSON.stringify({
  modelId: "6dd0bbfd-cd3b-4f2c-b13a-fe4545872e3d",
  instructions: "You are a helpful assistant.",
  mcp: [],
  tokenLength: 2000,
  temperature: 0.3,
  predefinedPrompts: [],
})}]);
```

> **Don't persist workspace-inherited MCPs.** When a room has a workspace, the workspace's MCPs are merged into the in-memory `mcp` list tagged with `fromWorkspace: true`. Strip those out before `UpdateRoomOptions` — only the room-owned MCPs should be saved. Filter with `mcp.filter((m) => !m?.fromWorkspace)`.

## Room messages

### Read history — `GetPlaygroundMessages`

```
GetPlaygroundMessages(roomId=["${roomId}"]);
```

Returns a flat array of `PixelMessage` objects, each with:

- `io: "INPUT" | "OUTPUT"` — user message vs. model response.
- `messageId`, `parentMessageId` — message tree. Walk `parentMessageId` to reconstruct branching history; messages with no parent attach to the room root.
- `modelId`, `modelType` — which model produced/received the message.
- `parts[]` — `{ type: "TEXT" | "THINKING" | "MEDIA" | "TOOL_CALL" | "TOOL_RESULT", ... }`. Render `TEXT` parts as chat bubbles; `MEDIA` carries `mediaInfo.fileLocation` for inline attachments; `TOOL_CALL` / `TOOL_RESULT` pair up via `toolCall.id` / `toolResult.toolCallId`.
- `tokens`, `dateCreated`, `ornaments.modelName`.
- `feedback?` on OUTPUT messages — `{ rating, feedbackText, ... }` if the user rated the response.

### Send a chat turn

For a simple conversational message, call `LLM(...)` with the room's `roomId`:

```
LLM(engine="${MODEL_ID}", roomId="${roomId}", command=["${prompt}"]);
```

The turn is automatically persisted to the room's history and will appear in the next `GetPlaygroundMessages` call. See the `model` skill for the full `LLM()` reference (history, structured outputs, attaching media with `media=`). For an autonomous, multi-turn agent loop instead of a single request/response, see the `agent-run` skill.

## Response shape

All room pixels follow the standard `runPixel` envelope; the room-specific payload lives at `pixelReturn[0].output`.

| Pixel                  | `output` shape                                                                |
| ---------------------- | ----------------------------------------------------------------------------- |
| `CreatePlaygroundRoom` | `{ roomId: string }`                                                          |
| `GetPlaygroundRooms`   | `[{ ROOM_ID, ROOM_NAME, DATE_CREATED, WORKSPACE_ID?, PINNED? }]`              |
| `GetWorkspaceRooms`    | `{ total_count, rooms: [{ room_id, room_name, date_updated }] }`              |
| `RenameRoom`           | success boolean                                                               |
| `PinRoom`              | success boolean                                                               |
| `RemoveUserRoom`       | success boolean                                                               |
| `SetRoomForInsight`    | success boolean                                                               |
| `GetRoomOptions`       | `{ OPTIONS?: { instructions, mcp, tokenLength, temperature, workspace?, predefinedPrompts } }` |
| `UpdateRoomOptions`    | success boolean                                                               |
| `GetPlaygroundMessages`| `PixelMessage[]` (see "Read history" above)                                   |

For the full `runPixel` envelope fields (`insightID`, `pixelReturn[].pixelExpression`, `timeToRun`, etc.), see the response-schema section of the `model` or `database` skill.

## Common patterns

### Open a room from a list

```typescript
import { runPixel } from "@semoss/sdk";

async function openRoom(roomId: string) {
  // Run on a fresh insight, then keep using that insightId for all turns.
  const { insightId, pixelReturn } = await runPixel<
    [PixelMessage[], { OPTIONS?: RoomOptions }, boolean]
  >(
    `GetPlaygroundMessages(roomId=["${roomId}"]);
     GetRoomOptions(roomId=${JSON.stringify(roomId)});
     SetRoomForInsight(roomId=${JSON.stringify(roomId)});`,
    "new",
  );

  const messages = pixelReturn[0].output;
  const options = pixelReturn[1].output.OPTIONS;
  return { insightId, messages, options };
}
```

### Pin toggle

```typescript
await runPixel(`PinRoom(roomId=["${roomId}"], pinned=[${!isFavorite}]);`);
```

### Search rooms with debounce

```typescript
const pixel = search
  ? `META | GetPlaygroundRooms(search="<encode>${search}</encode>", limit=25, offset=0, sort=["DESC"])`
  : `META | GetPlaygroundRooms(limit=25, offset=0, sort=["DESC"])`;

const { pixelReturn } = await runPixel(pixel);
const rooms = pixelReturn[0].output;
```

### Encoding rules

- **Free-text user input** inside a pixel string (search queries, chat prompts, system instructions) — wrap in `<encode>...</encode>`. The Pixel parser decodes the wrapper before executing. Do **not** also `encodeURIComponent`; doing both produces literal escape sequences in the executed pixel.
- **IDs and option blobs** (`roomId`, `workspaceId`, `roomOptions`) — pass plainly or via `JSON.stringify` for objects. No `<encode>` wrapper.

## Related

- `model` — `LLM()` reference; pair with a `roomId` for threaded chat turns.
- `agent-run` — autonomous, multi-turn agent loop against a room, instead of a single `LLM()` turn.
- `file-uploads` — two-step upload pattern for attaching media to a room's first user message.
- `vector` — embedding/retrieval pixels for grounding room responses on documents.
- `database` — query a database from inside a room (e.g., via an MCP tool or direct `SqlQuery()` call).
