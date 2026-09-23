# Ask Room Pixel Call (`AskRoom(...)`)

`AskRoom` runs one persistent model turn in a SEMOSS room and returns the input
and response messages used by room clients. It is the generic counterpart to
`AskPlayground`: it does not force the Playground project or apply Playground
theme filtering.

## Pixel Syntax

```pixel
AskRoom(
    engine          = ["<model-engine-id>"],
    roomId          = ["<room-id>"],
    parentMessageId = ["<message-id>"],
    command         = ["<prompt>"],
    image           = ["<uploaded-file>"],
    url             = ["https://..."],
    paramValues     = [{"temperature": 0.2}],
    responseParts   = [{"type": "TEXT", "text": "<partial-response>"}],
    hiddenMessage   = ["<cancellation-note>"]
);
```

`engine` and `command` are required. If `roomId` is omitted, the current
insight ID is used. New rooms derive their project from the current insight;
existing rooms retain their persisted project. `parentMessageId` branches the
new turn from a specific message in the room.

The system prompt comes from the room or its configured workspace. The reactor
always uses persistent room history; use `LLM(..., useHistory=false)` when a
stateless model call is required.

## Response

```json
{
  "inputMessage": {
    "messageId": "...",
    "parts": [{"type": "TEXT", "text": "Hello"}]
  },
  "responseMessage": {
    "messageId": "...",
    "parts": [{"type": "TEXT", "text": "Hi"}]
  },
  "extraMessages": []
}
```

Both messages use the standard SEMOSS client message shape. `extraMessages`
contains hidden input/response pairs created by cancellation handling and is
empty for ordinary turns.

When `responseParts` is present, `AskRoom` skips the model call and commits the
supplied partial assistant response. An optional `hiddenMessage` records that
the turn was stopped and returns the generated hidden pair in `extraMessages`.

## Tool Continuation

When `responseMessage` contains tool calls, submit each result through
`AddToolExecution(...)`. It returns `"Tool output added successfully"` while
other tool calls remain pending. Once all required tool results are present,
it continues the model and returns the same `{inputMessage, responseMessage}`
pair and `extraMessages` collection used by `AskRoom`. The tool reactor accepts
the same `responseParts` and `hiddenMessage` cancellation inputs.

## Related Reactors

- `AskPlayground` specializes this behavior by forcing `SYSTEM__PLAYGROUND`
  and applying Playground theme filtering.
- `LLM` is the lower-level model invocation API and can opt out of history.
- `LLM2` is deprecated.
