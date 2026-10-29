# Guardrail pipelines and hit limits

Any engine except a guardrail engine can run guardrail checks on its calls: model, database,
storage, vector and function engines alike. The checks, and the limits on how often they may flag
a room or a user, live in the engine's pipeline file. `PipelineInvocationHandler`, the proxy every
engine is wrapped in, applies them to each call.

## The pipeline file

The engine's SMSS key `PIPELINE` names the file in the engine's assets folder, usually
`pipeline.json`. The proxy reads it each time the engine is retrieved, so a saved change applies
right away, with no reload.

```json
{
  "pipelines": {
    "askRoom": {
      "input": [
        {
          "reactorClass": "prerna.reactor.interceptor.GenericGuardrailInputReactor",
          "params": {
            "checkId": "toxicity",
            "guardrailEngineId": "<guardrail id>",
            "inputMapping": { "prompt": "arg0" }
          }
        },
        {
          "reactorClass": "prerna.reactor.interceptor.GenericGuardrailInputReactor",
          "params": {
            "checkId": "pii",
            "guardrailEngineId": "<guardrail id>",
            "inputMapping": { "prompt": "arg0" },
            "maskOnGuardrailFailure": true
          }
        }
      ]
    }
  },
  "hitLimits": [
    { "limitId": "close-room", "checks": ["toxicity"], "hits": 3, "scope": "ROOM" },
    {
      "limitId": "lock-user",
      "checks": ["toxicity"],
      "hits": 5,
      "scope": "USER",
      "windowMinutes": 1440,
      "lockMinutes": 60,
      "message": "You are paused from this assistant for an hour."
    }
  ]
}
```

- **Pipelines.** `pipelines` maps an engine method, or `*` for every method, to its `input` checks
  (run before the method) and `output` checks (run on its result).
- **What a failed check does.** It blocks the call. An input check can instead mask the argument
  and let the call run (`maskOnGuardrailFailure`). On a call that returns a model response, an
  input check can answer with the guardrail's message (`respondWithGuardrailMessage`).
- **Check ids.** `checkId` names a check so a hit limit can count it. The settings editor gives
  each check the next id in order (`check0`, `check1`, ...) and keeps it across saves. A hit limit
  names checks only by their `checkId`, and `UpdateEngineGuardrailConfig` refuses a limit that
  names any other id. A check saved without one is named in the audit trail by its method, phase
  and position, such as `askRoom.input.1`.

## Hit limits

A hit is a check flagging a call, whether it blocks, masks or answers. A limit counts the hits of
the checks it names, and other checks' hits are ignored: name the two blocking checks and leave out
the masking one, and masked messages never count.

- **A ROOM limit** counts the room's hits. The hit that reaches `hits` closes the room and refuses
  the call.
- **A USER limit** counts the user's hits on this engine in the last `windowMinutes`. The hit that
  reaches `hits` locks the user out of the engine for `lockMinutes`, closes the room and refuses the
  call. A user is their id and login type. Hits from before the user's latest lock started are not
  counted, so a lock clears the count. A save that removes a user limit ends the locks it made.
- **`message`** is what the refused call returns. Without it, a room limit says the room was closed
  by a guardrail, and a lock says when it ends.

Every call to an engine that has limits is checked before any of its checks run, whether or not
its method has checks. Only the methods marked `@IgnoreEngineLogging`, the engine's getters and
other plumbing, are left out. The call is refused when:

- the user is locked out of the engine (the call's room is closed too), or
- the room is closed, or
- the room has reached a room limit.

No guardrail engine is called for a refused call. The check reads at most three things: the user's
lock, whether the room is closed, and the room's hits, check by check in one query.

Every limit a hit reaches applies. A hit that reaches a room limit and a user limit at once closes
the room and locks the user out, and the refused call says when the lock ends.

Hits and locks are stored in the model inference database (`GUARDRAIL_HIT`, `GUARDRAIL_LOCK`; see
[internal databases](../platform_services/internal_databases.md)), so they hold across the
cluster. When they cannot be read or written, the call is refused rather than let through
unchecked.

A call with no signed-in user is never locked out, and since it has no room of its own, no room is
closed for it. Its hits are still counted, and a hit that reaches a limit refuses that call.

## Audit trail

Every hit is recorded in the user audit trail on the guarded engine:

- a block as `GUARDRAIL_BLOCK`
- a mask as `GUARDRAIL_MASK`
- an answer as `GUARDRAIL_RESPOND`

The limits record:

- `GUARDRAIL_ROOM_CLOSE` and `GUARDRAIL_USER_LOCK` when a limit is reached
- `GUARDRAIL_LOCKED_REQUEST` for a call refused because of a lock
- `GUARDRAIL_LOCK_CLEAR` when an editor ends a lock early

Each event names the check, its guardrail engine, the method and phase, and for a limit its id,
count and threshold. It holds the values the check screened only when the engine keeps its input
and output. The engine's owners see these events on its Audit tab, without the screened values.
`GuardrailAudit` records them, and a failed audit write never changes the call's outcome.

## Editing

The engine's Settings > Guardrails section edits the file through these reactors. Each needs edit
access to the engine:

- `GetEngineGuardrailConfig(engine)` returns:
  - the pipelines and hit limits
  - the guardrail engines they name
  - the methods the engine's proxy can intercept: each method with an argument a guardrail can
    screen, whether its result can be screened, and whether it returns a model response
- `UpdateEngineGuardrailConfig(engine, map)` validates and writes the file. It ends the locks of any
  user limit the new file no longer has, each recorded as an unlock by the editor who saved.
- `GetEngineGuardrailLocks(engine)` lists the users locked out of the engine.
- `ClearEngineGuardrailLock(engine, userId, userType)` ends a user's lock early.
