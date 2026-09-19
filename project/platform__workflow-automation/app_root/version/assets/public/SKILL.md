---
name: workflow-automation
description: "Use when authoring, changing, running, or troubleshooting a SEMOSS Automation workflow. Covers the automation project MCP (GetAutomation, GetAutomationNodeDefinitions, AddAutomationStep, UpdateAutomationStep, UpdateAutomationCustomStep, RemoveAutomationStep, TriggerAutomation), the typed node graph, the run(scope) contract every node's Python must satisfy, trigger inputs and setup Python, and the rules each node type is validated against at save time."
---

# Automations in SEMOSS

An automation is a typed graph the platform executes node by node. The graph lives in
the project's `automation-workflow.json`, and each non-trigger node's Python lives in
its own file under `automation-nodes/`. You never touch either file. The active
automation project's MCP is the only authority for reading, changing, or running it.

Two kinds of node exist, and the difference governs every edit:

- **Generated** nodes hold configuration only. The server renders their Python from
  that configuration. Change them with `UpdateAutomationStep` by sending configuration.
- **Custom** nodes (`developer.python`, or any node a user has switched to custom) hold
  hand-written Python. Change them with `UpdateAutomationCustomStep` by sending source.

Sending source to a generated node, or configuration to a custom node, is rejected.

## Authoring sequence

Always in this order. Skipping step 1 or 2 is the most common cause of a rejected edit.

1. `GetAutomation` - the current graph, node IDs, configuration, node sources,
   `sourceHashes`, globals, and revision. Call it before every decision, and again
   after any change you did not make yourself.
2. `GetAutomationNodeDefinitions` - the versioned catalog of every supported node type
   with its configuration fields, defaults, engine category, ports, and capabilities.
   This is the schema source of truth. Do not guess a field name from this document.
3. Discover real identifiers with the catalog tools before using them. Never invent an
   engine, app, reactor, function, workspace, node, or output-variable ID.
4. Make one change per call: `AddAutomationStep`, `UpdateAutomationStep`,
   `UpdateAutomationCustomStep`, or `RemoveAutomationStep`.
5. Run it with `TriggerAutomation` **only when the user explicitly asks you to**.
   It is never a way to inspect the automation.

Never claim an operation succeeded unless the tool confirmed it. If a save is rejected,
read the error: the validator states the exact rule that failed.

## The run(scope) contract

Every node's Python, generated or custom, must bind a module-level `run`:

```python
def run(scope):
    return {"total": 4}
```

The runtime imports the node's module and calls `run(scope)`. A `run` nested inside a
class or another function does not count, and a module with no top-level `run` is
rejected at save time. Whatever `run` returns is stored as that node's output under its
output variable, so return a JSON-shaped value.

`scope` is a read-only mapping of the run's values, not a plain dict you can assign to:

- `date`, `triggered_at`, `run_id` - seeded by the runtime on every run.
- every trigger global input, by name.
- every earlier node's output, under that node's output variable.

Read with `scope["name"]` when the value is required, `scope.get("name")` when optional.
Assigning to `scope` raises. To pass data on, return it.

**`${...}` is resolved in generated node configuration only.** It has no meaning in
custom Python. In custom source read `scope["prior_output"]` directly. Generated source
resolves its own placeholders through `scope.resolve(...)`, which is a method on the
scope object - there is no module-level `resolve` function.

## Output variables

Every node except `trigger.start` and `control.if` needs one, and it must be:

- a valid Python identifier, not a Python keyword;
- unique across the whole graph, because a duplicate silently overwrites the earlier
  node's output in scope;
- not `date`, `triggered_at`, `run_id`, or `_automation_room_id`, which the runtime owns.

The canvas rewrites `${...}` references when a person renames a variable in the UI.
Nothing rewrites them for you. If you rename one through `UpdateAutomationStep`, find
every `${old_name}` in generated configuration and every `scope["old_name"]` in custom
source and update each one in the same pass, or those nodes will read a value that is
no longer in scope.

## Graph shape

- Exactly one `trigger.start`. It takes no incoming control edge and produces no output
  variable.
- Every node must be reachable from `trigger.start` by control edges. An orphan is
  rejected rather than silently skipped.
- `AddAutomationStep` takes `afterNodeId` to insert after an existing node. Omit it to
  append.
- `control.if` is the decision node. It evaluates ordered clauses and routes to the
  first match, with an `else` fallback. To attach a node to one of its branches, pass
  `afterNodeId` as the `control.if` node plus `branchPort` as either `case:<clause-id>`
  or `else`. `branchPort` is required there and must be omitted for every other parent.

## Node types and their rules

Read `GetAutomationNodeDefinitions` for the full field list. These are the constraints
the save-time validator enforces that the field list alone will not tell you.

### Database

Four operations, each of which renders a different SDK call:

| Node | SDK call | Statement it must contain |
|---|---|---|
| `database.query` | `execQuery` | `SELECT` or `WITH` |
| `database.insert` | `insertData` | `INSERT` |
| `database.update` | `updateData` | `UPDATE`, **and a `WHERE` clause** |
| `database.delete` | `removeData` | `DELETE`, **and a `WHERE` clause** |

`execQuery` only runs reads, which is why the writes each have their own method. The
statement type must match the node type; a `SELECT` in a delete node is rejected. The
`WHERE` requirement on update and delete exists because an automation runs unattended
and an unqualified statement rewrites or empties the whole table.

A generated database node cannot contain an unresolved `${...}` placeholder in its
query, and must contain exactly one statement. `database.query` also takes a bounded
`limit`.

### Vector

`vector.search`, `vector.add`, and `vector.delete` each persist a single required
`value`, which means a different thing per operation: the search text, the path to add,
or the comma-separated names to remove.

### Model, storage, function, app, agent

Configuration-driven; take their required fields from the catalog. Use
`function.execute` for a registered function and `app.pixel` for a pixel, rather than
reimplementing either in Python.

### Control

`control.wait` pauses for a bounded number of seconds. `control.if` is described above
and is always generated - it has no Python source of its own.

### developer.python

The escape hatch, and the last resort. Use it only for custom computation or an
integration no supported node provides. Prefer a supported generated node: it is
validated, rendered, and understood by the canvas, and a custom node is none of those.

## Trigger inputs and setup Python

The `trigger.start` node is the automation's parameter list. Its configuration holds
two things:

- **`globals`** - declared inputs, each with a `name` and a `defaultValue`. They are
  seeded into scope before any node runs, and the caller can override them per run
  through `TriggerAutomation`'s `inputs` argument. This is what makes an automation
  reusable instead of hardcoded.
- **`pythonSource`** - optional setup that runs once, before the first node. Its
  module-level variables, and anything its `run(scope)` returns, are added to scope for
  every node to read.

Use a global for a value a person should be able to change per run; use setup Python
for a value that must be computed when the run starts. A default is a static string
typed at design time, so "the last 7 days" cannot be one:

```python
import datetime

def run(scope):
    days = int(scope["lookback_days"])
    cutoff = datetime.date.today() - datetime.timedelta(days=days)
    return {"cutoff_date": cutoff.isoformat()}
```

With `lookback_days` declared as a global, every downstream node can now use
`${cutoff_date}` in generated configuration or `scope["cutoff_date"]` in custom source.

Unlike every other node, the trigger has no file under `automation-nodes/` - its setup
source lives in the node configuration itself.

## Editing custom source safely

`UpdateAutomationCustomStep` requires `expectedSourceHash`, which must be the exact
`sourceHashes[nodeId]` value from your most recent `GetAutomation`. If it does not
match, someone changed that node after you read it and the edit is rejected. Call
`GetAutomation` again, re-read the source, and reapply your change on top of the
current version. Never retry with a stale hash.

Send complete source, not a fragment: it replaces the file.

## Removing a node

`RemoveAutomationStep` deletes one node and reconnects an unambiguous sequential
control path around it. It is rejected when another node still references the removed
node's output, so clear those references first. Remove a node only when the user asked
for it.

## Finishing

Close with a short summary of what was confirmed changed, and say plainly whether the
automation was run. If anything was rejected and you could not resolve it, say what the
validator refused and why rather than describing the intent as done.
