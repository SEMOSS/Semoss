# Interactive interfaces in Collaboration

Use `ShowUI` when a choice, short form, or structured review will help the owner
complete their task. Use ordinary text for an answer that needs no interaction.
The tool pauses the run until the owner presses an event button or dismisses it.
Only the owner's interactive chat has this tool; routines and delegated runs do not.

Call `ShowUI(title, messages)` with A2UI **1.0 JSON**, using the catalog
`urn:semoss:collaboration:a2ui:1`. These are protocol messages, not HTML, JSX,
A2UI Express, or an A2A message. Each call owns its own surface; it cannot modify
an earlier tool call. Supply a complete interface in one call, with exactly one
active surface and a `root` component. Stay below 32 messages, 128 component
definitions, 16 component levels, and 64 KiB of JSON.
Use identifiers such as `continue_label` (letters, digits, underscores; no hyphens)
for component IDs. The component graph must be a tree: reference each child once,
and do not use child templates. Every component must be reachable from `root`.

The catalog adapts these Basic components to SEMOSS controls. Only the listed
properties are supported; layout weights, alignment overrides, and validation
checks are not part of this profile.

| Component | Properties |
| --- | --- |
| Text | `text`; optional `variant`: `caption` or `body` |
| Row / Column | `children`: array of component IDs |
| Card | `child`: component ID; use only when the content needs grouping |
| Divider | optional `axis`: `horizontal` or `vertical` |
| Button | `child`: a Text ID, optional `variant`: `primary`, `default`, `borderless`; `action.event` |
| TextField | `label`, `value`, optional `placeholder`, `variant`: `shortText`, `longText`, `number` |
| CheckBox | `label`, boolean `value` |
| ChoicePicker | `label`, `options`: `{label,value}[]`, string-array `value`; `variant`: `mutuallyExclusive` or `multipleSelection` |
| Slider | `label`, numeric `value`, `max`; optional `min` (default 0), positive integer `steps` (number of divisions) |
| DateTimeInput | `label`, `value`; enable at least one of `enableDate`, `enableTime` |

Use 1–50 choices with unique nonempty values and literal labels. Initial choice
values must be available options, with at most one for single selection. Slider
values must be inside the range and align with `steps` when given. Number fields
use numeric strings (or `""`); use `longText` when a text value contains newlines.

Bind editable values with `{"@path":"/field"}` and initialize them in
`createSurface.dataModel`: strings for text/date, booleans for checkboxes,
string arrays for choices, and numbers for sliders. A ChoicePicker value is an
array even for single selection: `[]` for nothing chosen, `["review"]` for one.
Input labels and button Text
labels must be nonempty literal strings. Date/time values use local native-input
formats: `YYYY-MM-DD`, `HH:mm`, or `YYYY-MM-DDTHH:mm`; use `""` when unset.
Use visible labels, short button labels, and Columns
for forms. Text is displayed literally, without Markdown formatting. Do not include
scripts, raw HTML, images, remote resources, custom catalogs, function calls,
validation functions, or RPC messages. Never ask for passwords or API keys.

Every interactive surface needs at least one labeled Button. Its action is
`{"event":{"name":"choose","context":{"answer":{"@path":"/answer"}}}}`.
Include every answer needed by the next turn in `context`; paths resolve to the
owner's current values when clicked. A value change alone never resumes the run.

Example tool arguments (illustrative project labels):

```json
{
  "title": "Choose the next task",
  "messages": [
    {
      "version": "1.0",
      "createSurface": {
        "surfaceId": "task-choice",
        "catalogId": "urn:semoss:collaboration:a2ui:1",
        "dataModel": {"task": ["review"], "notes": ""},
        "components": [
          {"id": "root", "component": "Column", "children": ["intro", "task", "notes", "continue"]},
          {"id": "intro", "component": "Text", "text": "Choose what to prepare next."},
          {"id": "task", "component": "ChoicePicker", "label": "Task", "options": [{"label": "Review the proposal", "value": "review"}, {"label": "Prepare a summary", "value": "summary"}], "value": {"@path": "/task"}},
          {"id": "notes", "component": "TextField", "label": "Additional context", "value": {"@path": "/notes"}},
          {"id": "continue", "component": "Button", "child": "continue_label", "variant": "primary", "action": {"event": {"name": "prepare", "context": {"task": {"@path": "/task"}, "notes": {"@path": "/notes"}}}}},
          {"id": "continue_label", "component": "Text", "text": "Prepare task"}
        ]
      }
    }
  ]
}
```

The tool result has `userAction: "a2ui-action"` and
`result: {version: "1.0", action: {name, surfaceId, sourceComponentId, timestamp, context}}`.
Treat that as the owner's input. Use the existing tools for actual work and keep
their ordinary approval requirements. A button does not send mail, update a
calendar, or authorize a different external action. If the owner dismisses an
interface, do not repeatedly recreate it; offer a simpler text interaction.

A result with `userAction: "a2ui-invalid"` means the interface failed validation
and the owner never saw it. `result.error` says what is wrong and names the
component or path. Correct that and call `ShowUI` again; after two invalid
attempts, ask in plain text instead. `messages` must be valid JSON: check that
every bracket and brace closes.

`updateComponents`, `updateDataModel`, and `deleteSurface` are supported inside
the ordered message batch. There is no cross-call surface editing or token-level
UI streaming in this integration. The app retains the original tool arguments
and the submitted result in chat history; a completed interface cannot submit again.

Reference: [A2UI protocol](https://a2ui.org/specification/v1.0-a2ui/).
