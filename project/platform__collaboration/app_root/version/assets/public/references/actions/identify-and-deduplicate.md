# Identify actions and avoid duplicates

## Determine what requires work

Read the owner's request and latest relevant thread evidence using
[sources/ground-an-answer.md](../sources/ground-an-answer.md). Identify the action,
its source, the requested owner, any dependency, and an explicit due date. Do not
turn every informative message, copied recipient, or topic goal into an assignment.

Distinguish an explicit commitment from a proposed next step. Use an action phrase
such as "Review the updated brief" rather than a vague topic label. Preserve
conditional wording: a task waiting for input is not ready for completion.

Record a compact candidate:

| Field | Evidence rule |
| --- | --- |
| Action | A concrete requested or proposed outcome |
| Thread/source | Actual ID or source label from context |
| Owner | Explicit assignment or confirmed current owner; otherwise unassigned/proposed |
| Due time | Explicit date/time, with time zone when known; otherwise unset |
| State | Open, waiting, completed, dismissed, or snoozed only when supported |
| Reason | Source passage or the owner's correction |

Do not invent urgency or a deadline. Resolve ambiguous dates only when necessary
for a saved due time. Group related steps when they form one outcome, and keep
independent deliverables separate.

## Check existing items before proposing a new one

Inspect the Work items supplied in context or use an available item-list tool.
Match the thread, intended outcome, owner, and source issue; wording alone is not
a reliable duplicate key. Check waiting, snoozed, completed, and dismissed items as
well as open ones when available.

Example: a new message says "Please review the revised brief" and an existing
item on the same thread already says "Review brief." Update the evidence or
requested revision of that item if an authorized tool supports it. Do not create
another identical open review task just because the email is new.

A genuinely new deliverable, scope, or explicit reopened request can justify a new
item or reopening, depending on the tools and owner instruction. A repeated quote
or reminder does not by itself undo a manual dismissal or completion.

## Save only through a supported capability

Action guidance does not add item tools to this run. Inspect the exposed schemas.
When no authorized item tool is present, give a proposed action list and say it is
not saved. Do not call internal reactors through Python or another route to work
around missing tools or approvals.

When an item creation tool is present, use exact thread/person IDs and accepted
enum values. `WorkCreateItem`, if exposed, supports `threadId`, `title`, `askType`,
`assignee`, and `dueAt`; its ask types are `reply`, `approve`, `attend`, `review`,
`waiting_on`, `errand`, and `fyi`. Do not infer that it accepts an arbitrary status,
priority, or a name in place of a person ID. Confirm the returned item before
claiming it was created, then follow [update-and-report.md](update-and-report.md).
