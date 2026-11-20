# Update and report action state

## Respect the current item

Read the latest item and the owner's requested change. Preserve manual title,
priority, due-time, owner, snooze, and dismissal choices unless the owner asked
to change them or a supported policy explicitly authorizes the update.

Use the actual item ID. A thread ID, message ID, or chat todo ID is not a Work item
ID. If several items could match, use their source and intended outcome to resolve
them; ask only when a mistaken update would affect a different task.

## Apply only supported changes

Use an authorized exposed item tool and its schema. Do not assume every reactor
is available to this run. In particular, `WorkUpdateItem`, when exposed, accepts
an item ID and changes such as title, status, due time, priority, snooze time,
suggestion acceptance, and reason. Its current schema has no assignee update field;
owner changes require a tool that actually supports them.

| State | Meaning to preserve |
| --- | --- |
| `open` | Work remains for the current owner |
| `waiting` | A dependency or another person's response is outstanding |
| `done` | The requested outcome is confirmed complete |
| `dismissed` | The owner closed the item without completing it |
| `snoozed` | The item is deferred until its supported snooze time |

Use only fields that change and retain the reason for material updates. An email
draft does not complete a send-email task. Reading a document does not complete
an approval task. A suggestion is not accepted unless a successful update or the
owner's actual action proves it.

An API field being writable does not authorize a change. Do not overwrite manual
priority or clear a due time to make the list simpler. If the supplied source is
older than the current item, resolve the conflict instead of restoring stale state.

## Avoid duplicate side effects

After an update, inspect its result and any returned history/change ID. On an
ambiguous failure, read the current item through an available tool before retrying.
Do not create a replacement item because an update response was delayed.

Do not promise undo from a history ID alone. Undo requires an available working
tool and a successful result. Chat `TodoWrite` tracks the agent's execution plan;
it does not create or update the owner's persistent Work items.

## Report what happened

State the actual item and successful changes, preserving distinctions between
proposed, saved, completed, deferred, and waiting. If the needed capability is absent,
return a useful proposed update and say it has not been saved. If an update failed,
give the specific failure and avoid claiming the old state was preserved unless
you checked it. Keep the final action list focused on the owner's next decisions.
