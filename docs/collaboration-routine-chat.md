# Chat about routine output

The latest completed inbox daily brief and weekly meeting agendas expose **Chat
about this**. Opening it creates a private Collaboration room whose first visible
assistant message is the exact saved output. No model or routine run is started
until the user sends a follow-up. Reopening the same output resumes its discussion;
a new routine run gets a separate room. **Open session** still opens the original
automation execution.

```text
WorkOpenRoutineRoom(
  routineId=["inbox-daily-brief"],
  startedAt=["2026-10-15T12:00:00Z"]
);
```

`routineId` is `inbox-daily-brief` or `weekly-meeting-agendas`. The response is
`{ "roomId": "…" }`. The UI sends the displayed run's `startedAt`; a changed latest
run is rejected with instructions to refresh. Missing, blank, running, and failed
outputs cannot open a discussion. The server resolves the owner from the signed-in
session and reads the saved summary itself.

Rooms use `SYSTEM__COLLABORATION`, the configured accessible Collaboration model,
and the existing chat lifecycle. Existing mail search and Microsoft 365 email and
calendar tools are available through the normal Collaboration assistant. Direct
Microsoft tools require the user's existing Microsoft sign-in and granted scopes;
scheduled-run credentials are not copied into the room. Existing tool approval
behavior is preserved.

Deploy the new backend reactor with the frontend change. No database migration is
needed. Deterministic room and initial-message identifiers let failed creation or
message persistence be retried without duplicating the conversation.

Focused verification: `WorkRoutineRoomUnitTests` exercises persistence, owner
isolation, reuse, and stale output; frontend routine tests exercise both actions,
pending and error states, response validation, and navigation. Live model and
Microsoft access remain integration checks after deployment.
