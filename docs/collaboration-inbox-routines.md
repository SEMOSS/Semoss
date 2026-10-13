# Collaboration inbox routine

The Routines page offers one routine: a private daily brief of the owner's
Collaboration topics and most recent Brain threads, also emailed to the owner.
New users are opted out. They choose a 24-hour time, one or more weekdays, an
IANA time zone, and a thread count from 1 to 100 (stored as `emailCount`). The
default is 08:00 on Monday through Friday, with 25 threads. The frontend starts
with the browser's time zone before the first configuration is saved.

## Prerequisites

- Collaboration must be enabled and its database initialized.
- The SEMOSS scheduler database must be available, scheduling must not be forced
  off, and `SCHEDULER_ENDPOINT` must be configured.
- `COLLAB_LLM_ENGINE_ID` must name a text model accessible to the user's primary
  account without relying on browser-only group or secondary-login grants.
- Brain must have recorded the user's Microsoft mailbox in `COLLAB_OWNER`.
- A renewable Microsoft sign-in for that exact mailbox must be available, either
  in the current session, the security database's `MS_GRAPH_CREDENTIAL` table, or
  an existing Microsoft mail/calendar subscription. It needs both an access token
  and a refresh token, with delegated `Mail.Read` or `Mail.ReadWrite` permission,
  and `Mail.Send` to email the brief.
- Python must be enabled (`USE_PYTHON`): the brief runs as an Automation
  definition whose steps execute in the owner's Python process.

Enabling the routine saves those credentials in the security database. Viewing
the page or saving a paused routine does not save credentials. Scheduled runs use
the stored connection and persist refreshed tokens without relying on a browser
session. No webhook subscription or public HTTPS callback is required; a local
deployment can run the daily scan. Existing Settings mail/calendar subscriptions
can supply the same account's credentials regardless of the resource watched.
Microsoft still enforces the token's granted mail permissions when reading mail.

If the Microsoft sign-in lacks a refresh token, sign out of Microsoft and connect
again in Brain with `offline_access` configured. The page distinguishes a missing
Brain mailbox link from missing renewable credentials. It does not infer that
an administrator disabled subscriptions. A previously saved routine can still be
paused when its connection is unavailable.

## Pixel API

```text
WorkGetInboxRoutine();
WorkSaveInboxRoutine(settings=[{
  "enabled": true,
  "time": "08:00",
  "days": ["MON", "TUE", "WED", "THU", "FRI"],
  "timezone": "America/New_York",
  "emailCount": 25
}]);
```

Save the same settings with `enabled: false` to pause. Both calls return the
routine settings, `configured`, prerequisite availability, `nextRunAt`, and
`lastRun`. The latter contains `status` (`running`, `completed`, or `failed`), UTC
ISO timestamps, `roomId`, `summary`, and `error`. Null-valued fields are omitted
by the existing Pixel JSON serializer; consumers normalize them to null.

`WorkRunInboxRoutine()` is the scheduler's fixed recipe. It uses the authenticated
owner and accepts no arbitrary mailbox, user, prompt, or agent parameters. It
checks opt-in, selected weekday, current account status, and a database claim
that permits one execution per local calendar day. It uses the owner's existing
delegated Microsoft token against `/me`; credentials never enter the routine
settings, scheduler recipe, or API result.

## Execution and storage

Each owner has one deterministic Quartz job and one `WORK_INBOX_ROUTINE` row.
Timezone-aware Quartz cron handles the selected weekdays; missed executions are
skipped. Configuration writes hold the routine row lock, and its owner identity
has a unique index. A cross-database scheduling failure leaves the routine
paused. Each execution rechecks consent before processing the brief.

Each execution runs the server-held definition
`prerna/collaboration/inbox-daily-brief.automation.json` (a classpath resource,
replaceable with an Automation builder export) through `RunAutomationDefinition`'s
inline runner, as the owner, with no Automation project. The run is stored in the
scheduler database under the owner's inline run scope with
`AUTOMATION_ID = collaboration:inbox-daily-brief`, and `WORK_INBOX_ROUTINE.RUN_ID`
names it. Before the run starts, the server refreshes the stored Microsoft token
and lends it to the run's session user, then restores the caller's own token
afterwards; a token refreshed during the run is saved back. A session with no
Microsoft token of its own keeps the lent one while the Brain sync the run started
(or its thread summaries) is still reading Graph through it. The recipient is the
mailbox's own `mail` (or `userPrincipalName`) address from Graph `/me`.

The definition is a linear graph:

1. `BrainSync()` imports new mail (and Teams when on) and classifies threads. An
   error, such as an import still running, does not stop the brief.
2. A Python step waits up to about five minutes for that sync job and its
   thread summaries.
3. `BrainListTopics` (active topics) and `BrainListThreads` (the chosen number
   of recent threads, with participants and summaries).
4. An `agent.run` step uses Automation's Java scope interpolation to add those
   results to the server-held briefing instruction. The `inbox_brief` harness and the owner's
   `COLLAB_LLM_ENGINE_ID` model writes the brief. The harness makes one model
   call, has no tool executor, rejects tool calls, and treats the data as
   untrusted. The brief is Markdown with Today, Needs your reply, FYI, and
   Sources sections; links may only point to URLs present in the data.
5. A Python step converts the Markdown to HTML (raw HTML in the brief is
   escaped, images are dropped, and links to URLs not found in the thread data
   are removed) and sends it with `MicrosoftOutlookSendMail` to the owner only.

The routine's own exclusion filter no longer applies; only Brain's import gating
does. The sync spends model calls on classification and thread summaries.

The latest result and status are stored in `WORK_INBOX_ROUTINE`, scoped to the
owner's ID and authentication type, and shown in Routines. If the brief was
written but the email failed, the routine still completes with the brief and
reports "The brief was saved but the email could not be sent." `ROOM_ID` names
the agent step's room for "Open session". A new brief replaces the routine's
latest result. Interrupted runs are reconciled with the durable Automation run
status; runs still unfinished after twenty minutes are cancelled and failed so
the next scheduled day can retry. The agent wait is limited to ten minutes.

Topics, threads, and the brief are also kept as Automation node outputs, which
the owner can read with `GetAutomationRun`. "Reset my data" deletes these runs;
there is no time-based retention.

## Deployment and verification

Deploy the updated SEMOSS Java classes through the normal application build and
restart process. Security startup adds `MS_GRAPH_CREDENTIAL` and its unique user
index; Collaboration startup adds the routine table, its `LAST_SUCCESS_AT` column,
and unique owner index through
the existing schema initialization. The new reactors use
normal classpath discovery; `inbox_brief` is registered in the agent harness
registry. No separate migration command or new dependency is required.

Verified with Java 21 using:

```sh
mvn -q -DskipTests compile
mvn -q -Dlicense.skip=true -Dtest=WorkInboxRoutineUnitTests,InboxBriefHarnessUnitTests,MicrosoftOutlookMailHelperUnitTests,BrainMemoryRecallUnitTests test
```

Focused tests cover configuration validation, owner isolation, DST-aware cron,
credential persistence only on opt-in, runs without a browser or webhook,
Settings credential reuse, repeat saves, failed scheduling, brief persistence,
duplicate-day execution, interrupted-run recovery, the JSON null contract,
token lending and restore, the Python requirement, an unsent brief, and a strict
validation of the shipped definition and its scope references. No real mailbox was read, no
model request was submitted, no live schedule was activated, and no application
was deployed or restarted during this work. Live OAuth refresh and clustered
Quartz execution remain deployment integration checks.
