# Collaboration inbox routine

The Routines page offers one routine: a private daily brief of the most recent
Outlook inbox emails. New users are opted out. They choose a 24-hour time, one or
more weekdays, an IANA time zone, and an email limit from 1 to 100. The default is
08:00 on Monday through Friday, scanning 25 messages. The frontend starts with
the browser's time zone before the first configuration is saved.

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
  and a refresh token, with delegated `Mail.Read` or `Mail.ReadWrite` permission.

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

The server reads the newest inbox messages using the existing Outlook helper,
then submits an ordinary durable `AgentRunService` request (the service used by
`RunAgent`). Its small `inbox_brief` harness makes one model call and has no tool
executor. A tool-call response is rejected. Email bodies are bounded to 1,200
characters per message and supplied as untrusted excerpts; attachments are not
read and emails are not changed or sent.

The prompt requests at most 120 words: one overview sentence and up to three
short bullets covering actions, explicit deadlines, and important changes.
Related emails are grouped, and routine notifications are omitted unless they
need action. The model is instructed to avoid per-email lists and extra sections.

The latest result and status are stored in `WORK_INBOX_ROUTINE`, scoped to the
owner's ID and authentication type. The output is shown in Routines. A standard
private room also retains the agent run, without the automatic tools or memory
hooks of an interactive Collaboration assistant. A new brief replaces the
routine's latest result. Interrupted runs are reconciled with durable agent
status; runs still unfinished after fifteen minutes are failed so the next
scheduled day can retry. Each model wait is limited to ten minutes.

## Deployment and verification

Deploy the updated SEMOSS Java classes through the normal application build and
restart process. Security startup adds `MS_GRAPH_CREDENTIAL` and its unique user
index; Collaboration startup adds the routine table and unique owner index through
the existing schema initialization. The new reactors use
normal classpath discovery; `inbox_brief` is registered in the agent harness
registry. No separate migration command or new dependency is required.

Verified with Java 21 using:

```sh
mvn -q -DskipTests compile
mvn -q -Dlicense.skip=true -Dtest=WorkInboxRoutineUnitTests,InboxBriefHarnessUnitTests,MicrosoftGraphCredentialDbUnitTests,MicrosoftGraphCredentialStoreUnitTests,MicrosoftTokenFillerUnitTests,MicrosoftLoginUtilsUnitTests,SecurityExternalConnectorsUtilsUnitTests test
```

All 48 focused tests passed. They cover configuration validation, owner isolation,
DST-aware cron, credential persistence only on opt-in, runs without a browser or
webhook, Settings credential reuse, refresh-token replacement, repeat saves, failed scheduling,
brief persistence, duplicate-day execution, interrupted-run recovery, the JSON
null contract, and rejection of tool requests. No real mailbox was read, no
model request was submitted, no live schedule was activated, and no application
was deployed or restarted during this work. Live OAuth refresh and clustered
Quartz execution remain deployment integration checks.
