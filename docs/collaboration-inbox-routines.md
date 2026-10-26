# Collaboration inbox routine

The Routines page offers one routine: a private daily brief of the owner's
Collaboration topics and most recent Brain threads, also emailed to the owner.
New users are opted out. They choose a 24-hour time, one or more weekdays, an
IANA time zone, and how far back to review email for replies, in hours or days.
The window is stored as `lookbackHours`, an integer from 1 to 720 (up to 30 days).
The default is 08:00 on Monday through Friday, reviewing the past 24 hours. The frontend starts
with the browser's time zone before the first configuration is saved.

Each run reviews pending reply candidates received within that rolling window,
measured in elapsed hours from the run, regardless of when the previous brief
succeeded. The routine writes reply-all drafts for the owner to review and saves
them under Drafts > Teamwork drafts in Outlook. It does not send those replies;
its only outgoing email is the daily brief sent to the owner. The Needs your
reply section summarizes each message and reports its draft's status and location.
Wider windows and repeated runs can revisit unanswered mail from an earlier brief
and create another draft for it; draft saving does not replace a prior run's draft.
Reply candidates are paged without an email-count cap; a large window on a busy
mailbox can still exceed the configured model's context limit.
Existing saved routines that only have `emailCount` use 24 hours; the obsolete
count is omitted from API responses and removed from storage on the next save.

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
  and a refresh token, with delegated `Mail.ReadWrite` permission to save drafts,
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
  "lookbackHours": 72
}]);
```

Save the same settings with `enabled: false` to pause. Both calls return the
routine settings, `configured`, prerequisite availability, `nextRunAt`, and
`lastRun`. The latter contains `status` (`running`, `completed`, or `failed`), UTC
ISO timestamps, `roomId`, `summary`, optional `html`, and `error`. `html` is the exact
email body saved by the latest successful delivery node; older runs without it
keep their Markdown `summary` as the display fallback. Both routines use this
contract, and the Markdown remains the assistant conversation's initial message.
The UI must render HTML in its isolated, sanitized email viewer. Null-valued
fields are omitted by the existing Pixel JSON serializer; consumers normalize
them to null.

Daily brief and weekly agenda get/save responses also include `flow`, a read-only
projection of the same server-held definition used for execution. It contains
only `nodes` (`id`, `type`, `label`, and optional numeric `position.x`/`position.y`)
and `edges` (`source`, `target`, `sourcePort`, `targetPort`). Definitions without
positions use the preview's automatic layout. Source, configuration, and run
details are excluded from the projection, which is available before opt-in.

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

The definition runs these stages, with a bounded loop for meeting notes:

1. `BrainSync()` imports new mail (and Teams when on) and classifies threads. An
   error, such as an import still running, does not stop the brief.
2. A Python step waits up to about five minutes for that sync job and its
   thread summaries.
3. A Python step collects pending reply candidates with `WorkListItems`, paging
   through email in received-time order until the `lookback_hours` cutoff, plus
   today's meetings and open action items. The lookback applies to reply
   candidates; meeting and action-item collection keep their existing scopes.
4. An `agent.run` step writes reply text for the pending messages, using the
   gathered context and examples of the owner's recent sent mail.
5. A Python step saves each generated reply through `MicrosoftOutlookReplyMail`
   with `replyAll=True` and `asDraft=True`, leaving Outlook to supply recipients
   and the quoted original. It creates Drafts > Teamwork drafts when needed and
   moves the saved drafts there. A failed folder move leaves the draft saved;
   a failed draft creation is reported in the brief. The step returns the actual
   draft statuses as `saved.replies`.
6. A Python step prepares the factual inventory: reply identities and draft statuses,
   exact duplicate action removal, normalized deadlines, and calendar order. Separate
   records and workspace deliverables are preserved. Meeting evidence excludes
   dated, generated daily or weekly briefs. Related subjects must match the normalized
   meeting title or share at least two meaningful words. Attachments linked to rejected
   threads are excluded too. This conservative filter can omit useful weak matches.
7. A Java-owned `control.loop` processes up to ten gathered meetings, one at a time.
   Each iteration makes one authenticated `LLM` call through a guarded Python step,
   using the owner's Collaboration model, no history, no tools, and the model's
   configured generation defaults. It requests a short combined meeting summary,
   distinct attachment takeaways, and at most three supported unresolved questions.
   Today's date is supplied explicitly; the current calendar entry takes precedence
   over historical cancellations, rescheduling, and attendance requests.
   The server binds the `brief_model` global only for this daily routine. A provider
   failure, tool response, or invalid JSON produces an unavailable note without
   stopping the brief. The guard is local because a failed `model.chat` node would
   stop the workflow; the shared Automation runtime and `inbox_brief` harness are unchanged.
8. A Python step assembles Markdown with Suggested priorities, Needs your reply,
   Today's meetings, and Open action items sections. Code supplies every retained
   reply, meeting, attachment name, action, date, link, and draft status. Model prose
   is escaped as text and bounded to 60 words per summary, 35 per attachment takeaway,
   and 40 per question. Invalid fields fall back independently; unavailable notes
   leave the meeting's factual entry and invitation link. A saved draft still needs
   the owner's review. Only gathered reply records receive a `Draft:` bullet.
9. A Python step converts the Markdown to HTML (raw HTML in the brief is
   escaped, images are dropped, and links to URLs not found in the thread data
   are removed) and sends it with `MicrosoftOutlookSendMail` to the owner only.

Suggested priorities opens the brief with three groups: 🔴 Due today (dated from
the owner's local day, with overdue items explicitly marked), 🟡 Due soon (the
next seven calendar days, plus explicitly urgent undated work marked as such),
and 🟢 Flexible (later or undated next actions). It selects at most five short
priority references; the detail sections still contain every retained record.
Code classifies recorded deadlines using the owner's time zone and the origin of
action-item steps so Brain's date-only deadlines stored as midnight UTC keep their
original calendar date. Missing deadlines are labeled "No recorded deadline";
message receipt dates and model prose never establish deadlines. Semantic duplicates
with different source records remain separate to avoid dropping distinct obligations.

The routine's own exclusion filter no longer applies; only Brain's import gating
does. The sync spends model calls on classification and thread summaries.

The latest result and status are stored in `WORK_INBOX_ROUTINE`, scoped to the
owner's ID and authentication type, and shown in Routines. If the brief was
written but the email failed, the routine still completes with the brief and
reports that the brief could not be delivered. The assembled daily brief does not
require a model room; "Open session" can create a discussion room from the saved
brief. A new brief replaces the routine's
latest result. Interrupted runs are reconciled with the durable Automation run
status; runs still unfinished after twenty minutes are cancelled and failed so
the next scheduled day can retry. The reply-drafting agent wait is limited to ten minutes.

Topics, threads, and the brief are also kept as Automation node outputs, which
the owner can read with `GetAutomationRun`. "Reset my data" deletes these runs;
there is no time-based retention.

## Deployment and verification

Deploy the updated SEMOSS Java classes through the normal application build and
restart process. The daily brief's `WorkRoutine` model binding and JSON definition
must be deployed together; a JSON-only update leaves `brief_model` unset. Security
startup adds `MS_GRAPH_CREDENTIAL` and its unique user
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

Focused tests cover lookback validation and saved-setting migration, owner isolation, DST-aware cron,
credential persistence only on opt-in, runs without a browser or webhook,
Settings credential reuse, repeat saves, failed scheduling, brief persistence,
duplicate-day execution, interrupted-run recovery, the JSON null contract,
token lending and restore, the Python requirement, an unsent brief, strict
validation of the shipped definition and its scope references, and an invariant
that replies are saved as reply-all drafts while only the owner's brief is sent.
The brief-specific Python checks execute the shipped sources with mailbox/model
calls stubbed, covering coverage, deadline rules, isolated model requests, fallback,
literal text and URL preservation through email rendering, and actual draft status.
Run them with the application's Python environment:

```sh
python -m unittest discover -s py/testing -p 'test_inbox*brief*.py' -v
```

Live OAuth refresh and clustered Quartz execution remain deployment integration checks.
