---
name: exports
description: Use when writing code in an app that generates downloadable documents or files - PDF reports, Word documents, Excel/CSV exports of query results - or that sends email, or that schedules a recurring job (nightly refresh, weekly emailed digest). Covers ToPdf(), ToDocx(), ToExcel(), ToCsv(), ToTsv(), ToTxt(), TableToXLSX(), PdfToImage(), EncryptPdf(), SendEmail(), and the scheduler pixels (ScheduleJob, ListAllJobs, ExecuteScheduledJob, PauseJobTrigger, ResumeJobTrigger, RemoveJobFromDB, SchedulerHistory) via @semoss/sdk's runPixel. Do not use for handing an existing file to the user (DownloadAsset, see app-bootstrap) or for saving files durably (see app-data).
---

# Exports, email, and scheduled jobs

Three capabilities that compose into the classic "generate a report, email it, every Monday" feature. All via `runPixel` from `@semoss/sdk`.

## The download-key pattern

Every document generator below returns a **download key** (`pixelReturn[0].output`, operationType includes `FILE_DOWNLOAD`). Turning that into a browser download is the same two-step flow app-bootstrap documents for `DownloadAsset`:

```typescript
import { runPixel, Env } from "@semoss/sdk";

const gen = await runPixel(`ToPdf(html=["<encode>${html}</encode>"], fileName=["report"]);`, insightId);
const fileKey = gen.pixelReturn[0].output as string;

// trigger the browser download
window.open(`${Env.MODULE}/api/engine/downloadFile?insightId=${insightId}&fileKey=${encodeURIComponent(fileKey)}`, "_blank");
```

The key is minted per insight and is not a stable/shareable URL - generate fresh per request.

## Documents from HTML or markdown

**`ToPdf`** - accepts `html` OR `markdown` (or `filePath`/`url` as the source), plus:

- `mustache=[true]` + `mustacheVars=[{...}]` - server-side templating: write the report once, fill in data per run
- `pdfPageNumbers=[true]`, `pdfPageNumbersIgnoreFirst`, `pdfStartPageNumbers` - pagination
- `pdfSignatureBlock=[true]`, `pdfSignatureLabel` - signature fields
- `fileName` - output name (key is still returned)

**`ToDocx`** - same core inputs (`html`/`markdown`/`filePath`/`url`, `mustache`, `mustacheVars`, `fileName`); produces a real .docx with tables, styling, and images.

```typescript
const pdf = await runPixel(
  `ToPdf(markdown=["<encode># Invoice {{number}}\n\n| Item | Cost |\n|---|---|\n{{#lines}}| {{item}} | {{cost}} |\n{{/lines}}</encode>"], mustache=[true], mustacheVars=[${JSON.stringify({ number: "1042", lines })}], pdfPageNumbers=[true], fileName=["invoice-1042"]);`,
  insightId,
);
```

Utilities: `PdfToImage(filePath, space)` rasterizes PDF pages to images; `EncryptPdf(filePath, password, allowPrint, allowCopy, allowModify, readOnly)` password-protects an existing PDF.

## Tabular exports from query results

The flat-file exporters consume the **task** produced by the previous step in a pipe chain:

```typescript
// query -> csv in one chain; returns the download key
const csv = await runPixel(
  `SqlQuery(database="${DATABASE_ID}", query="<encode>${sql}</encode>") | ToCsv(fileName=["export"]);`,
  insightId,
);
```

- `ToCsv` / `ToTsv` / `ToXml` - `task`, `fileName`, `filePath`, `appendTimestamp`
- `ToTxt` - adds `delimiter` for custom-delimited output
- `ToExcel` - `task`, `fileName`, `password` (optional password-protected .xlsx)
- `TableToXLSX` - `sheet`, `html`: renders an HTML `<table>` (styles included) into a styled sheet; returns a file location rather than a download key
- Frame data exports the same way: `Frame(frame=["f"]) | QueryAll() | ToCsv(fileName=["export"]);`

These exporters write into the insight space (they have no `space` argument). To keep a generated file durably, follow up with `SaveAppAssets`/`SaveAppAssetsBase64` (see app-data).

## Email

```typescript
await runPixel(
  `SendEmail(to=["person@example.com"], subject=["Weekly digest"], message=["<encode>${html}</encode>"], html=[true], attachments=["report.pdf"]);`,
  insightId,
);
// -> pixelReturn[0].output === true on success
```

- Recipients: `to`, `cc`, `bcc` (at least one required). `from` falls back to the server's configured SMTP sender; `smtpHost`/`smtpPort`/`username`/`password` are only needed to override server config.
- `html=[true]` sends an HTML body; `mustache=[true]` + `mustacheVars` template the message.
- `attachments` are file paths resolved against the insight space by default, or another space via `space` - so a `ToPdf` output in the same insight can be attached by name.
- The message body can also come from a file: `filePath` + `space` instead of `message`.

Email depends on the server having SMTP configured - surface the pixel error to the user rather than assuming delivery.

## Scheduled recurring jobs

A scheduled job runs a **pixel recipe** on a Quartz cron schedule, server-side, no browser needed. The caller must be a project editor (or admin); scheduling may be disabled on some installs (the pixel errors if so).

```typescript
// every Monday 08:00 in the server default tz: refresh + email
const job = await runPixel(
  `ScheduleJob(jobName=["weekly-digest"], jobGroup=["${APP_ID}"], cronExpression=["0 0 8 ? * MON"], recipe=["<encode>SqlQuery(database=\\"${DATABASE_ID}\\", query=\\"...\\") | ToCsv(fileName=[\\"digest\\"]); SendEmail(to=[\\"team@example.com\\"], subject=[\\"Digest\\"], message=[\\"attached\\"], attachments=[\\"digest.csv\\"]);</encode>"], triggerOnLoad=[false]);`,
  insightId,
);
// -> MAP with the job config including the server-generated jobId
```

Essentials:

- `jobGroup` **is the project id** - it scopes both permission checks and listing.
- `cronExpression` is Quartz syntax (6-7 fields, `?` for the unused day field); `cronTz` overrides the timezone.
- `triggerNow=[true]` fires immediately in addition to scheduling; `triggerOnLoad=[true]` fires on server restart.
- `recipeParameters` supplies parameter values when the recipe is parameterized; `jobTags` labels jobs for filtering.

Managing jobs:

| Pixel | Params | Purpose |
| --- | --- | --- |
| `ListAllJobs` | `project`, `jobTags`, `myJobs` (all optional) | Job configs keyed by jobId |
| `EditScheduledJob` | `jobId` + same params as ScheduleJob (+ `currentJobName`/`currentJobGroup` when renaming) | Update a job |
| `ExecuteScheduledJob` | `jobId`, `jobGroup` | Fire on demand |
| `PauseJobTrigger` / `ResumeJobTrigger` | `jobId`, `jobGroup` | Suspend / resume |
| `RemoveJobFromDB` | `jobId`, `jobGroup` (parallel lists ok) | Delete |
| `SchedulerHistory` | `limit`, `offset`, `filters`, `jobTags` | Run log: start/end, success, output per execution |

Surface `SchedulerHistory` in any scheduling UI - it is the only way users see whether last night's run succeeded.

The recipe runs headless as the scheduling user: it can use any pixel in these skills, but nothing that expects a browser (no download keys reach anyone - write outputs to project space or email them).
