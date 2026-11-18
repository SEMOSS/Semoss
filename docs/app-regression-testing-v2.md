# App regression testing v2

Deploy `feat/app-regression-test-v2` from **Semoss** and **semoss-ui** together with current
**Monolith dev**. Build/deploy the sources and restart the backend; pulling Git branches
alone does not update a running installation. No Monolith feature branch, CI changes or
manual skill upload is required. Include the shipped `platform__app-tests` toolbox and
`platform__app-regression-testing` skill in the deployment. Use the Node/pnpm versions in
the UI's current dev configuration. Provision the Chromium revision matching the backend's
Playwright dependency under the account that runs SEMOSS.

If upgrading from the experimental database-routing branch, use a clean backend build so
removed classes are not left in the deployed output. Close old test-preview tabs and remove
the old `app-test-worker.js` service-worker registration from the relevant Monolith origin
if one was installed. V2 installs no service worker and contains no database-routing filter.

## Try it

1. Publish an editable CODE app with unique `data-testid` attributes on the controls and
   results being tested. Open its normal App preview and the **Testing** tab next to Files
   and version control.
2. Click **+** → **Record workflow**. Perform the workflow inside the app iframe, including
   ordinary links, forms or SPA route changes between pages within that same app portal.
3. Click **Add check**, then select the app element whose text/visibility should be checked.
   The selection is recorded without activating that element. Stop recording.
4. Save with a name and a meaningful **description** explaining the workflow and expected
   result. A recording without an outcome check is not verified coverage.
5. Under **Test files**, click a filename to view its JSON.
   Use **Edit** for a form with test name, description, start page and individual steps,
   or **Delete** to confirm removal. Each step shows fields for its selected action,
   optional expected-page wait and nested iframe selectors. Steps can be added, removed
   and reordered. A description and at least one valid step are required. Recording id,
   version and last-edit timestamp are not editable; the backend updates the timestamp
   when saving. Failed writes keep the form open with its draft. View still shows the
   complete read-only JSON, and normal Files supports editing the underlying file.
   Definitions live at `app_root/version/assets/app-tests/tests/<id>.json`, visible
   through normal **Files** as `app-tests/tests`. Existing tests migrate from
   `app_root/app-tests/recordings` and `app_root/version/assets/app-tests/recordings` on first
   use; differing same-id copies stop migration rather than overwrite either file. Reports remain private at `app_root/app-tests/reports`.
   Legacy version-1 recordings missing descriptions receive generated descriptions when
   listed; review those descriptions for business accuracy. After edits through normal
   Files or chat, use **Refresh tests** in the Test files header.
6. Click **Run test** on the relevant file row, **Run all** beside **Refresh tests** for
   the whole saved suite, or ask the app's chat to run relevant saved tests. Polling and the
   report distinguish passes, assertion failures, missing targets, skipped workflows and runner
   errors. The **+** menu adds tests; replay actions live on file rows and in the list toolbar.
   **Run test** runs once in a background browser; the former three-replay Verify button
   is removed. The page visibility check provides only limited smoke coverage. The API
   still supports an explicit repeat count when a caller needs a stability check.

Recording/replay performs real app actions using the app's normal connections. V2 does not
create or substitute test databases. Use a suitably prepared app/environment for workflows
that write data. Recordings marked as requiring the old experimental database profile are
rejected instead of being replayed with normal connections.

## Multiple pages

The recorder remains attached to the normal iframe and reattaches to a new document after
full navigation. Each action captures an optional app-relative `pagePath`, including query
and hash components. Before replaying that action, Playwright waits for the expected page
URL, preventing an element with the same selector on another page from being clicked early.
This also works for SPA routes that change without replacing the document.

Agent-authored tests can use explicit `NAVIGATE` steps and `pagePath` to synchronize page
boundaries. For example:

```json
{
  "name": "Orders detail",
  "description": "Open Orders, select an order, and verify its detail heading.",
  "startPath": "",
  "steps": [
    {"type":"CLICK","selector":"[data-testid=\"orders-link\"]","pagePath":""},
    {"type":"CLICK","selector":"[data-testid=\"first-order\"]","pagePath":"orders.html"},
    {"type":"ASSERT_TEXT","selector":"[data-testid=\"heading\"]","text":"Order details","exact":true,"pagePath":"details.html"}
  ]
}
```

Legacy recordings without `pagePath` still replay using their existing actions/assertions.
The app/server must serve its deep links; the runner does not add missing SPA fallback
routes. Cross-origin pages, navigation into another app, independent popup/tab flows and
browser-history navigation without a recorded reproducible trigger are outside the supported
recording flow. Same-origin nested iframe actions retain their frame selectors and top-level
page route. The iframe must be accessible to the SEMOSS UI for manual recording.

## Platform integration

The six app-test reactors require explicit app targets, authorize editing where applicable,
store declarative JSON in non-public app assets (outside `assets/public`), and run bounded headless Playwright
replays. The initiating authenticated session is passed to a fresh browser context; no login
credentials are saved in test definitions. Each replay starts with fresh browser state, which
does not reset server-side data. The App Builder gets the shipped testing skill/toolbox via
normal system-project registration.


## Test discovery and selection

Deploy the updated backend, system toolbox/skill and UI together. The list contract is
now a summary, so older clients expecting steps in list responses must be updated.
`ListAppTestRecordings(project)` returns `{target, recordings: [{id, name, description}]}`.
`GetAppTestRecording(project, recordingId)` returns `{target, recording}` for exactly one UUID.
The Testing tab lists summaries, then loads the individual definition for View/Edit;
Delete needs no definition read. Errors leave the dialog recoverable, and it never shows
an empty editable file while a read is loading or has failed.

Run relevant recordings directly, without reading their bodies into chat first:

```text
RunAppTests(project=["<current app id>"], recordingIds=["<test UUID>"]);
RunAppTests(project=["<current app id>"], recordingIds=["<first UUID>", "<second UUID>"]);
RunAppTests(project=["<current app id>"], all=[true]);
RunAppTests(project=["<current app id>"], suite=["__platform_smoke__"]);
```

Choose exactly one of `recordingIds`, `suite`, or `all=true`. Missing/ambiguous selection
is rejected; `pages` alone is not a selection. The existing `suite` parameter remains a
single-UUID alias or the reserved smoke check; names are no longer accepted. Optional
`pages` filters exact recording names/start paths inside the explicit selection. Selected
UUIDs are loaded directly, so an invalid unrelated file does not block a targeted run.
An explicitly selected missing/invalid UUID fails without falling back to other tests.
Only `all=true` reads the complete saved suite for execution.

The system skill tells chat to discover summaries, select relevant tests, and read bodies
individually only when necessary. It discourages bulk reads through generic file tools.
Full runs are reserved for full-regression requests or justified broad shared changes;
unrelated successes do not establish coverage for a requested behavior. Project edit
access is still enforced. The project argument is explicit; this change does not add a
trusted server binding between a chat and its currently open app.


## Upload a test

Use **+** → **Upload test** to select one declarative test JSON file (non-empty, at most
256 KB). It can be an exported definition or a draft with `name`, a nonblank `description`
and 1–300 supported `steps`; omitted `startPath` defaults to app home. Version, if present,
must be 1. The platform assigns a fresh UUID and timestamp, so importing never replaces
an existing test just because the file contains the same id. Backend validation and edit
permissions apply. Unsupported fields, invalid JSON and failed writes display an error;
the import is not reported as successful. JavaScript/Playwright scripts are not supported.

The per-file Run test action passes exactly that UUID without reading its definition.
Run all explicitly sends `all=true` for the current app and is disabled for an empty list
or while recording, saving or running. Each test runs once. The page visibility smoke
check remains a separate, explicitly labeled action below the list.
