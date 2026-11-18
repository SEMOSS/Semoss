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
2. Click **Record**. Perform the workflow inside the app iframe, including ordinary links,
   forms or SPA route changes between pages within that same app portal.
3. Click **Add check**, then select the app element whose text/visibility should be checked.
   The selection is recorded without activating that element. Stop recording.
4. Save with a name and a meaningful **description** explaining the workflow and expected
   result. A recording without an outcome check is not verified coverage.
5. Under **Test files**, click a filename to view its JSON and select it for replay.
   Use **Edit** to update the definition or **Delete** to confirm its removal. Editing
   requires valid JSON, the original id/version, a description and at least one step;
   backend step validation also applies. Failed writes keep the editor open with its draft.
   Definitions live at `app_root/version/assets/app-tests/recordings/<id>.json`, visible
   through normal **Files** as `app-tests/recordings`. Existing recordings migrate from
   `app_root/app-tests/recordings` on first use; differing same-id copies stop migration
   rather than overwrite either file. Reports remain private at `app_root/app-tests/reports`.
   Legacy version-1 recordings missing descriptions receive generated descriptions when
   listed; review those descriptions for business accuracy. After edits through normal
   Files or chat, use **Refresh recordings** in the Test files header.
6. Click **Run test**, or ask the app's chat to run its saved tests. Polling and the report
   distinguish passes, assertion failures, missing targets, skipped workflows and runner
   errors. Recording controls and saved-test replay controls are grouped separately.
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

The five app-test reactors require explicit app targets, authorize editing where applicable,
store declarative JSON in non-public app assets (outside `assets/public`), and run bounded headless Playwright
replays. The initiating authenticated session is passed to a fresh browser context; no login
credentials are saved in test definitions. Each replay starts with fresh browser state, which
does not reset server-side data. The App Builder gets the shipped testing skill/toolbox via
normal system-project registration.
