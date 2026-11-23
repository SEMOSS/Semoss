---
name: app-regression-testing
description: Run saved Playwright regression recordings after changing a SEMOSS app, inspect evidence, repair understood app regressions within a limit, and report unverified behavior truthfully.
---

# Test an app after changing it

Use the platform **App Tests** toolbox (`app-tests`). Always pass the explicit target app project id. The toolbox context belongs to the toolbox, not to the app being edited.

1. Finish the app build. Normal tests exercise the published portal; when isolated H2 is enabled, tests snapshot the current built `assets/portals/` files. Rebuild after source changes; this feature does not run npm builds.
2. Call `ListAppTestRecordings(project=<current app id>)` to see only recording IDs, names and descriptions. Choose tests relevant to the requested behavior and changed shared components. Do not bulk-read the test directory or load every definition through file/Bash tools. Call `GetAppTestRecording(project=<same app>, recordingId=<one UUID>)` only when you need that test's steps or expectations; replaying it does not require reading its content first.
3. Run the relevant IDs with `RunAppTests(project=<same app>, recordingIds=[<UUID>, ...])`. Never omit the selection. Use `all=true` only when the user requests full regression coverage or a shared change justifies it; state why the broader run is needed. `recordingIds`, `suite` and `all=true` are mutually exclusive. `suite` accepts a single UUID or `__platform_smoke__`, never a name. Optional `pages` only narrows an explicit selection by exact recording name/start path. No relevant saved test means missing coverage: report it, and create a new test only with a known expected outcome; do not run unrelated tests as a substitute.
4. Check the returned `target` matches the app being edited. Poll `GetAppTestRun(runId)` about once per second while status is `RUNNING` or `QUEUED`; stop after three minutes and report a runner timeout if it has not finished.
5. Read `status`, `summary`, and each test's `classification`, `failedStep`, `reason`, `expected`, `actual`, and `evidence`.

| Result | Action |
| --- | --- |
| `COMPLETED` with real passed tests | Report verified pass counts and any skipped workflows. |
| `NO_TESTS` | Say no recordings matched. You may run `RunAppTests(project=<same app>, suite="__platform_smoke__")` for a limited page visibility check. Recommend recording a workflow and adding checks using the Testing workbench tab. Never claim missing workflows passed. |
| `scope: PAGE_VISIBILITY_SMOKE` | Report only that portal page content was visible and whether immediate unhandled JavaScript errors were observed. No saved workflow was exercised; even a passing smoke check does not verify app behavior. |
| `SKIPPED` / `NO_ASSERTIONS` | Actions replayed without verification. Report as unverified; add checks only with a known expected result. |
| `REGRESSION_SUSPECTED` | Investigate the step and evidence. Fix the app only when the cause is understood, then publish and run again. |
| `TARGET_NOT_FOUND` | The recorded element no longer exists. This may be a regression or an intended UI change; check whether your change removed or renamed it. Restore it only if the change was not meant to remove it, otherwise report it for review. Never edit or delete the test to make it pass. |
| `FLAKY` | The same recording gave different results on identical replays. Do not rely on it and do not claim a pass. Report it and, if it is a test you just saved, remove it with the user's approval. |
| `TEST_NEEDS_UPDATE` | Requires evidence of an intentionally changed expectation; report for review. Never infer intent merely from a selector failure. |
| `ERROR` | Report runner/authentication/environment failure. Do not infer that the app is broken. |
| `mode: MOCK` | Development fixture only; no browser or app was tested. Never describe fixture output as verification. |

Limit fix-and-run cycles to the returned `maxFixAttempts`. Never delete, weaken, edit or skip tests to manufacture a passing run. Do not run against another app. Report passed, failed, errored and skipped counts, the failing workflow/step, app changes, and remaining work. Reports are persisted privately beside the app under `app_root/app-tests/reports`, outside published assets.

The reserved `__platform_smoke__` suite is platform-owned and generated in memory. It checks only that the published portal has visible page content (the `body` or a visible descendant, including fixed-position layouts), plus unhandled JavaScript errors observed during that brief check. It saves no recording into the app and provides **no workflow coverage**. Treat an unknown app's smoke success as limited evidence, never as complete app validation. `runtimeErrorCount` counts observed errors without exposing raw exception payloads.

## Verify a recording you save

After `SaveAppTestRecording`, run `RunAppTests(project=<same app>, recordingIds=[<saved UUID>])` once. Report coverage only if it passes. A failed replay may reveal an app regression, a wrong expectation, a route mismatch, or an environment problem; inspect classification, failedStep and evidence before deciding. Preserve the test when its expectation remains valid. Correct an authored definition only when its mistake is understood; never delete or weaken a test to manufacture a pass. A recording needs at least one supported `ASSERT_*` check; without one it is `SKIPPED`.

## Stable targets

Give every interactive element and every value a test reads a unique `data-testid` (for example `button-equals`, `display-screen`) and select it as `[data-testid="button-equals"]`. Ids, labels, names and position paths change with ordinary edits; a test id is a contract. Where a deployment sets `APP_TEST_REQUIRE_TESTID=true`, saving a recording with any other selector is rejected.

Each replay runs in a fresh browser with a fixed viewport, `en-US` locale, UTC timezone, light color scheme, reduced motion and CSS animations and transitions off. A deployment may also set `APP_TEST_FIXED_TIME` (an ISO instant) to start the page clock at a fixed time for screens that show dates.

Definitions are stored at `app_root/version/assets/app-tests/tests/<id>.json`,
visible and editable in the app's normal Files panel under `app-tests/tests`.
The Testing panel also supports viewing, editing and deleting these definitions.
Keep each file's id equal to its UUID filename, version equal to 1, and a meaningful
nonblank description. Legacy definitions migrate from `app_root/app-tests/recordings` and
`app_root/version/assets/app-tests/recordings` on first use; private reports stay outside assets.
Use Refresh tests after editing files. The Testing tab offers Run test per file and an explicit
Run all action. The + menu offers Record workflow or Upload test (JSON imported as a new test ID).

## Recording format

`ListAppTestRecordings(project)` returns `recordings` containing only `id`, `name` and `description`, plus an `invalid` list of malformed filenames and validation reasons. Valid tests remain discoverable when another file is broken. Report invalid files as missing coverage; do not silently exclude them from a full-suite claim. Repair a known definition or, with authorization, use `DeleteAppTestRecording(project, fileName=<exact invalid basename>)`. This option refuses valid files; delete those by recordingId. `GetAppTestRecording(project, recordingId)` reads one full recording, including steps, when needed. `SaveAppTestRecording(project, recording)` requires `name`, a nonblank `description` explaining the workflow and expected outcome (at most 2000 characters), and `steps`. Optional fields are UUID `id` and app-relative `startPath` (empty for portal root). Give every test a meaningful description; for example, "Create an order on the checkout page, navigate to Orders, and verify the new order appears." Steps use CSS `selector`, optional nested iframe CSS selectors in `framePath`, and one of:

- `CLICK`, `ASSERT_VISIBLE`, `ASSERT_HIDDEN` (hidden or absent), `ASSERT_ABSENT` (zero matching elements)
- `ASSERT_VALUE` with string `value` for an input, textarea or single select; `ASSERT_CHECKED` with boolean `checked`; `ASSERT_COUNT` with integer `count`
- `WAIT_FOR` with `selector` and `state`: `visible`, `hidden`, `attached` or `detached`. Optional `timeoutMs` on any step is 100–30000 ms; route wait and action share that budget within the whole-run limit. Do not use sleeps or arbitrary scripts.
- `DRAG` with source `selector` and destination `targetSelector` in the same frame. Supported for ordinary DOM drag targets, not every custom canvas/pointer library.
- `FILL` / `SELECT` with string `value`. SELECT uses the HTML option **value**, which may differ from its visible label.
- `CHECK` with boolean `checked`
- `KEY` with string `key`
- `SCROLL` with numeric `x`, `y`
- `ASSERT_TEXT` with expected `text`; it is a substring match unless `exact` is `true`, which requires the element's whole text to equal `text`. Prefer `exact: true` for numbers and short values (a substring `9` also matches `19`).
- `NAVIGATE` with app-relative `path` (not `url`); `ASSERT_URL` with `path` checks the route without navigating.
- Optional `pagePath` on any step: the app-relative top-level URL expected before that step, including query/hash. The runner waits for that route rather than navigating again. Manual recording adds it automatically; include it in agent-authored multi-page workflows to prevent actions on the wrong page.

### Authoring rules and a multi-page example

- For a new test omit `id`; use the UUID returned by Save thereafter. `name` is a label, not an ID. When editing files directly, use a canonical lowercase UUID as both `id` and filename. Never use task IDs or the app/project ID as the test ID.
- Use exactly the uppercase step types listed above. Do not generate Playwright method names, `GOTO`, `WAIT`, or executable JavaScript. Use the tool recording schema; unsupported step errors identify the offending step and allowed types.
- All paths are relative to `portals/`: `index.html#/boards/b1`, `#/boards/b1`, `details.html` or the empty portal-root path. `/`, `/portals/index.html`, and full URLs are invalid. Preserve the actual entry filename, query and hash throughout a test; `#/...` resolves at the portal directory, not at an earlier `index.html` URL. Prefer hash routing for static React portals, unless server-side route fallback is known to exist.
- `pagePath` is the URL **before** that step. A click that opens details still has the board pagePath; the following step has the details pagePath. Do not insert NAVIGATE before each action: normal app links should exercise routing naturally.
- A text check on a `<select>` reads all option text, not the selected value. Use `ASSERT_VALUE` with the option value. To verify persistence, save and reopen before checking. Manual Add check records values for inputs/selects and checked state for checkbox/radio controls.
- Check `testDatabaseEnabled` in ListAppTestRecordings. When true, the platform automatically routes supported calls using the original database ID to disposable H2. Do not change engine IDs in app code or add `?env=test` unless the app itself independently requires it. Without platform isolation, preserve any existing app-specific test environment selection.

Example `recording` object for an app with these exact routes/targets (adapt after inspecting that app):

```json
{
  "name": "Open task and return to board",
  "description": "On the test database, open task t1 from board b1, verify its page, then return to the board.",
  "startPath": "index.html?env=test#/boards/b1",
  "steps": [
    {"type":"ASSERT_TEXT","selector":"[data-testid=\"environment-badge\"]","text":"TEST DATABASE","exact":true,"pagePath":"index.html?env=test#/boards/b1"},
    {"type":"CLICK","selector":"[data-testid=\"task-t1\"]","pagePath":"index.html?env=test#/boards/b1"},
    {"type":"ASSERT_VISIBLE","selector":"[data-testid=\"task-title\"]","pagePath":"index.html?env=test#/boards/b1/tasks/t1"},
    {"type":"CLICK","selector":"[data-testid=\"task-back\"]","pagePath":"index.html?env=test#/boards/b1/tasks/t1"},
    {"type":"ASSERT_VISIBLE","selector":"[data-testid=\"board-heading\"]","pagePath":"index.html?env=test#/boards/b1"}
  ]
}
```

Save it in the target app, then run only `recordingIds=["<UUID returned by Save>"]`. A Save/Run tool validation error is **not** a browser test result. Correct the named field/step and retry; do not claim a pass or silently switch to unrelated tests. A `suite` workaround still requires the same saved UUID; it is not a test name or path. After two validation failures, reread the current schema/skill and correct the named field instead of guessing. Do not bypass Save with an unvalidated file. If a valid payload keeps failing, report the exact tool error and deployed backend version. Poll only a runId returned by RunAppTests; saved test UUIDs and app IDs are not run IDs.

Record through the ordinary app iframe using the Testing workbench tab. Password fields are excluded; browser session credentials are never written into recordings. Normal replays use the initiating signed-in SEMOSS session. Isolated H2 replays use a cookie-free browser and a restricted test-only SDK context. Authentication failures require signing in again. Cross-origin or inaccessible iframe workflows cannot be recorded by this same-origin approach. Arbitrary JavaScript test execution is not supported. Deployments must provision the Chromium revision matching the application's Playwright dependency; user jobs never download browser binaries.


## Replay isolation and recovery

For normal tests, fresh browser state does not reset a database. Isolated H2 recreates schema and seed for each test and each repeated attempt; a manual preview has separate state and replay never reuses its modified rows. Only one real run per app can execute at a time on a backend node; wait for it to finish before starting another. Other nodes/users may still modify the same database. For stateful tests, optional `useVariables: true` expands `{{runId}}`, `{{testId}}`, `{{attemptId}}` and `{{uniqueId}}` in supported step strings. A uniqueId is shared by main and cleanup steps within an attempt and differs on replay. Opt in explicitly; unrecognized variables are rejected. Keep generated data distinguishable and disposable.

Optional `cleanupSteps` (at most 50) run after main steps, including a failed workflow, while the browser is open and within the remaining run budget. Cleanup is best effort, not a transaction rollback. Its result is reported separately; a cleanup failure makes an otherwise passing test fail. It cannot undo interrupted processes or guarantee database isolation. A FLAKY result describes differing outcomes, not proof of a runner defect or a database root cause.

`AMBIGUOUS_SELECTOR` means multiple targets matched; inspect the app and narrow to one intended element. Never select the first match merely to force a pass. `INVALID_SELECTOR` identifies malformed CSS. `WRONG_PAGE` means the expected route was not reached; compare safe expectedPage/actualPage (query values are redacted). `TARGET_NOT_FOUND` may include availableTestIds as hints, not evidence of the intended replacement. `framePath` is only for actual nested iframe elements, never list rows or containers. `CLEANUP_FAILED` requires inspecting the separately reported cleanup.

## Multiple pages

Links, forms and SPA navigation within the same app portal and iframe are supported. Exact route matching is the default. Only when the app intentionally treats portal directory/index.html and empty/#/#/ as its root aliases, set recording `routeMatch: "PORTAL_ROOT"`; query parameters still match exactly, including test environment selection. Other paths and trailing slashes remain distinct. The recorder reattaches after full document loads and tags actions with the current top-level route; replay waits for that route before finding the target. Use app-relative `NAVIGATE` when explicitly changing pages. Normal replays require the app/server to serve deep pages. Isolated H2 previews provide index.html fallback for extensionless SPA routes, and serve actual .html pages directly. React BrowserRouter must use the portal base path (for example the pathname of document.baseURI); hash routing also works. Other origins, other apps and popup/tab flows are outside this recorder's scope.

## Isolated H2 test database

`ListAppTestRecordings` also reports `testDatabaseEnabled`. For enabled apps, RunAppTests automatically creates a fresh H2 database from saved schema/seed SQL for every selected test and replay. No production rows are copied, no source schema is queried automatically, and app engine IDs remain unchanged. Check `mode: ISOLATED_H2` in the run result before describing a run as database-isolated.

Configure through Testing > Test Database or `ManageAppTestDatabase(project, action="save", configuration={enabled:true, sourceEngineId:"<original H2 ID>", schemaSql:"...", seedSql:"..."})`. First use action="get" to inspect existing settings. Derive schema/fixtures from known app requirements; use synthetic deterministic data. Do not invent a source engine ID or delete a working configuration. Saving validates the fixture in a temporary database. No JVM properties or operator files are needed.

The SQL surface supports one literal `SqlQuery`, single-table SELECT/DISTINCT, aliases/quoted identifiers, comparisons, AND/OR/NOT, IN/LIKE/BETWEEN/NULL checks, grouping, ordering, LIMIT/OFFSET and OFFSET/FETCH. COUNT/SUM/AVG/MIN/MAX are allowed; joins, subqueries and other functions remain unavailable. INSERT uses literal rows; UPDATE/DELETE require WHERE. Scalar and single-value-list Pixel arguments are supported. Each SQL statement auto-commits inside its disposable database; optional commit/transaction flags do not create multi-request transactions. Read-only GetAppAssets for this app’s snapshotted public JSON settings is supported; app-file writes are blocked. SDK config supplies a synthetic preview-local identity without real credentials/privileges. Init/LoadApp and SDK DropInsight are acknowledged without loading custom app reactors or Python. The platform core SDK import map can use a snapshotted preview-local SDK and chunks; other dependencies must be bundled. Other Pixel operations, Python/server-side code, external services and unmapped engines are unavailable. Inspect the first diagnostic: phase, code, sanitized request path, Pixel operation and redacted SQL shape. A STARTUP failure before any attempted action is not a failed workflow assertion. Missing assets and blocked endpoints/CSP have TEST_PREVIEW_* codes; TEST_DATABASE_SQL_ERROR identifies fixture SQL execution, while TEST_DATABASE_UNSUPPORTED identifies a supported-surface mismatch. Report these, TEST_DATABASE_SETUP_FAILED and UNSUPPORTED_DEPENDENCY as environment/compatibility issues, not application regressions. Do not infer a database failure from a blocked script or stylesheet. Never disable isolation, switch to normal connections, or weaken assertions as a workaround.

For manual recording, save/enable the fixture, click Open Test Preview in the Testing tab, then record in the same app iframe. Reset Test Preview restores seed state; Return to Normal App closes it. Previews expire after thirty minutes. Saved test routes must stay relative to portals/, without any preview URL, capability, environment ID or test-only hosting prefix. The open response contains a temporary credential; never include its previewPath in chat, files or reports. The preview is intended for supported trusted app builds, not arbitrary hostile JavaScript. Use bundled React/dependencies and relative build assets; the platform core SDK import map is also supported when the deployed frontend includes its SDK distribution. Single-node/sticky routing is required for manual previews.

With isolation disabled, recording and replay still perform real app actions through existing backend connections. Tests requiring the earlier experimental database-routing profile remain rejected rather than guessed.

## Persisted execution history

Every accepted real run writes compact history under `assets/app-tests/test-runs/<run-id>.json`, independently of chat or UI polling. Use `ListAppTestRuns(project, recordingId?)` for bounded summaries and `GetAppTestRunHistory(project, runId, recordingId?)` for one relevant result. History keeps 100 finished runs per app plus active runs; expired unfinished runs are reported INTERRUPTED. Do not bulk-read history or treat an old pass as verification of the current app/test definition. Definition hashes identify the test version that ran, not the app source revision. Detailed screenshots and captured page text remain in caller-private reports.

History resolves its local destination before worker dispatch. Local persistence failures and cloud replication/retention warnings are distinguished; a replication warning means the local result exists but may not be visible on other nodes. Report historyWarning even when the browser checks pass. Unknown live run IDs may have expired or belonged to a previous backend process; use the app-scoped persisted history tools when appropriate.
