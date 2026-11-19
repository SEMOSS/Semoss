---
name: app-regression-testing
description: Run saved Playwright regression recordings after changing a SEMOSS app, inspect evidence, repair understood app regressions within a limit, and report unverified behavior truthfully.
---

# Test an app after changing it

Use the platform **App Tests** toolbox (`app-tests`). Always pass the explicit target app project id. The toolbox context belongs to the toolbox, not to the app being edited.

1. Finish and publish the app change using its normal build/publish flow. Tests exercise the published app portal.
2. Call `ListAppTestRecordings(project=<current app id>)` to see only recording IDs, names and descriptions. Choose tests relevant to the requested behavior and changed shared components. Do not bulk-read the test directory or load every definition through file/Bash tools. Call `GetAppTestRecording(project=<same app>, recordingId=<one UUID>)` only when you need that test's steps or expectations; replaying it does not require reading its content first.
3. Run the relevant IDs with `RunAppTests(project=<same app>, recordingIds=[<UUID>, ...])`. Never omit the selection. Use `all=true` only when the user requests full regression coverage or a shared change justifies it; state why the broader run is needed. `recordingIds`, `suite` and `all=true` are mutually exclusive. `suite` accepts a single UUID or `__platform_smoke__`, never a name. Optional `pages` only narrows an explicit selection by exact recording name/start path. No relevant saved test means missing coverage: report it, and create a new test only with a known expected outcome; do not run unrelated tests as a substitute.
4. Check the returned `target` matches the app being edited. Poll `GetAppTestRun(runId)` about once per second while status is `RUNNING`; stop after three minutes and report a runner timeout if it has not finished.
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

After `SaveAppTestRecording`, run `RunAppTests(project=<same app>, recordingIds=[<saved UUID>])` once. Keep the recording only if it reports `PASSED`. A `FLAKY`, `TARGET_NOT_FOUND` or `FAILED` result means the recording is wrong or unstable: fix or remove it, and do not report it as coverage. A recording needs at least one check (`ASSERT_TEXT` or `ASSERT_VISIBLE`); without one it is `SKIPPED`.

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

`ListAppTestRecordings(project)` returns only `id`, `name` and `description` for recordings in that app. `GetAppTestRecording(project, recordingId)` reads one full recording, including steps, when needed. `SaveAppTestRecording(project, recording)` requires `name`, a nonblank `description` explaining the workflow and expected outcome (at most 2000 characters), and `steps`. Optional fields are UUID `id` and app-relative `startPath` (empty for portal root). Give every test a meaningful description; for example, "Create an order on the checkout page, navigate to Orders, and verify the new order appears." Steps use CSS `selector`, optional nested iframe CSS selectors in `framePath`, and one of:

- `CLICK`, `ASSERT_VISIBLE`
- `FILL` / `SELECT` with string `value`
- `CHECK` with boolean `checked`
- `KEY` with string `key`
- `SCROLL` with numeric `x`, `y`
- `ASSERT_TEXT` with expected `text`; it is a substring match unless `exact` is `true`, which requires the element's whole text to equal `text`. Prefer `exact: true` for numbers and short values (a substring `9` also matches `19`).
- `NAVIGATE` with app-relative `path`
- Optional `pagePath` on any step: the app-relative top-level URL expected before that step, including query/hash. The runner waits for that route rather than navigating again. Manual recording adds it automatically; include it in agent-authored multi-page workflows to prevent actions on the wrong page.

Record through the ordinary app iframe using the Testing workbench tab. Password fields are excluded; browser session credentials are never written into recordings. Fresh browser contexts use the initiating signed-in SEMOSS session. Authentication failures require signing in again. Cross-origin or inaccessible iframe workflows cannot be recorded by this same-origin approach. Arbitrary JavaScript test execution is not supported. Deployments must provision the Chromium revision matching the application's Playwright dependency; user jobs never download browser binaries.


## Multiple pages

Links, forms and SPA navigation within the same app portal and iframe are supported. The recorder reattaches after full document loads and tags actions with the current top-level route; replay waits for that route before finding the target. Use app-relative `NAVIGATE` when explicitly changing pages. Deep pages must be served by the app/server itself; the runner does not add missing SPA fallback routes. Other origins, other apps and popup/tab flows are outside this recorder's scope.

This version uses the app's existing backend connections. It does not provision, mock or substitute databases. Recording and replay perform real app actions; use an appropriately prepared app/environment for workflows that change data. Recordings requiring the earlier experimental database-routing profile are rejected rather than run with normal connections.

## Persisted execution history

Every accepted real run writes compact history under `assets/app-tests/test-runs/<run-id>.json`, independently of chat or UI polling. Use `ListAppTestRuns(project, recordingId?)` for bounded summaries and `GetAppTestRunHistory(project, runId, recordingId?)` for one relevant result. History keeps 100 finished runs per app plus active runs; expired unfinished runs are reported INTERRUPTED. Do not bulk-read history or treat an old pass as verification of the current app/test definition. Definition hashes identify the test version that ran, not the app source revision. Detailed screenshots and captured page text remain in caller-private reports.
