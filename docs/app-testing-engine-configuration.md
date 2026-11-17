# App tests: descriptions, multiple pages and test databases

## Deploy together

Pull `feat/app-regression-test-tools` in **Semoss, SemossWeb and Monolith**. Build/deploy
all three through your normal local deployment process and restart the backend. Merely
pulling source does not update the running server. The bundled `platform__app-tests`
toolbox and `platform__app-regression-testing` skill must be included in the deployment;
there is no manual skill upload or CI change.

The server needs its existing Playwright Chromium installation. Manual database-mode
recording additionally requires a same-origin app portal, HTTPS or localhost, service
workers, IndexedDB and browser navigation-client tracking. Another service worker covering
the app is treated as a conflict. Test mode blocks external requests, workers and websocket
connections; apps relying on these need additional support. Contexts are in server memory:
use a single backend or sticky routing, and start a new recording session after a restart.

## Try the feature

1. Open an editable CODE app and its **Tests/Testing** workbench panel.
2. Record a workflow, add an outcome check, stop, and save with a name and **description**.
   Description is required; existing recordings receive a generated description on listing.
   Run the saved test, or ask the app's chat to run it and report the actual assertions.
3. To test database substitution, expand **Test database configuration**. Choose **Create
   empty test database**, or enter a dedicated existing local file-backed H2/SQLite engine
   ID. Creation returns an empty engine; initialize its tables and fixture data using the
   normal database tools before recording. Nothing copies production rows automatically.
4. Enter the app's real database ID and the test database ID, then save the mapping. You
   need app edit access, real database view access and test database edit access. Add a
   mapping for every application database dependency. The SQL dialect must suit the app;
   H2 does not validate PostgreSQL-specific behavior.
5. Press **Record**. The normal iframe reloads into test mode before listeners attach.
   Interact with the database workflow and verify a value unique to the fixture database.
   Save with a description. The recording remembers the configuration UUID.
6. **Run test**. Its report should say it used test database mappings. Verify any expected
   writes in the test database and verify the corresponding production rows stayed intact.
   A separate normal app tab should continue to use its normal database.
7. Stop recording leaves the preview in test mode. **Exit test database mode** explicitly
   revokes that context and reloads the normal preview. Removing the configuration leaves
   its database intact; a recording requiring that profile then fails instead of replaying
   with the normal database.

Fixtures persist across recordings and repeated replays. Reset them explicitly when a test
requires a clean starting state; this change does not provide automatic fixture teardown.
Use unique `data-testid` attributes for recorded controls and outcome checks.

## Agent / Pixel example

```text
CreateAppTestDatabase(project=["APP_ID"], name=["Orders fixtures"], rdbmsType=["H2_DB"]);
GetEngineTestConfiguration(project=["APP_ID"]);
SaveEngineTestConfiguration(project=["APP_ID"], configuration=[{
  "name":"Orders fixtures",
  "mappings":[{"realEngineId":"REAL_ID","testEngineId":"TEST_ID"}]
}]);
RunAppTests(project=["APP_ID"], suite=["RECORDING_ID"], repeat=[3]);
GetAppTestRun(runId=["RETURNED_RUN_ID"]);
```

Initialize the new database's schema and fixture rows before saving/recording. Preserve a
profile's returned `id` when updating it. For agent-authored recording JSON, include
`description` and `engineTestConfigurationId` with the profile UUID. Do not include the
short-lived session token. Replay applies an existing profile automatically; required
missing/different profiles and corrupt configuration are errors.

## How routing works

The profile is saved separately from published app code at
`app-tests/engine-test-configuration.json`. The browser bridge adds a context header only
for the test iframe and its related clients. Monolith checks the signed-in caller, browser
session, app permission and expiry, then enters a request-scoped context. Its portal-serving
filter applies the same checks before returning an app document.

Fresh Insights bind to that context before asynchronous Pixel execution. SEMOSS database
resolution and cached database proxies use the test engine. Python engine/reactor callbacks
carry the executing Insight, rather than borrowing the shared Python workspace's normal
Insight. Bound test Insights remain marked after expiry/revocation, so later calls fail.
Replay uses a fresh headless browser context with the caller's authenticated session and
adds the same routing header; it checks backend support before opening the app. Reports
include `databaseMode` and the configuration summary, without session tokens.

This routes **SEMOSS-managed database access**, including its Python callbacks. It is not
a sandbox for arbitrary app/server code. Raw JDBC/DB-API connections, cached raw connections,
custom background executors, external APIs, non-database engines and browser storage are
outside its isolation. Audit/configure those dependencies separately before testing.
Dedicated local test storage is required because a different engine ID or cloned external
engine can still point at the production database.

## Multiple pages

Links, SPA route changes and full-document navigation **within the same app portal and
iframe** can be recorded/replayed. The recorder reattaches after document loads; an agent
can also write explicit app-relative `NAVIGATE` steps. Cross-origin pages, navigation to
another app and independent popup/tab workflows are outside the supported recording flow.
