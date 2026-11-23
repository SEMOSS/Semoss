# AI Core regression testing with isolated H2

The proposed H2 workflow fits the existing app Git, database, and Playwright code. The source review supports starting a small proof of concept after approval. Release depends on proving that a candidate can reach only its disposable database through the real SDK and Pixel stack.

## Branch comparison

Both repos started on clean copies of `dev`. The local branch `feat/app-regression-h2-isolation` in each repo starts at its locally available `origin/feat/app-regression-test-v2` tip. Counts show commits unique to each side of the comparison.

| Repository | `dev` | `v2` start point | `dev` only / `v2` only | `dev` only / `tools` only |
| --- | --- | --- | --- | --- |
| Semoss | `62cb68e2e7` | `7382dc956c` | 24 / 10 | 56 / 9 |
| semoss-ui | `34b30b8a1` | `19f3fdb01` | 14 / 7 | 62 / 13 |

- Compared with `dev`, Semoss `v2` adds the app-test Pixel reactors, editable JSON recordings, multipage Playwright replay, assertions, and persisted per-test history. semoss-ui `v2` adds the Testing workbench tab, iframe recorder, file and assertion editors, selected or all-test execution, diagnostics, and history.
- The branches diverged. Semoss `tools` at `3abe3491b2` adds a saved database mapping, expiring test contexts, and request and engine routing. semoss-ui `tools` at `e5843c6a9` adds the mapping form and a service-worker recording bridge. Its comparison with `dev` also includes unrelated collaboration changes. Use the matching `v2` pair for the regression feature and adapt the relevant routing code from `tools`.
- Monolith `dev` at `22d91453` has the standard SDK endpoint and published portal route. Monolith `origin/feat/app-regression-test-tools` at `ce5ed2d7` has the prototype request filter, portal checks, and context and worker endpoints. A real SDK proof of concept requires an HTTP boundary in Monolith or in a dedicated test deployment. The two feature branches alone cannot install the prototype filter.

## Current execution path

```mermaid
flowchart LR
    App["React app with InsightProvider"] --> SDK["SDK actions.run and runPixel"]
    SDK --> Web["Monolith NameServer /api/engine/runPixel"]
    Web --> Pixel["Insight and PixelJobRunner"]
    Pixel --> Reactor["Database, Query, Collect, and write reactors"]
    Reactor --> Engine["Query structs and Utility.getDatabase"]
    Engine --> JDBC["RDBMSNativeEngine and JDBC"]
    Panel["Testing workbench"] --> Tests["RunAppTests and AppTestRuns"]
    Tests --> Replay["AppTestRunner and Playwright Chromium"]
    Replay --> Portal["Current public_home project portal"]
    Portal --> App
```

- The Semoss React template mounts `InsightProvider`. In semoss-ui, `insight.store.ts` calls the SDK's `libs/sdk/src/api/base.ts`, which posts the expression and optional Insight ID to `/api/engine/runPixel` or `runPixelAsync`. Monolith's `NameServer` resolves the HTTP session and Insight, then uses `PixelJobRunner`. The database REST resource also executes Pixel through `ResourceUtility`. Separate entry points exist for reactor MCP, Python, WebSocket, and jobs.
- In Semoss, `DatabaseReactor` checks engine access and starts a query struct. `BasicIteratorTask` obtains a wrapper from `WrapperManager` using the resolved engine. `InsertReactor` can write through an engine or an RDBMS frame. `Utility.getEngine` delegates database lookup to `Utility.getDatabase`. `RDBMSNativeEngine` uses JDBC connections for reads and writes. The lookup is a useful routing point, with engine references and connections also retained elsewhere.
- `Project` initializes the repository under `app_root/version`. `ProjectGitTarget` resolves it, `GitBranchUtils` switches the project's working tree, and `GitCommitAgentHook` commits changes in the active folder. Reuse this repo and its permission model. The inspected publish flow records a portal timestamp. An authoritative published SHA, separate AI checkout, and detached candidate preview still need to be wired into this flow.
- The current `CodeRenderer` iframe and `AppTestRunner.appRoot` both point at `/public_home/<projectId>/portals/`. Monolith's `PublicHomeCheckFilter` can publish the portal on access. `BuildAndPublishAppReactor` builds from `assets/client` using the configured builder, then replaces `assets/portals` and publishes. Reuse the build service only with isolated inputs and outputs, a run-specific preview URL, and restricted credentials and network access.

## Integration findings

| Area | Existing code to reuse | Work required by the plan |
| --- | --- | --- |
| Scenarios and reporting | `AppTestRecordings`, `AppTestPlayback`, `AppTestRunner`, `AppTestHistory`, and the UI Testing panel. Playwright supports saved app-relative navigation, stable test IDs, explicit assertions, fresh browser contexts, and failure screenshots. Action-only recordings are skipped. | Load the approved suite independently of candidate edits. Adapt the runner to accept the isolated preview and scoped session. Save candidate SHA and suite, schema, migration, and fixture hashes. Extend diagnostics and cancellation. Keep run evidence behind project authorization. |
| Authorization and dependencies | `AbstractAppTestReactor` checks project edit access. `SecurityProjectUtils.getProjectDependencies` supplies declared dependencies. The prototype checks source-engine view and test-engine edit access. | Reuse project and engine permission checks. Authorize schema export, fixture approval, run creation, artifact access, and publication at their own boundaries. Bind the initiating user by ID and login type. Treat declared dependencies as input to validation and enforce the approved engine set at runtime. |
| H2 provisioning | `CreateEmptyRdbmsDatabaseReactor` creates a file-backed H2 engine and defaults to H2. Its normal path grants catalog ownership and pushes the engine to the cluster. `RDBMSNativeEngine` supplies JDBC, pool close, and H2 lifecycle primitives. | Give each run its own file, engine, and restricted runtime credentials. Use approved schema, ordered candidate migrations, and synthetic seed data. Supply fresh fixtures per run and explicit reset behavior for repeats or scenarios that mutate shared state. Keep disposable engines out of normal project and cluster lifecycle. Revoke access and drain work before closing connections, unregistering engines, and deleting files. Track cleanup for restart recovery. |
| H2 schema extraction | Semoss pins H2 `2.4.240` in `pom.xml`. H2 documents `SCRIPT NODATA NOSETTINGS NOPASSWORDS`, which requires H2 admin rights. The pinned source still emits users, roles, and supported stored function definitions. | Separate the privileged schema extraction operation from candidate execution. Return only approved app schema objects, validate the normalized baseline on a clean H2 file, and reject unsupported or side-effecting SQL in schema, migrations, and seed inputs. Verify the deployed H2 version before enabling extraction. [H2 SQL command reference](https://h2database.com/html/commands.html#script) · [H2 2.4.240 source](https://github.com/h2database/h2database/blob/version-2.4.240/h2/src/main/org/h2/command/dml/ScriptCommand.java) |

## Prototype coverage and gaps

The `tools` implementation covers useful pieces of the request path: an opaque token with expiry and revocation, request validation in Monolith, a fresh Insight binding, explicit Pixel job and Python callback propagation, and routing at `Utility.getDatabase` with a database argument check before selected reactors. The Playwright runner verifies the request filter before it opens the portal. The older recording UI needs a same-origin service worker. Reuse the server mechanisms where they fit the scoped preview.

The prototype uses manually initialized, persistent H2 or SQLite engines. It can start a run without a configured mapping, labels that run `APP_DEFAULT`, and uses the caller's browser session and current portal. The proposed regression flow must require a fresh H2 database for each run, tie its engine to that run server-side, use separate preview credentials, and report `ISOLATION_UNAVAILABLE` or `UNSUPPORTED_DEPENDENCY` when a prerequisite fails.

The prototype remaps managed database lookups. It also passes through system database IDs for platform metadata and leaves non-database engine arguments outside database routing. Candidate-facing policy must deny direct system database access while permitting the server's own metadata operations. Raw JDBC or Python DB-API connections, cached engines or connections, existing frames and Insights, pipelines, custom reactors and executors, scheduler jobs, WebSocket/MCP/Python endpoints, and external side effects require an explicit decision. Allow a path only after proving it inherits the run's restrictions. Reject unsupported paths at the server entry point or execution boundary.

## Go/no-go and first implementation scope

**Go for the Phase 0 proof of concept.** Use a single pinned worker, a small React fixture app, one supported file-backed H2 dependency, and an existing approved recorded write assertion. Use the real SDK route, a dedicated preview identity with no live-engine privileges, an exact candidate SHA, and a server-generated mapping to a new disposable H2 engine. Use a sacrificial source fixture to exercise normal/live behavior and compare its rows before and after.

**No-go for release until isolation is demonstrated.** Verify scoped reads and writes, denial of unmapped or system engines, rejected missing or invalid preview credentials, nested and asynchronous operations inheriting policy or being denied, non-database side-effect denial, concurrent run independence, stale session rejection, and cleanup on failure and cancellation. Verify that publishing and the source fixture remain untouched. Use an isolated worker without access to live engines if the shared runtime cannot enforce this boundary. Keep multi-node deployment gated on worker affinity or dedicated routing.

This report captures the source review before implementation. The proof of concept uses a dedicated loopback servlet, a run-scoped H2 connection, and an immutable snapshot of committed portal assets. Its deployment steps, demo, and remaining verification are in the [H2 proof of concept](h2-proof-of-concept.md).
