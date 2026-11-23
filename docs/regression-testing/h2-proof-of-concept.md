# Isolated H2 proof of concept

Run this on a single development worker with the matching Semoss, semoss-ui, and Monolith branches. The worker needs Playwright Chromium with its sandbox enabled. Use a disposable SEMOSS deployment and a synthetic source H2 engine.

## Prepare an approved run

1. Create a private file-backed H2 engine in the development deployment. Run the SQL in [the baseline](demo/baseline.sql) and insert a source-only sentinel. Give the app editor view access to the engine. As an administrator on this source fixture, use the result of **SCRIPT NODATA NOSETTINGS NOPASSWORDS** to review the app schema. Save the approved, normalized table and index statements as **baseline.sql**. Put synthetic test rows in **seed.sql** using [the example](demo/seed.sql).
2. Replace the development app's React root with [the self-contained demo](demo/App.tsx). Substitute its source engine ID. Build it with the existing app SDK bundled and relative asset paths, then commit the build output under **assets/portals/**. The proof of concept reads that Git tree directly. The same commit must contain the [approved recording](demo/recording.json) at **assets/app-tests/tests/94e82b66-61ce-449c-b6c2-b7e1dcbcfb71.json**. Use this commit as the approved baseline. Use a descendant commit for the candidate.
3. Create an operator-controlled directory and an empty **runs/** child. Create a child directory with the app's project ID and put **baseline.sql**, **seed.sql**, and **approval.json** in it. The app editor and candidate build must have no write access to this directory. Calculate SHA-256 hashes of the UTF-8 SQL files, for example with **Get-FileHash -Algorithm SHA256**.

~~~json
{
  "publishedSha": "<full lowercase Git SHA containing the approved recording>",
  "recordingId": "94e82b66-61ce-449c-b6c2-b7e1dcbcfb71",
  "originalEngineId": "<synthetic source H2 engine ID>",
  "schemaSha256": "<lowercase hash of baseline.sql>",
  "seedSha256": "<lowercase hash of seed.sql>",
  "migrations": {}
}
~~~

For a candidate with schema changes, add its full Git SHA and the hash of its approved SQL to **migrations**. Put those reviewed statements in **<candidate SHA>.sql** beside the baseline. Statements run in the order in the file after the baseline and before the seed.

4. Add these settings to the `RDF_Map.prop` loaded by Monolith and restart Tomcat. Use the absolute fixture directory on that worker and the HTTP port and context path of the same Tomcat instance. Keep the servlet path private to the worker at the reverse proxy.

~~~text
semoss.appRegressionRoot=/opt/semoss/h2-regression
semoss.appRegressionMonolithUrl=http://127.0.0.1:8080/Monolith
~~~

Existing JVM options with the same keys take precedence over the file. Remove the corresponding `-D` options to use the values in `RDF_Map.prop`.

5. In the app editor's Testing tab, paste the candidate SHA into **Candidate Commit SHA** and select **Run H2 Regression**. Alternatively, call the following Pixel as a signed-in app editor.

~~~text
RunAppTests(project=["APP_ID"], recordingIds=["94e82b66-61ce-449c-b6c2-b7e1dcbcfb71"], candidateCommitSha=["CANDIDATE_SHA"]);
GetAppTestRun(runId=["RETURNED_RUN_ID"]);
~~~

The report includes the tested commit and hashes of the approved recording and fixture inputs. Private reports use the current app-test report folder outside the Git worktree. A missing fixture, revoked session, unavailable servlet, or unsupported operation fails the isolated run.

## Verify the result

The demo reads the synthetic title, updates it, then reads the updated value. Confirm the recording passes and the source sentinel is unchanged. To exercise a regression, change the demo's update value from **Done** to **Pending**, rebuild the portal, and commit it. Replay the original recording against the new SHA and confirm the final assertion fails.

Run two candidate replays against the same source fixture and confirm each starts with the original seed. Check that both H2 directories disappear when the runs finish and that opening the published portal still shows its prior content. The focused Java tests also cover cross-run access, rejected operations, stale capabilities, and fixture cleanup.

## Supported surface

The isolated preview serves a small, committed static React build. Its SDK requests go through the scoped Monolith servlet and the real Pixel interpreter. The only candidate Pixel is one literal **SqlQuery** targeting the approved source alias. The worker routes it to a private, file-backed H2 connection with CRUD privileges. The proof of concept supports basic table and index definitions, column additions, literal seed inserts, and simple single-table reads and writes. It uses the approved recording at the configured Git revision and requires the replay to perform both a read and a write.

App builds that need ordinary SEMOSS sessions, other SDK endpoints, raw engine access, or external integrations need additional support. Schema approval, fixture export and normalization, build preparation, and published-SHA selection are operator-managed in this phase. The full production release still needs deployed isolation testing, cancellation and restart recovery, broader SDK coverage, and publication gating.
