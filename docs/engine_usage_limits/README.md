# Engine usage limits

For remaining work, priorities, acceptance criteria and reusable session handoffs,
see the [continuation backlog](TODO.md). Full model-style UI parity and release
verification are not complete.

The shared engine-usage core lives in `src/prerna/engine/impl/usage`.
`EngineUsageGuard` provides catalog-qualified admission, caching, and store
selection; `EngineUsagePolicyResolver` supplies the common source hierarchy.
Database reads are the first new adapter. Existing model entry points delegate
counter storage, reservations, window math, and login/team identity to shared
code; model policy resolution, pricing, Python ceilings, inference logging,
provider errors, and batch behavior remain on the existing model path.

## Compatibility boundaries

- Model tables, Java APIs, permission-batch payloads, and response fields are
  unchanged. The model Redis prefix remains `{semoss-model-usage}|`, including
  the existing `mul|` counter keys.
- New catalog policies use `ENGINEUSAGELIMIT` in the security database. Keys
  always include a catalog, even for cross-engine totals. Database defaults
  never apply to models, vectors, or internal system databases.
- New Redis counters use `{semoss-engine-usage}|`; each multi-counter operation
  stays in one Redis Cluster hash slot. Reads requiring counters fail closed
  on a cluster without Redis rather than granting each node a separate budget.
  A genuinely uncapped call with no counters does not require Redis.
- No database policy is defined by default. New catalogs use the additive
  `engineUsageLimit` grant field; the model `usageLimit` contract is unchanged.
  Database policies now participate in existing permission-batch transactions.
  The Pixel setter below is a compatibility entry point into that same path.

## Database read metrics

| Metric | Unit and behavior |
| --- | --- |
| `QUERIES` | One admitted read execution, including a dispatched read that fails. Preparation alone is not an execution. JDBC resets and independent count queries execute again and count again. |
| `ROWS` | Rows read into SEMOSS. JDBC counts successful cursor advances (including read-ahead); non-JDBC wrappers count iterator rows. Counts are cumulative over the rule's window, not merely a per-result maximum. |
| `COMPUTE_TIME` | Elapsed milliseconds spent executing and advancing a read. Caller idle time between cursor operations is excluded. It is not database CPU time. |

A row is charged atomically to every applicable row counter before being exposed
by the metered boundary. Two concurrent readers cannot both spend the same last
row allowance. Exhaustion raises an error and closes the result; it does **not**
silently present a truncated result as complete. A driver may already have fetched
more data internally: this is not a limit on rows scanned by the database.

Elapsed time is accounted for when the read closes. It is checked before the
next read, not reserved or used to cancel an already-running query. Concurrent
reads can therefore exceed a cumulative time budget. A hard statement timeout is
a separate JDBC/engine capability.

Writes (`executeUpdate`, write batches, DDL, DML, and the write reactor paths)
are not metered. A row-producing call through the explicit JDBC `executeQuery`
read API is treated as a read; this accounting is not SQL authorization or a
sandbox against side-effecting functions.

## Sources and authorization

The source order mirrors model limits:

1. Direct identity policy on this database.
2. Team policies, Unlimited first, then access-since order, then team identity.
   An exhausted team falls back to the next team, not to public/default policy.
3. Public use, only while the database is public.
4. User total across databases.
5. Platform default for databases.

The database's `ENGINE_TOTAL` ceiling always applies. A billed team's
`TEAM_TOTAL` also applies across databases. Undefined (`null`) and explicit
Unlimited differ: Undefined allows fallback; Unlimited stops fallback without
lifting ceilings. Rules in one limit must all pass.

Owners set database-specific policies. Cross-database user/team totals and the
platform default require `SecurityAdminUtils` proof of admin authorization.
The caller/actor always comes from the authenticated Insight, never JSON.
Owner reads omit cross-database defaults belonging to other people or teams.
Anonymous access retains its existing behavior only when no policies exist on
that engine or in catalog defaults. With policies present, a signed-in identity
is required rather than creating a synthetic guest quota identity. Calls with
no user are trusted system calls and bypass usage accounting, as on models.

**Database policies are grant-bound, matching models.** User and team policies
are saved in the same transaction as their grants, and removed with them.
Expired-grant cleanup also removes its policy. Stored direct policies without
an active grant never apply. Team policies require current membership and an
active team grant. Policies never grant access by themselves.

Owners (or admins using admin mode) change limits. Editors preserve limits when
changing levels and cannot remove or shorten limited access to lift a cap.
Omitting `engineUsageLimit` preserves the current policy; explicitly supplying
`null` clears it. `PUBLIC` is the public-use subject and the additive `ENGINE`
subject is the engine-wide total. The legacy `MODEL` subject remains model-only.
The standalone database setter requires an existing direct/team grant and
preserves its access level and end date; add access and a policy together using
a permission batch instead.

The permission response advertises `engineUsage.catalogType`, supported
`metrics`, public use, the engine total, and `canChangeEngineUsageLimits`.
The SDK reads this additive contract without changing model payloads. The
shared UI editor, rule controls, drafts, validation and window formatting are
reused by the model compatibility adapter and database profile. Database
member add/edit dialogs use these capabilities and existing permission APIs.

**UI parity is not complete:** database member-list quota columns and limited-access
delete/selection affordances, normal team dialogs, Bulk Access Management,
public/engine-total cards, admin defaults and personal database usage views
still need wiring and end-to-end verification. No new Monolith routes were
added in this pass; member dialogs use its existing permission endpoints.

The primary login owns personal counters; limits and teams on all linked logins
are considered. Login/team identity and an invalidation signal are shared with
the model path. Resolved plans expire after 30 seconds. Policy writes and the
existing grant/membership/public-change hooks invalidate new catalog plans on
the writing node immediately; other cluster nodes see changes within the cache
period. Model cache versions retain their existing behavior. Policy changes
apply to new executions; active reads retain their admitted plan.

Account, team, and engine deletion remove the corresponding policies. Team
renames move stored policies to their new keys. A changed team identity uses
new counter scopes; this does not rewrite the prior identity's history.

## Setting a policy through Pixel

For a database-total row budget of 100,000 rows per day:

```text
SetDatabaseUsageLimit(config=[{
  "key": {
    "catalog": "DATABASE",
    "source": "ENGINE_TOTAL",
    "engineId": "<database-id>"
  },
  "usageLimit": {
    "unlimited": false,
    "rules": [
      {"metric":"ROWS", "amount":100000, "window":"DAY", "counts":"SHARED"}
    ]
  }
}]);
```

A direct policy can combine all three metrics:

```text
SetDatabaseUsageLimit(config=[{
  "key": {
    "catalog":"DATABASE", "source":"DIRECT", "engineId":"<database-id>",
    "subjectId":"<user-id>", "subjectType":"NATIVE"
  },
  "usageLimit": {
    "unlimited":false,
    "rules":[
      {"metric":"QUERIES", "amount":60, "window":"MINUTE", "counts":"EACH_PERSON"},
      {"metric":"ROWS", "amount":10000, "window":"DAY", "counts":"EACH_PERSON"},
      {"metric":"COMPUTE_TIME", "amount":60000, "window":"DAY", "counts":"EACH_PERSON"}
    ]
  }
}]);
```

`GetDatabaseUsageLimits(database="<database-id>");` returns the owner's
engine-specific `{key, usageLimit}` entries, using the same DTO that Set accepts.
Set returns `{key, usageLimit}`. It is a full replacement: an omitted or null
`usageLimit` removes the policy. Use `{"unlimited":true,"rules":[]}` for explicit
Unlimited where permitted.

| Source | `engineId` | Subject | Permitted counts |
| --- | --- | --- | --- |
| `DIRECT` | required | user id and login type | `EACH_PERSON` |
| `TEAM` | required | team id and team type | either |
| `PUBLIC` | required | none | either |
| `ENGINE_TOTAL` | required | none | `SHARED` |
| `USER_TOTAL` | omitted | user id and login type | `EACH_PERSON` |
| `TEAM_TOTAL` | omitted | team id and team type | `SHARED` |
| `PLATFORM` | omitted | none | `EACH_PERSON` |

Windows use the existing UTC model math: minute/hour sliding approximation and
day/week/month/year calendar windows (Sunday-start weeks). `windowLength` is
optional, 1–59 for minutes or 1–24 for hours. Other windows have length 1.
Amounts are positive longs; duplicate metric/window/length/counts rules are
rejected. Totals and platform defaults cannot be Unlimited.

## Execution coverage and resource ownership

- Native and contextual relational engines: JDBC connections instrument read
  execution and result cursors, covering raw queries, prepared/deferred reads,
  reset, count queries, and normal engine-acquired JDBC reads. The prepared
  wrapper also instruments borrowed connections without taking ownership of
  their transactions or closing them.
- Non-JDBC engines: `WrapperManager.getRawWrapper` returns a metered iterator.
  Deferred reads obtain a ticket only when actually executed. These raw
  wrappers retain the original query structure; create a fresh wrapper to
  change it rather than mutating a deferred query. Bulk flush cannot bypass
  row accounting on a limited iterator.
- Legacy SELECT/CONSTRUCT factories (`getSWrapper`, `getCWrapper`, and
  `getChWrapper`) meter non-JDBC iterator records too, including
  `forEachRemaining`. JDBC legacy SELECTs use the underlying JDBC meter and
  propagate quota refusals rather than returning a seemingly empty result.
- Python database execution: `NativePyEngineWorker` installs the authenticated
  user for execution and cleanup, then restores the previous thread identity.
- Deferred Pixel responses: `PixelStreamUtility` carries the original job's
  authenticated actor onto the response thread. If a failure occurs after rows
  have begun streaming, the result stays valid JSON and carries `ERROR`,
  `output.incomplete: true`, and `output.errorMessage`. Early errors keep their
  existing string output. Clients must check Pixel error operation types.
- Only registered engines of the matching catalog are metered. Scratch frames,
  unregistered backing resources, and internal security, inference, audit,
  tracking, OWL, and insight databases are excluded, protecting internal/model
  persistence from recursive database accounting.

Trusted backend code must use these boundaries. Direct vendor/data-source
handles, unwrapped JDBC objects, non-JDBC execution outside `WrapperManager`,
and arbitrary procedures/mixed scripts through `Statement.execute` are not a
new sandboxed API. `execute` is recognized for parser-supported SELECTs;
`executeQuery` is the explicit metered JDBC read API. Existing access checks
remain mandatory before executing a query.

A ticket belongs to one execution. Result close, statement close/re-execution,
connection close, failure, and explicit wrapper close settle it once. The
framework does not commit, roll back, change auto-commit, or acquire ownership
of caller-owned connections.

## Recovery and operational considerations

Database usage records are counter-level amounts, not SQL or result contents,
in `ENGINEUSAGEHISTORY`. Completed reads persist their charges in a transaction;
cold counters are filled from that history. Changing a rule's amount retains
its counter. A newly introduced scope/metric/window starts with its recorded
history, which is zero if that counter was not previously tracked; arbitrary
past queries are not reconstructed from the older query-tracking logs.

Rows and queries update counters while the read runs; history is persisted on
close. Losing counter state during an active read (for example, process failure
or eviction), or failing to persist completed usage, can leave recovery history
incomplete. Known row/query quantities use a single atomic check-and-charge,
not an expiring reservation followed by settlement. Model reservations retain
their existing behavior. Unavailable counter/history reads fail closed.
Persistence failures surface during cleanup; callers must not suppress them or
treat partial results as successful. Keep cluster clocks synchronized and avoid
evicting active Redis counter keys.

History contains one entry per counter charged by a completed read, not one
entry per row. Operators may delete rows whose `EXPIRESAT` epoch-millisecond
value is in the past; expired windows are never needed for recovery. No
retention job is introduced in this change. Frontend implementation status is
listed above and in the continuation backlog. Strict per-row accounting
currently performs an atomic store operation for each row;
large-result throughput should be measured before production rollout.

## Adding another engine catalog

1. Register the catalog's supported metrics in
   `SecurityEngineUsageLimitUtils.CATALOG_METRICS`. The generic repository has
   catalog-parameterized reads, shared owner/admin validation, deletion hooks,
   and the same schema/history for all new adapters. Model remains deliberately
   unregistered until a separately verified migration is appropriate.
2. Reuse `EngineUsagePolicyResolver` and `EngineUsageGuard`, which already use
   the catalog in policy reads, counter scopes, and cache keys. Keep
   provider-specific prices, estimates, operations, and measurement in the
   execution adapter, not a copied resolver.
3. Admit at every actual execution boundary with the authenticated actor and
   a known admission quantity; consume subsequent known quantities atomically,
   record quantities known only after execution, and close tickets on success,
   failure, cancellation, and resource cleanup. Reuse the local/Redis stores,
   window math, history, and reservation primitives rather than copying them.
4. Add catalog-isolation, source/ceiling, recovery, concurrent admission, and
   lifecycle tests. Verify existing model behavior before enabling the adapter.

## Verification

See the [author-side verification record](verification.md) for commands, results,
known baseline failures, and release checks still pending.

Focused tests cover shared reservations and windows, concurrent last-row
consumption, source fallback and ceilings, policy authorization/schema/recovery,
JDBC lifecycle and transaction ownership, non-JDBC deferred reads and flush
protection, and model compatibility. Mocked Redis contract tests are not a live
Redis Cluster test. Deployment/driver-specific and end-to-end UI verification
remain separate release checks.

Relevant vendor contracts:

- [JDBC ResultSet, Java 21](https://docs.oracle.com/en/java/javase/21/docs/api/java.sql/java/sql/ResultSet.html)
- [JDBC Statement, Java 21](https://docs.oracle.com/en/java/javase/21/docs/api/java.sql/java/sql/Statement.html)
- [Redis scripting](https://redis.io/docs/latest/develop/programmability/eval-intro/)
- [Redis Cluster hash tags](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#hash-tags)
