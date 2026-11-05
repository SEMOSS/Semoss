# Engine usage framework: author-side verification

Verified on 2026-11-04 with Java 21.0.10 and Maven 3.9.15. This is author-side
verification, not an independent review or production approval.

**Gate: CHANGES REQUIRED.** Grant-bound backend integration, the SDK contract,
and shared editor/member add/edit integration are implemented, but the agreed UI
parity is unfinished. Required release verification also remains **BLOCKED** by
baseline failures and pending independent/live-system checks. Do not treat this
as a completed model-parity implementation.

Use the [prioritized continuation backlog](TODO.md) to select the next task and
record its completion criteria. Results below are historical evidence, not a
substitute for rerunning checks after later changes.

## Grant-bound parity follow-up (2026-11-04)

- Database grant policies now use existing permission-batch authorization and
  transactions. Owners/admin mode can change them; editors retain them and
  cannot remove or shorten limited access. Explicit null clears a policy;
  omission preserves it. Standalone setters preserve existing grant details
  and reject orphan direct/team policies.
- Direct policies require active grants. User/team expiration cleanup deletes
  the corresponding policy with its grant. Permission responses expose catalog
  capabilities without changing model fields.
- The SDK adds `engineUsageLimit` and optional engine capability metadata.
  Model and database profiles reuse one editor controller, rule controls,
  drafts, validation and formatting. Database member add/edit dialogs use the
  existing Monolith permission endpoints. Failed adds retain their draft and
  selected people.
- Remaining implementation: database member-list quota columns and limited-access
  delete/selection affordances, normal team dialogs, Bulk Access Management,
  public/engine-total cards, admin defaults, personal usage, cross-catalog copy
  handling, and their UI/integration coverage. No new Monolith routes were
  introduced in this follow-up; its unrelated documentation edits are intact.

| Follow-up check | Result |
| --- | --- |
| Focused Maven compile/tests | **PASS:** 311 tests, zero failures/errors/skips; includes eight new grant-bound permission tests. |
| Group-permission regression suite | **PASS:** 37 tests, zero failures/errors/skips. |
| Combined broader selection | **FAIL (verified baseline):** 392 tests, 391 pass, the same model-price assertion below fails. |
| SDK access/engine wire tests | **PASS:** 31 tests in two files. |
| Full SDK package test gate | **PASS:** 219 tests in 20 files; existing jsdom navigation warnings remain. |
| Shared editor/member/hook tests | **PASS:** 62 tests in eight files, including database metric/unit/mode checks, a member-add payload, stale-response rejection, retry, admin-mode isolation and Strict Mode/unmount cleanup. Existing async `act` warnings remain. |
| SDK build and SDK/shared TypeScript | **PASS:** declaration build and both owning-library type checks. |
| Scoped frontend Biome | **PASS:** repository-configured check of 33 files covering the SDK additions, shared editor/member files, exports and hook/tests. |
| Repository `pnpm check` | **FAIL:** 1,025 errors and 72 warnings across the broad repository check. The scoped feature check passes; broad baseline attribution was not completed. |
| Full client TypeScript | **FAIL:** diagnostics outside the changed files remain; no changed-file diagnostics. Broad baseline attribution was not completed, so this is not a passing application gate. |
| Java import policy | **PASS:** ad hoc AST inspection of 70 changed/untracked Java files, with added-code/untracked scope filtering; no introduced wildcard, unused-import or unjustified package-qualified reference candidates. Unrelated legacy qualifications are retained. |

Reproduce the expanded backend selection with the `FOCUSED` list below plus
`PermissionBatchEngineUsageLimitUnitTests` (311 tests), then add
`PermissionBatchUtilsUsageLimitUnitTests,SecurityGroupEngineUtilsUnitTests`
(392 tests). Frontend commands, run in their owning packages:

```bash
# libs/sdk
pnpm run build
pnpm exec tsc --pretty false --noEmit
pnpm exec vitest run --configLoader runner src/api/access.spec.ts src/api/engine-usage.spec.ts
pnpm test
# libs/shared
pnpm exec tsc --pretty false --noEmit
pnpm exec vitest run --configLoader runner src/components/model-usage-limit src/components/members src/hooks/use-engine-usage-access.test.tsx
# packages/client (fails; not an approved baseline waiver)
pnpm exec tsc --pretty false --noEmit
```

The following section records the earlier framework/import-cleanup baseline,
not the latest test counts.

## Original framework commands and outcomes

The direct Maven goals below avoid this repository's lifecycle-bound license
formatter, source-tree plugin-copy task, and Git-hook configuration. They compile
into `target/` and run local/mock tests; no deployment or shared engine is used.

```bash
FOCUSED='UsageTrackingLegacyWrapperUnitTests,PixelStreamUsageLimitsUnitTests,RedisUsageConsumptionUnitTests,QueryExecutionUtilityUnitTests,UsageLimiterUnitTests,JdbcReadUsageUnitTests,RdbmsReadUsageCleanupUnitTests,DatabaseReadWorkerIdentityUnitTests,UsageTrackingSelectWrapperUnitTests,DatabaseUsageLimitReactorUnitTests,SecurityEngineUsageLimitUtilsUnitTests,ModelUsage*UnitTests,LocalModelUsageCounterStoreUnitTests,SecurityModelUsageLimitUtilsUnitTests,ModelBatchUsageUnitTests,AbstractModelEngineUsageLimitsUnitTests,AbstractPythonModelEngineUsageCeilingUnitTests,RawPreparedRDBMSSelectWrapperUnitTests,RawRDBMSSelectWrapperUnitTests,WrapperManagerUnitTests,SecurityEngineUtilsUnitTests#testDeleteEngine*,SecurityAdminUtilsUnitTests#testDeleteUser*,AdminSecurityGroupUtilsUnitTests#testDeleteGroup*+testEditGroupDetailsAndPropagate*'

mvn -o compiler:compile compiler:testCompile surefire:test \
  -DargLine= -Dtest="$FOCUSED"

mvn -o surefire:test -DargLine= \
  -Dtest="$FOCUSED,PermissionBatchUtilsUsageLimitUnitTests"

mvn -o license:check

git -c core.whitespace=cr-at-eol diff --check -- src test docs
git diff --cached --check
```

| Check | Result |
| --- | --- |
| Compile and focused tests | **PASS:** 303 tests, zero failures/errors/skips. Includes model guard/window/counter/batch/Python-ceiling tests, database policy and lifecycle tests, a persisted-policy-to-native-JDBC read, concurrent last-row consumption, borrowed-connection regressions, and worker identity restoration. |
| Broader selection including permission batches | **FAIL (verified baseline):** 347 tests, one failure, zero errors/skips. The other 346 tests pass. |
| Repository-wide license check | **FAIL (verified baseline):** six unchanged existing test files lack the expected header. No changed/new file was flagged. |
| Scoped whitespace | **PASS** with existing CRLF line endings recognized; no staged changes. |
| Documentation | **PASS:** relative file links in the changed guides resolve. |

### Permission-view baseline failure

`PermissionBatchUtilsUsageLimitUnitTests.getPermissions_readsTheLimitsOnAModel`
at line 742 expects a null price and receives an empty map. It failed before
implementation with the same checkout/toolchain and fails identically after it.
The pre-change command was:

```bash
mvn -o compiler:compile compiler:testCompile surefire:test \
  -Dtest='ModelUsage*UnitTests,LocalModelUsageCounterStoreUnitTests,SecurityModelUsageLimitUtilsUnitTests,PermissionBatchUtilsUsageLimitUnitTests' \
  -DargLine=
```

That baseline discovered 181 tests and had this one failure. No unrelated price
behavior or test assertion was changed to manufacture a green build.

### License baseline failures

All six flagged files were verified byte-for-byte identical to `HEAD`; the POM
and license configuration were also unchanged:

- `test/prerna/collaboration/WorkRoutineDocumentsUnitTests.java`
- `test/prerna/ds/py/PandasCompatibilityTests.java`
- `test/prerna/auth/utils/ModelUsageLimitColumnMigrationUnitTests.java`
- `test/prerna/engine/impl/model/ModelRouterEngineTypeSafeUnitTests.java`
- `test/prerna/engine/impl/model/usage/ModelUsageBilledToUnitTests.java`
- `test/prerna/engine/impl/model/ModelRouterEngineTurnRoutingUnitTests.java`

The global header formatter was not run and these unrelated files were not edited.

## Import-only cleanup

The follow-up cleanup replaced 103 package-qualified references across 36 Java
files with normal imports and simple names, expanded wildcard imports in those
files, and removed unused imports from new feature files. Necessary nested-type
qualification is retained; unrelated legacy references and generated sources
were not swept into the cleanup. No global agent instructions or CI
configuration were changed in this pass.

The compile/focused, broader, license, and whitespace commands above were rerun:
303 focused tests pass, while the same one broader test and six legacy header
failures remain. A scoped scan found no remaining package-qualified references
in the feature's added code or new Java files.

Before/after cleanup snapshots were additionally compiled with the same Java
compiler and dependency classpath using `-g:none -proc:none`. All 67 emitted
class files were byte-for-byte identical, confirming no generated-bytecode
changes from import organization. The overall feature release gate remains
blocked by the existing prerequisites listed below.

## Risk checks

- **Authorization:** owner/admin writes, actor derivation from Insight, rejection
  of client admin claims, identity-scoped policies, and privacy of cross-database
  totals are covered. Engine-scoped policy saves now use the same permission
  transaction as the grant; cross-catalog defaults remain admin-only.
- **Persistence:** additive schema idempotence, model-table preservation, policy
  replacement/removal, account/team/engine cleanup, team rekeying, and completed
  counter recovery are covered with temporary H2 databases.
- **Concurrency/resources:** atomic multi-counter row consumption without an
  expiring intermediate hold, last-row contention, once-only settlement, SQL
  failure/refusal cleanup, deferred reads, reset refusal, bulk-flush protection,
  and caller-owned transactions are covered. Legacy SELECT/CONSTRUCT iterators
  include row and bulk-iteration coverage; deferred query changes cannot
  accidentally execute an old query. Non-pooled transaction monitors
  remain tied to the pre-metering handle rather than distinct proxy objects.
  New ticket/session locks use `ReentrantLock` around blocking store work.
- **Compatibility:** model namespaces and null-window behavior are explicitly
  tested. All five existing model Redis Lua scripts were compared byte-for-byte
  against `HEAD` and are unchanged. Existing model guards, batches, prices,
  Python ceilings, and wrapper tests are included; no model tables or
  inference-log formats were migrated.
- **Cross-layer:** the additive Pixel configuration accepts maps or JSON text,
  rejects fractional quantities instead of truncating them, and uses the same
  `{key, usageLimit}` DTO for reads and writes. Virtual-thread streaming tests
  verify original-job actor propagation, valid incomplete `ERROR` results after
  partial rows, unchanged early error strings, and normal data/primitive output.
  Model permission-batch fields, existing Monolith routes, Python payload
  fields and model-provider error contracts are unchanged. The additive SDK
  engine contract and shared editor/member integration are described above.

## Not run / release prerequisites

- Independent review of the schema, authorization, proxy lifecycle, and cluster
  behavior: **NOT RUN**, still required for this high-risk change.
- Live Redis/Redis Cluster and driver matrix: **NOT RUN**. Redis namespace/error
  tests and the new atomic consumption script's invocation contract are mocked.
  Legacy model scripts are unchanged; the new consumption script still needs
  live Redis validation.
- Large-result/cluster load tests: **NOT RUN**. Strict per-row accounting makes
  counter-store calls for every row and needs throughput validation.
- End-to-end Monolith/browser workflows and production upgrade: **NOT RUN**.
- Full `mvn verify`: **NOT RUN**; broad unrelated checks and lifecycle-bound
  source/Git mutations were intentionally avoided. The baseline failures above
  remain blockers rather than being silently waived.

Review the [operational limitations](README.md#recovery-and-operational-considerations)
before rollout, especially in-flight history loss and cumulative time overruns.
