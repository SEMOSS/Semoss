# Database usage limits: prioritized continuation backlog

Last updated: 2026-11-04. This is the cross-session work queue, not a completion
or production-readiness claim. **Overall gate: CHANGES REQUIRED.**

Start with the [implementation guide](README.md) and
[verification record](verification.md). The latter owns exact commands,
previous results, baseline evidence and release blockers. Recheck the checkout;
do not assume recorded results are still current.

## What is already implemented

Do not rebuild these as parallel database-only implementations:

- Shared admission, windows, counters, policy hierarchy and recovery primitives;
  database read adapters for JDBC, non-JDBC, legacy wrappers, Python and deferred
  Pixel responses. Writes remain unmetered.
- Grant-bound database policies in permission-batch transactions, owner/admin
  authorization, editor preservation, and user/team expiry cleanup.
- Additive SDK grant/capability types and parsing.
- One shared editor controller, rule controls, drafts, validation and formatting,
  with model compatibility and database presentation adapters.
- Database member **add/edit dialogs** through existing permission endpoints.
  The member-list quota column and delete/selection affordances are not done.

These have author-side coverage, not independent review or live-system approval.
The last recorded runs passed 311 focused backend tests, 37 group-permission
regressions, 219 SDK tests and 62 shared UI/hook tests. The broader Java run had
392 tests with the one documented model-price baseline failure.

## Priorities and recommended session order

- **P0:** correctness, merge or release gates. Start review and baseline triage
  now; environment-dependent checks can run alongside UI work. P0 does not mean
  every live-system test must finish before the next UI task starts.
- **P1:** required implementation to deliver the agreed model-style UI parity.
  These are not optional polish.
- Check a task only when its acceptance criteria are met and evidence is recorded.
  A blocked task stays unchecked with its prerequisite documented.

| Done | ID | Priority | Work item | Dependencies / suggested order |
| --- | --- | --- | --- | --- |
| [ ] | DBUL-01 | P0 | Independent backend/contract review and missing regression coverage | First; separate reviewer from the implementation author |
| [ ] | DBUL-02 | P0 | Re-establish baselines and resolve merge-gate disposition | Start early; can run separately from feature development |
| [ ] | DBUL-03 | P1 | Complete member-list quota display and protected actions | First small UI implementation session |
| [ ] | DBUL-04 | P1 | Wire ordinary team-access dialogs in both directions | After contract review; follows member work well |
| [ ] | DBUL-05 | P1 | Define and implement safe copy semantics | Before completing Bulk Access copy/conflict handling |
| [ ] | DBUL-06 | P1 | Complete Bulk Access Management integration | DBUL-05; reuse member/team rules rather than duplicating them |
| [ ] | DBUL-07 | P1 | Public-use and database-total settings cards | Existing grant/settings contract; independent of Bulk Access UI |
| [ ] | DBUL-08 | P1 | Admin defaults and cross-database user/team totals | Contract/API/SDK subtask before admin UI subtask |
| [ ] | DBUL-09 | P1 | Personal usage and limit-resolution views | Establish shared usage-read DTOs, coordinating with DBUL-08 |
| [ ] | DBUL-10 | P0 | Live Redis/Redis Cluster, multi-node and recovery validation | Reviewed core; isolated authorized environment |
| [ ] | DBUL-11 | P0 | Real-driver and execution-lifecycle matrix | Reviewed core; isolated authorized engines |
| [ ] | DBUL-12 | P0 | Per-row throughput and concurrency benchmarks | DBUL-10/11 representative setups |
| [ ] | DBUL-13 | P0 | Browser/HTTP journeys and upgrade validation | Relevant UI tasks complete; live prerequisites available |
| [ ] | DBUL-14 | P0 | Final independent review, operating decisions and rollout gate | All required implementation and verification tasks resolved |

Suggested development sequence: **01 → 03 → 04 → 05 → 06 → 07 → 08 → 09**.
Run **02** and the authorized portions of **10–12** in parallel where practical;
finish with **13–14**. These are work packages, not time estimates: split a large
item into sub-sessions rather than changing several public contracts at once.

## Task acceptance criteria

### DBUL-01 — Review backend correctness and the cross-layer contract

**Owners:** core, plus API/SDK consumers where the boundary is involved.

- Obtain a genuinely independent review of authorization, schema/storage,
  permission transactions, query proxies, actor propagation and cleanup.
- Verify owner, editor, reader and admin-mode behavior; linked login identities;
  direct/team/public/default precedence; team fallback and ceilings; Undefined
  versus Unlimited; removal, expiry, rename and re-add behavior.
- Examine validation-to-write races, concurrent grant/policy changes, omitted
  versus explicit-null fields, rollback and audit behavior. These are review
  targets, **not claims of confirmed defects**. Add focused regressions for gaps
  and fix confirmed issues without silently changing model semantics.
- Trace existing member endpoints, permission batches and the standalone Pixel
  setter to the authorized write path. Verify the setter preserves an existing
  grant's level/end date and cannot create an orphan user/team policy.
- Preserve model metrics, payloads, endpoints, source ordering, batch accounting,
  pricing/inference behavior, counter namespaces and existing Redis scripts.

**Done when:** review findings have evidence and disposition, blocking findings
are fixed, regression tests pass, and remaining environment checks are assigned
below. Author self-review alone does not close the independent-review criterion.

### DBUL-02 — Baselines and merge gates

**Owners:** each owning repository/package; do not mix their build histories.

- Reproduce the documented Java model-price assertion and six license-header
  baseline failures. Do not change unrelated price behavior or headers just to
  make this feature's checks green.
- Establish comparable base/working evidence for the full client TypeScript and
  repository-wide Biome failures. Their broad baseline attribution is currently
  unresolved; passing scoped checks is not a waiver.
- Fix introduced diagnostics. Route unrelated repairs separately and record any
  actual maintainer-approved gate disposition; do not invent an exception.
- Preserve unrelated working-tree changes and record exact toolchain versions,
  commands, test discovery/counts and PASS/FAIL/BLOCKED/NOT RUN outcomes.

**Done when:** required merge checks pass or have a documented, authorized
resolution, with no unresolved introduced diagnostics hidden by broad failures.

### DBUL-03 — Member list and protected actions

**Owner:** UI shared member components; SDK/core only if a real read-contract gap
is found.

- Show database quotas in the existing member list, with every rule, supported
  units and explicit Undefined/Unlimited states.
- Match model limited-access delete and bulk-selection behavior for editors;
  explain blocked actions. Server-side enforcement remains authoritative.
- Retain limits and expiration dates on level-only changes. Never render a
  failed/missing policy read as an editable empty limit.
- Test owner/editor/admin behavior, partial metadata failure, retries, row
  identity/login types, bulk selection and unchanged model behavior.

**Done when:** member list, add and edit flows agree on the same authoritative
policy and permissions, including keyboard-accessible explanations and actions.

### DBUL-04 — Ordinary team-access dialogs

**Owner:** UI team-access features and shared editor/profile components.

- Wire both giving a team access to a database and giving a resource to a team,
  including existing-grant editing and read-only views.
- Reuse the shared editor with server-advertised metrics. Support shared versus
  each-member rules, correct windows, and preserved grant level/end date.
- Preserve policies for non-owners and delete them with authorized grant removal.
- Test policy/permission submission together, failure retaining drafts, expiry,
  owner/admin restrictions and unchanged model team dialogs.

**Done when:** both directions use the same permission-batch semantics, not an
independent database policy-management workflow.

### DBUL-05 — Safe copy behavior

**Owners:** core copy-preview producer, SDK and UI copy/staging consumers.

- Define and test same-catalog copying, incompatible catalogs, omitted limits,
  explicit clears, existing target policies and owner/editor differences.
- Ensure model rules cannot become database rules or vice versa. Never map
  queries/rows to tokens/cost to fit a model-only type.
- Preserve the target's policy when the actor cannot change it; prevent copying
  access from silently clearing or lifting a cap.
- Carry catalog metadata through previews, conflicts, summaries and saved
  batches; make unsupported copy choices explicit rather than silently dropping
  required policy information.

**Done when:** a copy matrix is documented and tested across the actual producer,
SDK parser, UI staging and save path. Preserve legacy model copy behavior unless
an explicitly agreed compatibility change is necessary.

### DBUL-06 — Bulk Access Management

**Owner:** UI bulk-access feature; shared editor and SDK contracts.

- Show database user/team quotas and public/engine-total limit-only entries.
- Support staging, multi-row edits, restore/reset, validation, review summaries,
  conflict resolution and all-or-nothing saves.
- Detect quota-only changes; include explicit clears instead of dropping null
  fields. Preserve policies on level-only edits and editor operations.
- Apply DBUL-05 copy rules and the same limited-access removal/shortening rules
  as ordinary dialogs. Do not clone the entire model batch state machine.

**Done when:** database and model workflows both have tests for staging through
review/save, denied changes, stale data and failed saves retaining edits.

### DBUL-07 — Public-use and database-total cards

**Owner:** UI settings features, using existing engine settings/permission APIs.

- Reuse model card/editor patterns with catalog capabilities and database wording.
- Use `PUBLIC` for public use and `ENGINE` for the database total. Keep `MODEL`
  and the legacy model payload contract model-only.
- Keep the public/global access toggle separate from defining its usage policy;
  explain a stored public policy while the database is private.
- Enforce shared-only rules/no Unlimited for the total, owner/admin-only writes,
  accurate inactive/default states and failed-save feedback.

**Done when:** settings round-trip correctly and subsequent reads enforce the
saved policy without changing model cards or public-access authorization.

### DBUL-08 — Admin defaults and cross-database totals

**Owners:** core → Monolith → SDK → client admin features.

- [ ] **API/SDK sub-session:** trace existing model default/total operations;
  expose the missing catalog-aware reads/writes for platform defaults, user
  defaults (`USER_TOTAL`) and team ceilings (`TEAM_TOTAL`). Verify actual routes
  and registration; do not assume generic Monolith resources already exist.
- [ ] **UI sub-session:** adapt the existing admin surfaces, shared editor and
  summaries instead of creating parallel database-only forms.
- Derive actor/admin mode from authenticated server context; test non-admin
  denial and privacy of other people's/default policies.
- Test empty versus Unlimited choices where permitted, persistence, invalidation,
  catalog isolation and unchanged model defaults/totals.

**Done when:** all three source types can be read and managed through the
intended admin UI and affect resolution exactly as documented.

### DBUL-09 — Personal usage and resolution views

**Owners:** core usage-read services, Monolith/SDK and client/shared usage views.

- Establish catalog-aware usage/path responses for current usage, limits,
  remaining allowance, windows/resets, applied source, fallback and ceilings.
- Verify what exists before adding APIs. Configuration-only
  `GetDatabaseUsageLimits` is not a personal live-usage/path API.
- Use current counter state appropriately; completed history alone must not
  misrepresent active-read usage. Explain cumulative elapsed-time settlement.
- Reuse meters/path components and dashboard composition through metric profiles;
  preserve existing model path/usage payloads and privacy rules.
- Test self versus owner/admin inspection, hidden rules, empty/error states,
  window resets and accurate query/row/elapsed-time units.

**Done when:** a user can understand their effective database limits and usage
without exposing another person's private defaults or implying a hard timeout.

### DBUL-10 — Redis, cluster and recovery validation

**Owners:** core and an authorized isolated integration environment.

- Exercise real local Redis and Redis Cluster, including the new atomic consume
  script, hash-slot placement and unchanged model namespaces/scripts.
- Test concurrent last-query/last-row admission across nodes, outage/failover,
  reconnect, history restore and the documented cache-invalidation interval.
- Test counter loss/process failure during active reads and close/history-write
  failures. Record the actual recovery gap rather than claiming perfect durable
  quotas; propose a fix or obtain explicit operating-risk acceptance.

**Done when:** reproducible live results establish expected atomicity, failure
behavior, isolation and recovery limits. Mocked script invocation tests do not
close this item.

### DBUL-11 — Driver and lifecycle matrix

**Owners:** core adapters and authorized isolated test engines.

- Agree the supported driver/engine matrix and execute representative real JDBC
  and non-JDBC reads, including legacy SELECT/CONSTRUCT, prepared/deferred/count
  queries, reset, Python workers and asynchronous Pixel responses.
- Cover success, SQL/driver error, exhaustion, cancellation/client disconnect and
  every close path; verify no leaked handles or changed borrowed transactions.
- Verify read-ahead accounting, no double charging, writes/internal databases
  excluded, correct actor restoration, and valid incomplete/error responses.
- Document supported boundaries and any unmetered direct vendor-handle paths;
  do not represent accounting as SQL authorization or a sandbox.

**Done when:** supported scenarios pass with actual drivers and failures/resource
ownership are observable, not inferred solely from mocked wrappers.

### DBUL-12 — Throughput and concurrency

**Owners:** core and an authorized performance environment.

- Measure large-result reads with no caps, local counters and Redis counters;
  include multiple readers/nodes and each configured metric combination.
- Record per-row RPC impact, throughput, latency, memory, history growth and
  cleanup cost. Agree acceptable deployment budgets rather than inventing SLOs.
- Optimize only measured bottlenecks. Any batching/prefetch optimization must
  preserve atomic quota enforcement and truthful incomplete-result behavior.

**Done when:** results support the intended workload or a separately verified
optimization resolves the measured shortfall.

### DBUL-13 — End-to-end and upgrade validation

**Owners:** all three repositories; isolated authorized browser/API environment.

- Exercise owner/editor/reader/admin journeys through real HTTP, SDK, browser and
  database enforcement, including all newly completed UI surfaces.
- Test stale submissions, explicit clears, permission changes, expired grants,
  copy conflicts, quota exhaustion and partial streaming/error presentation.
- Check labels, keyboard/focus behavior, validation announcements, overlays and
  narrow-layout behavior under the mandatory React/design/accessibility guides.
- Validate schema initialization/idempotence, supported upgrade/deployment order,
  compatibility with missing capabilities and unchanged model tables/counters.

**Done when:** real producer/consumer journeys and upgrade evidence are recorded.
Unit/component tests do not substitute for HTTP/browser or upgrade coverage.

### DBUL-14 — Final review, operations and rollout

**Owners:** independent reviewer, maintainers and deployment/operator owners.

- Review the final combined diff after the UI/API work, not only the earlier
  backend checkpoint; close new authorization, compatibility and lifecycle risks.
- Confirm acceptance of cumulative row quotas/read-ahead and cumulative elapsed
  read time (not CPU time or a hard query timeout), including active-read overruns.
  Changing these semantics requires an explicit design decision.
- Resolve the recovery-risk decision from DBUL-10 and agree history retention,
  safe cleanup, counter eviction/clock requirements, monitoring and rollback.
  A new automated retention job is not implicitly authorized by this backlog.
- Refresh the implementation guide and verification record, remove stale status
  claims, and record every remaining FAIL/BLOCKED/NOT RUN prerequisite.

**Done when:** agreed model parity is delivered, applicable gates are satisfied,
required live checks and independent review are complete, and rollout is approved.
Do not label the feature READY because only the focused tests pass.

## Source map for later sessions

Paths below are relative to their owning checkout, not this document.

| Checkout | Start here |
| --- | --- |
| `Semoss/` | Under `src/prerna/auth/utils/`: `EngineUsagePermissionSupport.java`, `PermissionBatchUtils.java`, `SecurityEngineUsageLimitUtils.java`, `EngineUsagePolicyResolver.java`. Shared enforcement: `src/prerna/engine/impl/usage/` and `src/prerna/engine/impl/database/usage/`. |
| `Monolith/` | Under `src/prerna/semoss/web/services/local/auth/`: `PermissionBatchResource.java`, `AdminPermissionBatchResource.java`, `EngineAuthorizationResource.java`, `AdminEngineAuthorizationResource.java`; model admin reference `AdminModelUsageResource.java`. |
| `apache-tomcat-11.0.24/webapps/SemossWeb/` | Under `libs/sdk/src/api/`: `access.ts`, `engine-usage.ts`, `model-usage.ts`, `permissions.ts`. Under `libs/shared/src/`: `components/model-usage-limit/`, `components/members/`, `hooks/use-engine-usage-access.ts`. Under `packages/client/src/features/`: `bulk-access-management/`, `team-access-management/`, `team-resource-access/`, `team-usage-limit/`, `model-usage-limits/`, `my-model-usage/`, `usage-limit-path/`. |

Backend parity tests under `Semoss/test/prerna/auth/utils/` start with
`PermissionBatchEngineUsageLimitUnitTests.java` and
`SecurityEngineUsageLimitUtilsUnitTests.java`; existing model permission-batch
and usage tests remain mandatory regression references.

## Session startup, invariants and handoff

1. Read workspace/repository/local `AGENTS.md`, SEMOSS standards, this queue and
   the two guides linked above. For any React investigation, planning or edits,
   read the full UI `skills/react-standard.skill.md` plus the applicable package,
   design, form and accessibility guidance; reload after context loss.
2. Inventory **all three working trees**, including untracked files. Feature work
   was uncommitted at this checkpoint, alongside unrelated edits. Do not reset,
   discard, stage or commit someone else's changes. Do not depend on `/tmp`
   scripts/logs surviving into the next session.
3. Keep models backward-compatible and database totals separate. In grant
   payloads, omitted `engineUsageLimit` preserves and explicit null clears;
   the standalone Pixel setter is a full replacement with its own documented
   omission behavior. Do not conflate these contracts.
4. Reuse capability-driven components and shared primitives. Keep metric IDs and
   units honest; do not broaden model metrics/storage or create parallel policy
   management. Keep reads-only scope. No repository CI/dependency/tooling changes
   are authorized merely to enforce Java import hygiene.
5. Trace each changed boundary across actual producers/consumers. Existing
   Monolith documentation includes an unrelated JDBC-only/databaseReadLimit
   design; preserve unrelated edits and do not assume that is this implementation.
6. Run focused and owning-package checks, the author-side `pre-submit-quality`
   gate and the Java import check where applicable. Use the safe commands in
   `verification.md`; avoid lifecycle goals that rewrite sources or Git hooks.
   Live services, deployment, migrations and shared-engine mutations require
   explicit authorization and isolation.
7. Update this queue and the verification record with scope, files, exact checks,
   results, unresolved blockers and the next task. Check only completed tasks;
   independent review cannot be replaced by another pass in the author session.

Reusable session prompt:

> Continue database usage-limit task **DBUL-XX** from
> `Semoss/docs/engine_usage_limits/TODO.md`. Read its startup instructions and
> linked guides, inspect current working trees, and confirm the task's scope and
> dependencies. Preserve all existing model contracts and unrelated work. Use
> shared infrastructure/UI rather than a parallel database implementation. Make
> the smallest complete change for that task, run the applicable checks, then
> update the queue and verification evidence. Do not mark the overall feature
> ready while other required tasks or release gates remain open.
