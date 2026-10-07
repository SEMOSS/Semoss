# Automation Run Package

This package owns durable run state and the lifecycle of one Automation
execution. Follow the parent `../AGENTS.md` for the complete Automation
contract.

## Ownership

- `AutomationRunStore` is the concrete scheduler-database persistence owner.
- `AutomationRunExecutionService` claims and executes an immutable run
  snapshot in one run-owned Insight.
- `AutomationRunRegistry` is only the same-JVM cancellation and heartbeat fast
  path; the database remains authoritative.
- Run reactors trigger, inspect, list, resume, and cancel executions.

## Invariants

- Preserve project permission checks and the atomic submitted-to-running claim.
- Execute only the definition, inputs, and source captured for that run.
- Keep node transitions, cancellation, waits, and terminal status durable.
- A structured loop materializes independent child history rows for each
  pass. Keep the parent graph acyclic, use the ordinary node executors for body
  work, enforce pass and total-execution bounds before materializing rows, and
  keep scope isolated from the parent. A `while` loop may carry only the prior
  pass's declared body outputs into its next isolated scope.
- Keep frame display data scoped to the execution Insight. A frame is not a
  durable-history contract.
- Do not pass engine objects, arbitrary Java objects, or unbounded payloads
  through Python scope or browser responses.
- Do not add a persistence interface until there is a real second store.

## Verification

Keep focused tests under `test/prerna/reactor/automation/run`. Exercise claim
behavior, status transitions, cancellation, waits/resume, run snapshots, and
cleanup whenever their lifecycle changes.
