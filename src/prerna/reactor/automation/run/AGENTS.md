# Automation Run Package

This package owns the Java-first lifecycle of one Automation execution. Follow
the parent `../AGENTS.md` for the complete product contract.

## Ownership

- `AutomationRunExecutionService` claims an immutable run, creates the run
  Insight, walks the graph, dispatches typed handlers, advances Java scope, and
  records terminal state.
- `AutomationExecutionContext` owns live run values. Child loop contexts are
  isolated overlays, not separate workflow runtimes.
- `AutomationPixelExecutor` invokes existing authenticated SEMOSS Pixel/reactor
  paths and normalizes their result.
- `AutomationPythonExecutor` executes only trigger setup or
  `developer.python` source. Python never owns traversal or persistence.
- `AutomationRunStore` is the concrete scheduler-database persistence owner.
- `AutomationRunRegistry` is only the same-JVM cancellation/heartbeat fast
  path; the database remains authoritative.

## Invariants

- Preserve project/inline-scope authorization and the atomic
  submitted-to-running claim.
- Execute only the graph, inputs, configuration, and developer source captured
  in the immutable run snapshot.
- Keep Java scope authoritative and keyed by unique `outputVar` values.
- Dispatch generated nodes through their typed handler and existing SEMOSS
  policy owner. Do not turn the runtime back into generated Python.
- Keep tasks, frames, engines, Insight IDs, Python variables, and provider
  handles out of public output contracts.
- Database reads retain `SqlQuery` authorization/routing, import the task once
  into the run Insight, and expose an internal `RegisteredFrame` to Java scope.
  Table history is a bounded durable capture; it is not a raw task replay.
- Structured loops use the ordinary handlers for body work. Enforce iteration,
  item, execution, output, and cancellation bounds before or during execution.
  Preserve duplicate frame rows by querying with `distinct=false`.
- Keep node transitions, waits, cancellation, table captures, and terminal
  status durable. A live Insight is an execution resource, not the history
  store.
- Reconstruct a resumed scope from authorized persisted values. Never rerun a
  successful node to rebuild scope.
- Do not add a persistence interface until a real second store exists.

## Error handling and cleanup

- Log identifiers that help diagnose the run, node, and execution Insight, but
  never log secrets or entire large scope values.
- Mark the node failed before surfacing the exception to the run lifecycle.
- Close tasks/wrappers and failed frame registrations on every path.
- Remove or release run-owned live resources according to the intended wait and
  completion lifecycle.

## Verification

Keep focused tests under `test/prerna/reactor/automation/run`. Cover handler
selection, expression rendering, typed scope resolution, loop child history,
duplicate table rows, table capture/reload, status transitions, waits/resume,
cancellation, failed registration, bounds, and cleanup whenever those contracts
change.
