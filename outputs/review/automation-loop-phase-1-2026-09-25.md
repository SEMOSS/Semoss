# Automation loop phase 1

## Status

This work is parked for later review on dedicated backend and frontend branches. It is not intended to ship until the execution contract and UX are validated with representative automations.

## Current approach

- A loop is a first-class `control.loop` container in the parent automation graph.
- The parent graph stays acyclic. Iteration is owned by the Java execution service rather than represented with a circular control edge.
- The loop owns a nested acyclic body graph with ordinary automation nodes and control edges.
- The body has one entry node and may contain decisions and multiple routes.
- Phase 1 executes iterations sequentially with explicit maximum-iteration and body-size limits.
- Each iteration receives an isolated copy of the parent scope plus a loop context containing its index, total, first/last flags, current item when singular, and current batch.
- Body executions receive durable child history rows tied to the loop node and iteration index.
- Cancellation is checked between iterations and between body nodes.
- Nested loops and durable agent input waits inside a loop are intentionally unsupported in phase 1.

## Current authoring UX

- The loop appears as a distinct container card on the main automation canvas.
- Its collapsed state summarizes the repeated steps.
- Its expanded state contains a nested mini-canvas for the loop body.
- Users can insert steps at a route, edit body nodes, and use decisions inside the body.
- Expanding the loop increases its visual width and updates the surrounding React Flow node internals.

## Deliberately unresolved

### Input consumption

Batching must not become the only loop contract. The current implementation supports `batchSize`, but this should be treated as an optional execution strategy while the following are tested:

1. one item per iteration;
2. fixed-size batches;
3. a server-owned cursor or paged data reference;
4. streaming/provider-backed iteration for large tabular results.

The final contract should let the user express the intent to repeat work without requiring them to understand frames, cursors, or backend storage.

### Additional loop types

The container/body design is intended to support later modes without circular parent graphs:

- `forEach` for a collection;
- counted `for` ranges;
- bounded `while` conditions;
- paged or cursor-driven data iteration.

Every mode must remain bounded, cancellable, observable, and resumable before it is enabled.

## Before resuming implementation

- Decide the canonical loop-input contract after testing lists, database results, and large provider-backed data.
- Define retry, resume, and partial-failure behavior at the iteration level.
- Confirm whether collected results are inline, summarized, or stored as run-owned data references.
- Validate branch convergence and output availability inside the nested body.
- Improve expanded-container layout so neighboring parent nodes never overlap.
- Test history rendering for parent loop rows and per-iteration child rows.
- Add end-to-end coverage for cancellation, limits, branching, and empty inputs.

## Scope boundaries

The parked work does not introduce circular parent-graph edges, parallel iteration, nested loops, agent approval waits, or a requirement that all loops consume batches.
