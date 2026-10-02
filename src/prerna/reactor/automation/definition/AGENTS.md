# Automation Definition Package

This package owns the canonical workflow definition and authoring API. Follow
the parent `../AGENTS.md` for the complete Automation contract.

## Ownership

- Validate and normalize the typed graph.
- Define the server-owned node catalog and configuration schema.
- Persist the graph and per-node Python source as one definition aggregate.
- Render generated node source and expose authoring reactors.
- Evaluate bounded control conditions; never execute arbitrary expressions.

## Invariants

- The persisted graph is canonical. Do not create a second node contract in the
  UI, MCP metadata, or generated Python.
- A compound node may own a nested acyclic graph through `body`. Validate its
  nodes, edges, identifiers, sources, and permissions through the same catalog
  and definition services used by the parent graph.
- Authoring may save an incomplete acyclic draft; execution validation remains
  stricter and requires a connected runnable graph.
- Node types, ports, configuration fields, and output fields come from
  `AutomationNodeCatalog` and stable enums rather than string-prefix logic.
- Definition mutation must enter through `AutomationProjectService` so project
  permissions, locking, revision checks, asset sync, and MCP regeneration stay
  together.

## Verification

Keep focused tests under `test/prerna/reactor/automation/definition`. When a
node contract changes, cover validation, catalog serialization, generated
source, and scope descriptors as applicable.
