# Automation Project Package

This package owns the Automation project boundary. Follow the parent
`../AGENTS.md` for the complete Automation contract.

## Ownership

- Resolve Automation projects through standard SEMOSS project permissions.
- Own the inline run scope: one per login, used for runs that have no project.
- Serialize definition mutations under the project lock.
- Coordinate definition persistence, reference validation, edit timestamps,
  cluster synchronization, and derived MCP assets.
- Create new Automation projects and their starter definition.

## Invariants

- View and edit operations must use the corresponding project ACL check.
- A project lookup must reject an inline-prefixed id, so an uploaded project
  cannot claim another user's inline run scope.
- Keep graph and node-source publication atomic from the caller's perspective.
- MCP files are derived project assets, not a second workflow definition.
- Validate referenced engines, apps, and agent workspaces against the acting
  user before publishing or running the definition. An `agent.run` node without
  a workspace is allowed.
- Keep `AutomationProjectService` concrete; split it only when a responsibility
  gains an independently owned lifecycle.

## Verification

Test permission failures, revision conflicts, rollback behavior, reference
validation, and MCP regeneration when this boundary changes.
