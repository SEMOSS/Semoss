# Automation Agent Package

This package exposes only trace-linked agent-run operations for Automation.
Follow the parent `../AGENTS.md` for the complete Automation contract.

## Invariants

- Apply the Automation project ACL before accessing a child agent run.
- Require the exact persisted project/run/node/agent-run relationship; do not
  accept a caller-provided room or agent run as sufficient authorization.
- Use the standard Agent run service for retrieval and actions.
- Keep view and control authorization distinct.
- Do not move general agent execution into this package; it remains owned by
  `prerna.reactor.agent`.

## Verification

Cover missing authentication, project access, trace mismatch, view-only access,
and edit/control access whenever these reactors change.
