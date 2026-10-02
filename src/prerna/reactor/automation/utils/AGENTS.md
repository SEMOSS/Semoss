# Automation Runtime Utilities Package

This package is limited to shared runtime-boundary transformations. Follow the
parent `../AGENTS.md` for the complete Automation contract.

## Invariants

- Keep JSON serialization, bounded scope construction, and output-preview
  transformations deterministic and side-effect free.
- Reject unsupported or oversized values with clear errors.
- Do not place graph ownership, persistence, permissions, execution lifecycle,
  or project coordination here.
- Prefer a concrete owner in `definition`, `run`, `project`, or `agent` over a
  new generic helper.

## Verification

Keep serialization and boundary tests under
`test/prerna/reactor/automation/utils`, including size, type, date/time, and
error-path coverage.
