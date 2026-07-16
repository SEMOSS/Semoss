# SEMOSS — Basic Docker Compose

The simplest way to stand up a single-node SEMOSS instance backed by PostgreSQL.
Use this when you just want SEMOSS running locally and don't need object storage
or clustering.

## Services

| Service | Image | Purpose | Host ports |
|---------|-------|---------|------------|
| `semoss` | `quay.io/semoss/semoss-dev:5.4.0-SNAPSHOT-ubuntu22-latest` | The SEMOSS application server (single node). | `9090` → container `8080` |
| `db` | `postgres:latest` | PostgreSQL instance holding all of SEMOSS's system databases. | `5432` → `5432` |

`semoss` waits for `db` to pass its health check (`pg_isready`) before starting.

### What lives in Postgres

On first startup, [`init.sql`](init.sql) creates the SEMOSS system databases,
each configured via the `CUSTOM_*` environment variables in
[`compose.yaml`](compose.yaml):

- `semoss_localmaster` — local master (engine/project metadata)
- `semoss_security` — users, permissions, auth
- `semoss_scheduler` — scheduled jobs
- `semoss_themes` — UI themes
- `semoss_prompt` — saved prompts
- `semoss_modellogs` — model inference logs
- `semoss_usertracking` — user activity tracking
- `semoss_audit` — audit logs

### SEMOSS home volumes

SEMOSS home is spread across named volumes so data survives container restarts:
`semoss_db`, `semoss_project`, `semoss_model`, `semoss_vector`,
`semoss_storage`, `semoss_function`, `semoss_guardrail`, plus `pgdata` for
Postgres.

## Usage

From this directory:

```bash
# start everything (add -d to run detached)
docker compose up

# follow logs
docker compose logs -f semoss

# stop containers (keeps data)
docker compose down

# stop AND delete all data volumes (fresh start)
docker compose down -v
```

Then open **http://localhost:9090/#/** in your browser.

### Notes

- Native auth is enabled (`ENABLE_NATIVE` / `ENABLE_NATIVE_REGISTRATION`) so you
  can register a local account. This is for local/dev use only — integrate an
  external SSO for production.
- The Postgres credentials (`myuser` / `mypassword`) are hardcoded for
  convenience. Change them (and the matching `CUSTOM_*` vars) before exposing
  this anywhere.
- Python is enabled (`NETTY_PYTHON` / `NATIVE_PY_SERVER`); R is off (`R_ON: 'false'`).
