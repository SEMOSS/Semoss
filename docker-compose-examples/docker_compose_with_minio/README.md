# SEMOSS — Docker Compose with MinIO

A single-node SEMOSS instance backed by PostgreSQL **and** MinIO for S3-compatible
cloud storage. Use this when you want to exercise SEMOSS's cluster/cloud storage
mode (`SEMOSS_IS_CLUSTER: 'true'`) with an object store, without running a full
multi-node cluster.

## Services

| Service | Image | Purpose | Host ports |
|---------|-------|---------|------------|
| `semoss` | `quay.io/semoss/semoss-dev:5.4.0-SNAPSHOT-ubuntu22-latest` | The SEMOSS application server, configured to push/pull storage from MinIO. | `9090` → container `8080` |
| `db` | `postgres:latest` | PostgreSQL holding all SEMOSS system databases. | `5432` → `5432` |
| `minio` | `minio/minio:latest` | S3-compatible object storage used as the SEMOSS storage provider. | `9000` (S3 API), `9001` (web console) |

`semoss` waits for **both** `db` and `minio` to pass their health checks before
starting.

### Cloud storage wiring

SEMOSS is pointed at MinIO through these env vars in [`compose.yaml`](compose.yaml):

```
SEMOSS_IS_CLUSTER: 'true'
SEMOSS_STORAGE_PROVIDER: 'minio'
MINIO_BUCKET: 'semoss'
MINIO_ENDPOINT: 'http://minio:9000'
MINIO_ACCESS_KEY / MINIO_SECRET_KEY: minioadmin / minioadmin
```

On startup MinIO runs [`init-bucket.sh`](init-bucket.sh), which uses the MinIO
client (`mc`) to create the `semoss` bucket automatically.

### System databases

[`init.sql`](init.sql) creates the same SEMOSS system databases as the basic
example: `semoss_localmaster`, `semoss_security`, `semoss_scheduler`,
`semoss_themes`, `semoss_prompt`, `semoss_modellogs`, `semoss_usertracking`,
`semoss_audit`.

> Note: unlike the basic example, this compose file does not mount named volumes
> for SEMOSS home — the object store (MinIO) is the durable store for cluster
> mode. Postgres data persists in the `pgdata` volume and MinIO data in `minio_data`.

## Usage

From this directory:

```bash
docker compose up            # add -d to detach
docker compose logs -f semoss
docker compose down          # stop, keep data
docker compose down -v       # stop and wipe volumes (pgdata, minio_data)
```

- SEMOSS UI: **http://localhost:9090/#/**
- MinIO console: **http://localhost:9001** (login `minioadmin` / `minioadmin`)

### Notes

- Credentials (`myuser`/`mypassword`, `minioadmin`/`minioadmin`) are for local
  use only — change them before exposing this anywhere.
- Native auth is enabled for easy local registration; use external SSO for
  production.
