# SEMOSS — Docker Compose with MinIO + Redis (multi-node cluster)

A **two-node SEMOSS cluster** backed by PostgreSQL and MinIO, using **Redis** for
cross-node cluster synchronization (`RedisClusterSynchronizer`). Use this to test
clustered/HA behavior where multiple SEMOSS nodes must stay in sync and share the
same object storage.

## Services

| Service | Image | Purpose | Host ports |
|---------|-------|---------|------------|
| `semoss1` | `quay.io/semoss/semoss-dev:5.4.0-SNAPSHOT-ubuntu22-latest` | SEMOSS node 1. | `9090` → `8080` |
| `semoss2` | same image | SEMOSS node 2 (starts after `semoss1`). | `9091` → `8080` |
| `db` | `postgres:latest` | Shared PostgreSQL for all SEMOSS system databases. | `5432` → `5432` |
| `minio` | `minio/minio:latest` | Shared S3-compatible storage — the source of truth both nodes sync from. | `9000` (S3 API), `9001` (console) |
| `redis` | `redis:7` | Cluster synchronization backend shared by both nodes. | `6379` → `6379` |
| `redis-ui` | `redis/redisinsight:latest` | RedisInsight web UI for inspecting Redis. | `5540` → `5540` |

Both SEMOSS nodes wait for `db`, `minio`, and `redis` to be healthy.

### Cluster wiring

The compose file uses YAML anchors (`x-semoss-env`, `x-semoss-service`) so both
nodes share one config block. Key cluster settings:

```
SEMOSS_IS_CLUSTER: 'true'          # cloud storage / cluster mode
SEMOSS_STORAGE_PROVIDER: 'minio'   # shared MinIO bucket 'semoss'
SEMOSS_IS_CLUSTER_REDIS: 'true'    # use Redis for synchronization
REDIS_HOST: 'redis'  REDIS_PORT: '6379'
```

`HOST_IP` is **deliberately set per node** (`semoss1:8080`, `semoss2:8080`), not
in the shared block — each container must register as a distinct cluster member.

### Supporting pieces

- [`init.sql`](init.sql) creates the SEMOSS system databases (`semoss_localmaster`,
  `semoss_security`, `semoss_scheduler`, `semoss_themes`, `semoss_prompt`,
  `semoss_modellogs`, `semoss_usertracking`, `semoss_audit`).
- [`init-bucket.sh`](init-bucket.sh) creates the `semoss` MinIO bucket on startup.
- Persistent volumes: `pgdata`, `minio_data`, `redis_data`.

## Usage

From this directory:

```bash
docker compose up            # add -d to detach
docker compose logs -f semoss1 semoss2
docker compose down          # stop, keep data
docker compose down -v       # stop and wipe all volumes
```

- SEMOSS node 1: **http://localhost:9090/#/**
- SEMOSS node 2: **http://localhost:9091/#/**
- MinIO console: **http://localhost:9001** (`minioadmin` / `minioadmin`)
- RedisInsight: **http://localhost:5540** — add a connection to host `redis`,
  port `6379` (use the service name, **not** `localhost`).

### Notes

- Both nodes serve the same cluster; changes made through one should propagate to
  the other via Redis + shared MinIO storage.
- All credentials are local-dev defaults — change them before any real deployment,
  and integrate external SSO instead of native auth for production.
