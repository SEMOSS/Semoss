# SEMOSS — Docker Compose with MinIO + ZooKeeper (multi-node cluster)

A **two-node SEMOSS cluster** backed by PostgreSQL and MinIO, using **Apache
ZooKeeper** for cross-node cluster synchronization (`ClusterSynchronizer`). This
is the ZooKeeper-based counterpart to the Redis cluster example — use whichever
synchronizer matches your target environment.

## Services

| Service | Image | Purpose | Host ports |
|---------|-------|---------|------------|
| `semoss1` | `quay.io/semoss/semoss-dev:5.4.0-SNAPSHOT-ubuntu22-latest` | SEMOSS node 1. | `9090` → `8080` |
| `semoss2` | same image | SEMOSS node 2 (starts after `semoss1`). | `9091` → `8080` |
| `db` | `postgres:latest` | Shared PostgreSQL for all SEMOSS system databases. | `5432` → `5432` |
| `minio` | `minio/minio:latest` | Shared S3-compatible storage — the source of truth both nodes sync from. | `9000` (S3 API), `9001` (console) |
| `zookeeper` | `zookeeper:3.9` | Cluster synchronization backend (standalone mode) shared by both nodes. | `2181` → `2181` (client port) |

Both SEMOSS nodes wait for `db`, `minio`, and `zookeeper` to be healthy.

### Cluster wiring

The compose file uses YAML anchors (`x-semoss-env`, `x-semoss-service`) so both
nodes share one config block. Key cluster settings:

```
SEMOSS_IS_CLUSTER: 'true'          # cloud storage / cluster mode
SEMOSS_STORAGE_PROVIDER: 'minio'   # shared MinIO bucket 'semoss'
SEMOSS_IS_CLUSTER_ZK: 'true'       # use ZooKeeper for synchronization
ZK_SERVER: 'zookeeper:2181'
```

`HOST_IP` is **deliberately set per node** (`semoss1:8080`, `semoss2:8080`), not
in the shared block — each container must register as a distinct member in
ZooKeeper.

ZooKeeper runs standalone (`ZOO_STANDALONE_ENABLED: 'true'`) and whitelists the
four-letter commands `ruok,srvr,stat`; its health check uses `ruok` (expects
`imok`).

### Supporting pieces

- [`init.sql`](init.sql) creates the SEMOSS system databases (`semoss_localmaster`,
  `semoss_security`, `semoss_scheduler`, `semoss_themes`, `semoss_prompt`,
  `semoss_modellogs`, `semoss_usertracking`, `semoss_audit`).
- [`init-bucket.sh`](init-bucket.sh) creates the `semoss` MinIO bucket on startup.
- Persistent volumes: `pgdata`, `minio_data`, `zk_data`, `zk_datalog`.

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
- ZooKeeper client port: **localhost:2181** (e.g. `echo ruok | nc localhost 2181`
  should return `imok`).

### Notes

- Both nodes serve the same cluster; changes made through one should propagate to
  the other via ZooKeeper + shared MinIO storage.
- All credentials are local-dev defaults — change them before any real deployment,
  and integrate external SSO instead of native auth for production.
