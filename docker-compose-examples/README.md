# SEMOSS Docker Compose Examples

A set of Docker Compose files for standing up SEMOSS locally, from a single node
up to a two-node cluster. They live in one folder and share the two support
files below, so pick a variant and point `docker compose -f` at it.

## Variants

| File | What it stands up | Sync backend | Nodes |
|------|-------------------|--------------|-------|
| [semoss-with-postgres.yml](semoss-with-postgres.yml) | SEMOSS + PostgreSQL. The simplest local instance, no object storage or clustering. | none | 1 |
| [semoss-with-postgres-minio.yml](semoss-with-postgres-minio.yml) | SEMOSS + PostgreSQL + MinIO. Single node in cloud-storage mode (`SEMOSS_IS_CLUSTER: 'true'`) using MinIO as the S3-compatible storage provider. | none (storage only) | 1 |
| [semoss-with-postgres-minio-redis.yml](semoss-with-postgres-minio-redis.yml) | Two-node SEMOSS cluster + PostgreSQL + MinIO + Redis (+ RedisInsight UI). Nodes sync via `RedisClusterSynchronizer`. | Redis | 2 |
| [semoss-with-postgres-minio-zk.yml](semoss-with-postgres-minio-zk.yml) | Two-node SEMOSS cluster + PostgreSQL + MinIO + Apache ZooKeeper. Nodes sync via `ClusterSynchronizer`. The ZooKeeper counterpart to the Redis example. | ZooKeeper | 2 |

All four use the SEMOSS image
`quay.io/semoss/semoss-dev:5.4.0-SNAPSHOT-ubuntu22-latest` and the Postgres
credentials `myuser` / `mypassword` (local-dev defaults).

## Shared support files

- [init.sql](init.sql) - runs on first Postgres startup and creates the SEMOSS
  system databases: `semoss_localmaster`, `semoss_security`, `semoss_scheduler`,
  `semoss_themes`, `semoss_prompt`, `semoss_modellogs`, `semoss_usertracking`,
  `semoss_audit`. Used by every variant.
- [init-bucket.sh](init-bucket.sh) - runs on MinIO startup and uses the MinIO
  client (`mc`) to create the `semoss` bucket. Used by every variant except the
  basic one.

## Supporting engines

The [engines/](engines/) folder has optional standalone services (Weaviate,
Chroma, OpenSearch, pgvector vector DBs) you can run alongside SEMOSS, plus notes
on how to make the SEMOSS container connect to them. See
[engines/README.md](engines/README.md).

## Usage

All variants attach to a shared Docker network named `semoss-net` (so the optional
services in [engines/](engines/) can reach SEMOSS by container name). Create it
once before your first `up`:

```bash
docker network create semoss-net
```

Then, from this directory, choose one variant with `-f`:

```bash
# basic single node
docker compose -f semoss-with-postgres.yml up            # add -d to detach

# single node with MinIO object storage
docker compose -f semoss-with-postgres-minio.yml up

# two-node cluster with Redis synchronization
docker compose -f semoss-with-postgres-minio-redis.yml up

# two-node cluster with ZooKeeper synchronization
docker compose -f semoss-with-postgres-minio-zk.yml up
```

Common lifecycle commands (append the same `-f <file>`):

```bash
docker compose -f <file> logs -f semoss   # or semoss1 semoss2 for cluster variants
docker compose -f <file> down             # stop, keep data
docker compose -f <file> down -v          # stop and wipe all named volumes
```

> The variants use fixed `container_name`s (`semoss`, `db`, `minio`, ...), so run
> one variant at a time. `down` the current one before bringing up another.

## Endpoints by variant

| | basic | with-minio | with-minio-and-redis | with-minio-and-zk |
|--|:--:|:--:|:--:|:--:|
| SEMOSS node 1 | http://localhost:9090/#/ | http://localhost:9090/#/ | http://localhost:9090/#/ | http://localhost:9090/#/ |
| SEMOSS node 2 | - | - | http://localhost:9091/#/ | http://localhost:9091/#/ |
| Postgres | localhost:5432 | localhost:5432 | localhost:5432 | localhost:5432 |
| MinIO console | - | http://localhost:9001 | http://localhost:9001 | http://localhost:9001 |
| Sync UI / port | - | - | RedisInsight http://localhost:5540 | ZooKeeper localhost:2181 |

MinIO console login is `minioadmin` / `minioadmin`. In RedisInsight, add a
connection to host `redis` port `6379` (the service name, not `localhost`). For
ZooKeeper, `echo ruok | nc localhost 2181` should return `imok`.

## Notes

- The two cluster variants use YAML anchors (`x-semoss-env`, `x-semoss-service`)
  so both nodes share one config block. `HOST_IP` is deliberately set per node
  (`semoss1:8080`, `semoss2:8080`) so each container registers as a distinct
  cluster member.
- The basic variant mounts named volumes for SEMOSS home
  (`semoss_project`, `semoss_model`, ...); the MinIO variants use the object
  store as the durable store instead and only persist `pgdata` / `minio_data`
  (plus `redis_data` or `zk_data` / `zk_datalog`).
- Native auth is enabled for easy local registration. This is for local/dev use
  only - change the credentials and integrate an external SSO before exposing any
  of this.
- Python is enabled (`NETTY_PYTHON` / `NATIVE_PY_SERVER`); R is off (`R_ON: 'false'`).
