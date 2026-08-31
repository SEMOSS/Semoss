# Supporting Engines

Optional standalone services you can run alongside SEMOSS.
Each is a self-contained compose file - bring one up, point SEMOSS at it, tear it
down when done.

| File | Service | Host port(s) | Auth | SEMOSS engine |
|------|---------|--------------|------|---------------|
| [semoss-weaviate.yml](semoss-weaviate.yml) | Weaviate 1.38.6 | 8081 (-> 8080) REST, 50051 gRPC | API key `test-key` | vector (`WEAVIATE`) |
| [semoss-chroma.yml](semoss-chroma.yml) | Chroma 1.0.0 | 8000 | none | vector (`CHROMA`) |
| [semoss-opensearch.yml](semoss-opensearch.yml) | OpenSearch 2.19.5 + Dashboards | 9200 (API), 5601 (UI) | admin / password | vector (`OPEN_SEARCH`) |
| [semoss-pgvector.yml](semoss-pgvector.yml) | Postgres 18 + pgvector | 5433 (-> 5432) | pgvector / pgvector | vector (`PGVECTOR`) |
| [semoss-clickhouse.yml](semoss-clickhouse.yml) | ClickHouse 25.8 | 8123 (HTTP), 9010 (-> 9000 native) | clickhouse / clickhouse | database (`CLICKHOUSE`) |
| [semoss-minio.yml](semoss-minio.yml) | MinIO 2025-09-07 | 9100 (-> 9000 S3 API), 9101 (-> 9001 UI) | minioadmin / minioadmin | storage (`MINIO`) |
| [semoss-sftp.yml](semoss-sftp.yml) | atmoz/sftp (alpine) | 2222 (-> 22) | foo / pass | storage (`SFTP`) |
| [semoss-mail.yml](semoss-mail.yml) | GreenMail 2.1.13 | 3025 SMTP, 3110 POP3, 3143 IMAP, 3465/3995/3993 TLS, 8085 (-> 8080) API | semoss@semoss.local / semoss, reports@semoss.local / reports | function (`SMTP`, `POP3`, `IMAP`) |

```bash
docker compose -f semoss-weaviate.yml up -d      # or -chroma / -opensearch / -pgvector / -clickhouse / -minio / -sftp / -mail
docker compose -f semoss-weaviate.yml down       # add -v to also wipe the data volume
```

Quick health checks:

```bash
curl http://localhost:8081/v1/.well-known/ready          # weaviate
curl http://localhost:8000/api/v2/version                # chroma
curl -k -u admin:Str0ngVectorP@ss1 https://localhost:9200  # opensearch
docker exec semoss-pgvector pg_isready -U pgvector        # pgvector
curl http://localhost:8123/ping                          # clickhouse (returns "Ok.")
curl -i http://localhost:9100/minio/health/live          # minio (204 when ready)
docker exec semoss-sftp nc -z localhost 22 && echo ok    # sftp
curl http://localhost:8085/api/service/readiness          # mail (greenmail)
```

## Pointing SEMOSS at one

The settings each engine takes are on their own page, since they have little to
do with each other:

| Page | Engines |
|------|---------|
| [vector.md](vector.md) | Weaviate, Chroma, OpenSearch, pgvector |
| [database.md](database.md) | ClickHouse |
| [storage.md](storage.md) | MinIO, SFTP |
| [functions/](functions/README.md) | the mail engines: sending, reading, and Microsoft 365 |

## Networking: connecting from SEMOSS

These engine files and the SEMOSS compose files all share one Docker network,
`semoss-net`, so a dockerized SEMOSS reaches an engine **by its container name** -
no port mapping or `host.docker.internal` juggling needed.

Create the shared network once (it persists until you delete it); every SEMOSS and
engine file references it as external:

```bash
docker network create semoss-net
```

### Addressing

**SEMOSS in Docker** - use the container name and the *internal* port:

```
http://semoss-weaviate:8080                       (weaviate)
http://semoss-chroma:8000                         (chroma)
https://semoss-opensearch:9200                    (opensearch)
jdbc:postgresql://semoss-pgvector:5432/vectordb   (pgvector, internal port 5432)
jdbc:clickhouse://semoss-clickhouse:8123/semoss   (clickhouse, HTTP port 8123)
http://semoss-minio:9000                          (minio, internal S3 API port 9000)
semoss-sftp:22                                    (sftp, internal port 22)
semoss-mail:3025 / :3110 / :3143                   (mail, smtp / pop3 / imap)
```

**SEMOSS on your host** (not in Docker) - use `localhost` and the published port:

```
http://localhost:8081                               (weaviate)
http://localhost:8000                               (chroma)
https://localhost:9200                              (opensearch)
jdbc:postgresql://localhost:5433/vectordb           (pgvector, host port 5433)
jdbc:clickhouse://localhost:8123/semoss             (clickhouse, host port 8123)
http://localhost:9100                               (minio, host port 9100)
localhost:2222                                      (sftp, host port 2222)
localhost:3025 / :3110 / :3143                       (mail, same ports on the host)
```

> **pgvector port note:** pgvector is just Postgres, the same as the SEMOSS `db`
> service (host `5432`). To avoid a clash it is published on host `5433`
> (`5433:5432`). The in-container port is still `5432`, so SEMOSS-in-Docker
> connects on `5432` by container name; only host access uses `5433`. If `5433`
> is also taken, change the left side of `"5433:5432"` in
> [semoss-pgvector.yml](semoss-pgvector.yml).

> **ClickHouse port note:** JDBC uses the HTTP interface on `8123`. The native
> protocol port `9000` clashes with the MinIO S3 API in the SEMOSS compose files, so
> it is published on host `9010` (`9010:9000`) and only matters if you want to attach
> `clickhouse-client` from the host.

> **MinIO port note:** the SEMOSS compose files that bundle MinIO publish it on
> host `9000` (S3 API) and `9001` (console). The standalone file uses host `9100`
> and `9101` so both can run side by side. The in-container ports are still
> `9000` / `9001`, so SEMOSS-in-Docker connects on `9000` by container name; only
> host access uses `9100`. If those host ports are taken, change the left side of
> `"9100:9000"` / `"9101:9001"` in [semoss-minio.yml](semoss-minio.yml).

> **Mail port note:** GreenMail serves on `3025` / `3110` / `3143` instead of the
> real `25` / `110` / `143` so it needs no privileges, and it publishes them
> unmapped, so the port is the same from Docker and from the host. The mail engines
> default to the real ports, so `SMTP_PORT` / `POP3_PORT` / `IMAP_PORT` always have
> to be set for this server.

## Notes

- All credentials here (`test-key`, `admin` / `Str0ngVectorP@ss1`,
  `pgvector` / `pgvector`, `clickhouse` / `clickhouse`,
  `minioadmin` / `minioadmin`, `foo` / `pass`,
  `semoss@semoss.local` / `semoss`, `reports@semoss.local` / `reports`) are
  local-dev defaults - change them before using any of this beyond your machine.
- Each service uses fixed container names, so run one instance of each at a time.
