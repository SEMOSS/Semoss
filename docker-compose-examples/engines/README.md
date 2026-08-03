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

```bash
docker compose -f semoss-weaviate.yml up -d      # or semoss-chroma.yml / -opensearch.yml / -pgvector.yml / -clickhouse.yml
docker compose -f semoss-weaviate.yml down       # add -v to also wipe the data volume
```

Quick health checks:

```bash
curl http://localhost:8081/v1/.well-known/ready          # weaviate
curl http://localhost:8000/api/v2/version                # chroma
curl -k -u admin:Str0ngVectorP@ss1 https://localhost:9200  # opensearch
docker exec semoss-pgvector pg_isready -U pgvector        # pgvector
curl http://localhost:8123/ping                          # clickhouse (returns "Ok.")
```

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
```

**SEMOSS on your host** (not in Docker) - use `localhost` and the published port:

```
http://localhost:8081                               (weaviate)
http://localhost:8000                               (chroma)
https://localhost:9200                              (opensearch)
jdbc:postgresql://localhost:5433/vectordb           (pgvector, host port 5433)
jdbc:clickhouse://localhost:8123/semoss             (clickhouse, host port 8123)
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

## SEMOSS vector engine settings

Create the vector DB engine in SEMOSS with the settings below. Pick the
`HOSTNAME` value from the networking section above that matches your setup. The
parameter names are the SMSS property keys the engines read.

### Weaviate

```
VECTOR_TYPE          WEAVIATE
HOSTNAME             http://semoss-weaviate:8080   (SEMOSS in Docker; use http://localhost:8081 if SEMOSS runs on host)
API_KEY              test-key
WEAVIATE_CLASSNAME   default
WEAVIATE_GRPC_PORT   50051                         (gRPC port)
WEAVIATE_GRPC_HOST   <optional; defaults to the HOSTNAME host>
WEAVIATE_HTTP_PORT   <optional; defaults to 443 for https / 80 for http, or the port in HOSTNAME>
EMBEDDER_ENGINE_ID   <an existing embedder model engine>
```

Weaviate uses gRPC in addition to REST, but `WEAVIATE_GRPC_HOST` defaults to the
host parsed from `HOSTNAME`, so the settings above are all you need.

### Chroma

```
VECTOR_TYPE              CHROMA
HOSTNAME                 http://semoss-chroma:8000   (SEMOSS in Docker; use http://localhost:8000 if SEMOSS runs on host)
CHROMA_COLLECTION_NAME   <collection name>
EMBEDDER_ENGINE_ID       <an existing embedder model engine>
```

### OpenSearch

```
VECTOR_TYPE          OPEN_SEARCH
HOSTNAME             https://semoss-opensearch:9200    (SEMOSS in Docker; use https://localhost:9200 if SEMOSS runs on host)
USERNAME             admin
PASSWORD             Str0ngVectorP@ss1            (OPENSEARCH_INITIAL_ADMIN_PASSWORD)
INDEX_NAME           <index name>
EMBEDDER_ENGINE_ID   <an existing embedder model engine>
```

> OpenSearch serves HTTPS with a self-signed certificate, so use `https://` and
> make sure SEMOSS is allowed to trust/skip verification for it. Override the
> admin password by exporting `OPENSEARCH_INITIAL_ADMIN_PASSWORD` (or a `.env`
> file) before `up`; it must meet OpenSearch's complexity rules.

### pgvector

pgvector extends `RDBMSNativeEngine`, so it takes JDBC connection settings rather
than a plain `HOSTNAME`. SEMOSS creates the `vector` extension and the tables
automatically on first connect.

```
VECTOR_TYPE                    PGVECTOR
RDBMS_TYPE                     POSTGRES
DRIVER                         org.postgresql.Driver
CONNECTION_URL                 jdbc:postgresql://semoss-pgvector:5432/vectordb   (host: jdbc:postgresql://localhost:5433/vectordb)
USERNAME                       pgvector
PASSWORD                       pgvector
PGVECTOR_TABLE_NAME            <table name>
PGVECTOR_METADATA_TABLE_NAME   <metadata table name>
EMBEDDER_ENGINE_ID             <an existing embedder model engine>
```

## SEMOSS database engine settings

### ClickHouse

ClickHouse is a relational (OLAP) database, not a vector DB, so create it as a
database engine:

```
RDBMS_TYPE   CLICKHOUSE
HOSTNAME     semoss-clickhouse   (host: localhost)
PORT         8123
DATABASE     semoss
USERNAME     clickhouse
PASSWORD     clickhouse
```

ClickHouse has databases only, no schemas, so leave `SCHEMA` unset. Instead of
`HOSTNAME` / `PORT` / `DATABASE` you can set `CONNECTION_URL` directly to
`jdbc:clickhouse://semoss-clickhouse:8123/semoss`.

## Notes

- All credentials here (`test-key`, `admin` / `Str0ngVectorP@ss1`,
  `pgvector` / `pgvector`, `clickhouse` / `clickhouse`) are local-dev defaults -
  change them before using any of this beyond your machine.
- Each service uses fixed container names, so run one instance of each at a time.
- `EMBEDDER_ENGINE_ID` must reference an embedder model engine that already exists
  in your SEMOSS instance.
