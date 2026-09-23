# Vector engines

Vector databases you can run alongside SEMOSS, and the SMSS settings to point a
vector engine at each. Bring the container up first - see
[the engine index](README.md) for the compose commands and for how to address a
container from SEMOSS.

Create the vector DB engine in SEMOSS with the settings below. Pick the
`HOSTNAME` value from the networking section above that matches your setup. The
parameter names are the SMSS property keys the engines read.

## Weaviate

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

## Chroma

```
VECTOR_TYPE              CHROMA
HOSTNAME                 http://semoss-chroma:8000   (SEMOSS in Docker; use http://localhost:8000 if SEMOSS runs on host)
CHROMA_COLLECTION_NAME   <collection name>
EMBEDDER_ENGINE_ID       <an existing embedder model engine>
```

## OpenSearch

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

## pgvector

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

> Every one of these needs `EMBEDDER_ENGINE_ID` to reference an embedder model
> engine that already exists in your instance.

---

One of the [supporting engines](README.md).
