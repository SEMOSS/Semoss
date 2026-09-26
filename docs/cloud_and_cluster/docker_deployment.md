# Local Docker Setup

Use the checked-in Docker Compose examples to run SEMOSS locally. They include the web application, UI, runtime assets, and supporting platform services.

## Choose a local setup

| Goal | Guide |
| --- | --- |
| Try published images | [Semoss Docker Compose examples](../../docker-compose-examples/README.md) |
| Run local backend changes | [Monolith Docker quick start](https://github.com/SEMOSS/Monolith#quick-start-with-docker) |
| Test the FIPS development image | [Monolith FIPS examples](https://github.com/SEMOSS/Monolith/tree/dev/local-docker-testing/local-docker-compose-fips) |
| Add databases, vectors, storage, or functions | [Supporting engine examples](../../docker-compose-examples/engines/README.md) |

## Start a published image

Install Docker with Compose v2 and start the Docker daemon. From the Semoss repository root:

```bash
cd docker-compose-examples
docker network inspect semoss-net >/dev/null 2>&1 || docker network create semoss-net
docker compose -f semoss-with-postgres.yml up -d
docker compose -f semoss-with-postgres.yml logs -f semoss
```

Open [http://localhost:9090/#/](http://localhost:9090/#/) after startup. The example enables native registration for local use. PostgreSQL credentials belong to the database service; create a SEMOSS account through the application's registration flow.

The basic example runs SEMOSS and PostgreSQL. Other [Compose variants](../../docker-compose-examples/README.md#variants) add MinIO and two application nodes with Redis or ZooKeeper synchronization. Run one platform variant at a time because their container names and host ports overlap.

From the same directory, inspect or stop the selected stack:

```bash
docker compose -f semoss-with-postgres.yml ps
docker compose -f semoss-with-postgres.yml down
```

`down` retains named volumes. Adding `-v` deletes those volumes and their data. See [local Docker configuration](../deployment/docker_configuration.md) for ports, networking, and persistence.

## Run local backend changes

Follow [Monolith's Docker quick start](https://github.com/SEMOSS/Monolith#quick-start-with-docker) with sibling Semoss and Monolith checkouts. Its build script compiles both repositories, creates the `local-monolith` image, and invokes Trivy. The Monolith Compose examples use that local image.

The development image replaces the WAR and stages JavaScript assets onto a published base. Python/R assets, platform agent and skill projects, and frontend changes need their corresponding packaging. The local Dockerfile preserves the base image's `web.xml`; verify the running descriptor when testing route or filter changes.

## Add supporting engines

The [supporting engine examples](../../docker-compose-examples/engines/README.md) provide optional services and connection instructions:

| Engine type | Examples |
| --- | --- |
| [Vector](../../docker-compose-examples/engines/vector.md) | Weaviate, Chroma, OpenSearch, pgvector |
| [Database](../../docker-compose-examples/engines/database.md) | ClickHouse |
| [Storage](../../docker-compose-examples/engines/storage.md) | MinIO, SFTP |
| [Function](../../docker-compose-examples/engines/functions/README.md) | Mail sending, mail reading, and Microsoft 365 integration |

For example, from the Semoss repository root:

```bash
cd docker-compose-examples/engines
docker compose -f semoss-weaviate.yml up -d
```

The Semoss platform and supporting-engine examples share `semoss-net`. Use the service's container name and internal port when configuring a connection from SEMOSS in Docker. Starting a service does not register an engine in SEMOSS; follow its linked connection guide. Monolith's local examples create their own Compose network, so connect the application to `semoss-net` when using these supporting services by container name.

## Kubernetes and deployment properties

For Kubernetes and infrastructure deployment, or semoss-artifacts property configuration, see [SEMOSS-deployment](https://github.com/SEMOSS/SEMOSS-deployment).
