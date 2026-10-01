# Local Docker Configuration

The checked-in Compose files define the local stack: images, ports, networks, volumes, and service connections. Start with [local Docker setup](../cloud_and_cluster/docker_deployment.md), then use the guide for your selected examples:

- [Semoss Compose examples](../../docker-compose-examples/README.md) run published images.
- [Monolith Compose examples](https://github.com/SEMOSS/Monolith/tree/dev/local-docker-testing/local-docker-compose) run the locally built `local-monolith` image.
- [Supporting engines](../../docker-compose-examples/engines/README.md) add optional vector databases, databases, storage, and functions.

## Ports and networking

The basic platform examples expose SEMOSS on port `9090` and PostgreSQL on `5432`. The two-node examples also use `9091`. Check the selected Compose file for supporting service ports and stop overlapping stacks before switching examples.

Semoss's platform and supporting-engine examples use the external network `semoss-net`. Monolith's local examples create their own Compose network. To use the supporting-engine examples with Monolith by container name, connect the application to `semoss-net` as well.

Use container names and internal ports for connections between services on a shared Docker network. Use `localhost` and published ports when connecting from the host. The [supporting-engine networking guide](../../docker-compose-examples/engines/README.md#networking-connecting-from-semoss) lists both address forms.

## Persistence and initialization

The basic stack keeps PostgreSQL data and application resource directories in named volumes. MinIO variants use object storage for shared assets and persist their database and storage volumes. Inspect the selected YAML before changing volume mappings.

[init.sql](../../docker-compose-examples/init.sql) creates the example system databases on PostgreSQL's first initialization. Changing that script or startup credentials does not reinitialize an existing data volume. Apply database changes explicitly when keeping existing data.

`docker compose -f <file> down` stops the stack and retains named volumes. Adding `-v` deletes their data.

## Local code and assets

Rebuild `local-monolith` to pick up backend code changes, then recreate the application container using the selected Monolith Compose file. Its Dockerfile preserves the base image's `web.xml`; verify the active descriptor when testing servlet routes or filters. See [developer onboarding](../development_guides/java_developer_onboarding.md#run-locally) for the build workflow.

## Kubernetes and deployment properties

For Kubernetes and infrastructure deployment, or semoss-artifacts property configuration, see [SEMOSS-deployment](https://github.com/SEMOSS/SEMOSS-deployment).
