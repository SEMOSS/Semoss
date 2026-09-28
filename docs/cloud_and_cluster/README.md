# Cloud and Cluster Architecture

SEMOSS can run as a local application node or as multiple nodes sharing system databases and asset storage. Different kinds of state use different coordination mechanisms; enabling cloud storage does not make every object or session shared.

## Local setup and deployment

| Goal | Starting point |
| --- | --- |
| Run a local platform or try two-node synchronization | [Local Docker setup](docker_deployment.md) |
| Run local backend changes | [Monolith Docker quick start](https://github.com/SEMOSS/Monolith#quick-start-with-docker) |
| Add local databases, vectors, storage, or functions | [Supporting engine examples](../../docker-compose-examples/engines/README.md) |
| Configure Kubernetes, infrastructure, or semoss-artifacts properties | [SEMOSS-deployment](https://github.com/SEMOSS/SEMOSS-deployment) |

## State and coordination

| Concern | Mechanism |
| --- | --- |
| Security, catalog, scheduling, and other platform metadata | Configured system databases |
| Rooms, workspace configuration, runs, and pending actions | Model-inference database |
| Engine, project, skill, and room files | Local working copies plus configured central storage |
| Notifications that shared assets changed | Redis or ZooKeeper implementation of `IClusterSynchronizer` |
| Room-message hot cache and coordination | Optional Redis services around the durable room projection |
| Cross-node agent room turn lease | Redis-backed `ClusterRoomTurnLock` when enabled |
| Live agent events | Process-local buffers |
| Active frames, language workers, and request credentials | Execution-node/session state |

`ROOM.MESSAGES` remains authoritative for conversation history. Redis can accelerate and coordinate room operations; it does not replace the durable room/run databases. ZooKeeper asset synchronization does not supply the Redis agent turn lease.

## Main components

- [ClusterUtil](../../src/prerna/cluster/util/ClusterUtil.java): application-facing push/pull helpers and cloud-mode behavior.
- [CentralCloudStorage](../../src/prerna/cluster/util/clients/CentralCloudStorage.java): storage-backed synchronization of asset files.
- [ClusterSynchronizerFactory](../../src/prerna/cluster/sync/impl/ClusterSynchronizerFactory.java): selects Redis or ZooKeeper for asset coordination.
- [RoomMessageStore](../../src/prerna/engine/impl/model/RoomMessageStore.java): durable conversation projection and optional Redis integration.
- [ClusterRoomTurnLock](../../src/prerna/reactor/agent/run/ClusterRoomTurnLock.java): renewable per-room agent turn lease.

## Deployment implications

Use compatible application images and the same intended system databases, storage namespace, and coordination configuration across nodes. Configure distinct node identities where required by the examples.

Plan routing around HTTP/session state, live agent event buffers, and the executing node's live credentials. Durable run records support inspection and specific continuation paths; they are not a blanket guarantee that arbitrary active user work survives a node loss or resumes elsewhere.

For agents and skills, deploy platform project descriptors and asset folders as well as Java code. Ensure model-inference storage is initialized before expecting system agent workspace seeding to succeed.

## Detailed guides

- [Local Docker setup](docker_deployment.md)
- [Local Docker configuration](../deployment/docker_configuration.md)
- [Central asset storage](central_cloud_storage.md)
- [Redis and ZooKeeper synchronization](cluster_synchronization.md)
- [Agent durability and streaming](../agents/agent_runs.md#durability-and-cluster-behavior)
- [CI/CD workflows](github_actions_workflows.md)
