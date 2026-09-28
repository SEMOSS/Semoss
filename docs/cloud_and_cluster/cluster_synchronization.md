# Redis and ZooKeeper Cluster Synchronization

Asset synchronization lets SEMOSS nodes refresh local copies when engines, projects, or other shared assets change. The current abstraction is [IClusterSynchronizer](../../src/prerna/cluster/sync/IClusterSynchronizer.java), with Redis and ZooKeeper implementations.

## Select a backend

| Setting | Purpose |
| --- | --- |
| `SEMOSS_IS_CLUSTER` | Enables cloud/shared-asset behavior in the configured deployment |
| `SEMOSS_IS_CLUSTER_REDIS` | Selects Redis asset synchronization |
| `SEMOSS_IS_CLUSTER_ZK` | Selects ZooKeeper asset synchronization |
| Storage-provider settings | Select the central storage backend and connection details |
| `HOST_IP` in the Compose examples | Gives each application node a distinct advertised identity |

Configure one asset coordination backend. [ClusterSynchronizerFactory](../../src/prerna/cluster/sync/impl/ClusterSynchronizerFactory.java) chooses Redis first if both flags are enabled and logs a warning; relying on that precedence makes deployment intent unclear.

Use the exact settings in the [Redis Compose example](../../docker-compose-examples/semoss-with-postgres-minio-redis.yml) or [ZooKeeper Compose example](../../docker-compose-examples/semoss-with-postgres-minio-zk.yml). A single node can use shared object storage without either synchronization backend.

## Implementations

| Component | Responsibility |
| --- | --- |
| [ClusterUtil](../../src/prerna/cluster/util/ClusterUtil.java) | Entry points used by core operations to synchronize assets |
| [CentralCloudStorage](../../src/prerna/cluster/util/clients/CentralCloudStorage.java) | Transfer files to/from the configured storage backend |
| [IClusterSynchronizer](../../src/prerna/cluster/sync/IClusterSynchronizer.java) | Common coordination contract |
| [RedisClusterSynchronizer](../../src/prerna/cluster/sync/impl/RedisClusterSynchronizer.java) | Redis-backed notifications and coordination |
| [ZKClusterSynchronizer](../../src/prerna/cluster/sync/impl/ZKClusterSynchronizer.java) | ZooKeeper/Curator-backed notifications and coordination |

At a high level, a node updates its working copy, pushes the relevant assets, and publishes a change. Other nodes process the notification and refresh applicable loaded resources. The synchronizer carries coordination information; asset contents come from central storage.

The exact push/pull operation depends on the resource and mutation. Consult its `ClusterUtil` call and synchronizer implementation instead of assuming every filesystem write automatically publishes a change.

## Distinguish agent and room coordination

Redis also serves separate room/agent features:

- `RoomMessageStore` can use Redis as a hot projection and coordination layer while persisting conversation history in `ROOM.MESSAGES`.
- `ClusterRoomTurnLock` uses Redis for a renewable per-room agent lease when `AGENT_RUN_QUEUE_ENABLED` is enabled. The default follows Redis availability; an explicit opt-in still requires Redis.
- `AGENT_RUN_ACTIVE_TTL_MS` controls that lease's lifetime, not an agent's execution budget.

ZooKeeper asset synchronization alone does not enable these Redis services. Neither backend turns the process-local agent event buffer into a durable event log. See [agent runs](../agents/agent_runs.md).

## Verification

Run one of the two-node examples and check both nodes' startup logs for the intended backend and unique identities. Perform a supported resource update, confirm its central-storage copy, and verify that the other node refreshes the corresponding resource. Separately test room turns and live polling if the deployment uses agents across nodes.

For complete infrastructure examples, see [SEMOSS-deployment](https://github.com/SEMOSS/SEMOSS-deployment). For asset layout and transfer responsibilities, see [central storage](central_cloud_storage.md).
