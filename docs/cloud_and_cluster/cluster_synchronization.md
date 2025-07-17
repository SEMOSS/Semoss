# SEMOSS Cluster Synchronization with ZooKeeper

When SEMOSS is deployed in a clustered environment (multiple instances running concurrently), a mechanism is needed to ensure that all instances have a consistent view of shared assets (like engines and projects) and can react to changes made by other instances. SEMOSS uses Apache ZooKeeper for this coordination, facilitated by Java classes primarily within the `prerna.cluster.util` package.

## Overview of Synchronization

The general synchronization workflow is as follows:

1.  **Shared Asset Storage**: All shared assets (engine configurations, project files, insight data, etc.) are stored in a centralized location accessible to all cluster nodes, managed by `CentralCloudStorage` (see `central_cloud_storage.md`).
2.  **Change Notification**: When a SEMOSS instance modifies a shared asset and pushes it to `CentralCloudStorage` (e.g., a user creates a new engine or updates a project), it also publishes a notification to a specific path (ZNode) in ZooKeeper.
3.  **Listening for Changes**: Other SEMOSS instances in the cluster listen for changes on these ZooKeeper ZNodes.
4.  **Pulling Updates**: Upon receiving a notification of a change from ZooKeeper (that wasn't initiated by itself), an instance will typically use `CentralCloudStorage` to pull the updated asset from the central repository, thereby refreshing its local state or cache.

## Key Java Components

### 1. `prerna.cluster.util.ClusterUtil`

This utility class plays a crucial role in managing cluster-related operations and configurations.

*   **Cluster Mode Detection**:
    *   `IS_CLUSTER` (static boolean): Determines if the current SEMOSS instance is operating in a clustered mode. This is typically configured via the `SEMOSS_IS_CLUSTER` environment variable or an equivalent property in `RDF_Map.prop`.
    *   `IS_CLUSTER_ZK` (static boolean): Determines if ZooKeeper is enabled for synchronization within the cluster. Configured via `SEMOSS_IS_CLUSTER_ZK`.
*   **Wrappers for Central Storage**: It provides static wrapper methods (e.g., `ClusterUtil.pushEngine()`, `ClusterUtil.pullProject()`) that internally call methods on `CentralCloudStorage.getInstance()`. These wrappers first check if `IS_CLUSTER` is true.
*   **Triggering Synchronization**: After a successful push operation to `CentralCloudStorage` (e.g., after `pushEngine(engineId)`), if `IS_CLUSTER_ZK` is true, `ClusterUtil` calls methods on `ClusterSynchronizer` (e.g., `getClusterSynchronizer().publishEngineChange(...)`) to notify other nodes.

### 2. `prerna.cluster.util.ClusterSynchronizer`

This singleton class is the direct interface to Apache ZooKeeper for cluster synchronization tasks.

*   **Initialization**:
    *   Connects to ZooKeeper using Apache Curator framework (`CuratorFramework`). The ZooKeeper connection string is typically configured via `AppCloudClientProperties` (reading `ZK_SERVER_STRING` from `RDF_Map.prop` or environment variables).
    *   Identifies the current SEMOSS instance with a unique `host` ID (either from `HOST_IP` configuration or a randomly generated ID).
    *   Ensures base ZNodes for synchronization exist in ZooKeeper (e.g., `/sync/project` and `/sync/engine`).
*   **Listening for Changes (`createCacheListener`)**:
    *   Sets up `CuratorCacheListener` instances to watch for updates on specific ZNode paths in ZooKeeper. SEMOSS creates listener paths for each synchronized project and engine (e.g., `/sync/project/{projectId}` and `/sync/engine/{engineId}`).
    *   When a `CHILD_UPDATED` event is detected on a watched ZNode:
        1.  The listener deserializes the data stored in the ZNode. This data is a `Map<String, Object>` containing:
            *   `nodeId`: The unique ID of the SEMOSS instance that published the change.
            *   `methodName`: The name of a static method in `ClusterUtil` (e.g., "pullEngine", "pullOwl") that listening nodes should execute to get the update.
            *   `params`: A list of parameters to pass to this method.
        2.  **Crucially, if the `nodeId` from the ZNode data is different from the current instance's `host` ID** (meaning the change originated from another instance):
            *   It checks if the specific project or engine ID (derived from the ZNode path) is currently loaded or relevant to this instance (using `projectLoaded()` or `engineLoaded()`, which check `DIHelper` properties).
            *   If relevant, it uses Java reflection to invoke the specified `methodName` on the `ClusterUtil` class with the provided `params`. This typically triggers a `pull` operation from `CentralCloudStorage` for the updated asset.
*   **Publishing Changes**:
    *   `publishEngineChange(String engineId, String methodName, Object... params)`:
        *   Called by `ClusterUtil` after an engine asset is successfully pushed to `CentralCloudStorage`.
        *   Creates a data map containing the current instance's `host` ID, the `methodName` (e.g., "pullEngine"), and the `params` (e.g., the `engineId`).
        *   Serializes this map and writes it to the ZNode `/sync/engine/{engineId}`. This action triggers the listeners on other cluster nodes.
    *   `publishProjectChange(String projectId, String methodName, Object... params)`:
        *   Similar functionality for project-related changes, writing to `/sync/project/{projectId}`.

## Synchronization Logic Summary

1.  **Instance A modifies an asset** (e.g., updates an engine's SMSS file).
2.  Instance A (via `ClusterUtil`) calls `CentralCloudStorage` to **push the updated asset** to the shared cloud storage.
3.  After successful push, Instance A (via `ClusterUtil`) calls `ClusterSynchronizer.publishEngineChange(...)`.
4.  `ClusterSynchronizer` on Instance A **writes a message to the relevant ZooKeeper ZNode** (e.g., `/sync/engine/<engine_id>`). The message includes Instance A's ID, the method "pullEngine", and the engine ID.
5.  **Instance B (and other cluster members) have listeners** on `/sync/engine/<engine_id>`.
6.  The listener on Instance B is triggered by the ZNode update.
7.  Instance B's listener sees that the change was made by Instance A (not itself).
8.  Instance B's listener checks if it cares about this engine (e.g., if it has it loaded).
9.  If yes, it dynamically calls `ClusterUtil.pullEngine(<engine_id>)` using the information from the ZNode.
10. Instance B (via `ClusterUtil` and `CentralCloudStorage`) **pulls the updated asset** from the shared cloud storage.

This ZooKeeper-based notification system, combined with `CentralCloudStorage`, allows SEMOSS instances in a cluster to maintain eventual consistency of shared project and engine assets, facilitating a distributed and scalable environment.

## Configuration

*   **`RDF_Map.prop` (or environment variables)**:
    *   `SEMOSS_IS_CLUSTER=true`: Enables cluster mode.
    *   `SEMOSS_IS_CLUSTER_ZK=true`: Enables ZooKeeper synchronization.
    *   `ZK_SERVER_STRING`: The connection string for the ZooKeeper ensemble (e.g., "zkhost1:2181,zkhost2:2181").
    *   `HOST_IP` (optional): A unique identifier for the current SEMOSS instance; if not set, a random ID is generated.
    *   `SEMOSS_STORAGE_PROVIDER` and related credentials for `CentralCloudStorage`.
```
