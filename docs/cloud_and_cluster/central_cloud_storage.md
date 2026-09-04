# Centralized Cloud Storage for SEMOSS Assets

In a clustered or cloud-based SEMOSS deployment, a centralized storage mechanism is essential for managing shared assets like engines, projects, and insights. This ensures that all SEMOSS instances in the cluster operate on a consistent set of configurations and data. The `prerna.cluster.util.clients.CentralCloudStorage` class is the key Java component responsible for this functionality.

## Purpose and Design

The `CentralCloudStorage` class acts as an abstraction layer over various cloud storage services (and can also use a local file system for testing or specific setups). Its primary purposes are:

*   **Central Repository**: To provide a single, shared location where all critical SEMOSS assets (engine configurations, project definitions, insight data, user workspaces) are stored.
*   **Synchronization**: To enable individual SEMOSS instances in a cluster to push their local changes to this central store and pull updates from it, ensuring consistency across the cluster.
*   **Storage Abstraction**: To hide the specifics of the underlying storage provider (e.g., AWS S3, Azure Blob Storage, Google Cloud Storage, MinIO, or local disk) from the rest of the SEMOSS application.

## Key Component: `prerna.cluster.util.clients.CentralCloudStorage.java`

This class is implemented as a singleton and provides the core logic for interacting with the configured central storage.

### Initialization and Configuration

*   **Singleton Access**: Accessed via `CentralCloudStorage.getInstance()`.
*   **Storage Engine Backend**:
    *   Internally, `CentralCloudStorage` uses an instance of `prerna.engine.impl.storage.AbstractRCloneStorageEngine`. The specific implementation (e.g., `S3StorageEngine`, `AzureBlobStorageEngine`, `LocalFileSystemStorageEngine`) is determined at startup based on global SEMOSS configuration properties (likely from `RDF_Map.prop` or environment variables, managed via `prerna.cluster.util.ClusterUtil.STORAGE_PROVIDER`).
    *   Credentials and connection details for the chosen storage provider (e.g., API keys, bucket/container names, region, endpoints) are also loaded from these global configurations, potentially leveraging SEMOSS's secret management capabilities.
*   **Container/Blob Naming Conventions**:
    *   `CentralCloudStorage` defines standardized root "folder" or "container" names within the storage backend for different types of assets:
        *   `DATABASE_BLOB` (e.g., "semoss-db" or "db-") for database engine assets.
        *   `PROJECT_BLOB` (e.g., "semoss-project" or "project-") for project assets.
        *   `STORAGE_BLOB`, `MODEL_BLOB`, `VECTOR_BLOB`, `FUNCTION_BLOB`, `GUARDRAIL_BLOB`, `VENV_BLOB`, `USER_BLOB` for other asset types.
        *   Separate containers for associated images (e.g., `DB_IMAGES_BLOB`).
    *   The exact prefixing might vary slightly depending on the storage provider (e.g., Azure has different naming constraints).

### Core Functionalities

`CentralCloudStorage` provides methods to synchronize various types of SEMOSS assets:

*   **Engines (`IEngine.CATALOG_TYPE`)**:
    *   `pushEngine(String engineId)`:
        *   Takes an `engineId`.
        *   Retrieves the local engine's folder (e.g., `SEMOSS_HOME/db/<EngineName>__<engineId>/`) and its `.smss` file.
        *   Database engines are temporarily closed and removed from `DIHelper`'s cache before their folders are uploaded. Other engine types are closed when `holdsFileLocks()` is `true`.
        *   Syncs the entire local engine folder to a corresponding path in the central storage (e.g., `<DB_CONTAINER_PREFIX>/<engineId>/`).
        *   Copies the local `.smss` file to a separate location in central storage (e.g., `<DB_CONTAINER_PREFIX>/<engineId>-smss/`).
        *   Reopens an engine that was closed for synchronization, including when the transfer fails.
        *   Uses `EngineSyncUtility.getEngineLock(engineId)` to ensure exclusive access during the operation.
    *   `pullEngine(String engineId, IEngine.CATALOG_TYPE engineType, boolean engineAlreadyLoaded)`:
        *   Downloads the engine's folder and `.smss` file from the central store to the corresponding local SEMOSS directories.
        *   If `engineAlreadyLoaded` is `false` (meaning it's a new engine for this instance), it calls the appropriate `SMSSWatcher` (e.g., `SMSSWebWatcher.catalogEngine(...)`) to register the engine locally.
        *   Also uses `EngineSyncUtility` for locking.
    *   `pushEngineSmss(String engineId, ...)` / `pullEngineSmss(String engineId, ...)`: For synchronizing only the `.smss` configuration file of an engine.
    *   `deleteEngine(String engineId, IEngine.CATALOG_TYPE engineType)`: Deletes the engine's folder and SMSS file from the central storage.
    *   `copyLocalFileToEngineCloudFolder(...)`, `copyEngineCloudFileToLocalFile(...)`, `deleteEngineCloudFile(...)`: Methods for syncing individual files or subfolders within an engine's asset directory between local and cloud storage.

*   **Projects (`IProject`)**:
    *   `pushProject(String projectId)` / `pullProject(String projectId, boolean projectAlreadyLoaded)`: Similar logic to engine synchronization, but for project assets (project folder in `SEMOSS_HOME/project/` and project `.smss` file). Uses `ProjectSyncUtility.getProjectLock()`.
    *   `pushProjectSmss(...)` / `pullProjectSmss(...)`: For project SMSS file synchronization.
    *   `deleteProject(String projectId)`: Deletes project assets from central storage.

*   **Insights and Project-Specific Files**:
    *   `pushInsight(String projectId, String insightId)` / `pullInsight(String projectId, String insightId)`: Syncs the specific insight's folder (located within the project's version folder) to/from central storage.
    *   `pushInsightImage(...)`: Manages synchronization of insight thumbnail images.
    *   `pushInsightDB(String projectId)` / `pullInsightDB(String projectId)`: Specifically syncs the `insights_database.mv.db` (or `.sqlite`) file for a project.
    *   `pushProjectFolder(...)` / `pullProjectFolder(...)`: Generic methods to sync arbitrary subfolders within a project's assets (e.g., `assets/`, `version/`).

*   **User Assets/Workspaces**:
    *   `pushUserAssetOrWorkspace(...)` / `pullUserAssetOrWorkspace(...)`: Handles synchronization for user-specific assets or workspace projects stored under the `USER_BLOB` container.

### Synchronization and Locking

*   To prevent race conditions and ensure data integrity in a multi-instance environment, `CentralCloudStorage` uses `java.util.concurrent.locks.ReentrantLock`.
*   `EngineSyncUtility.getEngineLock(engineId)` and `ProjectSyncUtility.getProjectLock(projectId)` provide static methods to obtain a lock specific to an engine ID or project ID. This ensures that operations on the same asset are serialized across different threads or even different SEMOSS instances (if the lock mechanism is cluster-aware, though its direct cluster-awareness isn't detailed in this class itself; ZooKeeper likely handles distributed locking aspects).

#### Database audit files

Every database engine can own a local modification-audit database in its engine folder, such as `audit_log_database.mv.db`. This remains true for an external database whose primary data and connection are remote. An external database can therefore return `false` from `holdsFileLocks()` while its local H2 audit database still has an active background writer.

`pushEngine` closes all database engines before synchronizing their folders. Closing the engine flushes and closes the local audit database so the storage provider reads a stable file. Uploading a live H2 MVStore file is unsafe: compaction can shorten the file after an uploader records its original length, which can cause a zero-progress chunked upload and block other storage requests. The engine is reloaded in the `finally` path so both successful and failed transfers restore normal availability.

### Interaction with Cluster Synchronizer (ZooKeeper)

*   The constructor of `CentralCloudStorage` conditionally initializes `ClusterSynchronizer.getInstance()` if `ClusterUtil.IS_CLUSTER_ZK` is true.
*   This implies that after `CentralCloudStorage` performs an operation (like `pushEngine`), it would typically trigger a notification through the `ClusterSynchronizer` (which uses ZooKeeper) to inform other SEMOSS instances in the cluster about the change.
*   Other instances, upon receiving this notification, might then use `CentralCloudStorage` to `pull` the updated asset, ensuring their local caches and configurations are refreshed.

By leveraging an underlying `IStorageEngine` and coordinating with cluster synchronization mechanisms, `CentralCloudStorage` provides the foundation for managing shared SEMOSS assets in a distributed, cloud-native deployment.
