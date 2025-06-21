# `STORAGE` Engines

Storage engines in SEMOSS provide an abstraction layer for interacting with various file systems and object storage solutions. They are used for managing assets, reading/writing files, and supporting features that require persistent storage beyond traditional databases. These engines typically extend `prerna.engine.impl.storage.AbstractStorageEngine` and implement `prerna.engine.api.IStorageEngine`.

## Core Concepts for Storage Engines

### `prerna.engine.api.IStorageEngine` Interface

This interface defines the standard operations for storage engines:

*   `list(String path)`: Lists assets (files and/or folders) at a given storage path. Returns a list of asset names or paths.
*   `listDetails(String path)`: Provides a more detailed listing of assets, typically including names, sizes, types (file/folder), and last modified timestamps. Returns a list of maps, where each map represents an asset.
*   `syncLocalToStorage(String localPath, String storagePath, Map<String, Object> metadata)`: Synchronizes a local directory or file to a specified path in the storage engine. `metadata` can provide additional context.
*   `syncStorageToLocal(String storagePath, String localPath)`: Synchronizes an asset or directory from the storage engine to the local file system.
*   `copyToStorage(String localFilePath, String storageFolderPath, Map<String, Object> metadata)`: Copies a single local file to a specified folder path in the storage engine.
*   `copyToLocal(String storageFilePath, String localFolderPath)`: Copies a single file from the storage engine to a local folder.
*   `deleteFromStorage(String storagePath)` / `deleteFolderFromStorage(String storageFolderPath)`: Deletes a file or an entire folder (recursively) from the storage engine.
*   `getStorageType()`: Returns a `StorageTypeEnum` (e.g., `LOCAL_FS`, `AWS_S3`, `AZURE_BLOB_STORAGE`) indicating the type of storage system.
*   Other methods may include creating folders, checking existence, retrieving file streams, etc.

### `prerna.engine.impl.storage.AbstractStorageEngine` Class

This abstract class provides a basic foundation for storage engine implementations.

*   **Common Initialization**: Handles the initialization of common properties like `engineId`, `engineName`, and the `smssProp` (SMSS properties file). It also integrates with `SecretsFactory` to load any necessary credentials (e.g., API keys, access tokens) from a secure secret store if configured.
*   **Path Handling Utilities**: May include helper methods for normalizing paths or constructing storage-specific URIs, though much of this is left to concrete implementations.
*   **Core Logic**: Concrete implementations are responsible for implementing the actual logic for interacting with the specific storage system's API or file system commands.

### Extending for a New Storage System

To add support for a new storage system:

1.  **Implement `IStorageEngine`**.
2.  **Extend `AbstractStorageEngine`**: This provides the basic SMSS property handling and secret integration.
3.  **Implement Core Methods**: Provide concrete implementations for all methods defined in `IStorageEngine` (e.g., `list`, `copyToStorage`, `copyToLocal`, `deleteFromStorage`). This will involve using the target storage system's Java SDK or REST API client.
4.  **Authentication and Credentials**:
    *   Securely manage authentication. Read necessary credentials (API keys, connection strings, tokens) from the SMSS properties. These properties can be backed by SEMOSS's secret store for enhanced security.
    *   Implement any necessary client initialization or authentication handshake logic in the `open(Properties smssProp)` method.
5.  **Path Conventions**: Handle differences in path conventions between local file systems and the target object storage system (e.g., object keys vs. file paths, use of delimiters).
6.  **Error Handling**: Implement robust error handling for API calls, network issues, and permission problems.
7.  **SMSS Properties**: Define the necessary SMSS properties for your engine (e.g., endpoint URL, bucket name, API key names).
8.  **Define `StorageTypeEnum`**: Add a new value to `prerna.engine.api.StorageTypeEnum` for your new storage type.

## Example Implementations

### `prerna.engine.impl.storage.LocalFileSystemStorageEngine`
*   **Purpose**: Allows SEMOSS to interact with the local file system of the server on which it is running. This is often used for default asset storage, temporary file operations, or accessing locally mounted network drives.
*   **Implementation Highlights**:
    *   Uses standard Java `java.io.File` and `java.nio.file.Files` APIs for all operations (listing, reading, writing, deleting files and directories).
    *   Path resolution is typically relative to a configured root directory or absolute paths if permitted.
*   **SMSS Configuration**:
    *   May include a `ROOT_DIR` property to define a base directory, sandboxing file access to within this path for security. If not specified, it might operate with broader access, depending on the SEMOSS server's file permissions.

### `prerna.engine.impl.storage.S3StorageEngine`
*   **Purpose**: Connects to Amazon S3 (Simple Storage Service) for scalable object storage in the AWS cloud.
*   **Implementation Highlights**:
    *   Utilizes the AWS SDK for Java to interact with S3.
    *   Implements methods for listing S3 objects (files) and prefixes (folders), uploading files (put object), downloading files (get object), and deleting objects.
    *   Handles S3-specific concepts like bucket names and object keys.
*   **SMSS Configuration**:
    *   `S3_BUCKET`: The name of the S3 bucket.
    *   `AWS_ACCESS_KEY`, `AWS_SECRET_KEY`: AWS credentials (though using IAM roles via instance profiles is often preferred for security in EC2 environments, which the SDK can handle automatically).
    *   `AWS_REGION`: The AWS region where the bucket resides.
    *   `AWS_SESSION_TOKEN` (optional): For temporary credentials.
```
