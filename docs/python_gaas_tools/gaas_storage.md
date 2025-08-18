# GAAS Storage Interaction (`gaas_gpt_storage.py`)

The `py/gaas_gpt_storage.py` module provides the `StorageEngine` class, which serves as a Python proxy for interacting with SEMOSS storage engines. This enables Python-based Generative AI Agent Services (GAAS) or other Python scripts to perform file and directory operations on storage systems managed by the SEMOSS backend (e.g., local file system, AWS S3, Google Cloud Storage, Azure Blob Storage via RClone).

## `StorageEngine` Class

*   **Purpose**: The `StorageEngine` class allows Python code to interact with a specific SEMOSS `STORAGE` engine. It abstracts the communication details (via `ServerProxy`) needed to send commands to the Java backend where the actual storage operations occur.
*   **Inheritance**: It extends `gaas_server_proxy.ServerProxy`, which handles the low-level communication with the SEMOSS Java backend.

### Initialization

The constructor `__init__(self, engine_id=str, insight_id=None)`:
*   `engine_id` (str): **Required**. The ID of the target SEMOSS `STORAGE` engine (an `IStorageEngine` instance) configured in the Java backend.
*   `insight_id` (Optional[str]): The ID of the current insight. This can be used for context, especially when `space` parameter in methods is not defined, defaulting operations to the insight's space.
*   The constructor asserts that `engine_id` is provided and prints an initialization message.

### Key Methods and Functionality

All methods that interact with the storage engine translate their operations into Pixel scripts, which are then executed on the SEMOSS Java backend via `super().callReactor()`.

*   **`list(self, storagePath: str = None, insight_id: Optional[str] = None)`**:
    *   **Purpose**: Lists files and folders at a given `storagePath` within the configured storage engine.
    *   **Pixel Command**: `Storage("<engine_id>")|ListStoragePath(storagePath="<storagePath>");`
    *   **Output**: Returns the output from the Pixel execution, typically a list of file/folder names or paths.

*   **`listDetails(self, storagePath: str = None, insight_id: Optional[str] = None)`**:
    *   **Purpose**: Provides a more detailed listing of files and folders at `storagePath`, including metadata like size, type, and modification date.
    *   **Pixel Command**: `Storage("<engine_id>")|ListStoragePathDetails(storagePath="<storagePath>");`
    *   **Output**: Returns the output from the Pixel execution, usually a list of maps where each map contains details for an asset.

*   **`syncLocalToStorage(self, storagePath: str = None, localPath: str = None, space: Optional[str] = None, metadata: Optional[Dict] = {}, insight_id: Optional[str] = None)`**:
    *   **Purpose**: Synchronizes a local path (within SEMOSS's application context like an insight, project, or user space) to a path in the storage engine.
    *   **Inputs**:
        *   `storagePath`: Target path in the storage engine.
        *   `localPath`: Source path in the application space.
        *   `space` (Optional[str]): Defines the application space for `localPath` (e.g., project ID, "user", or defaults to current insight if `None`).
        *   `metadata` (Optional[Dict]): Custom metadata for the synced files (if supported by the storage backend).
    *   **Pixel Command**: `Storage("<engine_id>")|SyncLocalToStorage(storagePath="<storagePath>",filePath="<localPath>",space="<space>",metadata=[<metadata_json>]);`
    *   **Output**: Boolean indicating success/failure.

*   **`syncStorageToLocal(self, storagePath: str = None, localPath: str = None, space: Optional[str] = None, insight_id: Optional[str] = None)`**:
    *   **Purpose**: Synchronizes a path from the storage engine to a local application path.
    *   **Pixel Command**: `Storage("<engine_id>")|SyncStorageToLocal(storagePath="<storagePath>",filePath="<localPath>",space="<space>");`
    *   **Output**: Boolean indicating success/failure.

*   **`copyToLocal(self, storagePath: str = None, localPath: str = None, space: Optional[str] = None, insight_id: Optional[str] = None)`**:
    *   **Purpose**: Copies a file from the storage engine (`storagePath`) to a local application folder (`localPath`). (Pixel command uses `PullFromStorage`).
    *   **Pixel Command**: `Storage("<engine_id>")|PullFromStorage(storagePath="<storagePath>",filePath="<localPath>",space="<space>");`
    *   **Output**: Boolean indicating success/failure.

*   **`copyToStorage(self, storagePath: str = None, localPath: str = None, space: Optional[str] = None, metadata: Optional[Dict] = {}, insight_id: Optional[str] = None)`**:
    *   **Purpose**: Copies a local application file (`localPath`) to a path in the storage engine (`storagePath`). (Pixel command uses `PushToStorage`).
    *   **Pixel Command**: `Storage("<engine_id>")|PushToStorage(storagePath="<storagePath>",filePath="<localPath>",space="<space>",metadata=[<metadata_json>]);`
    *   **Output**: Boolean indicating success/failure.

*   **`deleteFromStorage(self, storagePath: str = None, leaveFolderStructure: bool = False, insight_id: Optional[str] = None)`**:
    *   **Purpose**: Deletes a file or folder from the storage engine.
    *   **Inputs**:
        *   `storagePath`: The path to delete in the storage engine.
        *   `leaveFolderStructure` (bool): If `True`, attempts to maintain parent folder structure (behavior depends on backend implementation).
    *   **Pixel Command**: `Storage("<engine_id>")|DeleteFromStorage(storagePath="<storagePath>",leaveFolderStructure=<leaveFolderStructureStr>);`
    *   **Output**: Boolean indicating success/failure.

*   **`to_langchain_storage(self)`**:
    *   **Purpose**: Transforms the `StorageEngine` instance into a Langchain `BaseStore`-compatible object. This allows the SEMOSS storage engine to be used within Langchain workflows that require a key-value store interface (though the fit might be partial as `BaseStore` is more generic).
    *   **Core Logic**:
        1.  Defines an inner class `SemossLangchainStorage` that inherits from `langchain_core.stores.BaseStore`.
        2.  The inner class constructor takes the `StorageEngine` instance.
        3.  It implements methods like `list`, `listDetails`, `syncLocalToStorage`, `syncStorageToLocal`, `copyToLocal`, `deleteFromStorage` by calling the corresponding methods of the outer `StorageEngine` instance.
        4.  Stubbed methods (`mdelete`, `mget`, `mset`, `yield_keys`) are present to fulfill the `BaseStore` interface but are not implemented.
    *   **Outputs**: An instance of `SemossLangchainStorage`.

### Interaction with SEMOSS `STORAGE` Engines

The `StorageEngine` Python class is a client to a specific SEMOSS `IStorageEngine` (e.g., `LocalFileSystemStorageEngine`, `S3StorageEngine`) running on the Java backend. All operations are translated into Pixel scripts and executed via the `ServerProxy`, meaning the Java engine handles the actual interaction with the file system or cloud storage service.

### Error Handling

*   Methods include `assert` statements for required parameters.
*   Errors from the Pixel execution or backend storage operations would typically be propagated via the `pixelReturn` object, potentially requiring the calling code to inspect this return for success or failure indicators.

### Example Usage (Conceptual)

```python
# Assuming gaas_server_proxy is configured and SEMOSS backend is running
# And a STORAGE engine with ID "my_s3_storage" is configured in SEMOSS

storage_engine_id = "my_s3_storage"
insight_id_context = "some_active_insight_id" # Optional

# Initialize the StorageEngine client
storage_tool = StorageEngine(engine_id=storage_engine_id, insight_id=insight_id_context)

# List items in a storage path
try:
    items = storage_tool.list(storagePath="my-folder/data/")
    if items:
        print("Items in storage path:", items)
except Exception as e:
    print(f"Error listing items: {e}")

# Copy a local file (relative to insight/project/user space) to storage
try:
    # Assuming 'my_file.txt' exists in the root of the current insight's space
    success = storage_tool.copyToStorage(localPath="my_file.txt", storagePath="my-folder/destination/")
    if success:
        print("File copied to storage successfully.")
    else:
        print("File copy to storage failed.")
except Exception as e:
    print(f"Error copying to storage: {e}")
```

This `StorageEngine` class enables Python GAAS components to manage and interact with files across various storage backends configured within SEMOSS.
