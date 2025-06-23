# GAAS Vector Database Interaction (`gaas_gpt_vector.py`)

The `py/gaas_gpt_vector.py` module provides the `VectorEngine` class, which serves as a Python proxy for interacting with SEMOSS `VECTOR` engines. This allows Python-based Generative AI Agent Services (GAAS) or other Python scripts to add documents, perform similarity searches, and manage data within vector databases configured in the SEMOSS backend.

## `VectorEngine` Class

*   **Purpose**: The `VectorEngine` class enables Python code to interact with a specific SEMOSS `VECTOR` engine (an `IVectorDatabaseEngine` instance). It abstracts the communication details (via `ServerProxy`) needed to send commands to the Java backend, where the actual interaction with the vector database (e.g., FAISS, Chroma, Pinecone) and the embedding model occurs.
*   **Inheritance**: It extends `gaas_server_proxy.ServerProxy`, which handles the low-level communication with the SEMOSS Java backend.
*   **`engine_type` (Class Attribute)**: Set to `"VECTOR"`.

### Initialization

The constructor `__init__(self, engine_id: str, insight_id: Optional[str] = None, insight_folder: Optional[str] = None)`:
*   `engine_id` (str): **Required**. The ID of the target SEMOSS `VECTOR` engine configured in the Java backend.
*   `insight_id` (Optional[str]): The ID of the current insight, used for context in backend operations.
*   `insight_folder` (Optional[str]): Described as "no longer used".
*   The constructor asserts that `engine_id` is provided and prints an initialization message.

### Key Methods and Functionality

All methods that interact with the vector engine translate their operations into Pixel scripts, which are then executed on the SEMOSS Java backend via `super().callReactor()`.

*   **`addDocument(self, file_paths: List[str], space: Optional[str] = None, param_dict: Optional[Dict] = {}, insight_id: Optional[str] = None) -> bool`**:
    *   **Purpose**: Ingests one or more source documents into the vector database. The backend SEMOSS `VECTOR` engine handles text extraction, chunking, embedding generation (using its configured embedder model), and storage.
    *   **Inputs**:
        *   `file_paths` (List[str]): A list of paths to the documents to be added. These paths are typically relative to a space defined by `insight_id` or `space`.
        *   `space` (Optional[str]): Specifies the context for `file_paths` (e.g., a project ID for project-level files, "user" for user-space files, or defaults to the current insight if `None`).
        *   `param_dict` (Optional[Dict]): Additional parameters for the backend operation (e.g., specifying an `index_class` for FAISS).
    *   **Pixel Command**: `CreateEmbeddingsFromDocuments(engine="<engine_id>",filePaths=<file_paths_list>,space=['<space>'],paramValues=[<param_dict_json>]);`
    *   **Output**: Boolean indicating success/failure of the operation.

*   **`addVectorCSVFile(self, file_paths: List[str], space: Optional[str] = None, param_dict: Optional[Dict] = {}, insight_id: Optional[str] = None) -> bool`**:
    *   **Purpose**: Adds documents that are already in the `VectorDatabaseCSVTable` format (pre-processed text chunks, potentially with pre-computed embeddings) to the vector database.
    *   **Pixel Command**: `CreateEmbeddingsFromVectorCSVFile(engine="<engine_id>",filePaths=<file_paths_list>,space=['<space>'],paramValues=[<param_dict_json>]);`
    *   **Output**: Boolean indicating success/failure.

*   **`removeDocument(self, file_names: List[str], param_dict: Optional[Dict] = {}, insight_id: Optional[str] = None) -> bool`**:
    *   **Purpose**: Removes specified documents (and their associated embeddings) from the vector database.
    *   **Inputs**:
        *   `file_names` (List[str]): A list of document names (or identifiers used during ingestion) to remove.
    *   **Pixel Command**: `RemoveDocumentFromVectorDatabase(engine="<engine_id>",fileNames=<file_names_list>,paramValues=[<param_dict_json>]);`
    *   **Output**: Boolean indicating success/failure.

*   **`nearestNeighbor(self, search_statement: str, limit: Optional[int] = 5, filters: Optional[Union[Dict, str]] = None, filters_str: Optional[str] = None, metafilters: Optional[Union[Dict, str]] = None, metafilters_str: Optional[str] = None, param_dict: Optional[Dict] = {}, insight_id: Optional[str] = None) -> List[Dict]`**:
    *   **Purpose**: Performs a similarity search in the vector database.
    *   **Inputs**:
        *   `search_statement` (str): The query text to search for.
        *   `limit` (Optional[int], default: 5): The maximum number of results to return.
        *   `filters` (Optional[Union[Dict, str]]): Metadata filters to apply during the search. Can be a dictionary (e.g., `{"Author": "Jane Doe"}`) or a pre-formatted filter string.
        *   `filters_str` (Optional[str]): Alternative way to pass pre-formatted filter string.
        *   `metafilters`, `metafilters_str`: Similar to `filters` but potentially for a different type of metadata or structured query (usage might depend on backend implementation).
    *   **Pixel Command**: `VectorDatabaseQuery(engine="<engine_id>",command=["<encoded_search_statement>"],limit=[<limit>],filters=[<filters_str>],metaFilters=[<metafilters_str>],paramValues=[<param_dict_json>]);`
        *   The method includes logic to format the `filters` and `metafilters` dictionaries into the string representation expected by the Pixel command.
    *   **Output**: A list of dictionaries, where each dictionary represents a search result and typically includes the content chunk, metadata, and similarity score.

*   **`listDocuments(self, param_dict: Optional[Dict] = {}, insight_id: Optional[str] = None) -> List[Dict]`**:
    *   **Purpose**: Lists the source documents that have been indexed in the vector database.
    *   **Pixel Command**: `ListDocumentsInVectorDatabase(engine="<engine_id>",paramValues=[<param_dict_json>]);`
    *   **Output**: A list of dictionaries, each representing an indexed document and its metadata.

*   **`to_langchain_vector_store(self)`**:
    *   **Purpose**: Transforms the `VectorEngine` instance into a Langchain `BaseRetriever`-compatible object. This allows the SEMOSS vector engine to be used within Langchain workflows.
    *   **Core Logic**:
        1.  Defines an inner class `SemossLangchainVector` that inherits from `langchain_core.retrievers.BaseRetriever`.
        2.  The inner class methods (`addDocs`, `removeDocs`, `similaritySearch`, `listDocs`) call the corresponding methods of the outer `VectorEngine` instance.
        3.  The `similaritySearch` method maps its results to Langchain `Document` objects.
        4.  The `_get_relevant_documents` method (required by `BaseRetriever`) calls `similaritySearch`.
    *   **Outputs**: An instance of `SemossLangchainVector`.

### Interaction with SEMOSS `VECTOR` Engines

*   The `VectorEngine` Python class is a client to a specific SEMOSS `IVectorDatabaseEngine` (e.g., `FaissDatabaseEngine`, `ChromaVectorDatabaseEngine`) running on the Java backend.
*   All operations, including document ingestion (which involves text processing and embedding generation by the backend engine) and similarity searches, are delegated to the Java engine via Pixel commands.
*   The Python class itself does not perform embedding or direct interaction with the underlying vector store technology; it relies on the backend SEMOSS engine to do so.

### Error Handling

*   Methods include `assert` statements for required parameters like `engine_id`, `file_paths`, and `search_statement`.
*   Errors from Pixel execution or backend vector engine operations would typically be propagated via the `pixelReturn` object.

### Example Usage (Conceptual)

```python
# Assuming gaas_server_proxy is configured and SEMOSS backend is running
# And a VECTOR engine with ID "my_document_kb" is configured in SEMOSS,
# which in turn is configured with an EMBEDDER_ENGINE_ID.

vector_engine_id = "my_document_kb"
insight_id_context = "some_active_insight_id" # Optional

# Initialize the VectorEngine client
vector_tool = VectorEngine(engine_id=vector_engine_id, insight_id=insight_id_context)

# Add documents to the vector store
# (These paths are relative to the insight/project/user space defined by insight_id or space param)
# try:
#     success = vector_tool.addDocument(file_paths=["docs/my_doc1.pdf", "notes/meeting_notes.txt"])
#     if success:
#         print("Documents added successfully.")
# except Exception as e:
#     print(f"Error adding documents: {e}")

# Perform a similarity search
try:
    query_text = "information about project alpha"
    search_results = vector_tool.nearestNeighbor(search_statement=query_text, limit=3)
    if search_results:
        print(f"Search results for '{query_text}':")
        for res in search_results:
            print(f"  Content: {res.get('Content')[:100]}...") # Print first 100 chars
            print(f"  Source: {res.get('SourceFile')}")
            print(f"  Score: {res.get('vector_score')}") # Score key might vary
            print("-" * 20)
except Exception as e:
    print(f"Error performing search: {e}")
```

This `VectorEngine` class provides a Python interface for GAAS components to leverage the powerful document indexing and semantic search capabilities of SEMOSS `VECTOR` engines.
