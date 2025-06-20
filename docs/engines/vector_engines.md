# `VECTOR` Engines

Vector engines in SEMOSS are designed to interface with vector databases or vector search libraries. These are essential for AI-powered features like semantic search, retrieval-augmented generation (RAG), and other applications that rely on finding similarities between text embeddings or other types of vector representations. They generally extend `prerna.engine.impl.vector.AbstractVectorDatabaseEngine` and implement `prerna.engine.api.IVectorDatabaseEngine`.

## Core Concepts for Vector Engines

### `prerna.engine.api.IVectorDatabaseEngine` Interface

This interface outlines the standard functionalities for vector database engines:

*   `addDocument(List<String> filePaths, Map<String, Object> parameters)`: A key method for ingesting source documents. This typically involves:
    1.  Optionally, extracting text from the files if they are not plain text (e.g., PDF, DOCX).
    2.  Chunking the extracted text into manageable segments.
    3.  Generating vector embeddings for each chunk using an associated `IModelEngine` (embedder).
    4.  Storing these embeddings along with the text chunks and any associated metadata into the vector database.
*   `addEmbeddings(String vectorCsvFile, Insight insight, Map<String, Object> parameters)`: Allows for bulk loading of pre-computed embeddings and their corresponding text/metadata, typically from a structured CSV file. Implementations also exist for `List<String>` of CSV files and `File` objects.
*   `addEmbedding(List<? extends Number> embedding, String source, String modality, String divider, String part, int tokens, String content, Map<String, Object> additionalMetadata)`: Adds a single pre-computed embedding with its detailed metadata.
*   `nearestNeighbor(Insight insight, String searchStatement, Number limit, Map<String, Object> parameters)`: The core search functionality. It takes a user's search query (text), generates its embedding using the configured embedder model, and then queries the vector database to find the `limit` most similar items (embeddings/chunks). Returns a list of results, often including the original text, metadata, and similarity scores. `parameters` can include filters for metadata.
*   `removeDocument(List<String> filePaths, Map<String, Object> parameters)`: Deletes specified documents and their associated embeddings from the vector database.
*   `listDocuments(Map<String, Object> parameters)`: Returns a list of the source documents that have been indexed in the vector database, often with metadata like file names and processing status.
*   `listAllRecords(Map<String, Object> parameters)`: Returns a list of all individual records or chunks stored in the vector database.
*   `getVectorDatabaseType()`: Returns a `prerna.engine.api.VectorDatabaseTypeEnum` (e.g., `FAISS`, `CHROMA`, `PINECONE`) identifying the specific vector store technology.
*   `userCanAccessEmbeddingModels(User user)`: Checks if the provided user has access to the underlying embedding models configured for this vector engine.

### `prerna.engine.impl.vector.AbstractVectorDatabaseEngine` Class

This abstract class provides a significant amount of common logic for vector database engine implementations:

*   **Embedder Integration**: Manages the configuration and use of an associated `IModelEngine` (specified by the `EMBEDDER_ENGINE_ID` property in the SMSS file) responsible for generating text embeddings. It calls this embedder engine when new documents are added or when a search statement needs to be vectorized.
*   **Text Processing Pipeline**:
    *   Often relies on a Python backend (via `prerna.ds.py.PyTranslator` and scripts like `py/vector_database.py`) for text extraction (from PDFs, etc.) and text chunking (splitting large texts into smaller, embeddable segments).
    *   Handles SMSS properties like `CONTENT_LENGTH` (max chunk size in tokens or characters), `CONTENT_OVERLAP` (overlap between chunks), `DEFAULT_CHUNK_UNIT` ('tokens' or 'characters'), and `DEFAULT_CHUNKING_METHOD` (e.g., 'recursive').
    *   `EXTRACTION_METHOD` (e.g., 'fitz' for PDFs) can also be configured.
*   **Local File Management**: Manages a local directory structure within the engine's folder (e.g., `/<BASE_ENGINE_FOLDER>/vector/<ENGINE_ID>/py/schema/<INDEX_CLASS>/`) for storing:
    *   `documents/`: Copies of the original source documents.
    *   `indexed_files/`: CSV files containing the extracted text, chunked text, and later, potentially their embeddings and metadata before being loaded into the actual vector DB.
*   **Python Server Lifecycle**: May start and manage its own Python TCP server process (via `prerna.om.ClientProcessWrapper`) if Python-based text processing is used.
*   **Configuration**: Loads and provides access to common SMSS properties related to chunking, embedding, and the specific vector database connection.
*   **Abstract Methods**: Concrete subclasses must implement methods specific to the target vector database, such as the actual storage/retrieval of vectors and metadata, and the execution of similarity search queries against that database. The `nearestNeighborCall` is a key abstract method.

### Extending for a New Vector Database

To integrate a new vector database:

1.  **Implement `IVectorDatabaseEngine`**.
2.  **Extend `AbstractVectorDatabaseEngine`**: This provides much of the document processing pipeline and embedder integration logic.
3.  **Database-Specific Logic**:
    *   Implement methods for connecting to the new vector database.
    *   Implement `addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, ...)` to take processed chunks and their embeddings (from the CSV table generated by the abstract class) and insert them into the target vector database using its client API. This includes handling both the vectors and associated metadata.
    *   Implement the abstract `nearestNeighborCall(...)` method. This involves:
        *   Taking the query vector (generated by the embedder via the abstract class).
        *   Executing a similarity search against the vector database using its specific API, including any metadata filters provided in the `parameters`.
        *   Formatting the database's search results into the expected `List<Map<String, Object>>` format.
    *   Implement `removeDocument(...)` to delete data from the vector database based on source file information.
    *   Implement `listDocuments(...)` and `listAllRecords(...)`.
4.  **SMSS Properties**: Define necessary SMSS properties for your engine (e.g., host, port, API keys, collection/index names, specific client settings).
5.  **Python Dependencies (if any)**: If the vector database has a preferred Python client or specific Python pre-processing needs not covered by the generic scripts, you might need to adjust the Python environment or provide custom scripts.
6.  **Define `VectorDatabaseTypeEnum`**: Add a new value to `prerna.engine.api.VectorDatabaseTypeEnum`.

## Example Implementations

### `prerna.engine.impl.vector.FaissDatabaseEngine`
*   **Purpose**: Interfaces with FAISS (Facebook AI Similarity Search), a library for efficient local similarity search. It's well-suited for scenarios where the vector index can be managed as local files or kept in memory.
*   **Implementation Highlights**:
    *   Typically manages FAISS index files directly on the file system.
    *   The `addEmbeddings` method would involve loading the existing index (if any), adding new vectors, and re-saving the index.
    *   `nearestNeighborCall` would load the FAISS index and use the FAISS Python library (via `PyTranslator`) to perform the search.
*   **SMSS Configuration**:
    *   `INDEX_PATH`: Path to the directory where FAISS index files are stored.
    *   `EMBEDDER_ENGINE_ID`: ID of the `IModelEngine` used for generating embeddings.
    *   `DEFAULT_INDEX_CLASS`: Name of the default index/collection.

### `prerna.engine.impl.vector.ChromaVectorDatabaseEngine`
*   **Purpose**: Connects to ChromaDB, an open-source, self-hostable or cloud-hosted embedding database.
*   **Implementation Highlights**:
    *   Uses the ChromaDB client API (often via Python, invoked through `PyTranslator`) to interact with the database.
    *   `addEmbeddings` involves creating or connecting to a Chroma collection and adding documents with their embeddings and metadata.
    *   `nearestNeighborCall` queries the Chroma collection using the client API.
*   **SMSS Configuration**:
    *   `CHROMA_HOST`, `CHROMA_PORT`: Connection details for the ChromaDB server.
    *   `DEFAULT_COLLECTION_NAME` (or `DEFAULT_INDEX_CLASS`): The default collection to use.
    *   `EMBEDDER_ENGINE_ID`.
    *   Authentication details if required by the ChromaDB instance.

Other implementations in SEMOSS exist for services like PineCone, Milvus, and databases with vector capabilities like PGVector (PostgreSQL) and OpenSearch/Elasticsearch.
```
