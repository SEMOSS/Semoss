package prerna.engine.api;

import java.io.File;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.engine.impl.vector.FileEmbeddingStatus;
import prerna.engine.impl.vector.VectorDatabaseCSVTable;
import prerna.engine.impl.vector.metadata.VectorDatabaseMetadataCSVTable;
import prerna.om.Insight;

/**
 * Interface defining the contract for vector database engines in the SEMOSS platform.
 * 
 * <p>This interface extends {@link IEngine} to provide specialized functionality for
 * vector databases that store and search high-dimensional embeddings. Vector databases
 * are essential for semantic search, similarity matching, and AI applications that work
 * with text, image, or other embedded representations.</p>
 * 
 * <p>Key capabilities include:</p>
 * <ul>
 * <li>Document ingestion and automatic embedding generation</li>
 * <li>Manual embedding insertion with metadata</li>
 * <li>Semantic search and nearest neighbor queries</li>
 * <li>Document and record management</li>
 * <li>Multi-modal embedding support (text, images)</li>
 * <li>Metadata association and filtering</li>
 * <li>User access control for embedding models</li>
 * </ul>
 * 
 * <p>The interface supports various vector database backends such as:</p>
 * <ul>
 * <li>Chroma for local vector storage</li>
 * <li>Pinecone for managed vector services</li>
 * <li>Weaviate for semantic search platforms</li>
 * <li>Custom vector database implementations</li>
 * </ul>
 * 
 * <p>Vector databases typically organize data into collections or indexes,
 * with each record containing an embedding vector, source metadata, and
 * content information for retrieval and ranking.</p>
 * 
 * @see {@link IEngine} for base engine functionality
 * @see {@link VectorDatabaseTypeEnum} for supported vector database types
 * @see {@link VectorDatabaseCSVTable} for structured embedding data
 * @see {@link FileEmbeddingStatus} for embedding operation results
 */
public interface IVectorDatabaseEngine extends IEngine {

	/**
	 * Configuration key for vector database type specification from frontend.
	 * This key cannot be used in SMSS files as it's reserved for frontend communication.
	 */
	String VECTOR_TYPE = "VECTOR_TYPE";

	/**
	 * Retrieves the type classification of this vector database engine.
	 * 
	 * <p>The vector database type determines which backend API and client
	 * should be used for vector operations, embedding storage, and similarity
	 * search functionality.</p>
	 * 
	 * @return The vector database type enumeration for this engine
	 * @see {@link VectorDatabaseTypeEnum} for available vector database types
	 */
	VectorDatabaseTypeEnum getVectorDatabaseType();
	
	/**
	 * Adds documents to the vector database with automatic embedding generation.
	 * 
	 * <p>This method processes the provided documents by extracting text content,
	 * generating embeddings using the engine's configured embedding model, and
	 * storing them in the vector database. The engine handles document parsing,
	 * chunking, and embedding creation automatically.</p>
	 * 
	 * @param filePaths List of absolute file paths to documents to be added
	 * @param parameters Additional parameters for document processing, such as
	 *                   insight objects, chunking strategies, or metadata options
	 * @return List of embedding statuses indicating success or failure for each document
	 * @throws Exception If document processing or embedding generation fails
	 * @see {@link FileEmbeddingStatus} for status information
	 */
	List<FileEmbeddingStatus> addDocument(List<String> filePaths, Map<String, Object> parameters) throws Exception;
	
	/**
	 * Adds embeddings to the vector database from CSV files.
	 * 
	 * <p>This method processes CSV files containing pre-computed vector embeddings
	 * and loads them into the vector database. The CSV format should contain
	 * embedding vectors along with associated metadata and document references.</p>
	 * 
	 * @param vectorCsvFiles List of absolute paths to CSV files containing embedding data
	 * @param insight The {@link Insight} context for the embedding operation
	 * @param parameters Additional parameters for CSV processing and embedding configuration
	 * @return List of embedding statuses indicating success or failure for each CSV file
	 * @throws Exception If CSV parsing or embedding storage fails
	 * @see {@link Insight} for execution context
	 * @see {@link FileEmbeddingStatus} for operation results
	 */
	List<FileEmbeddingStatus> addEmbeddings(List<String> vectorCsvFiles, Insight insight, Map<String, Object> parameters) throws Exception;

	/**
	 * Adds embeddings to the vector database from a single CSV file.
	 * 
	 * <p>This method processes a single CSV file containing pre-computed vector
	 * embeddings and loads them into the vector database. This is a convenience
	 * method for single-file operations.</p>
	 * 
	 * @param vectorCsvFilePath Absolute path to the CSV file containing embedding data
	 * @param insight The {@link Insight} context for the embedding operation
	 * @param parameters Additional parameters for CSV processing and embedding configuration
	 * @return List of embedding statuses for the processed CSV file
	 * @throws Exception If CSV parsing or embedding storage fails
	 * @see {@link Insight} for execution context
	 * @see {@link FileEmbeddingStatus} for operation results
	 */
	List<FileEmbeddingStatus> addEmbeddings(String vectorCsvFilePath, Insight insight, Map<String, Object> parameters) throws Exception;
	
	/**
	 * Adds embeddings to the vector database from CSV file objects.
	 * 
	 * <p>This method processes multiple {@link File} objects containing pre-computed
	 * vector embeddings and loads them into the vector database. This method accepts
	 * File instances rather than file paths for more flexible file handling.</p>
	 * 
	 * @param vectorCsvFiles List of {@link File} objects containing embedding data in CSV format
	 * @param insight The {@link Insight} context for the embedding operation
	 * @param parameters Additional parameters for CSV processing and embedding configuration
	 * @return List of embedding statuses indicating success or failure for each file
	 * @throws Exception If CSV parsing or embedding storage fails
	 * @see {@link Insight} for execution context
	 * @see {@link FileEmbeddingStatus} for operation results
	 */
	List<FileEmbeddingStatus> addEmbeddingFiles(List<File> vectorCsvFiles, Insight insight, Map<String, Object> parameters) throws Exception;
	
	/**
	 * Adds embeddings to the vector database from a single CSV file object.
	 * 
	 * <p>This method processes a single {@link File} object containing pre-computed
	 * vector embeddings and loads them into the vector database. This is a convenience
	 * method for single-file operations using File objects.</p>
	 * 
	 * @param vectorCsvFile {@link File} object containing embedding data in CSV format
	 * @param insight The {@link Insight} context for the embedding operation
	 * @param parameters Additional parameters for CSV processing and embedding configuration
	 * @return List of embedding statuses for the processed file
	 * @throws Exception If CSV parsing or embedding storage fails
	 * @see {@link Insight} for execution context
	 * @see {@link FileEmbeddingStatus} for operation results
	 */
	List<FileEmbeddingStatus> addEmbeddingFile(File vectorCsvFile, Insight insight, Map<String, Object> parameters) throws Exception;
	
	/**
	 * Adds embeddings to the vector database from a structured CSV table object.
	 * 
	 * <p>This method processes a {@link VectorDatabaseCSVTable} containing structured
	 * embedding data and loads it into the vector database. The table object provides
	 * a more structured approach to embedding data management with defined schemas.</p>
	 * 
	 * @param vectorCsvTable {@link VectorDatabaseCSVTable} object containing structured embedding data
	 * @param insight The {@link Insight} context for the embedding operation
	 * @param parameters Additional parameters for table processing and embedding configuration
	 * @return List of embedding statuses indicating success or failure for table rows
	 * @throws Exception If table processing or embedding storage fails
	 * @see {@link VectorDatabaseCSVTable} for table structure
	 * @see {@link Insight} for execution context
	 * @see {@link FileEmbeddingStatus} for operation results
	 */
	List<FileEmbeddingStatus> addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight, Map<String, Object> parameters) throws Exception;
	
	/**
	 * Inserts a single embedding directly into the vector database with detailed metadata.
	 * 
	 * <p>This method provides fine-grained control over individual embedding insertion,
	 * allowing specification of all metadata fields including source, modality, and
	 * content structure. Use this method for precise control over embedding storage.</p>
	 * 
	 * @param embedding The numerical vector embedding as a list of numbers
	 * @param source The source identifier or file path for the embedded content
	 * @param modality The content modality (e.g., "text", "image", "audio")
	 * @param divider The content division strategy used (e.g., "sentence", "paragraph", "chunk")
	 * @param part The part identifier within the divided content
	 * @param tokens The number of tokens in the original content
	 * @param content The original text content that was embedded
	 * @param additionalMetadata Additional custom metadata to store with the embedding
	 * @throws Exception If embedding insertion or indexing fails
	 */
	void addEmbedding(List<? extends Number> embedding, String source, String modality, String divider, String part, int tokens, String content, Map<String, Object> additionalMetadata) throws Exception;
	
	/**
	 * Removes documents from the vector database by file names.
	 * 
	 * <p>This method removes all embeddings and associated metadata for the specified
	 * documents from the vector database. This operation is typically irreversible
	 * and will affect similarity search results.</p>
	 * 
	 * @param fileNames List of file names to be removed from the vector database
	 * @param parameters Additional parameters for removal operation, such as collection
	 *                   names, index specifications, or confirmation flags
	 * @throws Exception If document removal or index updating fails
	 */
	void removeDocument(List<String> fileNames, Map<String, Object> parameters) throws Exception;
	
	/**
	 * Adds metadata to existing embeddings in the vector database.
	 * 
	 * <p>This method updates or adds metadata to existing embeddings without
	 * regenerating the vector embeddings themselves. This is useful for enriching
	 * stored embeddings with additional contextual information.</p>
	 * 
	 * @param metadataTable {@link VectorDatabaseMetadataCSVTable} containing metadata updates
	 * @throws Exception If metadata addition or database updating fails
	 * @see {@link VectorDatabaseMetadataCSVTable} for metadata structure
	 */
	void addMetadata(VectorDatabaseMetadataCSVTable metadataTable) throws Exception;
	
	/**
	 * Performs semantic similarity search against the vector database.
	 * 
	 * <p>This method converts the search statement into a vector embedding using the same
	 * embedding model used to create the stored document embeddings, then performs a
	 * nearest neighbor search to find the most semantically similar content. Results
	 * are ranked by similarity score.</p>
	 * 
	 * @param insight The {@link Insight} context for the search operation
	 * @param searchStatement The text query to search for semantically similar content
	 * @param limit Maximum number of results to return
	 * @param parameters Additional search parameters such as similarity thresholds,
	 *                   metadata filters, or collection specifications
	 * @return List of maps containing search results with content, metadata, and similarity scores
	 * @see {@link Insight} for execution context
	 */
	List<Map<String, Object>> nearestNeighbor(Insight insight, String searchStatement, Number limit, Map<String, Object> parameters);
	
	/**
	 * Lists all documents stored in the vector database with their metadata.
	 * 
	 * <p>This method returns information about all documents that have been indexed
	 * in the vector database. At minimum, file names are returned, but additional
	 * metadata such as file size, modification dates, and custom attributes may
	 * also be included based on the database implementation.</p>
	 * 
	 * @param parameters Additional parameters for document listing, such as collection
	 *                   filters, pagination options, or metadata inclusion flags
	 * @return List of maps containing document information including file names,
	 *         sizes, modification dates, and other available metadata
	 */
	List<Map<String, Object>> listDocuments(Map<String, Object> parameters);
	
	/**
	 * Lists all individual records or chunks stored in the vector database.
	 * 
	 * <p>This method returns detailed information about all embedding records,
	 * including individual chunks or segments that documents were divided into
	 * during the embedding process. This provides a more granular view than
	 * {@link #listDocuments(Map)} which shows document-level information.</p>
	 * 
	 * @param parameters Additional parameters for record listing, such as pagination
	 *                   limits, metadata filters, or collection specifications
	 * @return List of maps containing detailed record information including content,
	 *         embeddings, metadata, and chunk identifiers
	 * @see {@link #listDocuments(Map)} for document-level listing
	 */
	List<Map<String, Object>> listAllRecords(Map<String, Object> parameters);
	
	/**
	 * Gets the file system path where index files are stored for a specific index class.
	 * 
	 * <p>This method returns the directory path where the vector database stores
	 * its index files for the specified index class or collection. This is useful
	 * for file system operations, backup processes, or debugging.</p>
	 * 
	 * @param indexClass The index class or collection identifier
	 * @return Absolute file system path to the index files directory
	 */
	String getIndexFilesPath(String indexClass);
	
	/**
	 * Gets the file system path where document files are stored for a specific index class.
	 * 
	 * <p>This method returns the directory path where the vector database stores
	 * the original document files for the specified index class or collection.
	 * This path typically contains the source documents that were processed
	 * to create the embeddings.</p>
	 * 
	 * @param indexClass The index class or collection identifier
	 * @return Absolute file system path to the documents directory
	 */
	String getDocumentsFilesPath(String indexClass);
	
	/**
	 * Checks if a user has permission to access embedding models through this engine.
	 * 
	 * <p>This method validates whether the specified user has the necessary
	 * permissions to use embedding models for document processing and similarity
	 * search operations. This is part of the security and access control system.</p>
	 * 
	 * @param user The {@link User} to check permissions for
	 * @return true if the user can access embedding models, false otherwise
	 * @see {@link User} for user information and permissions
	 */
	boolean userCanAccessEmbeddingModels(User user);
	
}