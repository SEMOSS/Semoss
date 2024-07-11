package prerna.engine.api;

import java.util.List;
import java.util.Map;

import prerna.engine.impl.vector.VectorDatabaseCSVTable;
import prerna.om.Insight;

public interface IVectorDatabaseEngine extends IEngine {

	
	// this is what the FE sends for the type of storage we are creating
	// as a result, cannot be a key in the smss file
	String VECTOR_TYPE = "VECTOR_TYPE";
		
	/**
	 * Gets the type of the model inference engine.  The model engine type is often used to determine what client to use while running questions
	 * @return the type of the database 
	*/
	VectorDatabaseTypeEnum getVectorDatabaseType();
	
	/**
	 * This method is used to add documents to a vector database. The engine itself will determine how the the documents are
	 * processed and the embeddings are created.
	 * 
	 * @param filePaths		- List of absolute file paths
	 * @param parameters	- Additional parameters the engine might need to process the documents e.g. an insight object
	 */
	void addDocument(List<String> filePaths, Map<String, Object> parameters) throws Exception;
	
	/**
	 * 
	 * @param vectorCsvTable
	 * @param insight
	 * @throws Exception
	 */
	void addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight) throws Exception;
	
	/**
	 * Insert embeddings directly into the vector database
	 * 
	 * @param embedding
	 * @param source
	 * @param modality
	 * @param divider
	 * @param part
	 * @param tokens
	 * @param content
	 * @param additionalMetadata
	 * @throws Exception
	 */
	void addEmbedding(List<? extends Number> embedding, String source, String modality, String divider, String part, int tokens, String content, Map<String, Object> additionalMetadata) throws Exception;
	
	/**
	 * Remove document(s) from a vector store.
	 * 
	 * @param fileNames		- List of file names to be removed from the vector database
	 * @param parameters	- Additional parameters that might be needed e.g. if the vector database has more than one collection or index class.
	 */
	void removeDocument(List<String> fileNames, Map<String, Object> parameters);
	
	/**
	 * Perform a nearest neighbor or semantic search against a vector database. The searchStatement will be 
	 * converted to a vector using the same embedding model utilized to create the document(s) embeddings.
	 * 
	 * @param searchStatement
	 * @param limit
	 * @param parameters
	 * @return
	 */
	List<Map<String, Object>> nearestNeighbor(String searchStatement, Number limit, Map<String, Object> parameters);
	
	/**
	 * List the set of documents a vector database is made from. At a minimum the file names should be returned but 
	 * the file size and last modified date are also options.
	 * 
	 * @param parameters
	 * @return
	 */
	List<Map<String, Object>> listDocuments(Map<String, Object> parameters);
	
	
}