package prerna.engine.api;

import prerna.engine.impl.vector.AwsS3VectorDatabaseEngine;
import prerna.engine.impl.vector.AzureAISearchRestVectorDatabaseEngine;
import prerna.engine.impl.vector.ChromaVectorDatabaseEngine;
import prerna.engine.impl.vector.ElasticSearchRestVectorDatabaseEngine;
import prerna.engine.impl.vector.FaissDatabaseEngine;
import prerna.engine.impl.vector.MilvusVectorDatabaseEngine;
import prerna.engine.impl.vector.OpenSearchRestVectorDatabaseEngine;
import prerna.engine.impl.vector.PGVectorDatabaseEngine;
import prerna.engine.impl.vector.PineConeVectorDatabaseEngine;
import prerna.engine.impl.vector.ProxyVectorDatabaseEngine;
import prerna.engine.impl.vector.WeaviateVectorDatabaseEngine;

/**
 * Enumeration defining all available vector database engine types for embedding storage and similarity search.
 * 
 * <p>This enum provides a comprehensive registry of vector database engines that support
 * high-dimensional vector storage, embedding management, and similarity search operations.
 * Each enum value maps a vector database type identifier to its corresponding implementation
 * class for seamless integration with various vector database backends.</p>
 * 
 * <p>Supported vector database categories include:</p>
 * <ul>
 *   <li><strong>Cloud Vector Services:</strong> AWS S3, Azure AI Search, Pinecone</li>
 *   <li><strong>Open Source Solutions:</strong> Chroma, Weaviate, Milvus, FAISS</li>
 *   <li><strong>Search Engine Extensions:</strong> Elasticsearch, OpenSearch with vector support</li>
 *   <li><strong>Database Extensions:</strong> PostgreSQL with pgvector extension</li>
 *   <li><strong>Proxy Services:</strong> Generic proxy for multiple backends</li>
 * </ul>
 * 
 * <p>Vector databases are essential for AI applications involving semantic search,
 * recommendation systems, similarity matching, and retrieval-augmented generation (RAG).</p>
 * 
 * @see {@link IVectorDatabaseEngine} for the base vector database interface
 * @see {@link ChromaVectorDatabaseEngine} for Chroma integration
 * @see {@link PineConeVectorDatabaseEngine} for Pinecone cloud service
 * @see {@link WeaviateVectorDatabaseEngine} for Weaviate integration
 * @author SEMOSS
 */
public enum VectorDatabaseTypeEnum {

	/** AWS S3-based vector storage for scalable cloud deployments */
	AWS_S3("AWS_S3", AwsS3VectorDatabaseEngine.class.getName()),
	/** Azure AI Search with vector capabilities for Microsoft cloud environments */
	AZURE_AI_SEARCH("AZURE_AI_SEARCH", AzureAISearchRestVectorDatabaseEngine.class.getName()),
	/** Chroma open-source vector database for embeddings and similarity search */
	CHROMA("CHROMA", ChromaVectorDatabaseEngine.class.getName()),
	/** Elasticsearch with vector search capabilities via REST API */
	ELASTIC_SEARCH("ELASTIC_SEARCH", ElasticSearchRestVectorDatabaseEngine.class.getName()),
	/** Facebook AI Similarity Search (FAISS) for efficient similarity search */
	FAISS("FAISS", FaissDatabaseEngine.class.getName()),
	/** Milvus open-source vector database for AI applications */
	MILVUS("MILVUS", MilvusVectorDatabaseEngine.class.getName()),
	/** OpenSearch with vector search capabilities via REST API */
	OPEN_SEARCH("OPEN_SEARCH", OpenSearchRestVectorDatabaseEngine.class.getName()),
	/** PostgreSQL with pgvector extension for vector operations */
	PGVECTOR("PGVECTOR", PGVectorDatabaseEngine.class.getName()),
	/** Pinecone managed vector database service */
	PINECONE("PINECONE", PineConeVectorDatabaseEngine.class.getName()),
	/** Generic proxy engine for multiple vector database backends */
	PROXY("PROXY", ProxyVectorDatabaseEngine.class.getName()),
	/** Weaviate vector database with GraphQL API and semantic search */
	WEAVIATE("WEAVIATE", WeaviateVectorDatabaseEngine.class.getName()),
	;
		
	/** The human-readable name identifier for this vector database type */
	private String vectorDbName;
	/** The fully qualified class name of the implementing vector database engine */
	private String vectorDbClass;
	
	/**
	 * Constructs a vector database type enum with the specified name and implementation class.
	 * 
	 * @param vectorDbName The human-readable identifier for this vector database type
	 * @param vectorDbClass The fully qualified class name of the implementation
	 */
	VectorDatabaseTypeEnum(String vectorDbName, String vectorDbClass) {
		this.vectorDbName = vectorDbName;
		this.vectorDbClass = vectorDbClass;
	}
	
	/**
	 * Gets the fully qualified class name of the implementing vector database engine.
	 * 
	 * @return The complete class path for the vector database engine implementation
	 */
	public String getVectorDatabaseClass() {
		return this.vectorDbClass;
	}
	
	/**
	 * Gets the human-readable name identifier for this vector database type.
	 * 
	 * @return The vector database type name used for identification and configuration
	 */
	public String getVectorDatabaseName() {
		return this.vectorDbName;
	}
	
	/**
	 * Retrieves the vector database type enum that matches the specified name.
	 * 
	 * <p>This method performs a case-insensitive search through all available
	 * vector database types to find the one that matches the provided name. This is
	 * commonly used for configuration parsing and dynamic engine selection.</p>
	 * 
	 * @param name The vector database type name to search for (case-insensitive)
	 * @return The matching {@link VectorDatabaseTypeEnum} instance
	 * @throws IllegalArgumentException If no vector database type matches the provided name
	 */
	public static VectorDatabaseTypeEnum getEnumFromName(String name) {
		VectorDatabaseTypeEnum[] allValues = values();
		for(VectorDatabaseTypeEnum v : allValues) {
			if(v.getVectorDatabaseName().equalsIgnoreCase(name)) {
				return v;
			}
		}
		throw new IllegalArgumentException("Invalid input for name " + name);
	}
	
	/**
	 * Retrieves the vector database type enum that matches the specified implementation class.
	 * 
	 * <p>This method searches through all available vector database types to find
	 * the one that matches the provided implementation class name. This is useful
	 * for reverse lookup when you have a class name and need the corresponding enum.</p>
	 * 
	 * @param vectorDbClass The fully qualified class name to search for
	 * @return The matching {@link VectorDatabaseTypeEnum} instance, or null if not found
	 */
	public static VectorDatabaseTypeEnum getEnumFromClass(String vectorDbClass) {
		for(VectorDatabaseTypeEnum v : VectorDatabaseTypeEnum.values()) {
			if(vectorDbClass.equalsIgnoreCase(v.vectorDbClass)) {
				return v;
			}
		}
		return null;
	}
}
