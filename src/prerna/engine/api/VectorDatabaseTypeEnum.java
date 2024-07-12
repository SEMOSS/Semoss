package prerna.engine.api;

import prerna.engine.impl.vector.ChromaVectorDatabaseEngine;
import prerna.engine.impl.vector.FaissDatabaseEngine;
import prerna.engine.impl.vector.OpenSearchRestVectorDatabaseEngine;
import prerna.engine.impl.vector.OpenSearchVectorDatabaseEngine;
import prerna.engine.impl.vector.PGVectorDatabaseEngine;
import prerna.engine.impl.vector.PineConeVectorDatabaseEngine;
import prerna.engine.impl.vector.WeaviateVectorDatabaseEngine;

public enum VectorDatabaseTypeEnum {

	CHROMA("CHROMA", ChromaVectorDatabaseEngine.class.getName()),
	FAISS("FAISS", FaissDatabaseEngine.class.getName()),
	PGVECTOR("PGVECTOR", PGVectorDatabaseEngine.class.getName()),
	OPENSEARCH("OPENSEARCH", OpenSearchVectorDatabaseEngine.class.getName()),
	OPENSEARCH_REST("OPENSEARCH_REST", OpenSearchRestVectorDatabaseEngine.class.getName()),
	WEAVIATE("WEAVIATE", WeaviateVectorDatabaseEngine.class.getName()),
	PINECONE("PINECONE", PineConeVectorDatabaseEngine.class.getName());
		
	private String vectorDbName;
	private String vectorDbClass;
	
	VectorDatabaseTypeEnum(String vectorDbName, String vectorDbClass) {
		this.vectorDbName = vectorDbName;
		this.vectorDbClass = vectorDbClass;
	}
	
	/**
	 * 
	 * @return
	 */
	public String getVectorDatabaseClass() {
		return this.vectorDbClass;
	}
	
	/**
	 * 
	 * @return
	 */
	public String getVectorDatabaseName() {
		return this.vectorDbName;
	}
	
	/**
	 * 
	 * @param name
	 * @return
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
	 * Get the enum from the driver
	 * @param driver
	 * @return
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
