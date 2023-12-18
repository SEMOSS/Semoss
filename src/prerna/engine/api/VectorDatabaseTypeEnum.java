package prerna.engine.api;

import prerna.engine.impl.vector.FaissDatabaseEngine;

public enum VectorDatabaseTypeEnum {

	FAISS("FAISS", FaissDatabaseEngine.class.getName());
		
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
	
	public enum ParamValueOptions {
		ASCENDING("ascending", 						"Boolean flag to sort the responses by in ascending order"),
		CONTENT_LENGTH("contentLength", 			"The content length represents the upper limit of tokens within a chunk, as determined by the embedder's tokenizer."),
		CONTENT_OVERLAP("contentOverlap", 			"The number of tokens from prior chunks that are carried over into the current chunk when processing content."),
		COLUMNS_TO_INDEX("columnsToIndex",			"A list of column names in the data you want to create the embeddings from"),
		COLUMNS_TO_REMOVE("columnsToRemove",		"A list of column names in the data that you dont want to store in the database"),
		COLUMNS_TO_RETURN("columnsToReturn",		"A list of column names in the data you want returned in the response"),
		KEYWORD_SEARCH_PARAM("keywordSearchParam",  "Create keywords from the extracted chunks and use them to when to create embeddings."),
		RETURN_THRESHOLD("returnThreshold", 		"The minimun threshold every response should be under");

        private final String key;
        private final String description;
        
        
        ParamValueOptions(String key, String description) {
        	this.key = key;
            this.description = description;
        }

        public String getKey() {
    		return this.key;
    	}
        public static String getDescriptionFromKey(String key) {
    		for(ParamValueOptions e : ParamValueOptions.values()) {
    			if(e.key.equals(key)) {
    				return e.description;
    			}
    		}
    		// if we cannot find the description above
    		// it is not a standardized key
    		// so just return null
    		return null;
    	}
    }
}
