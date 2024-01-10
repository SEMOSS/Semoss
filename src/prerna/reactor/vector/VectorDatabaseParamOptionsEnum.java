package prerna.reactor.vector;

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.engine.api.VectorDatabaseTypeEnum;


public enum VectorDatabaseParamOptionsEnum {
	
		ASCENDING("ascending", 						"Boolean flag to sort the responses by in ascending order"),
		CHUNK_UNIT("chunkUnit", 					"The unit that detemines how to measures the length of given chunks. Options are \"tokens\" or \"characters\"."),
		CONTENT_LENGTH("contentLength", 			"The content length represents the upper limit of tokens within a chunk, as determined by the embedder's tokenizer."),
		CONTENT_OVERLAP("contentOverlap", 			"The number of tokens from prior chunks that are carried over into the current chunk when processing content."),
		COLUMNS_TO_INDEX("columnsToIndex",			"A list of column names in the data you want to create the embeddings from"),
		COLUMNS_TO_REMOVE("columnsToRemove",		"A list of column names in the data that you dont want to store in the database"),
		COLUMNS_TO_RETURN("columnsToReturn",		"A list of column names in the data you want returned in the response"),
		EXTRACTION_METHOD("extractionMethod",		"The name of the extraction method used to pull data from PDF(s). Options are \"fitz\" or \"default\"."),
		KEYWORD_SEARCH_PARAM("keywordSearchParam",  "Create keywords from the extracted chunks and use them to when to create embeddings."),
		RETURN_THRESHOLD("returnThreshold", 		"The minimun threshold every response should be under");
	
	    private final String key;
	    private final String description;
	    
	    
	    VectorDatabaseParamOptionsEnum(String key, String description) {
	    	this.key = key;
	        this.description = description;
	    }
	
	    public String getKey() {
			return this.key;
		}
	    public static String getDescriptionFromKey(String key) {
			for(VectorDatabaseParamOptionsEnum e : VectorDatabaseParamOptionsEnum.values()) {
				if(e.key.equals(key)) {
					return e.description;
				}
			}
			// if we cannot find the description above
			// it is not a standardized key
			// so just return null
			return null;
		}
    
	
	public enum CreateEmbeddingsParamOptions {
		
	    FAISS(VectorDatabaseTypeEnum.FAISS),
	    ;

		private static final String REQUIRED = "REQUIRED";
		private static final String OPTIONAL = "OPTIONAL";
		
	    private final VectorDatabaseTypeEnum vectorDbType;
	    private String[] paramKeys;
	    private Map<String, String> requirementStatusMap;

	    // Static block to initialize the requirement status map for each option
	    static {
	        for (CreateEmbeddingsParamOptions option : values()) {
	            option.requirementStatusMap = initializeRequirementStatusMap(option);
	            option.paramKeys = option.requirementStatusMap.keySet().toArray(new String[option.requirementStatusMap.size()]);
	        }
	    }

	    CreateEmbeddingsParamOptions(VectorDatabaseTypeEnum vectorDbType) {
	        this.vectorDbType = vectorDbType;
	    }

	    private static Map<String, String> initializeRequirementStatusMap(CreateEmbeddingsParamOptions option) {
	        Map<String, String> map = new LinkedHashMap<>();
	        // Set the requirement status for each parameter key based on the option
	        switch (option) {
	            case FAISS:
	            	map.put(VectorDatabaseParamOptionsEnum.CHUNK_UNIT.getKey(), OPTIONAL);
	            	map.put(VectorDatabaseParamOptionsEnum.COLUMNS_TO_INDEX.getKey(), OPTIONAL);
	            	map.put(VectorDatabaseParamOptionsEnum.COLUMNS_TO_REMOVE.getKey(), OPTIONAL);
	            	map.put(VectorDatabaseParamOptionsEnum.CONTENT_LENGTH.getKey(), OPTIONAL);
	            	map.put(VectorDatabaseParamOptionsEnum.CONTENT_OVERLAP.getKey(), OPTIONAL);
	            	map.put(VectorDatabaseParamOptionsEnum.EXTRACTION_METHOD.getKey(), OPTIONAL);
	            	map.put(VectorDatabaseParamOptionsEnum.KEYWORD_SEARCH_PARAM.getKey(), OPTIONAL);
	                break;
	            default:
	                throw new IllegalArgumentException("Vector database type is undefined for " + option);
	            // Add more cases as needed
	        }
	        
	        return map;
	    }

	    public VectorDatabaseTypeEnum getVectorDbType() {
	        return this.vectorDbType;
	    }

	    public String[] getParamOptionsKeys() {
	        return this.paramKeys;
	    }

	    public String getRequirementStatus(String paramKey) {
	        return this.requirementStatusMap.getOrDefault(paramKey, OPTIONAL);
	    }
	    
	    /**
		 * 
		 * @param name
		 * @return
		 */
		public static CreateEmbeddingsParamOptions getEnumFromVectorDbType(VectorDatabaseTypeEnum vectorDbType) {
			CreateEmbeddingsParamOptions[] allValues = values();
			for(CreateEmbeddingsParamOptions v : allValues) {
				if(v.getVectorDbType() == vectorDbType) {
					return v;
				}
			}
			throw new IllegalArgumentException("Invalid input for vector database type " + vectorDbType.getVectorDatabaseName());
		}
	}
	
	public enum VectorQueryParamOptions {
		
	    FAISS(VectorDatabaseTypeEnum.FAISS),
	    ;

		private static final String REQUIRED = "REQUIRED";
		private static final String OPTIONAL = "OPTIONAL";
		
	    private final VectorDatabaseTypeEnum vectorDbType;
	    private String[] paramKeys;
	    private Map<String, String> requirementStatusMap;

	    // Static block to initialize the requirement status map for each option
	    static {
	        for (VectorQueryParamOptions option : values()) {
	            option.requirementStatusMap = initializeRequirementStatusMap(option);
	            option.paramKeys = option.requirementStatusMap.keySet().toArray(new String[option.requirementStatusMap.size()]);
	        }
	    }

	    VectorQueryParamOptions(VectorDatabaseTypeEnum vectorDbType) {
	        this.vectorDbType = vectorDbType;
	    }

	    private static Map<String, String> initializeRequirementStatusMap(VectorQueryParamOptions option) {
	        Map<String, String> map = new LinkedHashMap<>();
	        // Set the requirement status for each parameter key based on the option
	        switch (option) {
	            case FAISS:
	            	map.put(VectorDatabaseParamOptionsEnum.ASCENDING.getKey(), OPTIONAL);
	            	map.put(VectorDatabaseParamOptionsEnum.COLUMNS_TO_RETURN.getKey(), OPTIONAL);
	            	map.put(VectorDatabaseParamOptionsEnum.RETURN_THRESHOLD.getKey(), OPTIONAL);
	            	break;
	            default:
	                throw new IllegalArgumentException("Vector database type is undefined for " + option);
	            // Add more cases as needed
	        }
	        
	        return map;
	    }

	    public VectorDatabaseTypeEnum getVectorDbType() {
	        return this.vectorDbType;
	    }

	    public String[] getParamOptionsKeys() {
	        return this.paramKeys;
	    }
	
	    public String getRequirementStatus(String paramKey) {
	        return this.requirementStatusMap.getOrDefault(paramKey, OPTIONAL);
	    }
	    
	    /**
		 * 
		 * @param name
		 * @return
		 */
		public static VectorQueryParamOptions getEnumFromVectorDbType(VectorDatabaseTypeEnum vectorDbType) {
			VectorQueryParamOptions[] allValues = values();
			for(VectorQueryParamOptions v : allValues) {
				if(v.getVectorDbType() == vectorDbType) {
					return v;
				}
			}
			throw new IllegalArgumentException("Invalid input for vector database type " + vectorDbType.getVectorDatabaseName());
		}
	}
}
