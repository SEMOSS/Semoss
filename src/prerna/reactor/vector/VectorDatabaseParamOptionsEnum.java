/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.vector;

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.engine.api.VectorDatabaseTypeEnum;

public enum VectorDatabaseParamOptionsEnum {

	// @formatter:off
	CHUNK_UNIT("chunkUnit", 					"The unit that detemines how to measures the length of given chunks. Options are \"tokens\" or \"characters\"."),
	CONTENT_LENGTH("contentLength", 			"The content length represents the upper limit of tokens within a chunk, as determined by the embedder's tokenizer."),
	CONTENT_OVERLAP("contentOverlap", 			"The number of tokens from prior chunks that are carried over into the current chunk when processing content."),
	COLUMNS_TO_INDEX("columnsToIndex",			"A list of column names in the data you want to create the embeddings from"),
	COLUMNS_TO_REMOVE("columnsToRemove",		"A list of column names in the data that you dont want to store in the database"),
	COLUMNS_TO_RETURN("columnsToReturn",		"A list of column names in the data you want returned in the response"),
	EXTRACTION_METHOD("extractionMethod",		"The name of the extraction method used to pull data from PDF(s). Options are \"fitz\" or \"default\"."),
	CHUNKING_METHOD("chunkingMethod",            "The chunking method to be used during text splitting. Options are \"token\" (default), \"recursive\", or \"semantic\"."),
	CUSTOM_DOCUMENT_PROCESSOR("customDocumentProcessor",					"Boolean flag to determine whether a custom function is being used to process a document"),
	CUSTOM_DOCUMENT_PROCESSOR_FUNCTION_ID("customDocumentProcessorFunctionID",			"Indicates the function ID used to custom process a document"),
	KEYWORD_SEARCH_PARAM("keywordSearchParam",  "Create keywords from the extracted chunks and use them to when to create embeddings."),
	RETURN_THRESHOLD("returnThreshold", 		"The minimum threshold every response should be under"),
	USE_HYBRID_SEARCH("useHybridSearch", 		"Option that determines whether hybrid search is used (if possible) or default vector search. Default is hybrid search if not otherwise specified in the vector db configuration")
	;
	// @formatter:on

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
		for (VectorDatabaseParamOptionsEnum e : VectorDatabaseParamOptionsEnum.values()) {
			if (e.key.equals(key)) {
				return e.description;
			}
		}
		// if we cannot find the description above
		// it is not a standardized key
		// so just return null
		return null;
	}

	public enum CreateEmbeddingsParamOptions {

		FAISS(VectorDatabaseTypeEnum.FAISS),;

		private static final String REQUIRED = "REQUIRED";
		private static final String OPTIONAL = "OPTIONAL";

		private final VectorDatabaseTypeEnum vectorDbType;
		private String[] paramKeys;
		private Map<String, String> requirementStatusMap;

		// Static block to initialize the requirement status map for each option
		static {
			for (CreateEmbeddingsParamOptions option : values()) {
				option.requirementStatusMap = initializeRequirementStatusMap(option);
				option.paramKeys = option.requirementStatusMap.keySet()
						.toArray(new String[option.requirementStatusMap.size()]);
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
				map.put(VectorDatabaseParamOptionsEnum.CUSTOM_DOCUMENT_PROCESSOR.getKey(), OPTIONAL);
				map.put(VectorDatabaseParamOptionsEnum.CUSTOM_DOCUMENT_PROCESSOR_FUNCTION_ID.getKey(), OPTIONAL);
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
			for (CreateEmbeddingsParamOptions v : allValues) {
				if (v.getVectorDbType() == vectorDbType) {
					return v;
				}
			}
			throw new IllegalArgumentException(
					"Invalid input for vector database type " + vectorDbType.getVectorDatabaseName());
		}
	}

	public enum VectorQueryParamOptions {

		FAISS(VectorDatabaseTypeEnum.FAISS),;

		private static final String REQUIRED = "REQUIRED";
		private static final String OPTIONAL = "OPTIONAL";

		private final VectorDatabaseTypeEnum vectorDbType;
		private String[] paramKeys;
		private Map<String, String> requirementStatusMap;

		// Static block to initialize the requirement status map for each option
		static {
			for (VectorQueryParamOptions option : values()) {
				option.requirementStatusMap = initializeRequirementStatusMap(option);
				option.paramKeys = option.requirementStatusMap.keySet()
						.toArray(new String[option.requirementStatusMap.size()]);
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
			for (VectorQueryParamOptions v : allValues) {
				if (v.getVectorDbType() == vectorDbType) {
					return v;
				}
			}
			throw new IllegalArgumentException(
					"Invalid input for vector database type " + vectorDbType.getVectorDatabaseName());
		}
	}
}
