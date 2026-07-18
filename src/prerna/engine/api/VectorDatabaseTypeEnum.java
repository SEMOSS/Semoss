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
import prerna.engine.impl.vector.QdrantVectorDatabaseEngine;
import prerna.engine.impl.vector.WeaviateVectorDatabaseEngine;

public enum VectorDatabaseTypeEnum {

	AWS_S3("AWS_S3", AwsS3VectorDatabaseEngine.class.getName()),
	AZURE_AI_SEARCH("AZURE_AI_SEARCH", AzureAISearchRestVectorDatabaseEngine.class.getName()),
	CHROMA("CHROMA", ChromaVectorDatabaseEngine.class.getName()),
	ELASTIC_SEARCH("ELASTIC_SEARCH", ElasticSearchRestVectorDatabaseEngine.class.getName()),
	FAISS("FAISS", FaissDatabaseEngine.class.getName()),
	MILVUS("MILVUS", MilvusVectorDatabaseEngine.class.getName()),
	OPEN_SEARCH("OPEN_SEARCH", OpenSearchRestVectorDatabaseEngine.class.getName()),
	PGVECTOR("PGVECTOR", PGVectorDatabaseEngine.class.getName()),
	PINECONE("PINECONE", PineConeVectorDatabaseEngine.class.getName()),
	PROXY("PROXY", ProxyVectorDatabaseEngine.class.getName()),
	QDRANT("QDRANT", QdrantVectorDatabaseEngine.class.getName()),
	WEAVIATE("WEAVIATE", WeaviateVectorDatabaseEngine.class.getName()),
	;
		
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
