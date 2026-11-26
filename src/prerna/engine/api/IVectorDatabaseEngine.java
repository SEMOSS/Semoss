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

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.engine.impl.vector.FileEmbeddingStatus;
import prerna.engine.impl.vector.VectorDatabaseCSVTable;
import prerna.engine.impl.vector.metadata.VectorDatabaseMetadataCSVTable;
import prerna.logging.IgnoreEngineLogging;
import prerna.om.Insight;
import prerna.util.Constants;

public interface IVectorDatabaseEngine extends IEngine {

	// this is what the FE sends for the type of storage we are creating
	// as a result, cannot be a key in the smss file
	String VECTOR_TYPE = "VECTOR_TYPE";

	/**
	 * Gets the type of the model inference engine. The model engine type is often
	 * used to determine what client to use while running questions
	 * 
	 * @return the type of the database
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	VectorDatabaseTypeEnum getVectorDatabaseType();

	/**
	 * The id of the model the vector database makes its embeddings with, its
	 * {@link Constants#EMBEDDER_ENGINE_ID} setting or the older ENCODER_ID.
	 *
	 * @return the embedding model's id, or null when the vector database names none
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	default String getEmbedderEngineId() {
		Properties smssProp = getSmssProp();
		if (smssProp == null) {
			return null;
		}
		for (String key : new String[] { Constants.EMBEDDER_ENGINE_ID, "ENCODER_ID" }) {
			String embedderEngineId = smssProp.getProperty(key);
			if (embedderEngineId != null && !(embedderEngineId = embedderEngineId.trim()).isEmpty()) {
				return embedderEngineId;
			}
		}
		return null;
	}

	/**
	 * This method is used to add documents to a vector database. The engine itself
	 * will determine how the the documents are processed and the embeddings are
	 * created.
	 * 
	 * @param filePaths  - List of absolute file paths
	 * @param parameters - Additional parameters the engine might need to process
	 *                   the documents e.g. an insight object
	 * @return
	 */
	@EngineAccess(AccessPermissionEnum.EDIT)
	List<FileEmbeddingStatus> addDocument(List<String> filePaths, Map<String, Object> parameters) throws Exception;

	/**
	 * 
	 * @param vectorCsvFiles
	 * @param insight
	 * @param parameters
	 * @return
	 * @throws Exception
	 */
	@EngineAccess(AccessPermissionEnum.EDIT)
	List<FileEmbeddingStatus> addEmbeddings(List<String> vectorCsvFiles, Insight insight,
			Map<String, Object> parameters) throws Exception;

	/**
	 * 
	 * @param vectorCsvFilePath
	 * @param insight
	 * @param parameters
	 * @throws Exception
	 */
	@EngineAccess(AccessPermissionEnum.EDIT)
	List<FileEmbeddingStatus> addEmbeddings(String vectorCsvFilePath, Insight insight, Map<String, Object> parameters)
			throws Exception;

	/**
	 * 
	 * @param vectorCsvFiles
	 * @param insight
	 * @param parameters
	 * @return
	 * @throws Exception
	 */
	@EngineAccess(AccessPermissionEnum.EDIT)
	List<FileEmbeddingStatus> addEmbeddingFiles(List<File> vectorCsvFiles, Insight insight,
			Map<String, Object> parameters) throws Exception;

	/**
	 * 
	 * @param vectorCsvFile
	 * @param insight
	 * @param parameters
	 * @throws Exception
	 */
	@EngineAccess(AccessPermissionEnum.EDIT)
	List<FileEmbeddingStatus> addEmbeddingFile(File vectorCsvFile, Insight insight, Map<String, Object> parameters)
			throws Exception;

	/**
	 * 
	 * @param vectorCsvTable
	 * @param insight
	 * @param parameters
	 * @return
	 * @throws Exception
	 */
	@EngineAccess(AccessPermissionEnum.EDIT)
	List<FileEmbeddingStatus> addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight,
			Map<String, Object> parameters) throws Exception;

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
	@EngineAccess(AccessPermissionEnum.EDIT)
	void addEmbedding(List<? extends Number> embedding, String source, String modality, String divider, String part,
			int tokens, String content, Map<String, Object> additionalMetadata) throws Exception;

	/**
	 * Remove document(s) from a vector store.
	 * 
	 * @param fileNames  - List of file names to be removed from the vector database
	 * @param parameters - Additional parameters that might be needed e.g. if the
	 *                   vector database has more than one collection or index
	 *                   class.
	 */
	@EngineAccess(AccessPermissionEnum.EDIT)
	void removeDocument(List<String> fileNames, Map<String, Object> parameters) throws Exception;

	/**
	 * 
	 * @param metadataTable
	 */
	@EngineAccess(AccessPermissionEnum.EDIT)
	void addMetadata(VectorDatabaseMetadataCSVTable metadataTable) throws Exception;

	/**
	 * Perform a nearest neighbor or semantic search against a vector database. The
	 * searchStatement will be converted to a vector using the same embedding model
	 * utilized to create the document(s) embeddings.
	 * 
	 * @param insight
	 * @param searchStatement
	 * @param limit
	 * @param parameters
	 * @return
	 */
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	List<Map<String, Object>> nearestNeighbor(Insight insight, String searchStatement, Number limit,
			Map<String, Object> parameters);

	/**
	 * List the set of documents a vector database is made from. At a minimum the
	 * file names should be returned but the file size and last modified date are
	 * also options.
	 * 
	 * @param parameters
	 * @return
	 */
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	List<Map<String, Object>> listDocuments(Map<String, Object> parameters);

	/**
	 * List all the records/chunks present in the vector database
	 * 
	 * @param parameters
	 * @return
	 */
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	List<Map<String, Object>> listAllRecords(Map<String, Object> parameters);

	/**
	 * 
	 * @param indexClass
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	String getIndexFilesPath(String indexClass);

	/**
	 * 
	 * @param indexClass
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	String getDocumentsFilesPath(String indexClass);

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	boolean userCanAccessEmbeddingModels(User user);

}