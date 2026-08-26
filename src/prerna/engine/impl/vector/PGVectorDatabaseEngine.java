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
package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.f4b6a3.uuid.alt.GUID;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.pgvector.PGvector;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.CopyFilesToEngineRunner;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.engine.api.ICustomEmbeddingsFunctionEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.vector.metadata.VectorDatabaseMetadataCSVRow;
import prerna.engine.impl.vector.metadata.VectorDatabaseMetadataCSVTable;
import prerna.engine.impl.vector.metadata.VectorDatabaseMetadataCSVWriter;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryOpaqueSelector;
import prerna.reactor.vector.VectorDatabaseParamOptionsEnum;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.QueryExecutionUtility;
import prerna.util.Settings;
import prerna.util.Utility;
import prerna.util.sql.PGVectorQueryUtil;

public class PGVectorDatabaseEngine extends RDBMSNativeEngine implements IVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(PGVectorDatabaseEngine.class);

	private static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.disableHtmlEscaping().create();

	public static final String PGVECTOR_TABLE_NAME = "PGVECTOR_TABLE_NAME";
	public static final String PGVECTOR_METADATA_TABLE_NAME = "PGVECTOR_METADATA_TABLE_NAME";

	private int contentLength = 512;
	private int contentOverlap = 0;

	private String defaultChunkUnit;
//	protected String defaultExtractionMethod;

	protected boolean customDocumentProcessor = false;
	protected String customDocumentProcessorFunctionID = null;
	protected boolean customDocumentProcessorNeedStorage = false;

	private String embedderEngineId = null;
	private String keywordGeneratorEngineId = null;
	private String distanceMethod = null;

	private String vectorTableName = null;
	private String vectorTableMetadataName = null;
	private File schemaFolder;

	// our paradigm for how we store files
	private String defaultIndexClass;
	private List<String> indexClasses;

	private ClientProcessWrapper cpw = null;
	// python server
	private PyTranslator pyTranslator = null;
	private File pyDirectoryBasePath;

	private boolean modelPropsLoaded = false;

	private boolean removeDocsFlag = true;

	// string substitute vars
	private Map<String, String> vars = new HashMap<>();
	private final ReentrantLock startServerLock = new ReentrantLock();

	private PGVectorQueryUtil pgVectorQueryUtil = new PGVectorQueryUtil();

	// maintain details in the log database
	protected boolean inferenceLogsEnbaled = Utility.isModelInferenceLogsEnabled();

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.distanceMethod = smssProp.getProperty(Constants.DISTANCE_METHOD);
		this.vectorTableName = smssProp.getProperty(PGVECTOR_TABLE_NAME);
		if (this.vectorTableName == null || (this.vectorTableName = this.vectorTableName.trim()).isEmpty()) {
			throw new NullPointerException("Must define the vector db table name");
		}
		this.vectorTableMetadataName = smssProp.getProperty(PGVECTOR_METADATA_TABLE_NAME);
		if (this.vectorTableMetadataName == null
				|| (this.vectorTableMetadataName = this.vectorTableMetadataName.trim()).isEmpty()) {
			this.vectorTableMetadataName = this.vectorTableName + "_METADATA";
		}

		Connection conn = null;
		try {
			conn = getConnection();
			PGvector.addVectorType(conn);
			initSQL(this.vectorTableName, this.vectorTableMetadataName);
		} catch (SQLException e) {
			classLogger.error("Failed to initialize PGVector tables '{}' and '{}'", this.vectorTableName,
					this.vectorTableMetadataName, e);
			throw e;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(this, conn, null, null);
		}

		if (this.smssProp.containsKey(Constants.CONTENT_LENGTH)) {
			this.contentLength = Integer.parseInt(this.smssProp.getProperty(Constants.CONTENT_LENGTH));
		}
		if (this.smssProp.containsKey(Constants.CONTENT_OVERLAP)) {
			this.contentOverlap = Integer.parseInt(this.smssProp.getProperty(Constants.CONTENT_OVERLAP));
		}

		this.keepInputOutput = Boolean.parseBoolean(this.smssProp.getProperty(Constants.KEEP_INPUT_OUTPUT));

		this.defaultChunkUnit = "tokens";
		if (this.smssProp.containsKey(Constants.DEFAULT_CHUNK_UNIT)) {
			this.defaultChunkUnit = this.smssProp.getProperty(Constants.DEFAULT_CHUNK_UNIT).toLowerCase().trim();
			if (!this.defaultChunkUnit.equals("tokens") && !this.defaultChunkUnit.equals("characters")) {
				throw new IllegalArgumentException("DEFAULT_CHUNK_UNIT should be either 'tokens' or 'characters'");
			}
		}

//		this.defaultExtractionMethod = this.smssProp.getProperty(Constants.EXTRACTION_METHOD, "None");
		this.distanceMethod = this.smssProp.getProperty(Constants.DISTANCE_METHOD, "Cosine Similarity");

		this.defaultIndexClass = "default";
		if (this.smssProp.containsKey(Constants.INDEX_CLASSES)) {
			this.defaultIndexClass = this.smssProp.getProperty(Constants.INDEX_CLASSES);
		}

		// smss properties for custom document processing
		if (this.smssProp.containsKey(Constants.CUSTOM_DOCUMENT_PROCESSOR)) {
			this.customDocumentProcessor = Boolean
					.parseBoolean(this.smssProp.getProperty(Constants.CUSTOM_DOCUMENT_PROCESSOR));
		}
		if (this.smssProp.containsKey(Constants.CUSTOM_DOCUMENT_PROCESSOR_FUNCTION_ID)) {
			this.customDocumentProcessorFunctionID = this.smssProp
					.getProperty(Constants.CUSTOM_DOCUMENT_PROCESSOR_FUNCTION_ID);
		}
		if (this.smssProp.containsKey(Constants.CUSTOM_DOCUMENT_PROCESSOR_USE_STORAGE)) {
			this.customDocumentProcessorNeedStorage = Boolean
					.parseBoolean(this.smssProp.getProperty(Constants.CUSTOM_DOCUMENT_PROCESSOR_USE_STORAGE));
		}
		// highest directory (first layer inside vector db base folder)
		String engineDir = EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, this.engineId,
				this.engineName);
		this.pyDirectoryBasePath = new File(Utility.normalizePath(engineDir + "/py/"));

		// second layer - This holds all the different "tables". The reason we want this
		// is to easily and quickly grab the sub folders
		this.schemaFolder = new File(engineDir, "schema");
		if (!this.schemaFolder.exists()) {
			this.schemaFolder.mkdirs();
		}

		// third layer - All the separate tables,classes, or searchers that can be added
		// to this db
		this.indexClasses = new ArrayList<>();
		for (File file : this.schemaFolder.listFiles()) {
			if (file.isDirectory() && !file.getName().equals("temp")) {
				this.indexClasses.add(file.getName());
			}
		}
	}

	/**
	 * 
	 * @param table
	 * @throws SQLException
	 */
	private void initSQL(String table, String metadataTable) throws SQLException {
		String createMainTable = pgVectorQueryUtil.createEmbeddingsTable(table);
		String createMetaTable = pgVectorQueryUtil.createEmbeddingsMetadataTable(metadataTable);
		execCreateStatement(createMainTable);
		execCreateStatement(createMetaTable);

		pgVectorQueryUtil.createOWL(this, table, metadataTable);
	}

	/**
	 * 
	 * @param createQuery
	 * @throws SQLException
	 */
	private void execCreateStatement(String createQuery) throws SQLException {
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			classLogger.info("Executing create table for {} = {}",
					SmssUtilities.getUniqueName(this.engineName, this.engineId), createQuery);
			stmt.execute(createQuery);
		} catch (SQLException e) {
			classLogger.warn("Unable to create the table {}", createQuery);
			classLogger.error("Failed to execute create-table statement: {}", createQuery, e);
		} finally {
			if (this.dataSource != null) {
				ConnectionUtils.closeAllConnections(conn, stmt);
			} else {
				ConnectionUtils.closeAllConnections(null, stmt);
			}
		}
	}

	/**
	 * 
	 */
	protected void verifyModelProps() {
		// This could get moved depending on other vector db needs
		// This is to get the Model Name and Max Token for an encoder -- we need this to
		// verify chunks aren't getting truncated
		this.embedderEngineId = this.smssProp.getProperty(Constants.EMBEDDER_ENGINE_ID);
		if (this.embedderEngineId == null || (this.embedderEngineId = this.embedderEngineId.trim()).isEmpty()) {

			// check legacy key....
			this.embedderEngineId = this.smssProp.getProperty("ENCODER_ID");
			if (this.embedderEngineId == null || (this.embedderEngineId = this.embedderEngineId.trim()).isEmpty()) {
				throw new IllegalArgumentException("Must define the embedder engine id for this vector database using "
						+ Constants.EMBEDDER_ENGINE_ID);
			}

			this.smssProp.put(Constants.EMBEDDER_ENGINE_ID, embedderEngineId);
		}

		IModelEngine modelEngine = Utility.getModel(embedderEngineId);
		if (modelEngine == null) {
			throw new NullPointerException(
					"Could not find the defined embedder engine id for this vector database with value = "
							+ this.embedderEngineId);
		}

		Properties modelProperties = modelEngine.getSmssProp();
		if (modelProperties.isEmpty() || !modelProperties.containsKey(Constants.MODEL)) {
			throw new IllegalArgumentException("Embedder engine exists but does not contain key " + Constants.MODEL);
		}

		this.smssProp.put(Constants.MODEL, modelProperties.getProperty(Constants.MODEL));
		this.smssProp.put(IModelEngine.MODEL_TYPE, modelProperties.getProperty(IModelEngine.MODEL_TYPE));
		if (!modelProperties.containsKey(Constants.MAX_TOKENS)) {
			this.smssProp.put(Constants.MAX_TOKENS, "None");
		} else {
			this.smssProp.put(Constants.MAX_TOKENS, modelProperties.getProperty(Constants.MAX_TOKENS));
		}

		// model engine responsible for creating keywords
		this.keywordGeneratorEngineId = this.smssProp.getProperty(Constants.KEYWORD_ENGINE_ID);
		if (this.keywordGeneratorEngineId != null
				&& !(this.keywordGeneratorEngineId = this.keywordGeneratorEngineId.trim()).isEmpty()) {
			// pull the model smss if needed
			Utility.getModel(this.keywordGeneratorEngineId);
			this.smssProp.put(Constants.KEYWORD_ENGINE_ID, this.keywordGeneratorEngineId);
		} else {
			// add it to the smss prop so the string substitution does not fail
			this.smssProp.put(Constants.KEYWORD_ENGINE_ID, "");
		}

		if (this.smssProp.getProperty(Constants.REMOVE_DOCS_FLAG) != null) {
			this.removeDocsFlag = Boolean.valueOf(this.smssProp.getProperty(Constants.REMOVE_DOCS_FLAG));
		}

		for (Object smssKey : this.smssProp.keySet()) {
			String key = smssKey.toString();
			this.vars.put(key, this.smssProp.getProperty(key));
		}

		this.modelPropsLoaded = true;
	}

	@Override
	public List<FileEmbeddingStatus> addEmbeddings(List<String> vectorCsvFiles, Insight insight,
			Map<String, Object> parameters) throws Exception {
		List<FileEmbeddingStatus> fileStatusList = new ArrayList<>();
		for (String vectorCsvFile : vectorCsvFiles) {
			VectorDatabaseCSVTable vectorCsvTable = VectorDatabaseCSVTable.initCSVTable(new File(vectorCsvFile));
			fileStatusList = addEmbeddings(vectorCsvTable, insight, parameters);
		}
		return fileStatusList;
	}

	@Override
	public List<FileEmbeddingStatus> addEmbeddings(String vectorCsvFile, Insight insight,
			Map<String, Object> parameters) throws Exception {
		VectorDatabaseCSVTable vectorCsvTable = VectorDatabaseCSVTable.initCSVTable(new File(vectorCsvFile));
		return addEmbeddings(vectorCsvTable, insight, parameters);
	}

	@Override
	public List<FileEmbeddingStatus> addEmbeddingFiles(List<File> vectorCsvFiles, Insight insight,
			Map<String, Object> parameters) throws Exception {
		List<FileEmbeddingStatus> fileStatusList = new ArrayList<>();
		for (File vectorCsvFile : vectorCsvFiles) {
			try {
				VectorDatabaseCSVTable vectorCsvTable = VectorDatabaseCSVTable.initCSVTable(vectorCsvFile);
				fileStatusList = addEmbeddings(vectorCsvTable, insight, parameters);
			} catch (Exception e) {
				classLogger.error("Failed to add embeddings from CSV file: {}", vectorCsvFile.getAbsolutePath(), e);
				// File failed completely
				String errorMessage = "Embedding failed for " + vectorCsvFile.getName();
				if (e.getMessage() != null && !e.getMessage().isEmpty()) {
					errorMessage = errorMessage + ": " + e.getMessage();
				}
				fileStatusList.add(new FileEmbeddingStatus(vectorCsvFile.getName(), "FAILED", 0, 0, 0,
						buildEmbeddingError(errorMessage, e)));
			}
		}
		return fileStatusList;
	}

	/**
	 * 
	 * @param message
	 * @param e
	 * @return
	 */
	private Map<String, Object> buildEmbeddingError(String message, Exception e) {
		Map<String, Object> error = new HashMap<>();
		error.put(Constants.ERROR_MESSAGE, message != null ? message : "Embedding failed");
		if (e != null) {
			error.put(Constants.TECH_ERROR_MESSAGE, e.toString());
		}
		return error;
	}

	@Override
	public List<FileEmbeddingStatus> addEmbeddingFile(File vectorCsvFile, Insight insight,
			Map<String, Object> parameters) throws Exception {
		VectorDatabaseCSVTable vectorCsvTable = VectorDatabaseCSVTable.initCSVTable(vectorCsvFile);
		return addEmbeddings(vectorCsvTable, insight, parameters);
	}

	@Override
	public List<FileEmbeddingStatus> addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight,
			Map<String, Object> parameters) throws Exception {
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}

		if (!modelPropsLoaded) {
			verifyModelProps();
		}

		// if we were able to extract files, begin embeddings process
		IModelEngine embeddingsEngine = Utility.getModel(this.embedderEngineId);
		// send all the strings to embed in one shot
		try {
			vectorCsvTable.generateAndAssignEmbeddings(embeddingsEngine, insight);
		} catch (Exception e) {
			classLogger.error("Failed to generate embeddings for CSV data using model engine: {}",
					this.embedderEngineId, e);
			throw new IllegalArgumentException(
					"Error occurred creating the embeddings for the generated chunks. Detailed error message = "
							+ e.getMessage());
		}

		String psString = "INSERT INTO " + this.vectorTableName
				+ " (EMBEDDING, SOURCE, MODALITY, DIVIDER, PART, TOKENS, CONTENT) " + "VALUES (?,?,?,?,?,?,?)";

		// Track insert status per file
		Map<String, Integer> fileRecordCountMap = new HashMap<>();
		Map<String, Integer> fileInsertedCountMap = new HashMap<>();

		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = this.getConnection();
			PGvector.addVectorType(conn);
			ps = conn.prepareStatement(psString);

//			if (parameters.containsKey(VectorDatabaseParamOptionsEnum.KEYWORD_SEARCH_PARAM.getKey())) {
//				IModelEngine keywordEngine = Utility.getModel(this.keywordGeneratorEngineId);
//				dataForTable.setKeywordEngine(keywordEngine);
//			}

			final int batchSize = 1000;
			int count = 0;
			for (VectorDatabaseCSVRow row : vectorCsvTable.getRows()) {
				int index = 1;
				ps.setObject(index++, new PGvector(row.getEmbeddings()));
				ps.setString(index++, row.getSource());
				ps.setString(index++, row.getModality());
				ps.setString(index++, row.getDivider());
				ps.setString(index++, row.getPart());
				ps.setInt(index++, row.getTokens());
				ps.setString(index++, row.getContent());
				ps.addBatch();

				fileRecordCountMap.put(row.getSource(), fileRecordCountMap.getOrDefault(row.getSource(), 0) + 1);
				// batch commit based on size
				if (++count % batchSize == 0) {
					classLogger.info("Executing embeddings batch .... row num = {}", count);
					int[] results = ps.executeBatch();
					updateInsertCounts(results, vectorCsvTable, fileInsertedCountMap);
				}
			}

			// well, we are done looping through now
			classLogger.info("Executing final embeddings batch .... row num = {}", count);
			int[] results = ps.executeBatch();
			updateInsertCounts(results, vectorCsvTable, fileInsertedCountMap);
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to insert embeddings into table: {}", this.vectorTableName, e);
			throw e;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(this, conn, ps, null);
		}

		if (parameters != null && parameters.containsKey(AbstractVectorDatabaseEngine.METADATA)) {
			Map<String, Map<String, Object>> metadata = (Map<String, Map<String, Object>>) parameters
					.get(AbstractVectorDatabaseEngine.METADATA);
			if (!metadata.isEmpty()) {
				String tempMetadataFile = insight.getInsightFolder() + "/metadata" + Utility.getRandomString(6)
						+ ".csv";
				VectorDatabaseMetadataCSVWriter writer = new VectorDatabaseMetadataCSVWriter(tempMetadataFile);
				writer.bulkWriteRow(metadata);
				try {
					addMetadata(VectorDatabaseMetadataCSVTable.initCSVTable(new File(tempMetadataFile)));
				} catch (SQLException | IOException e) {
					classLogger.error("Failed to add metadata rows to table: {}", this.vectorTableMetadataName, e);
					throw e;
				}
			}
		}
		// Generate file-wise embedding status
		List<FileEmbeddingStatus> fileStatusList = new ArrayList<>();
		for (Map.Entry<String, Integer> entry : fileRecordCountMap.entrySet()) {
			String file = entry.getKey();
			int total = entry.getValue();
			int inserted = fileInsertedCountMap.getOrDefault(file, 0);
			int failed = total - inserted;

			String status = inserted == total ? "SUCCESS" : inserted == 0 ? "FAILED" : "PARTIAL";

			fileStatusList.add(new FileEmbeddingStatus(file, status, inserted, failed, total));
		}

		return fileStatusList;
	}

	/**
	 * Method to update file-inserted counts
	 * 
	 * @param results
	 * @param table
	 * @param fileInsertedCountMap
	 */
	private void updateInsertCounts(int[] results, VectorDatabaseCSVTable table,
			Map<String, Integer> fileInsertedCountMap) {
		List<VectorDatabaseCSVRow> rows = table.getRows();
		for (int i = 0; i < results.length; i++) {
			VectorDatabaseCSVRow row = rows.get(i);
			String source = row.getSource();
			if (results[i] != PreparedStatement.EXECUTE_FAILED) {
				fileInsertedCountMap.put(source, fileInsertedCountMap.getOrDefault(source, 0) + 1);
			}
		}
	}

	@Override
	public void addEmbedding(List<? extends Number> embedding, String source, String modality, String divider,
			String part, int tokens, String content, Map<String, Object> additionalMetadata) throws SQLException {
		// just do the insertion
		String psString = "INSERT INTO " + this.vectorTableName
				+ " (EMBEDDING, SOURCE, MODALITY, DIVIDER, PART, TOKENS, CONTENT) " + "VALUES (?,?,?,?,?,?,?)";

		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = this.getConnection();
			ps = conn.prepareStatement(psString);

			int index = 1;
			ps.setObject(index++, new PGvector(embedding));
			ps.setString(index++, source);
			ps.setString(index++, modality);
			ps.setString(index++, divider);
			ps.setString(index++, part);
			ps.setInt(index++, tokens);
			ps.setString(index++, content);

			int result = ps.executeUpdate();
			if (result == PreparedStatement.EXECUTE_FAILED) {
				throw new SQLException("Error inserting embeddings data");
			}

			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to insert embedding row into table: {}", this.vectorTableName, e);
			throw e;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(this, conn, ps, null);
		}
	}

	@Override
	public void removeDocument(List<String> fileNames, Map<String, Object> parameters) throws IOException {

		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}

		List<String> sourceNames = new ArrayList<>();
		for (String document : fileNames) {
			String documentName = FilenameUtils.getName(document);
			File f = new File(document);
			if (f.exists() && f.getName().endsWith(".csv")) {
				sourceNames.addAll(VectorDatabaseCSVTable.pullSourceColumn(f));
			} else {
				sourceNames.add(documentName);
			}
		}

		final String DOCUMENT_FOLDER = this.schemaFolder.getAbsolutePath() + "/" + indexClass + "/"
				+ AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME;
		List<String> filesToRemoveFromCloud = new ArrayList<String>();

		String deleteQuery = "DELETE FROM " + this.vectorTableName + " WHERE SOURCE=?";
		String deleteMetaQuery = "DELETE FROM " + this.vectorTableMetadataName + " WHERE SOURCE=?";
		Connection conn = null;
		PreparedStatement ps = null;
		PreparedStatement metaPs = null;
		int[] results = null;
		try {
			conn = this.getConnection();
			ps = conn.prepareStatement(deleteQuery);
			metaPs = conn.prepareStatement(deleteMetaQuery);
			for (String document : sourceNames) {
				String documentName = Paths.get(document).getFileName().toString();
				// remove the physical documents
				File documentFile = new File(DOCUMENT_FOLDER, documentName);
				if (documentFile.exists()) {
					FileUtils.forceDelete(documentFile);
					filesToRemoveFromCloud.add(documentFile.getAbsolutePath());
				}

				// remove the results from the db
				int parameterIndex = 1;
				ps.setString(parameterIndex++, documentName);
				ps.addBatch();

				parameterIndex = 1;
				metaPs.setString(parameterIndex++, documentName);
				metaPs.addBatch();
			}
			results = ps.executeBatch();
			// since metadata is optional
			// its fine if no rows updated
			metaPs.executeBatch();

			for (int j = 0; j < results.length; j++) {
				if (results[j] == PreparedStatement.EXECUTE_FAILED) {
					throw new IllegalArgumentException("Error removing data for row " + j);
				}
			}

			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to remove documents from PGVector for index class: {}", indexClass, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(this, conn, ps, metaPs);
		}

		if (ClusterUtil.IS_CLUSTER) {
			Thread.ofVirtual().start(new DeleteFilesFromEngineRunner(engineId, this.getCatalogType(),
					filesToRemoveFromCloud.stream().toArray(String[]::new)));
		}
	}

	@Override
	public void addMetadata(VectorDatabaseMetadataCSVTable metadataTable) throws SQLException {
		String psString = "INSERT INTO " + this.vectorTableMetadataName
				+ " (SOURCE, ATTRIBUTE, STR_VALUE, INT_VALUE, NUM_VALUE, BOOL_VALUE, DATE_VAL, TIMESTAMP_VAL) "
				+ "VALUES (?,?,?,?,?,?,?,?)";

		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = this.getConnection();
			ps = conn.prepareStatement(psString);

			final int batchSize = 1000;

			int count = 0;
			for (VectorDatabaseMetadataCSVRow row : metadataTable.getRows()) {
				int index = 1;
				ps.setString(index++, row.getSource());
				ps.setString(index++, row.getAttribute());
				if (row.getStrValue() == null) {
					ps.setNull(index++, java.sql.Types.VARCHAR);
				} else {
					ps.setString(index++, row.getStrValue());
				}
				if (row.getIntValue() == null) {
					ps.setNull(index++, java.sql.Types.INTEGER);
				} else {
					ps.setInt(index++, row.getIntValue());
				}
				if (row.getNumValue() == null) {
					ps.setNull(index++, java.sql.Types.DOUBLE);
				} else {
					ps.setDouble(index++, row.getNumValue().doubleValue());
				}
				if (row.getBoolValue() == null) {
					ps.setNull(index++, java.sql.Types.BOOLEAN);
				} else {
					ps.setBoolean(index++, row.getBoolValue());
				}
				if (row.getDateValue() == null) {
					ps.setNull(index++, java.sql.Types.DATE);
				} else {
					ps.setDate(index++, java.sql.Date.valueOf(row.getDateValue().getZonedDateTime().toLocalDate()));
				}
				if (row.getTimestampValue() == null) {
					ps.setNull(index++, java.sql.Types.TIMESTAMP);
				} else {
					ps.setTimestamp(index++,
							java.sql.Timestamp.from(row.getDateValue().getZonedDateTime().toInstant()));
				}

				ps.addBatch();

				// batch commit based on size
				if (++count % batchSize == 0) {
					classLogger.info("Executing metadata batch .... row num = {}", count);
					int[] results = ps.executeBatch();
					for (int j = 0; j < results.length; j++) {
						if (results[j] == PreparedStatement.EXECUTE_FAILED) {
							throw new SQLException("Error inserting data for row " + j);
						}
					}
				}
			}

			// well, we are done looping through now
			classLogger.info("Executing final metadata batch .... row num = {}", count);
			int[] results = ps.executeBatch();
			for (int j = 0; j < results.length; j++) {
				if (results[j] == PreparedStatement.EXECUTE_FAILED) {
					throw new SQLException("Error inserting metadata data for row " + j);
				}
			}

			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to insert metadata rows into table: {}", this.vectorTableMetadataName, e);
			throw e;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(this, conn, ps, null);
		}
	}

	public List<Map<String, Object>> nearestNeighborCall(Insight insight, String searchStatement, Number limit,
			Map<String, Object> parameters) {
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}

		if (!this.modelPropsLoaded) {
			verifyModelProps();
		}

		List<IQueryFilter> filters = null;
		List<IQueryFilter> metaFilters = null;
		if (parameters.containsKey(AbstractVectorDatabaseEngine.FILTERS_KEY)) {
			filters = PGVectorQueryFitlerTranslationHelper.convertFilters(
					(List<IQueryFilter>) parameters.get(AbstractVectorDatabaseEngine.FILTERS_KEY),
					this.vectorTableName);
		}
		if (parameters.containsKey(AbstractVectorDatabaseEngine.METADATA_FILTERS_KEY)) {
			metaFilters = PGVectorQueryMetaFitlerTranslationHelper.convertFilters(
					(List<IQueryFilter>) parameters.get(AbstractVectorDatabaseEngine.METADATA_FILTERS_KEY),
					this.vectorTableMetadataName);
		}

		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.COLUMNS_TO_RETURN.getKey())) {
		}

		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.RETURN_THRESHOLD.getKey())) {
		}

		IModelEngine engine = Utility.getModel(this.embedderEngineId);
		EmbeddingsModelEngineResponse embeddingsResponse = engine
				.embeddings(Arrays.asList(new String[] { searchStatement }), insight, null);

		final String tablePrefix = this.vectorTableName + "__";
//		final String metaTablePrefix = this.vectorTableMetadataName+"__";

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(
				new QueryColumnSelector(tablePrefix + VectorDatabaseCSVTable.SOURCE, VectorDatabaseCSVTable.SOURCE));
		qs.addSelector(new QueryColumnSelector(tablePrefix + VectorDatabaseCSVTable.MODALITY,
				VectorDatabaseCSVTable.MODALITY));
		qs.addSelector(
				new QueryColumnSelector(tablePrefix + VectorDatabaseCSVTable.DIVIDER, VectorDatabaseCSVTable.DIVIDER));
		qs.addSelector(new QueryColumnSelector(tablePrefix + VectorDatabaseCSVTable.PART, VectorDatabaseCSVTable.PART));
		qs.addSelector(
				new QueryColumnSelector(tablePrefix + VectorDatabaseCSVTable.TOKENS, VectorDatabaseCSVTable.TOKENS));
		qs.addSelector(
				new QueryColumnSelector(tablePrefix + VectorDatabaseCSVTable.CONTENT, VectorDatabaseCSVTable.CONTENT));
		// Determine the distanceMethod to use for the query
		// Store the result in the "Score" field,
		if ("Cosine Similarity".equalsIgnoreCase(distanceMethod)) {
			// '<=>' cosine similarity operator
			// cosine distance is between -1 and 1
			// Using 1 - cosine distance converts the distance metric into a similarity
			// metric.
			// 1 = identical
			// 0 = orthogonal
			// -1 = opposite
			// so need to show results as desc
			qs.addSelector(new QueryOpaqueSelector(
					"1 - (EMBEDDING <=> '" + embeddingsResponse.getResponse().get(0) + "')", "Score"));
			// This allows us to sort results by similarity in descending order
			// (from most similar to least similar).
			qs.addOrderBy("Score", "DESC");
		} else {
			// '<->' Euclidean (L2) distance operator
			// The POWER function is used to square the distance to avoid the computational
			// cost of square roots
			// This also ensures all distance values are non-negative, which is important
			// for optimization
			qs.addSelector(new QueryOpaqueSelector(
					"POWER((EMBEDDING <-> '" + embeddingsResponse.getResponse().get(0) + "'),2)", "Score"));
			qs.addOrderBy("Score", "ASC");
		}
		if (filters != null && !filters.isEmpty()) {
			qs.addExplicitFilter(new GenRowFilters(filters), true);
		}
		if (metaFilters != null && !metaFilters.isEmpty()) {
			// also need the join
			qs.addRelation(this.vectorTableName, this.vectorTableMetadataName, "inner.join");
			qs.addExplicitFilter(new GenRowFilters(metaFilters), true);
		}
		if (limit != null) {
			qs.setLimit(limit.longValue());
		}

		List<Map<String, Object>> vectorSearchResults = QueryExecutionUtility.flushRsToMap(this, qs);
		return vectorSearchResults;
	}

	@Override
	public List<Map<String, Object>> listDocuments(Map<String, Object> parameters) {
		final String tablePrefix = this.vectorTableName + "__";
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(tablePrefix + VectorDatabaseCSVTable.SOURCE, "fileName"));
		List<Map<String, Object>> sourcesInPostgresDb = QueryExecutionUtility.flushRsToMap(this, qs);

		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}

		File documentsDir = new File(this.schemaFolder.getAbsolutePath() + "/" + indexClass + "/"
				+ AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME);
		if (documentsDir.exists() && documentsDir.isDirectory()) {
			for (Map<String, Object> fileInPostgresDb : sourcesInPostgresDb) {
				String fileName = (String) fileInPostgresDb.get("fileName");

				File thisF = new File(documentsDir, fileName);
				if (thisF.exists() && thisF.isFile()) {
					long fileSizeInBytes = thisF.length();
					double fileSizeInMB = (double) fileSizeInBytes / (1024);
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					String lastModified = dateFormat.format(new Date(thisF.lastModified()));

					// add file size and last modified into the map
					fileInPostgresDb.put("fileSize", fileSizeInMB);
					fileInPostgresDb.put("lastModified", lastModified);
				}
			}
		}

		return sourcesInPostgresDb;
	}

	@Override
	public List<Map<String, Object>> listAllRecords(Map<String, Object> parameters) {
		final String tablePrefix = this.vectorTableName + "__";
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(
				new QueryColumnSelector(tablePrefix + VectorDatabaseCSVTable.SOURCE, VectorDatabaseCSVTable.SOURCE));
		qs.addSelector(new QueryColumnSelector(tablePrefix + VectorDatabaseCSVTable.MODALITY,
				VectorDatabaseCSVTable.MODALITY));
		qs.addSelector(
				new QueryColumnSelector(tablePrefix + VectorDatabaseCSVTable.DIVIDER, VectorDatabaseCSVTable.DIVIDER));
		qs.addSelector(new QueryColumnSelector(tablePrefix + VectorDatabaseCSVTable.PART, VectorDatabaseCSVTable.PART));
		qs.addSelector(
				new QueryColumnSelector(tablePrefix + VectorDatabaseCSVTable.TOKENS, VectorDatabaseCSVTable.TOKENS));
		qs.addSelector(
				new QueryColumnSelector(tablePrefix + VectorDatabaseCSVTable.CONTENT, VectorDatabaseCSVTable.CONTENT));
		// order by
		qs.addOrderBy(tablePrefix + VectorDatabaseCSVTable.SOURCE);
		qs.addOrderBy(tablePrefix + VectorDatabaseCSVTable.DIVIDER);
		qs.addOrderBy(tablePrefix + VectorDatabaseCSVTable.PART);
		return QueryExecutionUtility.flushRsToMap(this, qs);
	}

	@Override
	public IEngine.CATALOG_TYPE getCatalogType() {
		return IEngine.CATALOG_TYPE.VECTOR;
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return this.getVectorDatabaseType().toString();
	}

	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.PGVECTOR;
	}

	@Override
	public void close() throws IOException {
		this.modelPropsLoaded = false;
		if (this.cpw != null) {
			this.cpw.shutdown(true);
		}
		super.close();
	}

	/**
	 * Methods below should really be an exact match to the same method names
	 * 
	 */

	/**
	 * 
	 * @return
	 */
	@Override
	public boolean keepInputOutput() {
		return this.keepInputOutput;
	}

	/**
	 * This method is meant to be overriden so that we dont need to copy/paste the
	 * startServer code for every implementation
	 * 
	 * @return
	 */
	private String[] getServerStartCommands() {
		return (AbstractVectorDatabaseEngine.TOKENIZER_INIT_SCRIPT).split(PyUtils.PY_COMMAND_SEPARATOR);
	}

	/**
	 * 
	 * @param port
	 */
	private void startServer(int port) {
		this.startServerLock.lock();
		try {
			// already created by another thread
			if (this.cpw != null && this.cpw.getSocketClient() != null && this.cpw.getSocketClient().isConnected()) {
				return;
			}
			if (!modelPropsLoaded) {
				verifyModelProps();
			}
			if (!this.pyDirectoryBasePath.exists()) {
				this.pyDirectoryBasePath.mkdirs();
			}

			// check if we have already created a process wrapper
			ClientProcessWrapper cpwToInit = new ClientProcessWrapper();
			if (this.cpw != null) {
				this.cpw.shutdown(false);
			}

			String timeout = "30";
			if (this.smssProp.containsKey(Constants.IDLE_TIMEOUT)) {
				timeout = this.smssProp.getProperty(Constants.IDLE_TIMEOUT);
			}

			boolean debug = false;

			// pull the relevant values from the smss
			String forcePort = this.smssProp.getProperty(Settings.FORCE_PORT);
			String customClassPath = this.smssProp.getProperty("TCP_WORKER_CP");
			String loggerLevel = this.smssProp.getProperty(Settings.LOGGER_LEVEL, "WARNING");
			String venvEngineId = this.smssProp.getProperty(Constants.VIRTUAL_ENV_ENGINE, null);
			String venvPath = venvEngineId != null ? Utility.getVenvEngine(venvEngineId).pathToExecutable() : null;

			if (port < 0) {
				// port has not been forced
				if (forcePort != null && !(forcePort = forcePort.trim()).isEmpty()) {
					try {
						port = Integer.parseInt(forcePort);
						debug = true;
					} catch (NumberFormatException e) {
						// ignore
						classLogger.warn("Vector Database {} has an invalid FORCE_PORT value", this.engineName);
					}
				}
			}

			// if we have a python specific user, make sure that user can access the schema
			// folder
			setVectorFolderPermissions();

			String serverDirectory = this.pyDirectoryBasePath.getAbsolutePath();
			// it has to be -- don't change this unless you can send engine calls from
			// python
			boolean nativePyServer = true;
			try {
				cpwToInit.createProcessAndClient(nativePyServer, null, port, venvPath, serverDirectory, customClassPath,
						debug, timeout, loggerLevel);
			} catch (Exception e) {
				classLogger.error("Failed to create python process client for PGVector database: {}",
						SmssUtilities.getUniqueName(this.engineName, this.engineId), e);
				throw new IllegalArgumentException("Unable to connect to server for pgvector databse.");
			}

			// create the py translator
			Insight processInsight = new Insight();
			InsightStore.getInstance().put(processInsight);
			this.pyTranslator = new PyTranslator(cpwToInit.getSocketClient(), processInsight);

			try {
				String[] commands = getServerStartCommands();
				// replace the vars
				StringSubstitutor substitutor = new StringSubstitutor(this.vars);
				for (int commandIndex = 0; commandIndex < commands.length; commandIndex++) {
					String resolvedString = substitutor.replace(commands[commandIndex]);
					commands[commandIndex] = resolvedString;
				}
				pyTranslator.runEmptyPyNoCancelTrace(commands);

				// for debugging...
				classLogger.info("Initializing {} python process with commands >>> {}",
						SmssUtilities.getUniqueName(this.engineName, this.engineId), String.join("\n", commands));

				// finally set the cpw in the class
				this.cpw = cpwToInit;
			} catch (Exception e) {
				// set the model props to false
				// incase those values were incorrect
				modelPropsLoaded = false;
				classLogger.error("Failed to initialize python start commands for PGVector database: {}",
						SmssUtilities.getUniqueName(this.engineName, this.engineId), e);
				if (cpwToInit != null) {
					classLogger.warn(
							"Able to start the python process for the vector database {} but the start script failed.",
							SmssUtilities.getUniqueName(this.engineName, this.engineId));
					cpwToInit.shutdown(false);
				}
				throw e;
			}
		} finally {
			this.startServerLock.unlock();
		}
	}

	private void checkSocketStatus() {
		if (this.cpw == null || this.cpw.getSocketClient() == null || !this.cpw.getSocketClient().isConnected()) {
			this.startServer(-1);
		}
	}

	@Override
	public List<FileEmbeddingStatus> addDocument(List<String> filePaths, Map<String, Object> parameters)
			throws Exception {
		List<FileEmbeddingStatus> fileStatusList = new ArrayList<>();
		if (!modelPropsLoaded) {
			verifyModelProps();
		}

		checkSocketStatus();

		if (removeDocsFlag) {
			try {
				this.removeDocument(filePaths, parameters);
			} catch (Exception ignore) {
				// we are only removing just in case
				// if something doesn't exist, just ignore the exception
			}
		}

		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}

		int chunkMaxTokenLength = this.contentLength;
		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.CONTENT_LENGTH.getKey())) {
			chunkMaxTokenLength = (int) parameters.get(VectorDatabaseParamOptionsEnum.CONTENT_LENGTH.getKey());
		}

		int tokenOverlapBetweenChunks = this.contentOverlap;
		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.CONTENT_OVERLAP.getKey())) {
			tokenOverlapBetweenChunks = (int) parameters.get(VectorDatabaseParamOptionsEnum.CONTENT_OVERLAP.getKey());
		}

		String chunkUnit = this.defaultChunkUnit;
		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.CHUNK_UNIT.getKey())) {
			chunkUnit = (String) parameters.get(VectorDatabaseParamOptionsEnum.CHUNK_UNIT.getKey());
		}

//		String extractionMethod = this.defaultExtractionMethod;
//		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.EXTRACTION_METHOD.getKey())) {
//			extractionMethod = (String) parameters.get(VectorDatabaseParamOptionsEnum.EXTRACTION_METHOD.getKey());
//		}

		Insight insight = getInsight(parameters.get(Constants.INSIGHT));
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}

		File indexFilesFolder = new File(this.schemaFolder + "/" + indexClass,
				AbstractVectorDatabaseEngine.INDEXED_FOLDER_NAME);
		// store the actual files we are extracting from
		// since we move this into the vector folder
		// we need to delete them if they fail
		// TODO: potentially look at loading these from insight and only pushing to the
		// vector db catalog on success
		List<File> movedDocuments = new ArrayList<>();
		try {
			// first we need to extract the text from the document
			File indexDirectory = new File(this.schemaFolder, indexClass);
			File documentDir = new File(indexDirectory, AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME);
			if (!documentDir.exists()) {
				documentDir.mkdirs();
			}

			if (!indexFilesFolder.exists()) {
				indexFilesFolder.mkdirs();
			}
			if (!this.indexClasses.contains(indexClass)) {
				addIndexClass(indexClass);
			}

			List<File> extractedFiles = new ArrayList<>();
			// create a list to store all the net new files so we
			// can push them to the cloud
			List<String> filesToCopyToCloud = new ArrayList<>();
			String chunkingStrategy = PyUtils.determineStringType(parameters.getOrDefault("chunkingStrategy", "ALL"));

			// move the documents from insight into documents folder
			for (String fileName : filePaths) {
				File fileInInsightFolder = new File(Utility.normalizePath(fileName));

				// Double check that they are files and not directories
				if (!fileInInsightFolder.isFile()) {
					continue;
				}

				File destinationFile = new File(documentDir, fileInInsightFolder.getName());

				// Check if the destination file exists, and if so, delete it
				try {
					if (destinationFile.exists()) {
						FileUtils.forceDelete(destinationFile);
					}
					FileUtils.moveFileToDirectory(fileInInsightFolder, documentDir, true);
				} catch (IOException e) {
					classLogger.error("Failed to move document '{}' to vector documents directory '{}'",
							fileInInsightFolder.getAbsolutePath(), documentDir.getAbsolutePath(), e);
					throw new IllegalArgumentException("Unable to remove previously created file for "
							+ destinationFile.getName() + " or move it to the document directory");
				}

				movedDocuments.add(destinationFile);
				filesToCopyToCloud.add(destinationFile.getAbsolutePath());

				String documentName = FilenameUtils.getBaseName(destinationFile.getName());
				File extractedFile = new File(indexFilesFolder.getAbsolutePath() + "/" + documentName + ".csv");
				String extractedFileName = extractedFile.getAbsolutePath().replace("\\", "/");
				try {
					if (extractedFile.exists()) {
						FileUtils.forceDelete(extractedFile);
					}
					String docLower = destinationFile.getName().toLowerCase();

					if (docLower.endsWith(".csv")) {
						classLogger.info(
								"You are attempting to load in a structured table for {}. Hopefully the structure is the right format we expect",
								documentName);
						// copy csv over
						FileUtils.copyFileToDirectory(destinationFile, indexFilesFolder);
					} else {
						classLogger.info("Extracting text from document {}", documentName);
						// determine which text extraction method to use
						boolean processed = false;
						int rowsCreated = -1;
						if (this.customDocumentProcessor) {
							if (this.customDocumentProcessorFunctionID == null
									|| this.customDocumentProcessorFunctionID.isEmpty()) {
								throw new IllegalArgumentException(
										"Must define custom document processing function engine id in the SMSS");
							}
							if (this.customDocumentProcessorFunctionID == null
									|| this.customDocumentProcessorFunctionID.isEmpty()) {
								throw new IllegalArgumentException(
										"Must define custom document processing function engine id in the SMSS");
							}
							IFunctionEngine functionEngine = Utility
									.getFunctionEngine(this.customDocumentProcessorFunctionID);
							if (!(functionEngine instanceof ICustomEmbeddingsFunctionEngine)) {
								throw new IllegalArgumentException(
										"Vector Database owner has incorrectly setup a custom embeddings function that is not an ICustomEmbeddingsFunctionEngine");
							}
							ICustomEmbeddingsFunctionEngine customEmbeddings = (ICustomEmbeddingsFunctionEngine) functionEngine;
							if (customEmbeddings.canProcessDocument(destinationFile)) {
								parameters.put(Constants.CUSTOM_DOCUMENT_PROCESSOR_USE_STORAGE,
										this.customDocumentProcessorNeedStorage);
								rowsCreated = customEmbeddings.processDocument(extractedFile.getAbsolutePath(),
										destinationFile, parameters);
								processed = true;
							}
						}

						if (!processed) {
							rowsCreated = VectorDatabaseUtils.convertFilesToCSV(extractedFile.getAbsolutePath(),
									destinationFile);
						}

						// check to see if the file data was extracted
						if (rowsCreated < 1) {
							// no text was extracted so delete the file
							FileUtils.forceDelete(extractedFile); // delete the csv
							FileUtils.forceDelete(destinationFile); // delete the input file e.g pdf
							filesToCopyToCloud.remove(destinationFile.getAbsolutePath());
							continue;
						}

						classLogger.info("Creating chunks from extracted text for {}", documentName);

						StringBuilder splitTextCommand = new StringBuilder();
						splitTextCommand.append("vector_database.split_text(csv_file_location = '")
								.append(extractedFileName).append("', chunk_unit = '").append(chunkUnit)
								.append("', chunk_size = ").append(chunkMaxTokenLength).append(", chunk_overlap = ")
								.append(tokenOverlapBetweenChunks).append(", chunking_strategy = ")
								.append(chunkingStrategy).append(", cfg_tokenizer = cfg_tokenizer)");

						pyTranslator.runScriptNoCancelTrace(splitTextCommand.toString());
					}

					extractedFiles.add(extractedFile);
				} catch (Exception e) {
					String errorMessage = "Unable to process document " + destinationFile.getName();
					classLogger.error("Failed to process document: {}", destinationFile.getName(), e);
					fileStatusList.add(new FileEmbeddingStatus(destinationFile.getName(), "FAILED", 0, 0, 0,
							buildEmbeddingError(errorMessage, e)));
					extractedFile.delete(); // delete the csv if it was created
					destinationFile.delete(); // delete the input file e.g pdf
				}
			}

			if (extractedFiles.size() == 0) {
				StringBuilder fileNamesAttemptedUpload = new StringBuilder("[");
				boolean first = true;
				for (File document : movedDocuments) {
					if (!first) {
						fileNamesAttemptedUpload.append(",");
					}
					fileNamesAttemptedUpload.append(FilenameUtils.getName(document.getName()));
				}
				fileNamesAttemptedUpload.append("]");
				throw new IllegalArgumentException("Unable to extract any text from " + fileNamesAttemptedUpload);
			}

			fileStatusList.addAll(addEmbeddingFiles(extractedFiles, insight, parameters));

			if (ClusterUtil.IS_CLUSTER) {
				// push the actual documents over to the cloud
				Thread.ofVirtual().start(new CopyFilesToEngineRunner(this.engineId, this.getCatalogType(),
						filesToCopyToCloud.stream().toArray(String[]::new)));
			}
		} catch (Exception e) {
			classLogger.error("Failed to add documents to PGVector for index class: {}", indexClass, e);
			// delete files moved into vector db documents folder
			for (File document : movedDocuments) {
				document.delete();
			}
			throw e;
		} finally {
			cleanUpAddDocument(indexFilesFolder);
		}
		return fileStatusList;
	}

	@Override
	public List<Map<String, Object>> nearestNeighbor(Insight insight, String searchStatement, Number limit,
			Map<String, Object> parameters) {
		if (parameters == null) {
			parameters = new HashMap<String, Object>();
		}

		ZonedDateTime inputTime = ZonedDateTime.now();
		List<Map<String, Object>> vectorSearchResponse = nearestNeighborCall(insight, searchStatement, limit,
				parameters);
		ZonedDateTime outputTime = ZonedDateTime.now();

		// @formatter:off
		if (inferenceLogsEnbaled && this.keepInputOutput) {
			String messageId = GUID.v7().toUUID().toString();
			Thread inferenceRecorder = new Thread(new ModelEngineInferenceLogsWorker (
					/*messageId*/messageId, 
					/*transactionId*/messageId, 
					/*messageMethod*/"nearestNeighbor", 
					/*engine*/this, 
					/*insightId*/insight.getInsightId(),
					/*projectContextId*/insight.getContextProjectId(),
					/*projectId*/insight.getProjectId(),
					/*user*/insight.getUser(),
					/*sessionId*/ThreadStore.getSessionId(),
					/*roomId*/ThreadStore.getInsightId(),
					/*context*/null, 
					/*prompt*/searchStatement,
					/*fullPrompt*/null,
					/*promptTokens*/null,
					/*inputTime*/inputTime, 
					/*response*/GSON.toJson(vectorSearchResponse),
					/*responseTokens*/null,
					/*outputTime*/outputTime
					));
			inferenceRecorder.start();
		}
		// @formatter:on

		return vectorSearchResponse;
	}

	protected void addIndexClass(String indexClass) {
		this.indexClasses.add(indexClass);
	}

	protected void cleanUpAddDocument(File indexFilesFolder) {
		try {
			FileUtils.forceDelete(indexFilesFolder);
		} catch (IOException e) {
			classLogger.error("Failed to clean up temporary add-document folder: {}",
					indexFilesFolder.getAbsolutePath(), e);
		}
	}

	/**
	 * 
	 * @param indexClass
	 * @return
	 */
	@Override
	public String getIndexFilesPath(String indexClass) {
		throw new IllegalArgumentException("Indexed files are not persisted for PGVector");
	}

	/**
	 * 
	 * @param indexClass
	 * @return
	 */
	@Override
	public String getDocumentsFilesPath(String indexClass) {
		if (indexClass == null || (indexClass = indexClass.trim()).isEmpty()) {
			indexClass = this.defaultIndexClass;
		}
		if (!this.indexClasses.contains(indexClass)) {
			throw new IllegalArgumentException("Unable to retieve document csv from a directory that does not exist");
		}
		return Utility.normalizePath(this.schemaFolder.getAbsolutePath() + "/" + indexClass + "/"
				+ AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME);
	}

	@Override
	public boolean userCanAccessEmbeddingModels(User user) {
		if (!modelPropsLoaded) {
			verifyModelProps();
		}

		if (this.embedderEngineId != null) {
			if (!SecurityEngineUtils.userCanViewEngine(user, this.embedderEngineId)) {
				throw new IllegalArgumentException("Embeddings model " + this.embedderEngineId
						+ " does not exist or user does not have access to this model");
			}
		}

		if (this.keywordGeneratorEngineId != null && !this.keywordGeneratorEngineId.trim().isEmpty()) {
			if (!SecurityEngineUtils.userCanViewEngine(user, this.keywordGeneratorEngineId)) {
				throw new IllegalArgumentException("Keyword model " + this.keywordGeneratorEngineId
						+ " does not exist or user does not have access to this model");
			}
		}

		return true;
	}

	private Insight getInsight(Object insightObj) {
		if (insightObj instanceof String) {
			return InsightStore.getInstance().get(insightObj);
		} else {
			return (Insight) insightObj;
		}
	}

	/**
	 * 
	 */
	private void setVectorFolderPermissions() {
		// if we have a python specific user, make sure that user can access the schema
		// folder
		String pythonUser = Utility.getDIHelperProperty(Settings.PY_SERVER_USER);
		if (pythonUser != null && !pythonUser.trim().isEmpty()) {
			try {
				Utility.setOwnerAndGroupPermissionsRecursively(this.schemaFolder);
			} catch (IOException e) {
				classLogger.error("Failed to set owner/group permissions on vector schema folder: {}",
						this.schemaFolder.getAbsolutePath(), e);
			} catch (InterruptedException e) {
				classLogger.error("Failed to set owner/group permissions on vector schema folder: {}",
						this.schemaFolder.getAbsolutePath(), e);
			}
		}
	}
}
