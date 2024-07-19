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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.pgvector.PGvector;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.CopyFilesToEngineRunner;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
import prerna.ds.py.PyUtils;
import prerna.ds.py.TCPPyTranslator;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
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
	
	private static final String DIR_SEPARATOR = "/";
	private static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	public static final String PGVECTOR_TABLE_NAME = "PGVECTOR_TABLE_NAME";
	public static final String PGVECTOR_METADATA_TABLE_NAME = "PGVECTOR_METADATA_TABLE_NAME";

	private int contentLength = 512;
	private int contentOverlap = 0;
	private String defaultChunkUnit;
	private String defaultExtractionMethod;
	private String defaultIndexClass;
	
	private String embedderEngineId = null;
	private String keywordGeneratorEngineId = null;
	
	private String vectorTableName = null;
	private String vectorTableMetadataName = null;
	private File schemaFolder;
	private	List<String> indexClasses;

	// python server
	private TCPPyTranslator pyt = null;
	private File pyDirectoryBasePath;
	private ClientProcessWrapper cpw = null;
	
	private boolean modelPropsLoaded = false;
	
	// string substitute vars
	private Map<String, String> vars = new HashMap<>();

	private PGVectorQueryUtil pgVectorQueryUtil = new PGVectorQueryUtil();

	// maintain details in the log database
	protected boolean keepInputOutput = false;
	protected boolean inferenceLogsEnbaled = Utility.isModelInferenceLogsEnabled();
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.vectorTableName = smssProp.getProperty(PGVECTOR_TABLE_NAME);
		if(this.vectorTableName == null || (this.vectorTableName=this.vectorTableName.trim()).isEmpty()) {
			throw new NullPointerException("Must define the vector db table name");
		}
		this.vectorTableMetadataName = smssProp.getProperty(PGVECTOR_METADATA_TABLE_NAME);
		if(this.vectorTableMetadataName == null || (this.vectorTableMetadataName=this.vectorTableMetadataName.trim()).isEmpty()) {
			this.vectorTableMetadataName = this.vectorTableName + "_METADATA";
		}
		
		String engineDir = EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, this.engineId, this.engineName);
		this.pyDirectoryBasePath = new File(Utility.normalizePath(engineDir + DIR_SEPARATOR + "py" + DIR_SEPARATOR));

		// This holds all the different "tables". The reason we want this is to easily and quickly grab the sub folders
		this.schemaFolder = new File(engineDir, "schema");
		if(!this.schemaFolder.exists()) {
			this.schemaFolder.mkdirs();
		}

		this.indexClasses = new ArrayList<>();
		
		Connection conn = null;
		try {
			conn = getConnection();
			PGvector.addVectorType(conn);
			initSQL(this.vectorTableName);
		} catch(SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
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
			if (!this.defaultChunkUnit.equals("tokens") && !this.defaultChunkUnit.equals("characters")){
	            throw new IllegalArgumentException("DEFAULT_CHUNK_UNIT should be either 'tokens' or 'characters'");
			}
		}
		
		this.defaultExtractionMethod = this.smssProp.getProperty(Constants.EXTRACTION_METHOD, "None");
		
		this.defaultIndexClass = "default";
		if (this.smssProp.containsKey(Constants.INDEX_CLASSES)) {
			this.defaultIndexClass = this.smssProp.getProperty(Constants.INDEX_CLASSES);
		}
	}
	
	/**
	 * 
	 * @param table
	 * @throws SQLException
	 */
	private void initSQL(String table) throws SQLException {
		String createTable = pgVectorQueryUtil.createEmbeddingsTable(table);
		//creating the default embeddings table
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			classLogger.info(">>>>> " + createTable);
			stmt.execute(createTable);
		} catch(SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SQLException("Unable to create the table " + table);
		} finally {
			if(this.dataSource != null) {
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
		// This is to get the Model Name and Max Token for an encoder -- we need this to verify chunks aren't getting truncated
		this.embedderEngineId = this.smssProp.getProperty(Constants.EMBEDDER_ENGINE_ID);
		if (this.embedderEngineId == null || (this.embedderEngineId=this.embedderEngineId.trim()).isEmpty()) {
			
			// check legacy key....
			this.embedderEngineId = this.smssProp.getProperty("ENCODER_ID");
			if (this.embedderEngineId == null || (this.embedderEngineId=this.embedderEngineId.trim()).isEmpty()) {
				throw new IllegalArgumentException("Must define the embedder engine id for this vector database using " + Constants.EMBEDDER_ENGINE_ID);
			}
			
			this.smssProp.put(Constants.EMBEDDER_ENGINE_ID, embedderEngineId);
		}
		
		IModelEngine modelEngine = Utility.getModel(embedderEngineId);
		if (modelEngine == null) {
			throw new NullPointerException("Could not find the defined embedder engine id for this vector database with value = " + this.embedderEngineId);
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
		if (this.keywordGeneratorEngineId != null && !(this.keywordGeneratorEngineId=this.keywordGeneratorEngineId.trim()).isEmpty()) {
			// pull the model smss if needed
			Utility.getModel(this.keywordGeneratorEngineId);
			this.smssProp.put(Constants.KEYWORD_ENGINE_ID, this.keywordGeneratorEngineId);
		} else {
			// add it to the smss prop so the string substitution does not fail
			this.smssProp.put(Constants.KEYWORD_ENGINE_ID, "");
		}
		
		for (Object smssKey : this.smssProp.keySet()) {
			String key = smssKey.toString();
			this.vars.put(key, this.smssProp.getProperty(key));
		}
		
		this.modelPropsLoaded = true;
	}
	
	@Override
	public void addEmbeddings(List<String> vectorCsvFiles, Insight insight, Map<String, Object> parameters) throws Exception {
		for(String vectorCsvFile : vectorCsvFiles) {
			VectorDatabaseCSVTable vectorCsvTable = VectorDatabaseCSVTable.initCSVTable(new File(vectorCsvFile));
			addEmbeddings(vectorCsvTable, insight, parameters);
		}
	}
	
	@Override
	public void addEmbeddings(String vectorCsvFile, Insight insight, Map<String, Object> parameters) throws Exception {
		VectorDatabaseCSVTable vectorCsvTable = VectorDatabaseCSVTable.initCSVTable(new File(vectorCsvFile));
		addEmbeddings(vectorCsvTable, insight, parameters);
	}
	
	@Override
	public void addEmbeddingFiles(List<File> vectorCsvFiles, Insight insight, Map<String, Object> parameters) throws Exception {
		for(File vectorCsvFile : vectorCsvFiles) {
			VectorDatabaseCSVTable vectorCsvTable = VectorDatabaseCSVTable.initCSVTable(vectorCsvFile);
			addEmbeddings(vectorCsvTable, insight, parameters);
		}
	}
	
	@Override
	public void addEmbeddingFile(File vectorCsvFile, Insight insight, Map<String, Object> parameters) throws Exception {
		VectorDatabaseCSVTable vectorCsvTable = VectorDatabaseCSVTable.initCSVTable(vectorCsvFile);
		addEmbeddings(vectorCsvTable, insight, parameters);
	}
	
	@Override
	public void addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight, Map<String, Object> parameters) throws SQLException {
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}
		
		if (!modelPropsLoaded) {
			verifyModelProps();
		}
		
		// if we were able to extract files, begin embeddings process
		IModelEngine embeddingsEngine = Utility.getModel(this.embedderEngineId);
		String psString = "INSERT INTO " 
				+ this.vectorTableName 
				+ " (EMBEDDING, SOURCE, MODALITY, DIVIDER, PART, TOKENS, CONTENT) "
				+ "VALUES (?,?,?,?,?,?,?)";
		
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = this.getConnection();
			ps = conn.prepareStatement(psString);
				
//			if (parameters.containsKey(VectorDatabaseParamOptionsEnum.KEYWORD_SEARCH_PARAM.getKey())) {
//				IModelEngine keywordEngine = Utility.getModel(this.keywordGeneratorEngineId);
//				dataForTable.setKeywordEngine(keywordEngine);
//			}

			final int batchSize = 1000;
			
			vectorCsvTable.generateAndAssignEmbeddings(embeddingsEngine, insight);
			int count = 0;
			for (VectorDatabaseCSVRow row: vectorCsvTable.getRows()) {
				int index = 1;
				ps.setObject(index++, new PGvector(row.getEmbeddings()));
				ps.setString(index++, row.getSource());
				ps.setString(index++, row.getModality());
				ps.setString(index++, row.getDivider());
				ps.setString(index++, row.getPart());
				ps.setInt(index++, row.getTokens());
				ps.setString(index++, row.getContent());
				ps.addBatch();
				
				// batch commit based on size
				if (++count % batchSize == 0) {
					classLogger.info("Executing batch .... row num = " + count);
					int[] results = ps.executeBatch();
					for(int j=0; j<results.length; j++) {
						if(results[j] == PreparedStatement.EXECUTE_FAILED) {
							throw new SQLException("Error inserting data for row " + j);
						}
					}
				}
			}
			
			// well, we are done looping through now
			classLogger.info("Executing final batch .... row num = " + count);
			int[] results = ps.executeBatch();
            for(int j=0; j<results.length; j++) {
                if(results[j] == PreparedStatement.EXECUTE_FAILED) {
                    throw new SQLException("Error inserting data for row " + j);
                }
            }
            
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(this, conn, ps, null);
		}
	}
	
	@Override
	public void addEmbedding(List<? extends Number> embedding, String source, String modality, String divider,
			String part, int tokens, String content, Map<String, Object> additionalMetadata) throws SQLException {
		
		// just do the insertion
		
		String psString = "INSERT INTO " 
				+ this.vectorTableName 
				+ " (EMBEDDING, SOURCE, MODALITY, DIVIDER, PART, TOKENS, CONTENT) "
				+ "VALUES (?,?,?,?,?,?,?)";
		
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
			if(result == PreparedStatement.EXECUTE_FAILED) {
                throw new SQLException("Error inserting data");
            }
	            
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(this, conn, null, null);
		}
	}

	@Override
	public void removeDocument(List<String> fileNames, Map<String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}
		final String DOCUMENT_FOLDER = this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR + AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME;
		List<String> filesToRemoveFromCloud = new ArrayList<String>();
		
		String deleteQuery = "DELETE FROM "+this.vectorTableName+" WHERE SOURCE=?";
		Connection conn = null;
		PreparedStatement ps = null;
		int[] results = null;
		try {
			conn = this.getConnection();
			ps = conn.prepareStatement(deleteQuery);
			for (String document : fileNames) {
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
			}
			results = ps.executeBatch();
			
			for(int j=0; j<results.length; j++) {
	            if(results[j] == PreparedStatement.EXECUTE_FAILED) {
	                throw new IllegalArgumentException("Error inserting data for row " + j);
	            }
	        }
			
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(this, conn, ps, null);
		}
		
		if (ClusterUtil.IS_CLUSTER) {
			Thread deleteFilesFromCloudThread = new Thread(new DeleteFilesFromEngineRunner(engineId, this.getCatalogType(), filesToRemoveFromCloud.stream().toArray(String[]::new)));
			deleteFilesFromCloudThread.start();
		}
	}

	public List<Map<String, Object>> nearestNeighborCall(Insight insight, String searchStatement, Number limit, Map<String, Object> parameters) {
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}
		
		if (!this.modelPropsLoaded) {
			verifyModelProps();
		}
		
		String searchFilters = null;
		if (parameters.containsKey("filters")) {
			
			
		}
		
		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.COLUMNS_TO_RETURN.getKey())) {}

		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.RETURN_THRESHOLD.getKey())) {}

		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.ASCENDING.getKey())) {}

		
		IModelEngine engine = Utility.getModel(this.embedderEngineId);
		EmbeddingsModelEngineResponse embeddingsResponse = engine.embeddings(Arrays.asList(new String[] {searchStatement}), insight, null);

		StringBuilder searchQueryBuilder = new StringBuilder("SELECT ");
		searchQueryBuilder.append("SOURCE AS \""+VectorDatabaseCSVTable.SOURCE+"\",")
						  .append("MODALITY AS \""+VectorDatabaseCSVTable.MODALITY+"\",")
						  .append("DIVIDER AS \""+VectorDatabaseCSVTable.DIVIDER+"\",")
						  .append("PART AS \""+VectorDatabaseCSVTable.PART+"\",")
						  .append("TOKENS AS \""+VectorDatabaseCSVTable.TOKENS+"\",")
						  .append("CONTENT AS \""+VectorDatabaseCSVTable.CONTENT+"\",")
						  .append("POWER((EMBEDDING <-> '"+ embeddingsResponse.getResponse().get(0) + "'),2) AS \"Score\"")
						  .append(" FROM ")
						  .append(this.vectorTableName)
						  .append(" ORDER BY \"Score\" ASC ");
		if(limit != null) {
			searchQueryBuilder.append("LIMIT ").append(limit);
		}
		
		HardSelectQueryStruct qs = new HardSelectQueryStruct();
		qs.setEngine(this);
		qs.setEngineId(this.engineId);
		qs.setQsType(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);		
		qs.setQuery(searchQueryBuilder.toString());
		
		List<Map<String, Object>> vectorSearchResults = QueryExecutionUtility.flushRsToMap(this, qs);
		return vectorSearchResults;
	}

	@Override
	public List<Map<String, Object>> listDocuments(Map<String, Object> parameters) {
		StringBuilder searchQueryBuilder = new StringBuilder("SELECT DISTINCT SOURCE AS \"fileName\" FROM ").append(this.vectorTableName);
		
		HardSelectQueryStruct qs = new HardSelectQueryStruct();
		qs.setEngine(this);
		qs.setEngineId(this.engineId);
		qs.setQsType(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);		
		qs.setQuery(searchQueryBuilder.toString());
		
		List<Map<String, Object>> sourcesInPostgresDb = QueryExecutionUtility.flushRsToMap(this, qs);
		
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}

		File documentsDir = new File(this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR + AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME);
		if(documentsDir.exists() && documentsDir.isDirectory()) {
			for(Map<String, Object> fileInPostgresDb : sourcesInPostgresDb) {
				String fileName = (String) fileInPostgresDb.get("fileName");
				
				File thisF = new File(documentsDir, fileName);
				if(thisF.exists() && thisF.isFile()) {
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
		if(this.cpw != null) {
			this.cpw.shutdown(true);
		}
		super.close();
	}
	
	//////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////
	
	/**
	 * Methods below should really be an exact match to the same method names
	 * 
	 */
	
	/**
	 * 
	 * @return
	 */
	public boolean keepInputOutput() {
		return this.keepInputOutput;
	}

	/**
	 * This method is meant to be overriden so that we dont need to copy/paste the startServer code for every implementation
	 * @return
	 */
	private String[] getServerStartCommands() {
		return (AbstractVectorDatabaseEngine.TOKENIZER_INIT_SCRIPT).split(PyUtils.PY_COMMAND_SEPARATOR);
	}
	
	/**
	 * 
	 * @param port
	 */
	private synchronized void startServer(int port) {
		// already created by another thread
		if(this.cpw != null && this.cpw.getSocketClient() != null && this.cpw.getSocketClient().isConnected()) {
			return;
		}
		if (!modelPropsLoaded) {
			verifyModelProps();
		}
		if(!this.pyDirectoryBasePath.exists()) {
			this.pyDirectoryBasePath.mkdirs();
		}
		// check if we have already created a process wrapper
		if(this.cpw == null) {
			this.cpw = new ClientProcessWrapper();
		}
		
		String timeout = "30";
		if(this.smssProp.containsKey(Constants.IDLE_TIMEOUT)) {
			timeout = this.smssProp.getProperty(Constants.IDLE_TIMEOUT);
		}
		
		if(this.cpw.getSocketClient() == null) {
			boolean debug = false;
			
			// pull the relevant values from the smss
			String forcePort = this.smssProp.getProperty(Settings.FORCE_PORT);
			String customClassPath = this.smssProp.getProperty("TCP_WORKER_CP");
			String loggerLevel = this.smssProp.getProperty(Settings.LOGGER_LEVEL, "WARNING");
			String venvEngineId = this.smssProp.getProperty(Constants.VIRTUAL_ENV_ENGINE, null);
			String venvPath = venvEngineId != null ? Utility.getVenvEngine(venvEngineId).pathToExecutable() : null;
			
			if(port < 0) {
				// port has not been forced
				if(forcePort != null && !(forcePort=forcePort.trim()).isEmpty()) {
					try {
						port = Integer.parseInt(forcePort);
						debug = true;
					} catch(NumberFormatException e) {
						// ignore
						classLogger.warn("Vector Database " + this.engineName + " has an invalid FORCE_PORT value");
					}
				}
			}
			
			String serverDirectory = this.pyDirectoryBasePath.getAbsolutePath();
			boolean nativePyServer = true; // it has to be -- don't change this unless you can send engine calls from python
			try {
				this.cpw.createProcessAndClient(nativePyServer, null, port, venvPath, serverDirectory, customClassPath, debug, timeout, loggerLevel);
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to connect to server for faiss databse.");
			}
		} else if (!this.cpw.getSocketClient().isConnected()) {
			this.cpw.shutdown(false);
			try {
				this.cpw.reconnect();
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Failed to start TCP Server for Faiss Database = " + this.engineName);
			}
		}

		// create the py translator
		pyt = new TCPPyTranslator();
		pyt.setSocketClient(this.cpw.getSocketClient());
		
		String[] commands = getServerStartCommands();
		// replace the vars
		StringSubstitutor substitutor = new StringSubstitutor(this.vars);
		for(int commandIndex = 0; commandIndex < commands.length;commandIndex++) {
			String resolvedString = substitutor.replace(commands[commandIndex]);
			commands[commandIndex] = resolvedString;
		}
		pyt.runEmptyPy(commands);
		
		// for debugging...
		classLogger.info("Initializing " + SmssUtilities.getUniqueName(this.engineName, this.engineId) 
							+ " ptyhon process with commands >>> " + String.join("\n", commands));
	}

	private void checkSocketStatus() {
		if(this.cpw == null || this.cpw.getSocketClient() == null || !this.cpw.getSocketClient().isConnected()) {
			this.startServer(-1);
		}
	}
	
	@Override
	public void addDocument(List<String> filePaths, Map<String, Object> parameters) throws Exception {
		if (!modelPropsLoaded) {
			verifyModelProps();
		}
		
		this.removeDocument(filePaths, parameters);

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

		String extractionMethod = this.defaultExtractionMethod;
		if (parameters.containsKey(VectorDatabaseParamOptionsEnum.EXTRACTION_METHOD.getKey())) {
			chunkUnit = (String) parameters.get(VectorDatabaseParamOptionsEnum.EXTRACTION_METHOD.getKey());
		}
		
		Insight insight = getInsight(parameters.get(AbstractVectorDatabaseEngine.INSIGHT));
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}
		
		File indexFilesFolder = new File(this.schemaFolder + DIR_SEPARATOR + indexClass, AbstractVectorDatabaseEngine.INDEXED_FOLDER_NAME);
		try {
			// first we need to extract the text from the document
			// TODO change this to json so we never have an encoding issue
			checkSocketStatus();

			File indexDirectory = new File(this.schemaFolder, indexClass);
			File documentDir = new File(indexDirectory, AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME);
			if(!documentDir.exists()) {
				documentDir.mkdirs();
			}

			boolean filesAppoved = VectorDatabaseUtils.verifyFileTypes(filePaths, new ArrayList<>(Arrays.asList(documentDir.list())));
			if (!filesAppoved) {
				throw new IllegalArgumentException("Currently unable to mix csv with non-csv file types.");
			}

			if (!indexFilesFolder.exists()) {
				indexFilesFolder.mkdirs();
			}
			if (!this.indexClasses.contains(indexClass)) {
				addIndexClass(indexClass);
			}

			List<File> extractedFiles = new ArrayList<>();
			List<String> filesToCopyToCloud = new ArrayList<>(); // create a list to store all the net new files so we can push them to the cloud
			String chunkingStrategy = PyUtils.determineStringType(parameters.getOrDefault("chunkingStrategy", "ALL"));

			// move the documents from insight into documents folder
			Set<File> fileToExtractFrom = new HashSet<File>();
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
					classLogger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("Unable to remove previously created file for " + destinationFile.getName() + " or move it to the document directory");
				}

				// add it to the list of files we need to extract text from
				fileToExtractFrom.add(destinationFile);
				
				// add it to the list of files that need to be pushed to the cloud in a new thread
				filesToCopyToCloud.add(destinationFile.getAbsolutePath());
			}
			
			// loop through each document and attempt to extract text
			for (File document : fileToExtractFrom) {
				String documentName = Utility.normalizePath(document.getName().split("\\.")[0]);
				File extractedFile = new File(indexFilesFolder.getAbsolutePath() + DIR_SEPARATOR + documentName + ".csv");
				String extractedFileName = extractedFile.getAbsolutePath().replace(FILE_SEPARATOR, DIR_SEPARATOR);
				try {
					if (extractedFile.exists()) {
						FileUtils.forceDelete(extractedFile);
					}
					String docLower = document.getName().toLowerCase();
					
					if(docLower.endsWith(".csv")) {
						classLogger.info("You are attempting to load in a structured table for " + documentName + ". Hopefully the structure is the right format we expect");
						// copy csv over
						FileUtils.copyFileToDirectory(document, indexFilesFolder);
					} else {
						classLogger.info("Extracting text from document " + documentName);
						// determine which text extraction method to use
						int rowsCreated;
						if (extractionMethod.equals("fitz") && document.getName().toLowerCase().endsWith(".pdf")) {
							StringBuilder extractTextFromDocScript = new StringBuilder();
							extractTextFromDocScript.append("vector_database.extract_text(source_file_name = '")
								.append(document.getAbsolutePath().replace(FILE_SEPARATOR, DIR_SEPARATOR))
								.append("', target_folder = '")
								.append(this.schemaFolder.getAbsolutePath().replace(FILE_SEPARATOR, DIR_SEPARATOR) + DIR_SEPARATOR + indexClass + DIR_SEPARATOR + "extraction_files")
								.append("', output_file_name = '")
								.append(extractedFileName)
								.append("')");
							Number rows = (Number) pyt.runScript(extractTextFromDocScript.toString());

							rowsCreated = rows.intValue();
						} else {
							rowsCreated = VectorDatabaseUtils.convertFilesToCSV(extractedFile.getAbsolutePath(), document);
						}

						// check to see if the file data was extracted
						if (rowsCreated <= 1) {
							// no text was extracted so delete the file
							FileUtils.forceDelete(extractedFile); // delete the csv
							FileUtils.forceDelete(document); // delete the input file e.g pdf
							continue;
						}

						classLogger.info("Creating chunks from extracted text for " + documentName);

						StringBuilder splitTextCommand = new StringBuilder();
						splitTextCommand.append("vector_database.split_text(csv_file_location = '")
							.append(extractedFileName)
							.append("', chunk_unit = '")
							.append(chunkUnit)
							.append("', chunk_size = ")
							.append(chunkMaxTokenLength)
							.append(", chunk_overlap = ")
							.append(tokenOverlapBetweenChunks)
							.append(", chunking_strategy = ")
							.append(chunkingStrategy)
							.append(", cfg_tokenizer = cfg_tokenizer)");
						
						pyt.runScript(splitTextCommand.toString());
					}

					// add it to the list of files that need to be pushed to the cloud in a new thread
					filesToCopyToCloud.add(document.getAbsolutePath());
					extractedFiles.add(extractedFile);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("Unable to remove old or create new text extraction file for " + documentName);
				}
			}
			
			if (extractedFiles.size() > 0) {
				addEmbeddingFiles(extractedFiles, insight, parameters);
				
				if (ClusterUtil.IS_CLUSTER) {
					// push the actual documents over to the cloud
					Thread copyFilesToCloudThread = new Thread(new CopyFilesToEngineRunner(this.engineId, this.getCatalogType(), filesToCopyToCloud.stream().toArray(String[]::new)));
					copyFilesToCloudThread.start();
				}
			}
		} finally {
			cleanUpAddDocument(indexFilesFolder);
		}
	}
	
	@Override
	public List<Map<String, Object>> nearestNeighbor(Insight insight, String searchStatement, Number limit, Map <String, Object> parameters) {
		if(parameters == null) {
			parameters = new HashMap<String, Object>();
		}

		ZonedDateTime inputTime = ZonedDateTime.now();
		List<Map<String, Object>> vectorSearchResponse = nearestNeighborCall(insight, searchStatement, limit, parameters);
		ZonedDateTime outputTime = ZonedDateTime.now();

		if (inferenceLogsEnbaled) {
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			Thread inferenceRecorder = new Thread(new ModelEngineInferenceLogsWorker (
					/*messageId*/UUID.randomUUID().toString(), 
					/*messageMethod*/"nearestNeighbor", 
					/*engine*/this, 
					/*insight*/insight,
					/*context*/null, 
					/*prompt*/searchStatement,
					/*fullPrompt*/null,
					/*promptTokens*/null,
					/*inputTime*/inputTime, 
					/*response*/gson.toJson(vectorSearchResponse),
					/*responseTokens*/null,
					/*outputTime*/outputTime
					));
			inferenceRecorder.start();
		}

		return vectorSearchResponse;
	}
	
	protected void addIndexClass(String indexClass) {
		this.indexClasses.add(indexClass);
	}
	
	protected void cleanUpAddDocument(File indexFilesFolder) {
		try {
			FileUtils.forceDelete(indexFilesFolder);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	/**
	 * 
	 * @param indexClass
	 * @return
	 */
	public String getIndexFilesPath(String indexClass) {
		throw new IllegalArgumentException("Indexed files are not persisted for PGVector");
	}
	
	/**
	 * 
	 * @param indexClass
	 * @return
	 */
	public String getDocumentsFilesPath(String indexClass) {
		if(indexClass == null || (indexClass=indexClass.trim()).isEmpty()) {
			indexClass = this.defaultIndexClass;
		}
		if (!this.indexClasses.contains(indexClass)) {
			throw new IllegalArgumentException("Unable to retieve document csv from a directory that does not exist");
		}
		return Utility.normalizePath(this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR + AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME);
	}
	
	@Override
	public boolean userCanAccessEmbeddingModels(User user) {
		if (!modelPropsLoaded) {
			verifyModelProps();
		}
		
		if(this.embedderEngineId != null) {
			if(!SecurityEngineUtils.userCanViewEngine(user, this.embedderEngineId)) {
				throw new IllegalArgumentException("Embeddings model " + this.embedderEngineId + " does not exist or user does not have access to this model");
			}
		}
		
		if(this.keywordGeneratorEngineId != null) {
			if(!SecurityEngineUtils.userCanViewEngine(user, this.keywordGeneratorEngineId)) {
				throw new IllegalArgumentException("Keyword model " + this.keywordGeneratorEngineId + " does not exist or user does not have access to this model");
			}
		}
		
		return true;
	}
	
	private Insight getInsight(Object insightObj) {
		if (insightObj instanceof String) {
			return InsightStore.getInstance().get((String) insightObj);
		} else {
			return (Insight) insightObj;
		}
	}
}
