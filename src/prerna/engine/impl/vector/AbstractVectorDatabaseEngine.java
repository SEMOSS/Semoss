package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.TCPPyTranslator;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.SmssUtilities;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;
import prerna.util.sql.RDBMSUtility;

public abstract class AbstractVectorDatabaseEngine implements IVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractVectorDatabaseEngine.class);
	
	public static final String LATEST_VECTOR_SEARCH_STATEMENT = "LATEST_VECTOR_SEARCH_STATEMENT";
	public static final String KEYWORD_ENGINE_ID = "KEYWORD_ENGINE_ID";
	public static final String INSIGHT = "insight";
	
	public static final String DIR_SEPARATOR = "/";
	public static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public static final String INDEX_CLASS = "indexClass";

	protected String engineId = null;
	protected String engineName = null;
	
	protected Properties smssProp = null;
	protected String smssFilePath = null;
	
	protected String encoderName = null;
	protected String encoderType = null;

	protected int contentLength = 512;
	protected int contentOverlap = 0;
	
	protected String defaultChunkUnit;
	protected String defaultExtractionMethod;
	protected String defaultIndexClass;
	
	protected String distanceMethod;
	
	protected ClientProcessWrapper cpw = null;
	// python server
	protected TCPPyTranslator pyt = null;
	protected File pyDirectoryBasePath = null;
	
	protected boolean modelPropsLoaded = false;
	protected File schemaFolder;

	// string substitute vars
	protected Map<String, String> vars = new HashMap<>();
	
	@Override
	public void open(String smssFilePath) throws Exception {
		setSmssFilePath(smssFilePath);
		this.open(Utility.loadProperties(smssFilePath));
	}
	
	@Override
	public void open(Properties smssProp) throws Exception {
		setSmssProp(smssProp);
		this.engineId = this.smssProp.getProperty(Constants.ENGINE);
		this.engineName = this.smssProp.getProperty(Constants.ENGINE_ALIAS);

		if (this.smssProp.containsKey(Constants.CONTENT_LENGTH)) {
			this.contentLength = Integer.parseInt(this.smssProp.getProperty(Constants.CONTENT_LENGTH));
		}
		if (this.smssProp.containsKey(Constants.CONTENT_OVERLAP)) {
			this.contentOverlap = Integer.parseInt(this.smssProp.getProperty(Constants.CONTENT_OVERLAP));
		}
		
		this.defaultChunkUnit = "tokens";
		if (this.smssProp.containsKey(Constants.DEFAULT_CHUNK_UNIT)) {
			this.defaultChunkUnit = this.smssProp.getProperty(Constants.DEFAULT_CHUNK_UNIT).toLowerCase().trim();
			if (!this.defaultChunkUnit.equals("tokens") && !this.defaultChunkUnit.equals("characters")){
	            throw new IllegalArgumentException("DEFAULT_CHUNK_UNIT should be either 'tokens' or 'characters'");
			}
		}
		
		this.defaultExtractionMethod = this.smssProp.getProperty(Constants.EXTRACTION_METHOD, "None");
		this.distanceMethod = this.smssProp.getProperty(Constants.DISTANCE_METHOD, "Cosine Similarity");
		this.defaultIndexClass = "default";
		if (this.smssProp.containsKey(Constants.INDEX_CLASSES)) {
			this.defaultIndexClass = this.smssProp.getProperty(Constants.INDEX_CLASSES);
		}
	}
	
	/**
	 * 
	 * @param input
	 * @return
	 */
	protected String fillVars(String input) {
		StringSubstitutor sub = new StringSubstitutor(vars);
		String resolvedString = sub.replace(input);
		return resolvedString;
	}
	
	/**
	 * 
	 * @param insightObj
	 * @return
	 */
	protected Insight getInsight(Object insightObj) {
		if (insightObj instanceof String) {
			return InsightStore.getInstance().get((String) insightObj);
		} else {
			return (Insight) insightObj;
		}
	}
	
	/**
	 * 
	 */
	protected void checkSocketStatus() {
		if(this.cpw == null || this.cpw.getSocketClient() == null || !this.cpw.getSocketClient().isConnected()) {
			this.startServer(-1);
		}
	}

	@Override
	public List<Map<String, Object>> listDocuments(Map<String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}

		File documentsDir = new File(this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR + "documents");

		List<Map<String, Object>> fileList = new ArrayList<>();

		File[] files = documentsDir.listFiles();
		if (files != null) {
			for (File file : files) {
				String fileName = file.getName();
				long fileSizeInBytes = file.length();
				double fileSizeInMB = (double) fileSizeInBytes / (1024);
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				String lastModified = dateFormat.format(new Date(file.lastModified()));

				Map<String, Object> fileInfo = new HashMap<>();
				fileInfo.put("fileName", fileName);
				fileInfo.put("fileSize", fileSizeInMB);
				fileInfo.put("lastModified", lastModified);
				fileList.add(fileInfo);
			}
		} 

		return fileList;
	}
	
	/**
	 * 
	 */
	protected void verifyModelProps() {
		// This could get moved depending on other vector db needs
		// This is to get the Model Name and Max Token for an encoder -- we need this to verify chunks aren't getting truncated
		String embedderEngineId = this.smssProp.getProperty(Constants.EMBEDDER_ENGINE_ID);
		if (embedderEngineId == null || (embedderEngineId=embedderEngineId.trim()).isEmpty()) {
			embedderEngineId = this.smssProp.getProperty("ENCODER_ID");
			if (embedderEngineId == null || (embedderEngineId=embedderEngineId.trim()).isEmpty()) {
				throw new IllegalArgumentException("Embedder Engine ID is not provided.");
			}
			
			this.smssProp.put(Constants.EMBEDDER_ENGINE_ID, embedderEngineId);
		}
		
		IModelEngine modelEngine = Utility.getModel(embedderEngineId);
		if (modelEngine == null) {
			throw new IllegalArgumentException("Model Engine must be created and contain MODEL");
		}
		
		Properties modelProperties = modelEngine.getSmssProp();
		if (modelProperties.isEmpty() || !modelProperties.containsKey(Constants.MODEL)) {
			throw new IllegalArgumentException("Model Engine must be created and contain MODEL");
		}
		
		this.smssProp.put(Constants.MODEL, modelProperties.getProperty(Constants.MODEL));
		this.smssProp.put(IModelEngine.MODEL_TYPE, modelProperties.getProperty(IModelEngine.MODEL_TYPE));
		if (!modelProperties.containsKey(Constants.MAX_TOKENS)) {
			this.smssProp.put(Constants.MAX_TOKENS, "None");
		} else {
			this.smssProp.put(Constants.MAX_TOKENS, modelProperties.getProperty(Constants.MAX_TOKENS));
		}

		// model engine responsible for creating keywords
		String keywordGeneratorEngineId = this.smssProp.getProperty(KEYWORD_ENGINE_ID);
		if (keywordGeneratorEngineId != null && !(keywordGeneratorEngineId=keywordGeneratorEngineId.trim()).isEmpty()) {
			// pull the model smss if needed
			Utility.getModel(keywordGeneratorEngineId);
			this.smssProp.put(KEYWORD_ENGINE_ID, keywordGeneratorEngineId);
		} else {
			// add it to the smss prop so the string substitution does not fail
			this.smssProp.put(KEYWORD_ENGINE_ID, "");
		}
		
		for (Object smssKey : this.smssProp.keySet()) {
			String key = smssKey.toString();
			this.vars.put(key, this.smssProp.getProperty(key));
		}
		
		modelPropsLoaded = true;
	}

	/**
	 * 
	 * @param lease
	 */
	protected abstract void startServer(int lease);

	@Override
	public void setEngineId(String engineId) {
		this.engineId = engineId;
	}

	@Override
	public String getEngineId() {
		return this.engineId;
	}

	@Override
	public void setEngineName(String engineName) {
		this.engineName = engineName;
	}

	@Override
	public String getEngineName() {
		return this.engineName;
	}

	@Override
	public void setSmssFilePath(String smssFilePath) {
		this.smssFilePath = smssFilePath;
	}

	@Override
	public String getSmssFilePath() {
		return this.smssFilePath;
	}

	@Override
	public void setSmssProp(Properties smssProp) {
		this.smssProp = smssProp;
	}

	@Override
	public Properties getSmssProp() {
		return this.smssProp;
	}

	@Override
	public Properties getOrigSmssProp() {
		return null;
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
	public void close() throws IOException {
		if(this.cpw != null) {
			this.cpw.shutdown(true);
		}
	}
	
	@Override
	public void delete() {
		classLogger.debug("Delete vector database engine " + SmssUtilities.getUniqueName(this.engineName, this.engineId));
		try {
			this.close();
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		File engineFolder = new File(EngineUtility.getSpecificEngineBaseFolder(getCatalogType(), this.engineId, this.engineName));
		if(engineFolder.exists()) {
			classLogger.info("Delete vector database engine folder " + engineFolder);
			try {
				FileUtils.deleteDirectory(engineFolder);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		} else {
			classLogger.info("Vector Database engine folder " + engineFolder + " does not exist");
		}
		
		classLogger.info("Deleting vector database engine smss " + this.smssFilePath);
		File smssFile = new File(this.smssFilePath);
		try {
			FileUtils.forceDelete(smssFile);
		} catch(IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		// remove from DIHelper
		UploadUtilities.removeEngineFromDIHelper(this.engineId);
	}
	
	@Override
	public boolean holdsFileLocks() {
		return false;
	}
	
	@Override
	public void addEmbedding(List<? extends Number> embedding, String source, String modality, String divider,
			String part, int tokens, String content, Map<String, Object> additionalMetadata) throws Exception {
		// TODO Auto-generated method stub
		// TODO Implement for each engine type and remove from Abstract
	}
	
	
	@Override
	public void addEmbeddings(File vectorCsvFile, Insight insight) throws Exception {
		VectorDatabaseCSVTable vectorCsvTable = VectorDatabaseCSVTable.initCSVTable(vectorCsvFile);
		addEmbeddings(vectorCsvTable, insight);
	}
	
	@Override
	public void addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight) throws Exception {
		// TODO Auto-generated method stub
		// TODO Implement for each engine type and remove from Abstract
	}
	
}
