package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyUtils;
import prerna.ds.py.TCPPyTranslator;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Settings;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

public abstract class AbstractVectorDatabaseEngine implements IVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(AbstractVectorDatabaseEngine.class);
	
	protected static final String TOKENIZER_INIT_SCRIPT = "from genai_client import get_tokenizer;"
			+ "cfg_tokenizer = get_tokenizer(tokenizer_name = '${MODEL}', max_tokens = ${MAX_TOKENS}, tokenizer_type = '${MODEL_TYPE}');"
			+ "import vector_database;";
	
	public static final String LATEST_VECTOR_SEARCH_STATEMENT = "LATEST_VECTOR_SEARCH_STATEMENT";
	public static final String VECTOR_SEARCHER_NAME = "VECTOR_SEARCHER_NAME";
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
	
	protected boolean modelPropsLoaded = false;
	protected String embedderEngineId = null;
	protected String keywordGeneratorEngineId = null;
	
	protected String defaultChunkUnit;
	protected String defaultExtractionMethod;
	
	// our paradigm for how we store files
	protected String defaultIndexClass;
	protected String vectorDatabaseSearcher = null;
	protected List<String> indexClasses;
	
	protected String distanceMethod;
	
	protected ClientProcessWrapper cpw = null;
	// python server
	protected TCPPyTranslator pyt = null;
	protected File pyDirectoryBasePath = null;
	
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
		
		// highest directory (first layer inside vector db base folder)
		String engineDir = EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, this.engineId, this.engineName);
		this.pyDirectoryBasePath = new File(Utility.normalizePath(engineDir + DIR_SEPARATOR + "py" + DIR_SEPARATOR));
		
		// second layer - This holds all the different "tables". The reason we want this is to easily and quickly grab the sub folders
		this.schemaFolder = new File(engineDir, "schema");
		if(!this.schemaFolder.exists()) {
			this.schemaFolder.mkdirs();
		}
		
		// third layer - All the separate tables,classes, or searchers that can be added to this db
		this.indexClasses = new ArrayList<>();
        for (File file : this.schemaFolder.listFiles()) {
            if (file.isDirectory() && !file.getName().equals("temp")) {
            	this.indexClasses.add(file.getName());
            }
        }
        
		this.vectorDatabaseSearcher = Utility.getRandomString(6);
		this.smssProp.put(VECTOR_SEARCHER_NAME, this.vectorDatabaseSearcher);
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

	/**
	 * 
	 */
	protected void checkSocketStatus() {
		if(this.cpw == null || this.cpw.getSocketClient() == null || !this.cpw.getSocketClient().isConnected()) {
			this.startServer(-1);
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
	 * This method is meant to be overriden so that we dont need to copy/paste the startServer code for every implementation
	 * @return
	 */
	protected String[] getServerStartCommands() {
		return (AbstractVectorDatabaseEngine.TOKENIZER_INIT_SCRIPT).split(PyUtils.PY_COMMAND_SEPARATOR);
	}
	
	/**
	 * 
	 * @param port
	 */
	protected synchronized void startServer(int port) {
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
		StringBuilder intitPyCommands = new StringBuilder("\n");
		for (String command : commands) {
			intitPyCommands.append(command).append("\n");
		}
		classLogger.info("Initializing " + SmssUtilities.getUniqueName(this.engineName, this.engineId) + " ptyhon process with commands >>> " + intitPyCommands.toString());
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
	 * @param content
	 * @param insight
	 * @return
	 */
	protected Float[] getEmbeddings(String content, Insight insight) {
		IModelEngine embeddingsEngine = Utility.getModel(this.embedderEngineId);
		List <Double> embeddingsResponse = embeddingsEngine.embeddings(Arrays.asList(new String[] {content}), getInsight(insight), null).getResponse().get(0);
		Float [] retFloat = new Float[embeddingsResponse.size()];
		for(int vecIndex = 0; vecIndex < retFloat.length; vecIndex++) {
			retFloat[vecIndex] = (Float)embeddingsResponse.get(vecIndex).floatValue();
		}
		
		return retFloat;
	}

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
	
}
