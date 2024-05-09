package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyUtils;
import prerna.ds.py.TCPPyTranslator;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.util.Constants;
import prerna.util.Settings;
import prerna.util.Utility;

public class OpenSearchVectorDatabaseEngine extends AbstractVectorDatabaseEngine {
	
	private static final Logger classLogger = LogManager.getLogger(OpenSearchVectorDatabaseEngine.class);

	protected String vectorDatabaseSearcher = null;

	private File schemaFolder;

	private List<String> indexClasses;
	private static final String VECTOR_SEARCHER_NAME = "VECTOR_SEARCHER_NAME";
	private static final String DIR_SEPARATOR = "/";
	private static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	private static final String openSearchInitScript = "import vector_database;${VECTOR_SEARCHER_NAME} = vector_database.OpenSearchConnector(embedder_engine_id = '${EMBEDDER_ENGINE_ID}', username = '${USERNAME}', password = '${PASSWORD}', index_name = '${INDEX_NAME}', hosts = ['${HOSTS}'], distance_method = '${DISTANCE_METHOD}')";

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		// highest directory (first layer inside vector db base folder)
		this.pyDirectoryBasePath = this.connectionURL + "py" + DIR_SEPARATOR;
		this.cacheFolder = new File(pyDirectoryBasePath.replace(FILE_SEPARATOR, DIR_SEPARATOR));

		// second layer - This holds all the different "tables". The reason we want this
		// is to easily and quickly grab the sub folders
		this.schemaFolder = new File(this.connectionURL, "schema");
		if (!this.schemaFolder.exists()) {
			this.schemaFolder.mkdirs();
		}
		this.smssProp.put(Constants.WORKING_DIR, this.schemaFolder.getAbsolutePath());

		// third layer - All the separate tables,classes, or searchers that can be added
		// to this db
		this.indexClasses = new ArrayList<>();
		for (File file : this.schemaFolder.listFiles()) {
			if (file.isDirectory() && !file.getName().equals("temp")) {
				this.indexClasses.add(file.getName());
			}
		}

		this.vectorDatabaseSearcher = Utility.getRandomString(6);

		this.smssProp.put(VECTOR_SEARCHER_NAME, this.vectorDatabaseSearcher);
	}
	
	protected void verifyModelProps() {
		// TODO need to add check for URL and auth and index name as of right now. 
		String embedderEngineId = this.smssProp.getProperty(Constants.EMBEDDER_ENGINE_ID);
		if (embedderEngineId == null) {
			embedderEngineId = this.smssProp.getProperty("ENCODER_ID");
			if (embedderEngineId == null) {
				throw new IllegalArgumentException("Embedder Engine ID is not provided.");
			}
			
			this.smssProp.put(Constants.EMBEDDER_ENGINE_ID, embedderEngineId);
		}
		
		if(this.smssProp.getProperty(Constants.USERNAME) == null) { throw new IllegalArgumentException("Username is not provided."); }
		if(this.smssProp.getProperty(Constants.PASSWORD) == null) { throw new IllegalArgumentException("Password is not provided."); }
		if(this.smssProp.getProperty(Constants.HOSTS) == null) { throw new IllegalArgumentException("HOSTS is not provided."); }
		if(this.smssProp.getProperty(Constants.INDEX_NAME) == null) { throw new IllegalArgumentException("INDEX_NAME is not provided."); }
		
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
		
		for (Object smssKey : this.smssProp.keySet()) {
			String key = smssKey.toString();
			this.vars.put(key, this.smssProp.getProperty(key));
		}
		
		modelPropsLoaded = true;
	}

	protected synchronized void startServer(int port) {
		// already created by another thread
		if(this.cpw != null && this.cpw.getSocketClient() != null && this.cpw.getSocketClient().isConnected()) {
			return;
		}
				
		if(!modelPropsLoaded) {
			verifyModelProps();
		}
		
		// break the commands seperated by ;
		String [] commands = (openSearchInitScript).split(PyUtils.PY_COMMAND_SEPARATOR);
		
		// replace the Vars
		for(int commandIndex = 0; commandIndex < commands.length;commandIndex++) {
			commands[commandIndex] = fillVars(commands[commandIndex]);
		}
		
		// Start and connect to the open search instance
		if(!this.cacheFolder.exists()) {
			this.cacheFolder.mkdirs();
		}
		
		// check if we have already created a process wrapper
		if(this.cpw == null) {
			this.cpw = new ClientProcessWrapper();
		}
		
		String timeout = "30";
		if(this.smssProp.containsKey(Constants.IDLE_TIMEOUT)) {
			timeout = this.smssProp.getProperty(Constants.IDLE_TIMEOUT);
		}
		
		
		
		if (this.cpw.getSocketClient() == null) {
			boolean debug = false;

			// pull the relevant values from the smss
			String forcePort = this.smssProp.getProperty(Settings.FORCE_PORT);
			String customClassPath = this.smssProp.getProperty("TCP_WORKER_CP");
			String loggerLevel = this.smssProp.getProperty(Settings.LOGGER_LEVEL, "WARNING");
			String venvEngineId = this.smssProp.getProperty(Constants.VIRTUAL_ENV_ENGINE, null);
			String venvPath = venvEngineId != null ? Utility.getVenvEngine(venvEngineId).pathToExecutable() : null;
			
			String indexName = null;
			String clusterUrl = null;
			String username = null;
			String password = null;
			if(this.smssProp.getProperty("INDEX_NAME") != null) { indexName = this.smssProp.getProperty("INDEX_NAME");}
			if(this.smssProp.getProperty("CLUSTER_URL") != null) { clusterUrl = this.smssProp.getProperty("CLUSTER_URL");}
			if(this.smssProp.getProperty("USERNAME") != null) { username = this.smssProp.getProperty("USERNAME");}
			if(this.smssProp.getProperty("PASSWORD") != null) { password = this.smssProp.getProperty("PASSWORD");}
			
			if (port < 0) {
				// port has not been forced
				if (forcePort != null && !(forcePort = forcePort.trim()).isEmpty()) {
					try {
						port = Integer.parseInt(forcePort);
						debug = true;
					} catch (NumberFormatException e) {
						// ignore
						classLogger
								.warn("OpenSearch connection " + this.engineName + " has an invalid FORCE_PORT value");
					}
				}
			}
			String serverDirectory = this.cacheFolder.getAbsolutePath();
			boolean nativePyServer = true; // it has to be -- don't change this unless you can send engine calls from
											// python
			try {
				this.cpw.createProcessAndClient(nativePyServer, null, port, venvPath, serverDirectory, customClassPath,
						debug, timeout, loggerLevel);
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
				throw new IllegalArgumentException(
						"Failed to start TCP Server for OpenSearch Connection  = " + this.engineName);
			}
		}
		
		// create the py translator
		pyt = new TCPPyTranslator();
		pyt.setSocketClient(this.cpw.getSocketClient());
	
		// TODO remove once bug is caught / fixed
		StringBuilder intitPyCommands = new StringBuilder("\n");
		for (String command : commands) {
			intitPyCommands.append(command).append("\n");
		}		
		classLogger.info("Initializing OpenSearch Connection with the following py commands >>>" + intitPyCommands.toString());
		pyt.runEmptyPy(commands);
		
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List<Map<String, Object>> nearestNeighbor(String question, Number limit, Map <String, Object> parameters) {
		StringBuilder callMaker = new StringBuilder();
		
		checkSocketStatus();
		
		Insight insight = getInsight(parameters.get(INSIGHT));
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}
		
		// Create the call to search against the connection 
		callMaker.append(this.vectorDatabaseSearcher).append(".knn_search(");
		
		// make the question arg
		callMaker.append("question=\"\"\"")
				 .append(question.replace("\"", "\\\""))
				 .append("\"\"\"");

		callMaker.append(", insight_id='")
				 .append(insight.getInsightId())
				 .append("'");
		callMaker.append(", limit='")
		 .append(limit.toString())
		 .append("'");
		
		// TODO  add in fields, coulmns to return 
		// close the method
 		callMaker.append(")");
 		classLogger.info("Running >>>" + callMaker.toString());
		Object output = pyt.runScript(callMaker.toString(), insight);
		return (List<Map<String, Object>>) output;
	}
	
	public void checkSocketStatus() {
		if(this.cpw == null || this.cpw.getSocketClient() == null || !this.cpw.getSocketClient().isConnected()) {
			this.startServer(-1);
		}

	}

	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.OPENSEARCH;
	}

	@Override
	public void addDocument(List<String> filePaths, Map<String, Object> parameters) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeDocument(List<String> fileNames, Map<String, Object> parameters) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<Map<String, Object>> listDocuments(Map<String, Object> parameters) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	// not needed anmore
	public Insight getInsight(Object insightObj) {
		if (insightObj instanceof String) {
			return InsightStore.getInstance().get((String) insightObj);
		} else {
			return (Insight) insightObj;
		}
	}
	
}