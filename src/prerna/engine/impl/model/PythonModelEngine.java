package prerna.engine.impl.model;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyUtils;
import prerna.ds.py.TCPPyTranslator;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.tcp.PayloadStruct;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Settings;
import prerna.util.Utility;


/**
 * This class is responsible for creating a {@code IModelEngine} class that is directly linked to 
 * a python process. The corresponding python class should handle all method implementations. This java class is 
 * simply mechanism to forward calls to the python process.
 */
public class PythonModelEngine extends AbstractModelEngine {
	
	private static final Logger classLogger = LogManager.getLogger(PythonModelEngine.class);

	// python server
	protected String prefix = null;
	protected String workingDirectory;
	protected String workingDirectoryBasePath = null;
	
	protected TCPPyTranslator pyt = null;
	protected File cacheFolder;
	private ClientProcessWrapper cpw = null;
	
	private String varName = null;
	
	// string substitute vars
	Map<String, String> vars = new HashMap<>();
	
	private Map<String, ArrayList<Map<String, Object>>> chatHistory = new Hashtable<>();
	
	@Override
	public void open(String smssFilePath) throws Exception {
		setSmssFilePath(smssFilePath);
		this.open(Utility.loadProperties(smssFilePath));
	}
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		
		if(!this.smssProp.containsKey(Settings.VAR_NAME)) {
			String randomString = "v_" + Utility.getRandomString(6);
			this.varName = randomString;
			this.smssProp.put(Settings.VAR_NAME, randomString);
		} else {
			this.varName = this.smssProp.getProperty(Settings.VAR_NAME);
		}
					
		// vars for string substitution
		for (Object smssKey : this.smssProp.keySet()) {
			String key = smssKey.toString();
			this.vars.put(key, this.smssProp.getProperty(key));
		}
	}

	
	/**
	 * This method is responsible for starting the python process that is linked to this model engine.
	 * 
	 * @param port		The port number to use when creating the server/client connection.
	 */
	protected void startServer(int port) {
		// spin the server
		// start the client
		// get the startup command and parameters - at some point we need a better way than the command
		
		// execute all the basic commands
		String initCommands = this.smssProp.getProperty(Constants.INIT_MODEL_ENGINE);
		
		// break the commands seperated by ;
		String [] commands = initCommands.split(ModelEngineConstants.PY_COMMAND_SEPARATOR);
		
		// replace the Vars
		for(int commandIndex = 0; commandIndex < commands.length;commandIndex++) {
			commands[commandIndex] = fillVars(commands[commandIndex]);
		}
		
		if(this.workingDirectoryBasePath == null) {
			this.createCacheFolder();
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
						classLogger.warn("Model " + this.getEngineName() + " has an invalid FORCE_PORT value");
					}
				}
			}
			
			String serverDirectory = this.cacheFolder.getAbsolutePath();
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
				throw new IllegalArgumentException("Failed to start TCP Server for Faiss Database = " +this.getEngineName());
			}
		}
		
		// create the py translator
		pyt = new TCPPyTranslator();
		pyt.setSocketClient(this.cpw.getSocketClient());
		
		pyt.runEmptyPy(commands);
		
		// run a prefix command
		setPrefix();
	}
	
	/**
	 * This method checks whether the socket client is instantiated and connected.
	 */
	protected void checkSocketStatus() {
		if(this.cpw == null || this.cpw.getSocketClient() == null || !this.cpw.getSocketClient().isConnected()) {
			this.startServer(-1);
		}
	}
	
	private void setPrefix() {		
		PayloadStruct prefixPayload = new PayloadStruct();
		prefixPayload.payload = new String[] {"prefix", this.cpw.getPrefix()};
		prefixPayload.operation = PayloadStruct.OPERATION.CMD;
		
		this.cpw.getSocketClient().executeCommand(prefixPayload);
	}
	

	@Override
	public AskModelEngineResponse askCall(String question, String context, Insight insight, Map<String, Object> parameters) {
		checkSocketStatus();
				
		StringBuilder callMaker = new StringBuilder().append(varName).append(".ask(");
		callMaker.append("question=\"\"\"").append(question.replace("\"", "\\\"")).append("\"\"\"");
		if(context != null) {
			callMaker.append(",").append("context=\"\"\"").append(context.replace("\"", "\\\"")).append("\"\"\"");	
		}
		
		if (Utility.isModelInferenceLogsEnabled() && !parameters.containsKey("full_prompt")) { // have to check that inference logs are enabled so that query works
			String history = getConversationHistory(insight.getUserId(), insight.getInsightId());
			if(history != null) //could still be null if its the first question in the convo
				callMaker.append(",").append("history=").append(history);
		}
		
		if(parameters != null) {
			Iterator <String> paramKeys = parameters.keySet().iterator();
			while(paramKeys.hasNext()) {
				String key = paramKeys.next();
				callMaker.append(",").append(key).append("=");
				Object value = parameters.get(key);
				callMaker.append(PyUtils.determineStringType(value));
			}
		}
		
		if(this.prefix != null) {
			callMaker.append(", prefix='").append(prefix).append("'");
		}
		callMaker.append(")");
		classLogger.info("Running >>>" + callMaker.toString());
		Object output = pyt.runScript(callMaker.toString(), insight);

		return AskModelEngineResponse.fromObject(output);
	}
	

	@Override
	protected EmbeddingsModelEngineResponse embeddingsCall(List<String> stringsToEmbed, Insight insight, Map<String, Object> parameters) {
		checkSocketStatus();
			 	
		String pythonListAsString = PyUtils.determineStringType(stringsToEmbed);
		
		StringBuilder callMaker = new StringBuilder();

		callMaker.append(varName)
				 .append(".embeddings(strings_to_embed = ")
				 .append(pythonListAsString);
				 
		if(this.prefix != null) {
			callMaker.append(", prefix='").append(this.prefix).append("'");
		}
		
		callMaker.append(")");
		
		Object responseObject = pyt.runScript(callMaker.toString(), insight);
		
		EmbeddingsModelEngineResponse embeddingsResponse = EmbeddingsModelEngineResponse.fromObject(responseObject);
 		
		return embeddingsResponse;
	}

	@Override
	protected Object modelCall(Object input, Insight insight, Map<String, Object> parameters) {
		checkSocketStatus();
				
		StringBuilder callMaker = new StringBuilder(varName);
		
		String inputAsString = PyUtils.determineStringType(input);
		callMaker.append(".model(input = ")
				 .append(inputAsString);
				 
		if (parameters != null && !parameters.isEmpty()) {
			callMaker.append(", **")
					 .append(PyUtils.determineStringType(parameters));
		}

		callMaker.append(")");
		
		Object output = pyt.runScript(callMaker.toString(), insight);


		return output;
	}

	@Override
	public void close() throws IOException {
		if(this.cpw != null) {
			this.cpw.shutdown(true);
		}
	}
	
	private void createCacheFolder() {
		// create a generic folder
		this.workingDirectory = "MODEL_" + Utility.getRandomString(6);
		this.workingDirectoryBasePath = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/" + workingDirectory;
		this.cacheFolder = new File(workingDirectoryBasePath);
		
		// make the folder if one does not exist
		if(!this.cacheFolder.exists()) {
			this.cacheFolder.mkdir();
		}
	}

	private String fillVars(String input) {
		StringSubstitutor sub = new StringSubstitutor(vars);
		String resolvedString = sub.replace(input);
		return resolvedString;
	}
	
	protected String getConversationHistoryFromInferenceLogs(String insightId, String userId){
		List<Map<String, Object>> convoHistoryFromDb = ModelInferenceLogsUtils.doRetrieveConversation(userId, insightId, "ASC");
		if (convoHistoryFromDb.size() > 0) {
			for (Map<String, Object> record : convoHistoryFromDb) {
				Object messageData = record.get("MESSAGE_DATA");
				Map<String, Object> mapHistory = new HashMap<String, Object>();
				if (record.get("MESSAGE_TYPE").equals("RESPONSE")) {

					mapHistory.put(ModelEngineConstants.ROLE, "assistant");
					mapHistory.put(ModelEngineConstants.MESSAGE_CONTENT, messageData);
			            

				} else {
					mapHistory.put(ModelEngineConstants.ROLE, "user");
					mapHistory.put(ModelEngineConstants.MESSAGE_CONTENT, messageData);
				}
		        chatHistory.get(insightId).add(mapHistory);
			}
			ArrayList<Map<String, Object>> convoHistory = chatHistory.get(insightId);
			StringBuilder convoList = new StringBuilder("[");
			boolean isFirstElement = true;
			for (Map<String, Object> record : convoHistory) {
				if (!isFirstElement) {
					convoList.append(",");
				} else {
					isFirstElement = false;
				}
				Object priorContent = PyUtils.determineStringType(record);
		        convoList.append(priorContent);
			}
			convoList.append("]");
			return convoList.toString();
		}
		return null;
	}
	
	protected String getConversationHistory(String userId, String insightId){
		if (this.keepsConversationHistory()){
			if (chatHistory.containsKey(insightId)) {
				ArrayList<Map<String, Object>> convoHistory = chatHistory.get(insightId);
				StringBuilder convoList = new StringBuilder("[");
				boolean isFirstElement = true;
				for (Map<String, Object> record : convoHistory) {
					if (!isFirstElement) {
						convoList.append(",");
					} else {
						isFirstElement = false;
					}
					Object priorContent = PyUtils.determineStringType(record);
			        convoList.append(priorContent);
				}
				convoList.append("]");
				return convoList.toString();
			} 
			else {
				// we want to start a conversation
				ArrayList<Map<String, Object>> userNewChat = new ArrayList<Map<String, Object>>();
				chatHistory.put(insightId, userNewChat);
				String dbConversation = getConversationHistoryFromInferenceLogs(insightId, userId);
				return dbConversation;
			}
		}
		return null;
	}
	
	public String getWorkingDirectoryName() {
		return this.workingDirectory;
	}
	
	public String getWorkingDirectoryBasePath() {
		return this.workingDirectoryBasePath;
	}

	@Override
	public ModelTypeEnum getModelType() {
		// TODO Auto-generated method stub
		return null;
	}
}
