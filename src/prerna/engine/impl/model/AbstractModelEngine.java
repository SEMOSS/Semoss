package prerna.engine.impl.model;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyUtils;
import prerna.ds.py.TCPPyTranslator;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker;
import prerna.om.Insight;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.NativePySocketClient;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.Settings;
import prerna.util.Utility;

public abstract class AbstractModelEngine implements IModelEngine {
	
	private static final Logger classLogger = LogManager.getLogger(AbstractModelEngine.class);

	private static final String DIR_SEPERATOR = "/";
	private static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	protected static final String RESPONSE = "response";
	protected static final String NUMBER_OF_TOKENS_IN_PROMPT = "numberOfTokensInPrompt";
	protected static final String NUMBER_OF_TOKENS_IN_RESPONSE = "numberOfTokensInResponse";
	
	protected String engineId = null;
	protected String engineName = null;
	
	protected Properties smssProp = null;
	protected String smssFilePath = null;
	
	protected String [] requiredVars = new String [] {Settings.VAR_NAME, Settings.PROMPT_STOPPER};
	protected boolean keepConversationHistory = false;
	protected boolean keepInputOutput = false;

	// python server
	TCPPyTranslator pyt = null;
	NativePySocketClient socketClient = null;
	Process p = null;
	String port = null;
	String prefix = null;
	String workingDirectory;
	String workingDirectoryBasePath = null;
	File cacheFolder;
	
	// string substitute vars
	Map vars = new HashMap();
	
	private Map<String, ArrayList<Map<String, Object>>> chatHistory = new Hashtable<>();
	
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
		
		for (String var : requiredVars) {
			if(!this.smssProp.containsKey(var)) {
				String randomString = "v_" + Utility.getRandomString(6);
				this.smssProp.put(var, randomString);
			}
		}
		if (!this.smssProp.containsKey(ModelEngineConstants.CUR_DIR)) {
			String curDir = new File(smssFilePath).getParent().replace(FILE_SEPARATOR, DIR_SEPERATOR);
			this.smssProp.put(ModelEngineConstants.CUR_DIR, curDir);
		}
		if (!this.smssProp.containsKey(ModelEngineConstants.ENGINE_DIR)) {
			this.smssProp.put(ModelEngineConstants.ENGINE_DIR, this.smssProp.get(ModelEngineConstants.CUR_DIR) + "/" + this.engineName + "__" + this.engineId);
		}
		
		this.keepConversationHistory = Boolean.parseBoolean(this.smssProp.getProperty(ModelEngineConstants.KEEP_CONVERSATION_HISTORY));
		this.keepInputOutput = Boolean.parseBoolean(this.smssProp.getProperty(ModelEngineConstants.KEEP_INPUT_OUTPUT));
				
		if (this.smssProp.containsKey(ModelEngineConstants.KEEP_CONTEXT)) {
			boolean keepContext = Boolean.parseBoolean(this.smssProp.getProperty(ModelEngineConstants.KEEP_CONTEXT));
			this.keepConversationHistory = keepContext;
			this.keepInputOutput = keepContext;
		}
			
		// vars for string substitution
		this.vars = new HashMap<>(this.smssProp);
	}

	@Override
	public void startServer() {
		// spin the server
		// start the client
		// get the startup command and parameters - at some point we need a better way than the command
		
		// execute all the basic commands
		String initCommands = (String) smssProp.get(Constants.INIT_MODEL_ENGINE);
		
		// break the commands seperated by ;
		String [] commands = initCommands.split(ModelEngineConstants.PY_COMMAND_SEPARATOR);
		
		// replace the Vars
		for(int commandIndex = 0; commandIndex < commands.length;commandIndex++) {
			commands[commandIndex] = fillVars(commands[commandIndex]);
		}
		port = Utility.findOpenPort();
		
		String timeout = "15";
		if(smssProp.containsKey(Constants.IDLE_TIMEOUT))
			timeout = smssProp.getProperty(Constants.IDLE_TIMEOUT);

		if (this.workingDirectoryBasePath == null) {
			this.createCacheFolder();
		}

		String venvEngineId = this.smssProp.getProperty(Constants.VIRTUAL_ENV_ENGINE, null);
		String venvPath = venvEngineId != null ? Utility.getVenvEngine(venvEngineId).pathToExecutable() : null;
			
		String loggerLevel = this.smssProp.getProperty(Settings.LOGGER_LEVEL, "INFO");
		Object [] outputs = Utility.startTCPServerNativePy(this.workingDirectoryBasePath, port, venvPath, timeout, loggerLevel);
		this.p = (Process) outputs[0];
		this.prefix = (String) outputs[1];
		
		socketClient = new NativePySocketClient();
		socketClient.connect("127.0.0.1", Integer.parseInt(port), false);
		
		// connect the client
		connectClient();
		
		// create the py translator
		pyt = new TCPPyTranslator();
		pyt.setSocketClient(socketClient);
		pyt.runEmptyPy(commands);	
		
		// run a prefix command
		setPrefix(this.prefix);
	}
	
	private void setPrefix(String prefix)
	{
		String [] alldata = new String[] {"prefix", prefix};
		PayloadStruct prefixPayload = new PayloadStruct();
		prefixPayload.payload = alldata;
		prefixPayload.operation = PayloadStruct.OPERATION.CMD;
		PayloadStruct ps = (PayloadStruct)socketClient.executeCommand(prefixPayload);
	}
	

	/**
	 * Abstract method, child classes should construct their input / output here
	 * 
	 * @param question
	 * @param context
	 * @param insight
	 * @param parameters
	 * @return
	 */
	public abstract Map<String, Object> askQuestion(String question, String context, Insight insight, Map<String, Object> parameters);

	@Override
	public Map<String, Object> ask(String question, String context, Insight insight, Map<String, Object> parameters) {
		//Map<String, String> output = new HashMap<String, String>();
		//TODO turn into threads
		if(this.socketClient == null || !this.socketClient.isConnected()) {
			this.startServer();
		}
		
		Map<String, Object> retMap = null;
		String messageId = UUID.randomUUID().toString();
		if(parameters == null) {
			parameters = new HashMap<String, Object>();
		}
		
		
		if (Utility.isModelInferenceLogsEnabled()) {
			LocalDateTime inputTime = LocalDateTime.now();
			retMap = askQuestion(question, context, insight, parameters);
			LocalDateTime outputTime = LocalDateTime.now();
			
			String response = retMap.get(RESPONSE) + "";
			
			if (keepConversationHistory) {
				Map<String, Object> inputMap = new HashMap<String, Object>();
				Map<String, Object> outputMap = new HashMap<String, Object>();
				inputMap.put(ModelEngineConstants.ROLE, "user");
				inputMap.put(ModelEngineConstants.MESSAGE_CONTENT, question);
				outputMap.put(ModelEngineConstants.ROLE, "assistant");
				outputMap.put(ModelEngineConstants.MESSAGE_CONTENT, response);
		        
				if (chatHistory.containsKey(insight.getInsightId())) {
			        chatHistory.get(insight.getInsightId()).add(inputMap);
			        chatHistory.get(insight.getInsightId()).add(outputMap);
				}
			}
			Thread inferenceRecorder = new Thread(new ModelEngineInferenceLogsWorker (
					messageId, 
					"ask", 
					this, 
					insight,
					context, 
					question,
					getTokens(retMap.get(NUMBER_OF_TOKENS_IN_PROMPT)),
					inputTime, 
					response,
					getTokens(retMap.get(NUMBER_OF_TOKENS_IN_RESPONSE)),
					outputTime
			));
			inferenceRecorder.start();
		} else {
			retMap = askQuestion(question, context, insight, parameters);
		}
		
		retMap.put("messageId", messageId);
		retMap.put("roomId", insight.getInsightId());

		return retMap;
	}

	@Override
	public Object embeddings(List<String> stringsToEncode, Insight insight, Map <String, Object> parameters) {
		if(this.socketClient == null || !this.socketClient.isConnected())
			this.startServer();
		
		String varName = smssProp.getProperty(ModelEngineConstants.VAR_NAME);
	 	
		String pythonListAsString = PyUtils.determineStringType(stringsToEncode);
		
		StringBuilder callMaker = new StringBuilder();

		callMaker.append(varName)
				 .append(".embeddings(strings_to_embed = ")
				 .append(pythonListAsString);
				 
		if(this.prefix != null) {
			callMaker.append(", prefix='").append(this.prefix).append("'");
		}
		
		callMaker.append(")");
		
		Map<String, Object> output;
		classLogger.info("Making embeddings call on engine " + this.engineId);
		
		if (Utility.isModelInferenceLogsEnabled()) {
			String messageId = UUID.randomUUID().toString();
			LocalDateTime inputTime = LocalDateTime.now();
			output = (Map<String, Object>) pyt.runScript(callMaker.toString(), insight);
			LocalDateTime outputTime = LocalDateTime.now();
			Thread inferenceRecorder = new Thread(new ModelEngineInferenceLogsWorker (
					messageId, 
					"embeddings", 
					this, 
					insight, 
					null,
					"",
					getTokens(output.get(NUMBER_OF_TOKENS_IN_PROMPT)),
					inputTime, 
					"",
					getTokens(output.get(NUMBER_OF_TOKENS_IN_RESPONSE)),
					outputTime
			));
			inferenceRecorder.start();
		} else {
			output = (Map<String, Object>) pyt.runScript(callMaker.toString(), insight);
		}
		classLogger.info("Embeddings Received from engine " + this.engineId);
 			
		return output;
	}
	
	@Override
	public Object model(Object input, Insight insight, Map <String, Object> parameters) {
		if(this.socketClient == null || !this.socketClient.isConnected())
			this.startServer();

		
		String varName = smssProp.getProperty(ModelEngineConstants.VAR_NAME);
		
		StringBuilder callMaker = new StringBuilder(varName);
		
		String inputAsString = PyUtils.determineStringType(input);
		callMaker.append(".model(input = ")
				 .append(inputAsString);
				 
		if (parameters != null && !parameters.isEmpty()) {
			callMaker.append(", **")
					 .append(PyUtils.determineStringType(parameters));
		}

		callMaker.append(")");
		
		Object output;
		if (Utility.isModelInferenceLogsEnabled()) {			
			String messageId = UUID.randomUUID().toString();
			LocalDateTime inputTime = LocalDateTime.now();
			output = pyt.runScript(callMaker.toString(), insight);
			LocalDateTime outputTime = LocalDateTime.now();
			Thread inferenceRecorder = new Thread(new ModelEngineInferenceLogsWorker (
					messageId,
					"model", 
					this,
					insight,
					null,
					inputAsString,
					null,
					inputTime, 
					PyUtils.determineStringType(output),
					null,
					outputTime
			));
			inferenceRecorder.start();
		} else {
			output = pyt.runScript(callMaker.toString(), insight);
		}
		return output;
	}

	@Override
	public void close() throws IOException {
		if(this.socketClient != null && this.socketClient.isConnected() ) {
			this.socketClient.stopPyServe(cacheFolder.getAbsolutePath());
			this.socketClient.close();
			
			// we delete this directory so need to reset
			this.workingDirectoryBasePath = null;
		}
		if(this.p != null && this.p.isAlive()) {
			this.p.destroy();
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
	
	public boolean connectClient() {
		Thread t = new Thread(socketClient);
		t.start();
		while(!socketClient.isReady())
		{
			synchronized(socketClient)
			{
				try 
				{
					socketClient.wait();
					classLogger.info("Setting the socket client ");
				} catch (InterruptedException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}								
			}
		}
		return false;
	}
	
	private String fillVars(String input) {
		StringSubstitutor sub = new StringSubstitutor(vars);
		String resolvedString = sub.replace(input);
		return resolvedString;
	}
	
	public String getConversationHistoryFromInferenceLogs(String insightId, String userId){
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
	
	public String getConversationHistory(String userId, String insightId){
		if (keepConversationHistory){
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
	
	public Integer getTokens(Object numTokens) {
		if (numTokens instanceof Long) {
			return ((Long) numTokens).intValue();
		} else if (numTokens instanceof Double) {
			return ((Double) numTokens).intValue();
		} else if (numTokens instanceof String){
			return Integer.valueOf((String) numTokens);
		} else {
			return null;
		}
	}
	
	public boolean keepsConversationHistory() {
		return this.keepConversationHistory;
	}
	
	public boolean keepInputOutput() {
		return this.keepInputOutput;
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
		return this.smssProp;
	}

	@Override
	public IEngine.CATALOG_TYPE getCatalogType() {
		return IEngine.CATALOG_TYPE.MODEL;
	}
	
	@Override
	public String getCatalogSubType(Properties smssProp) {
		return this.getModelType().toString();
	}
	
	public String getWorkingDirectoryName() {
		return this.workingDirectory;
	}
	
	public String getWorkingDirectoryBasePath() {
		return this.workingDirectoryBasePath;
	}
	
	@Override
	public void delete() {
		classLogger.debug("Delete model engine " + SmssUtilities.getUniqueName(this.engineName, this.engineId));
		try {
			this.close();
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		File engineFolder = new File(
				EngineUtility.getSpecificEngineBaseFolder
					(IEngine.CATALOG_TYPE.FUNCTION, this.engineId, this.engineName)
				);
		if(engineFolder.exists()) {
			classLogger.info("Delete model engine folder " + engineFolder);
			try {
				FileUtils.deleteDirectory(engineFolder);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		} else {
			classLogger.info("Model engine folder " + engineFolder + " does not exist");
		}
		
		classLogger.info("Deleting model engine smss " + this.smssFilePath);
		File smssFile = new File(this.smssFilePath);
		try {
			FileUtils.forceDelete(smssFile);
		} catch(IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		// remove from DIHelper
		String engineIds = (String)DIHelper.getInstance().getEngineProperty(Constants.ENGINES);
		engineIds = engineIds.replace(";" + this.engineId, "");
		// in case we are at the start
		engineIds = engineIds.replace(this.engineId + ";", "");
		DIHelper.getInstance().setEngineProperty(Constants.ENGINES, engineIds);
		DIHelper.getInstance().removeEngineProperty(this.engineId);
	}
	
	@Override
	public boolean holdsFileLocks() {
		return false;
	}
}
