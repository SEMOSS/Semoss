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

import prerna.ds.py.TCPPyTranslator;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker;
import prerna.om.Insight;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.NativePySocketClient;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Settings;
import prerna.util.Utility;

public abstract class AbstractModelEngine implements IModelEngine {
	
	private static final Logger classLogger = LogManager.getLogger(AbstractModelEngine.class);

	private static final String DIR_SEPERATOR = "/";
	private static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	public static final String PY_COMMAND_SEPARATOR = ";";
	public static final String CUR_DIR = "CUR_DIR";
	public static final String ENGINE_DIR = "ENGINE_DIR";
	public static final String MESSAGE_CONTENT = "content";
	public static final String ROLE = "role";
	
	protected String engineId = null;
	protected String engineName = null;
	
	protected Properties smssProp = null;
	protected String smssFilePath = null;
	
	protected String [] requiredVars = new String [] {Settings.VAR_NAME, Settings.PROMPT_STOPPER};
	protected boolean keepConversationHistory = false;

	// python server
	TCPPyTranslator pyt = null;
	NativePySocketClient socketClient = null;
	Process p = null;
	String port = null;
	String prefix = null;
	String workingDirecotry;
	String workingDirectoryBasePath;
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
		if (!this.smssProp.containsKey(CUR_DIR)) {
			String curDir = new File(smssFilePath).getParent().replace(FILE_SEPARATOR, DIR_SEPERATOR);
			this.smssProp.put(CUR_DIR, curDir);
		}
		if (!this.smssProp.containsKey(ENGINE_DIR)) {
			this.smssProp.put("ENGINE_DIR", this.smssProp.get(CUR_DIR) + "/" + this.engineName + "__" + this.engineId);
		}
		
		this.keepConversationHistory = Boolean.parseBoolean(this.smssProp.getProperty("KEEP_CONTEXT"));
		
		// create a generic folder
		this.workingDirecotry = "MODEL_" + Utility.getRandomString(6);
		this.workingDirectoryBasePath = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/" + workingDirecotry;
		this.cacheFolder = new File(workingDirectoryBasePath);
		
		// make the folder if one does not exist
		if(!this.cacheFolder.exists()) {
			this.cacheFolder.mkdir();
		}
			
		// vars for string substitution
		this.vars = new HashMap(this.smssProp);
	}

	@Override
	public void startServer() {
		// spin the server
		// start the client
		// get the startup command and parameters - at some point we need a better way than the command
		
		// execute all the basic commands
		String initCommands = (String) smssProp.get(Constants.INIT_MODEL_ENGINE);
		
		// break the commands seperated by ;
		String [] commands = initCommands.split(PY_COMMAND_SEPARATOR);
		
		// replace the Vars
		for(int commandIndex = 0; commandIndex < commands.length;commandIndex++) {
			commands[commandIndex] = fillVars(commands[commandIndex]);
		}
		port = Utility.findOpenPort();

		Object [] outputs = Utility.startTCPServerNativePy(this.workingDirectoryBasePath, port);
		this.p = (Process) outputs[0];
		this.prefix = (String) outputs[1];
		
		socketClient = new NativePySocketClient();
		socketClient.connect("127.0.0.1", Integer.parseInt(port), false);
		
		// connect the client
		connectClient();
		
		// create the py translator
		pyt = new TCPPyTranslator();
		pyt.setClient(socketClient);
		pyt.runEmptyPy(commands);	
		
		// run a prefix command
		setPrefix(this.prefix);
	}
	
	private void setPrefix(String prefix)
	{
		String [] alldata = new String[] {"prefix", prefix};
		PayloadStruct prefixPayload = new PayloadStruct();
		prefixPayload.payload = alldata;
		prefixPayload.operation = PayloadStruct.OPERATION.PYTHON;
		PayloadStruct ps = (PayloadStruct)socketClient.executeCommand(prefixPayload);
		//System.err.println(ps);
		
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
	public abstract String askQuestion(String question, String context, Insight insight, Map<String, Object> parameters);

	@Override
	public Map<String, String> ask(String question, String context, Insight insight, Map<String, Object> parameters) {
		//Map<String, String> output = new HashMap<String, String>();
		//TODO turn into threads
		if(!this.socketClient.isConnected()) {
			this.startServer();
		}
		
		String response = null;
		String messageId = UUID.randomUUID().toString();
		String roomId = null;

		if (Utility.isModelInferenceLogsEnabled()) {
			if(parameters == null) {
				parameters = new HashMap<String, Object>();
			}
			roomId = (String) parameters.get("ROOM_ID");

			// everything should be recorded so we always need a roomId
			if (roomId == null) {
				roomId = UUID.randomUUID().toString();
				parameters.put("ROOM_ID",roomId);
			}
			
			LocalDateTime inputTime = LocalDateTime.now();
			response = askQuestion(question, context, insight, parameters);
			LocalDateTime outputTime = LocalDateTime.now();
			
			if (keepConversationHistory) {
				Map<String, Object> inputMap = new HashMap<String, Object>();
				Map<String, Object> outputMap = new HashMap<String, Object>();
				inputMap.put(ROLE, "user");
				inputMap.put(MESSAGE_CONTENT, question);
				outputMap.put(ROLE, "assistant");
				outputMap.put(MESSAGE_CONTENT, response);
		        
				if (chatHistory.containsKey(roomId)) {
			        chatHistory.get(roomId).add(inputMap);
			        chatHistory.get(roomId).add(outputMap);
				}
			}
			ModelEngineInferenceLogsWorker inferenceRecorder = new ModelEngineInferenceLogsWorker(roomId, messageId, "ask", this, insight, context, question, inputTime, response, outputTime);
			inferenceRecorder.run();
		} else {
			response = askQuestion(question, context, insight, parameters);
		}
		
		Map<String, String> retMap = new HashMap<>();
		retMap.put("response", response);
		retMap.put("messageId", messageId);
		retMap.put("roomId", roomId);
		return retMap;
	}

	public Object embeddings(String question, Insight insight, Map <String, Object> parameters) {
		if(!this.socketClient.isConnected())
			this.startServer();
		String varName = (String) smssProp.get("VAR_NAME");
	
		StringBuilder callMaker = new StringBuilder().append(varName).append(".embeddings(");
		callMaker.append("question=\"").append(question).append("\"").append(")");
		Object output;
		if (Utility.isModelInferenceLogsEnabled()) {
			String roomId = null;
			if(parameters != null) {
				if (parameters.containsKey("ROOM_ID")) { 
					roomId = (String) parameters.get("ROOM_ID");
				}
			}
			// everything should be recorded so we always need a roomId
			if (roomId == null) {
				roomId = insight.getInsightId();
			}
			
			String messageId = UUID.randomUUID().toString();
			LocalDateTime inputTime = LocalDateTime.now();
			output = pyt.runScript(callMaker.toString());
			LocalDateTime outputTime = LocalDateTime.now();
			
			if (keepConversationHistory) {
				Map<String, Object> inputMap = new HashMap<String, Object>();
				Map<String, Object> outputMap = new HashMap<String, Object>();
				inputMap.put(ROLE, "user");
				inputMap.put(MESSAGE_CONTENT, question);
				outputMap.put(ROLE, "assistant");
				outputMap.put(MESSAGE_CONTENT, output);
			}
			
			ModelEngineInferenceLogsWorker inferenceRecorder = new ModelEngineInferenceLogsWorker(roomId, messageId, "embeddings", this, insight, null, question, inputTime, ModelInferenceLogsUtils.determineStringType(output), outputTime);
			inferenceRecorder.run();
		} else {
			output = pyt.runScript(callMaker.toString());
		}
		return output;
	}
	
	@Override
	public void stopModel() {
		try {
			socketClient.crash();
			FileUtils.deleteDirectory(cacheFolder);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
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
	
	public boolean keepsConversationHistory() {
		return this.keepConversationHistory;
	}
	
	public String getConversationHistoryFromInferenceLogs(String roomId, String userId){
		List<Map<String, Object>> convoHistoryFromDb = ModelInferenceLogsUtils.doRetrieveConversation(userId, roomId, "ASC");
		if (convoHistoryFromDb.size() > 0) {
			for (Map<String, Object> record : convoHistoryFromDb) {
				Object messageData = record.get("MESSAGE_DATA");
				Map<String, Object> mapHistory = new HashMap<String, Object>();
				if (record.get("MESSAGE_TYPE").equals("RESPONSE")) {

					mapHistory.put(ROLE, "assistant");
					mapHistory.put(MESSAGE_CONTENT, messageData);
			            

				} else {
					mapHistory.put(ROLE, "user");
					mapHistory.put(MESSAGE_CONTENT, messageData);
				}
		        chatHistory.get(roomId).add(mapHistory);
			}
			ArrayList<Map<String, Object>> convoHistory = chatHistory.get(roomId);
			StringBuilder convoList = new StringBuilder("[");
			boolean isFirstElement = true;
			for (Map<String, Object> record : convoHistory) {
				if (!isFirstElement) {
					convoList.append(",");
				} else {
					isFirstElement = false;
				}
				Object priorContent = ModelInferenceLogsUtils.determineStringType(record);
		        convoList.append(priorContent);
			}
			convoList.append("]");
			return convoList.toString();
		}
		return null;
	}
	
	public String getConversationHistory(String userId, String roomId){
		if (keepConversationHistory){
			if (chatHistory.containsKey(roomId)) {
				ArrayList<Map<String, Object>> convoHistory = chatHistory.get(roomId);
				StringBuilder convoList = new StringBuilder("[");
				boolean isFirstElement = true;
				for (Map<String, Object> record : convoHistory) {
					if (!isFirstElement) {
						convoList.append(",");
					} else {
						isFirstElement = false;
					}
					Object priorContent = ModelInferenceLogsUtils.determineStringType(record);
			        convoList.append(priorContent);
				}
				convoList.append("]");
				return convoList.toString();
			} 
			else {
				// we want to start a conversation
				ArrayList<Map<String, Object>> userNewChat = new ArrayList<Map<String, Object>>();
				chatHistory.put(roomId, userNewChat);
				String dbConversation = getConversationHistoryFromInferenceLogs(roomId, userId);
				return dbConversation;
			}
		}
		return null;
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
		return this.workingDirecotry;
	}
	
	public String getWorkingDirectoryBasePath() {
		return this.workingDirectoryBasePath;
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}
	
	@Override
	public void delete() {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public boolean holdsFileLocks() {
		return false;
	}
}
