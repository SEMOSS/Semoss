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
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.ds.py.TCPPyTranslator;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker;
import prerna.om.Insight;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.tcp.client.NativePySocketClient;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Settings;
import prerna.util.Utility;


public abstract class AbstractModelEngine implements IModelEngine {
	
	private static final String DIR_SEPERATOR = "/";
	private static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	private static final Logger logger = LogManager.getLogger(AbstractModelEngine.class);
	
	public static final String PY_COMMAND_SEPARATOR = ";";
	public static final String CUR_DIR = "CUR_DIR";
	public static final String ENGINE_DIR = "ENGINE_DIR";
	public static final String MESSAGE_CONTENT = "content";
	public static final String ROLE = "role";
	
	protected String engineId = null;
	protected String engineName = null;
	protected boolean keepConversationHistory = false;
	
	//TODO think through this
	protected Properties generalEngineProp = null;
	protected String smssFilePath = null;
	protected String [] requiredVars = new String [] {Settings.VAR_NAME, Settings.PROMPT_STOPPER};
	
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
	public void loadModel(String modelSmssFilePath) {
		try {
			if (modelSmssFilePath != null) {
				logger.info("Loading Model - " + Utility.cleanLogString(FilenameUtils.getName(modelSmssFilePath)));
				setSmssFilePath(modelSmssFilePath);
				setSmssProp(Utility.loadProperties(modelSmssFilePath));
			}
			if(this.generalEngineProp != null) {
				this.engineId = generalEngineProp.getProperty(Constants.ENGINE);
				this.engineName = generalEngineProp.getProperty(Constants.ENGINE_ALIAS);
				
				for (String var : requiredVars) {
					if(!generalEngineProp.containsKey(var)) {
						String randomString = "v_" + Utility.getRandomString(6);
						generalEngineProp.put(var, randomString);
					}
				}
				if (!generalEngineProp.containsKey(CUR_DIR)) {
					String curDir = new File(modelSmssFilePath).getParent().replace(FILE_SEPARATOR, DIR_SEPERATOR);
					generalEngineProp.put(CUR_DIR, curDir);
				}
				if (!generalEngineProp.containsKey(ENGINE_DIR)) {
					generalEngineProp.put("ENGINE_DIR", generalEngineProp.get(CUR_DIR) + "/" + this.engineName + "__" + this.engineId);
				}
				
				keepConversationHistory = Boolean.parseBoolean((String) generalEngineProp.get("KEEP_CONTEXT"));
				
				// create a generic folder
				this.workingDirecotry = "EM_MODEL_" + Utility.getRandomString(6);
				this.workingDirectoryBasePath = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/" + workingDirecotry;
				this.cacheFolder = new File(workingDirectoryBasePath);
				
				// make the folder if one does not exist
				if(!cacheFolder.exists())
					cacheFolder.mkdir();
				
				// vars for string substitution
				vars = new HashMap(generalEngineProp);
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Unable to load model details from the SMSS file");
		}
	}

	@Override
	public void startServer() {
		// spin the server
		// start the client
		// get the startup command and parameters - at some point we need a better way than the command
		
		// execute all the basic commands
		String initCommands = (String) generalEngineProp.get(Constants.INIT_MODEL_ENGINE);
		
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
	}

	@Override
	public Map<String, String> ask(String question, String context, Insight insight, Map<String, Object> parameters) {
		//Map<String, String> output = new HashMap<String, String>();
		//TODO turn into threads
		if(!this.socketClient.isConnected()) {
			this.startServer();
		}
		
		String response = null;
		String messageId = UUID.randomUUID().toString();

		if (Utility.isModelInferenceLogsEnabled()) {
			if(parameters == null) {
				parameters = new HashMap<String, Object>();
			}
			String roomId = (String) parameters.get("ROOM_ID");
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
		            
		        chatHistory.get(roomId).add(inputMap);
		        chatHistory.get(roomId).add(outputMap);
			}
			ModelEngineInferenceLogsWorker inferenceRecorder;
			if (context != null) {
				inferenceRecorder = new ModelEngineInferenceLogsWorker(roomId, messageId, "ask", this, insight, context + "\n" + question, inputTime, response, outputTime);
			} else {
				inferenceRecorder = new ModelEngineInferenceLogsWorker(roomId, messageId, "ask", this, insight, question, inputTime, response, outputTime);
			}
			inferenceRecorder.run();
		} else {
			response = askQuestion(question, context, insight, parameters);
		}
		
		Map<String, String> retMap = new HashMap<>();
		retMap.put("response", response);
		retMap.put("messageId", messageId);
		return retMap;
	}

	// Abstract method, child classes should construct their input / output here
	public abstract String askQuestion(String question, String context, Insight insight, Map<String, Object> parameters);
	
	public Object embeddings(String question, Insight insight, Map <String, Object> parameters) {
		if(!this.socketClient.isConnected())
			this.startServer();
		String varName = (String) generalEngineProp.get("VAR_NAME");
	
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
			
			ModelEngineInferenceLogsWorker inferenceRecorder = new ModelEngineInferenceLogsWorker(roomId, messageId, "embeddings", this, insight, question, inputTime, ModelInferenceLogsUtils.determineStringType(output), outputTime);
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
			logger.error(Constants.STACKTRACE, e);
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
					logger.info("Setting the socket client ");
				} catch (InterruptedException e) {
					logger.error(Constants.STACKTRACE, e);
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
	
	public String getConversationHistory(String roomId, String userId){
		// TODO make not a db call?
		
		List<Map<String, Object>> convoHistory = ModelInferenceLogsUtils.doRetrieveConversation(roomId, userId);
				
		StringBuilder convoList = new StringBuilder("[");
		for (Map<String, Object> record : convoHistory) {
			// Convert Map<String, Object> to Map<Object, Object>
			
			Object priorContent = record.get("MESSAGE_DATA");
			String priorContentString = (String) priorContent;
	        convoList.append(priorContentString).append(",");
		}
		convoList.append("]");
		return convoList.toString();
	}
	
	public String getConversationHistory(String roomId){
		if (keepConversationHistory){
			if (chatHistory.containsKey(roomId)) {
				ArrayList<Map<String, Object>> convoHistory = chatHistory.get(roomId);
				StringBuilder convoList = new StringBuilder("[");
				for (Map<String, Object> record : convoHistory) {
					Object priorContent = ModelInferenceLogsUtils.determineStringType(record);
			        convoList.append(priorContent).append(",");
				}
				convoList.append("]");
				return convoList.toString();
			}
			else {
				// we want to start a conversation
				ArrayList<Map<String, Object>> userNewChat = new ArrayList<Map<String, Object>>();
				chatHistory.put(roomId, userNewChat);
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
		this.generalEngineProp = smssProp;
	}

	@Override
	public Properties getSmssProp() {
		return this.generalEngineProp;
	}

	@Override
	public Properties getOrigSmssProp() {
		return this.generalEngineProp;
	}

	@Override
	public String getCatalogType(Properties smssProp) {
		return IModelEngine.CATALOG_TYPE;
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
	
	public boolean keepsConversationHistory() {
		return this.keepConversationHistory;
	}
	
}
