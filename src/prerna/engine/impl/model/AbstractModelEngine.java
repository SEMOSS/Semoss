package prerna.engine.impl.model;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.ds.py.TCPPyTranslator;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.reactor.job.JobReactor;
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
	
	@Override
	public void loadModel(String modelSmssFilePath) {
		try {
			if (modelSmssFilePath != null) {
				logger.info("Loading Model - " + Utility.cleanLogString(FilenameUtils.getName(modelSmssFilePath)));
				setSmssFilePath(modelSmssFilePath);
				setSmssProp(Utility.loadProperties(modelSmssFilePath));
			}
			if(this.generalEngineProp != null) {
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
					String engineName = generalEngineProp.getProperty(Constants.ENGINE_ALIAS);
					String engineID = generalEngineProp.getProperty(Constants.ENGINE);
					generalEngineProp.put("ENGINE_DIR", generalEngineProp.get(CUR_DIR) + "/" + engineName + "__" + engineID);
				}
				
				keepConversationHistory = Boolean.parseBoolean((String) generalEngineProp.get("KEEP_CONTEXT"));
				
				// create a generic folder
				this.workingDirecotry = "EM_MODEL_" + Utility.getRandomString(6);
				this.workingDirectoryBasePath = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/" + workingDirecotry;
				this.cacheFolder = new File(workingDirectoryBasePath);
				
				// make the folder if one does not exist
				if(!cacheFolder.exists())
					cacheFolder.mkdir();
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
		
		// replace the Vars -- dont think we need this anymore
//		for(int commandIndex = 0; commandIndex < commands.length;commandIndex++) {
//			commands[commandIndex] = fillVars(commands[commandIndex]);
//		}
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
		Map<String, String> output = new HashMap<String, String>();
		if(!this.socketClient.isConnected())
			this.startServer();
		
		// has to be set to null until we figure out NativePyEngineWorker getting insight
		String sessionId = null;
		if (insight.getVarStore().containsKey(JobReactor.SESSION_KEY)) {
			sessionId = (String) insight.getVarStore().get(JobReactor.SESSION_KEY).getValue();
		}
		// assumption, if project level, then they will be inferencing through a saved insight
		String projectId = insight.getProjectId();
		User user = insight.getUser();
		String userId = user.getPrimaryLoginToken().getId();
		String response;
		String roomId = (String) parameters.get("ROOM_ID");

		// everything should be recorded so we always need a roomId
		if (roomId == null) {
			roomId = UUID.randomUUID().toString();
		}
		
		// TODO this needs to be moved to wherever we "publish" a new LLM/agent
		if (!ModelInferenceLogsUtils.doModelIsRegistered(this.getEngineId())) {
			ModelInferenceLogsUtils.doCreateNewAgent(this.getEngineId(), this.getEngineName(), null, 
					this.getModelType().toString(), user.getPrimaryLoginToken().getId());
		}
		
		checkIfConversationExists(user, roomId, projectId, question);
		
		String messageId = UUID.randomUUID().toString();
		LocalDateTime inputTime = LocalDateTime.now();
		if (keepConversationHistory) { //python client needs to be configured to add history
			Map<String, Object> inputOutputMap = new HashMap<String, Object>();
			inputOutputMap.put(ROLE, "user");
			inputOutputMap.put(MESSAGE_CONTENT, question);
			response = askQuestion(question, context, insight, parameters);
			ModelInferenceLogsUtils.doRecordMessage(messageId, 
													"INPUT",
													constructPyDictFromMap(inputOutputMap),
													getTokenSizeString(question),
													inputTime,
													roomId,
													this.getEngineId(),
													sessionId,
													userId
													);
			
			// TODO need to find a way to pass this back
			inputOutputMap.put(ROLE, "assistant");
			inputOutputMap.put(MESSAGE_CONTENT, response);
			ModelInferenceLogsUtils.doRecordMessage(messageId, 
													"RESPONSE",
													constructPyDictFromMap(inputOutputMap),
													getTokenSizeString(response),
													roomId,
													this.getEngineId(),
													sessionId,
													userId
													);
		} else {
			response = askQuestion(question, context, insight, parameters);
			ModelInferenceLogsUtils.doRecordMessage(messageId, 
													"INPUT",
													null,
													getTokenSizeString(question),
													inputTime,
													roomId,
													this.getEngineId(),
													sessionId,
													userId
													);
			
			ModelInferenceLogsUtils.doRecordMessage(messageId, 
													"RESPONSE",
													null,
													getTokenSizeString(response),
													roomId,
													this.getEngineId(),
													sessionId,
													userId
													);
		}
		// TODO make these constants so we can start to track where they are being used
		output.put("ROOM_ID",roomId);
		output.put("MESSAGE_ID",messageId);
		output.put("RESPONSE",response);
		return output;
	}

	// Abstract method, child classes should construct their input / output here
	protected abstract String askQuestion(String question, String context, Insight insight, Map<String, Object> parameters);
	
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
	
	public void checkIfConversationExists (User user, String roomId, String projectId, String question){
		// TODO make not a db call?
		if (!ModelInferenceLogsUtils.doCheckConversationExists(roomId)) {
			String roomName = generateRoomTitle(question);
			ModelInferenceLogsUtils.doCreateNewConversation(roomId, roomName, "", 
					   "{}", user.getPrimaryLoginToken().getId(), this.getModelType().toString(), true, projectId, this.getEngineId());
			logger.info("New inference started by " + user.getPrimaryLoginToken().getUsername());
		}
	}
	
	public String generateRoomTitle(String originalQuestion) {
		StringBuilder summarizeStatement = new StringBuilder("summarize \\\"");
		summarizeStatement.append(originalQuestion);
		summarizeStatement.append("\\\" in less than 8 words. Please exclude all punctuation from the response.");
		String roomTitle = askQuestion(summarizeStatement.toString(), null, null, null);
		return roomTitle;
	}
	
	// this is good for python dictionaries but also for making sure we can easily construct 
	// the logs into model inference python list, since everything is python at this point.
    public static String constructPyDictFromMap(Map<String,Object> theMap) {
    	StringBuilder theDict = new StringBuilder("{");
    	for (Entry<String, Object> entry : theMap.entrySet()) {
    		theDict.append(determineStringType(entry.getKey())).append(":").append(determineStringType(entry.getValue())).append(",");
    	}
    	theDict.append("}");
    	return theDict.toString();
    }
    
    /* This is basically a utility method that attemps to generate the python code (string) for a java object.
	 * It currently only does base types.
	 * Potentially move it in the future but just keeping it here for now
	*/
    @SuppressWarnings("unchecked")
    public static String determineStringType(Object obj) {
    	if (obj instanceof Integer || obj instanceof Double || obj instanceof Long) {
    		return String.valueOf(obj);
    	} else if (obj instanceof Map) {
    		return constructPyDictFromMap((Map<String, Object>) obj);
    	} else if (obj instanceof ArrayList || obj instanceof Object[] || obj instanceof List) {
    		StringBuilder theList = new StringBuilder("[");
    		List<Object> list;
    		if (obj instanceof ArrayList<?>) {
    			list = (ArrayList<Object>) obj;
    		} else if ((obj instanceof Object[])) {
    			list = Arrays.asList((Object[]) obj);
    		} else {
    			list = (List<Object>) obj;
    		}
    		
			for (Object subObj : list) {
				theList.append(determineStringType(subObj)).append(",");
        	}
			theList.append("]");
			return theList.toString();
    	} else if (obj instanceof Boolean) {
    		String boolString = String.valueOf(obj);
    		// convert to py version
    		String cap = boolString.substring(0, 1).toUpperCase() + boolString.substring(1);
    		return cap;
    	} else if (obj instanceof Set<?>) {
    		StringBuilder theSet = new StringBuilder("{");
    		Set<?> set = (Set<?>) obj;
			for (Object subObj : set) {
				theSet.append(determineStringType(subObj)).append(",");
        	}
			theSet.append("}");
			return theSet.toString();
    	} else {
    		return "\'"+String.valueOf(obj).replace("'", "\\'").replace("\n", "\\n") + "\'";
    	}
    }
    
    public static Integer getTokenSizeString(String input) {
        if (input == null || input.trim().isEmpty()) {
            return 0;
        }

        //TODO should we be using the model tokenizer?
        StringTokenizer str_arr = new StringTokenizer(input);
 
        return str_arr.countTokens();
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
	public MODEL_TYPE getModelType() {
		return MODEL_TYPE.PROCESS;
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
	
}
