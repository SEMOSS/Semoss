package prerna.engine.impl.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


import java.util.Map.Entry;

import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;
import prerna.util.Constants;

public class OpenAiEngine extends EmbeddedModelEngine {
		
	@SuppressWarnings("unchecked")
	@Override
	public String ask(String question, String context, Insight insight, Map <String, Object> parameters) 
	{
		if(!this.socketClient.isConnected())
			this.startServer();
		
		if (!ModelInferenceLogsUtils.doModelIsRegistered(this.getEngineId())) {
			ModelInferenceLogsUtils.doCreateNewAgent(this.getEngineId(), this.getEngineName(), null, 
					this.getModelType().toString(), false, insight.getUser().getPrimaryLoginToken().getId());
		}
		
		String varName = (String)prop.get("VAR_NAME");
		boolean keepContext = Boolean.parseBoolean((String)prop.get("KEEP_CONTEXT"));
		
		String roomId = null;
		String messageId = null;
		String history = null;
		if (parameters.containsKey("ROOM_ID")){
			roomId = (String) parameters.get("ROOM_ID");
			parameters.remove("ROOM_ID");
			messageId = UUID.randomUUID().toString();
			
			if (keepContext && roomId != null && !roomId.isEmpty()) {
				history = getConversationHistory(roomId, insight.getUser().getPrimaryLoginToken().getId());
			}
		}
	
		StringBuilder callMaker = new StringBuilder().append(varName).append(".ask(");
		callMaker.append("question=\"").append(question).append("\"");
		if(context != null)
			callMaker.append(",").append("context=").append(context);
		if(history != null)
			callMaker.append(",").append("history=").append(history);
		
		Iterator <String> paramKeys = parameters.keySet().iterator();
		while(paramKeys.hasNext())
		{
			String key = paramKeys.next();
			callMaker.append(",").append(key).append("=");
			Object value = parameters.get(key);
			if(value instanceof String)
			{
				callMaker.append("'").append(value+"").append("'");
			}
			else
			{
				callMaker.append(value+"");
			}
		}
		callMaker.append(")");
		System.out.println(callMaker.toString());
		Object output = pyt.runScript(callMaker.toString());
		
		Map<Object, Object> outputMap = (Map<Object,Object>) output;
		if (keepContext && roomId != null && !roomId.isEmpty()) {
			
			//TODO placeholder for inference logs insert
			ModelInferenceLogsUtils.doRecordMessage(messageId, 
													"INPUT",
													"{'role': 'user', 'content': '"+question+"'}",
													roomId,
													this.getEngineId(),
													null,
													null,
													false,
													null,
													insight.getUser().getPrimaryLoginToken().getId()
													);
			
			ModelInferenceLogsUtils.doRecordMessage(messageId, 
													"RESPONSE",
													constructPyDictFromMap(outputMap),
													roomId,
													this.getEngineId(),
													null,
													null,
													false,
													null,
													insight.getUser().getPrimaryLoginToken().getId()
													);
		}
		return (String) outputMap.get("content");
	}
	
	// TODO placeholder for inference logs retrieval
	public String getConversationHistory(String roomId, String userId){
		// TODO make not a db call?
		if (ModelInferenceLogsUtils.doCheckConversationExists(roomId)) {
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
		else {
			ModelInferenceLogsUtils.doCreateNewConversation(roomId, "TestRoom", "TestDescription", 
					   "{}", userId, this.getModelType().toString(), true, null, this.getEngineId());
		}
		return null;
	}
	
    public static String constructPyDictFromMap(Map<Object,Object> theMap) {
    	StringBuilder theDict = new StringBuilder("{");
    	for (Entry<Object, Object> entry : theMap.entrySet()) {
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
    		return constructPyDictFromMap((Map<Object, Object>) obj);
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
    
	@Override
	public String getEngineId() {
		return prop.getProperty(Constants.ENGINE);
	}
	
	@Override
	public MODEL_TYPE getModelType() {
		return MODEL_TYPE.PROCESS;
	}

	@Override
	public String getEngineName() {
		return prop.getProperty(Constants.ENGINE_ALIAS);
	}
}
