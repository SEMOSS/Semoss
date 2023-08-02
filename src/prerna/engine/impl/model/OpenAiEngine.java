package prerna.engine.impl.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


import java.util.Map.Entry;

import prerna.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;

public class OpenAiEngine extends EmbeddedModelEngine {
	
	private Map<String, ArrayList<Map<Object, Object>>> chatHistory = new Hashtable<>();
	
	@SuppressWarnings("unchecked")
	@Override
	public String ask(String question, String context, Insight insight, Map <String, Object> parameters) 
	{
		if(!this.socketClient.isConnected())
			this.startServer();
		
		String varName = (String)prop.get("VAR_NAME");
		boolean keepContext = Boolean.parseBoolean((String)prop.get("KEEP_CONTEXT"));
		
		String convoId = null;
		String messageId = null;
		String history = null;
		if (parameters.containsKey("CONVO_ID")){
			convoId = (String) parameters.get("CONVO_ID");
			parameters.remove("CONVO_ID");
			messageId = UUID.randomUUID().toString();
			
			if (keepContext && convoId != null && !convoId.isEmpty()) {
				history = getConversationHistory(convoId, insight.getUser().getPrimaryLoginToken().getId());
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
		if (keepContext && convoId != null && !convoId.isEmpty()) {
			
			//TODO placeholder for inference logs insert
//			ModelInferenceLogsUtils.doRecordMessage(messageId, 
//					convoId, 
//					insight.getUser().getPrimaryLoginToken().getId(), 
//					getEngineId(), 
//					"{'role': 'user', 'content': '"+question+"'}", 
//					"INPUT");
			
//			ModelInferenceLogsUtils.doRecordMessage(messageId, 
//					convoId, 
//					insight.getUser().getPrimaryLoginToken().getId(), 
//					getEngineId(), 
//					constructPyDictFromMap(outputMap), 
//					"RESPONSE");
			
			Map<Object, Object> inputQuestion = new HashMap<Object, Object>();
			inputQuestion.put("role","user");
			inputQuestion.put("content",question);
			chatHistory.get(insight.getUser().getPrimaryLoginToken().getId()+convoId).add(inputQuestion);
			chatHistory.get(insight.getUser().getPrimaryLoginToken().getId()+convoId).add(outputMap);
		}
		return (String) outputMap.get("content");
	}
	
	// temporary method while we implement inference logs db
	public String getConversationHistory(String convoId, String userId){
		if (chatHistory.containsKey(userId+convoId)) {
			ArrayList<Map<Object, Object>> convoHistory = chatHistory.get(userId+convoId);
			StringBuilder convoList = new StringBuilder("[");
			for (Map<Object, Object> record : convoHistory) {
				Object priorContent = determineStringType(record);
		        convoList.append(priorContent).append(",");
			}
			convoList.append("]");
			return convoList.toString();
		}
		else {
			// we want to start a conversation
			ArrayList<Map<Object, Object>> userNewChat = new ArrayList<Map<Object, Object>>();
			chatHistory.put(userId+convoId, userNewChat);
		}
		return null;
	}
	
	public void deleteConversation(String convoId, String userId) {
		if (chatHistory.containsKey(userId+convoId)) {
			this.chatHistory.remove(userId+convoId);
		}
	}
	
	// TODO placeholder for inference logs retrieval
	public String getContext(String convoId, String userId){
		// TODO make not a db call?
		if (ModelInferenceLogsUtils.doCheckConvoExists(convoId)) {
			List<Map<String, Object>> convoHistory = ModelInferenceLogsUtils.doRetrieveConvo(convoId, userId);
					
			StringBuilder convoList = new StringBuilder("[");
			for (Map<String, Object> record : convoHistory) {
				// Convert Map<String, Object> to Map<Object, Object>
				
				Object priorContent = record.get("CONTENT");
				String priorContentString = (String) priorContent;
		        convoList.append(priorContentString).append(",");
			}
			convoList.append("]");
			return convoList.toString();
		}
		else {
			ModelInferenceLogsUtils.doCreateNewConversation(convoId, "TestConvo1", userId);
		}
		return null;
	}
	
	public static String escapeSingleQuote(String input) {
        return input.replace("'", "\\'");
    }
	
    public static String replaceNewlineWithBackslash(String input) {
        return input.replace("\n", "\\");
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
		return (String) prop.get("ENGINE");
	}
}
