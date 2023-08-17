package prerna.engine.impl.model;

import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;
import prerna.util.Utility;

public class OpenAiEngine extends AbstractModelEngine {
	private static final Logger logger = LogManager.getLogger(OpenAiEngine.class);

	@Override
	public String askQuestion(String question, String context, Insight insight, Map <String, Object> parameters) 
	{
		String varName = (String) generalEngineProp.get("VAR_NAME");
	
		StringBuilder callMaker = new StringBuilder().append(varName).append(".ask(");
		callMaker.append("question=\"").append(question).append("\"");
		if(context != null)
			callMaker.append(",").append("context=\"").append(context).append("\"");
		
		
		if(parameters != null) {
			if (parameters.containsKey("ROOM_ID")) { //always have to remove roomId so we dont pass it to py client
				String roomId = (String) parameters.get("ROOM_ID");
				parameters.remove("ROOM_ID");
				if (Utility.isModelInferenceLogsEnabled()) { // have to check that inference logs are enabled so that query works
					String history = getConversationHistory(insight.getUserId(), roomId);
					if(history != null) //could still be null if its the first question in the convo
						callMaker.append(",").append("history=").append(history);
				}
			}

			Iterator <String> paramKeys = parameters.keySet().iterator();
			while(paramKeys.hasNext()) {
				String key = paramKeys.next();
				callMaker.append(",").append(key).append("=");
				Object value = parameters.get(key);
				if(value instanceof String)
				{
					callMaker.append("'").append(value+"").append("'");
				}
				else
				{
					callMaker.append(ModelInferenceLogsUtils.determineStringType(value));
				}
			}
		}
		callMaker.append(")");
		OpenAiEngine.logger.info("Running >>>" + callMaker.toString());
		Object output = pyt.runScript(callMaker.toString());
		return (String) output;
	}

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.OPEN_AI;
	}
}
