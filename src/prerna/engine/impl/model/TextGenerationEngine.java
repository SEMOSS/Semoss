package prerna.engine.impl.model;

import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;
import prerna.util.Utility;

public class TextGenerationEngine  extends AbstractModelEngine {

	private static final Logger classLogger = LogManager.getLogger(TextGenerationEngine.class);
	
	@Override
	public String askQuestion(String question, String context, Insight insight, Map<String, Object> parameters) {
		
		String varName = (String) smssProp.get("VAR_NAME");
		
		StringBuilder callMaker = new StringBuilder().append(varName).append(".ask(");
		callMaker.append("question=\"\"\"").append(question.replace("\"", "\\\"")).append("\"\"\"");
		if(context != null)
			callMaker.append(",").append("context=\"\"\"").append(context.replace("\"", "\\\"")).append("\"\"\"");
		
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
				if (key.equals("full_prompt"))
				{
					callMaker.append(ModelInferenceLogsUtils.determineStringType(value));
				}
				else if(value instanceof String){
					callMaker.append("'").append(value+"").append("'");
				}
				else
				{
					callMaker.append(ModelInferenceLogsUtils.determineStringType(value));
				}
			}
		}
		if(this.prefix != null)
			callMaker.append(", prefix='").append(prefix).append("'");
		callMaker.append(")");
		classLogger.info("Running >>>" + callMaker.toString());
		Object output = pyt.runScript(callMaker.toString(), insight);
		return output+"";
	}

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.TEXT_GENERATION;
	}

	
}
