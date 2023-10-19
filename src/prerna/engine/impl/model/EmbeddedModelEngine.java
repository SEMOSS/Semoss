package prerna.engine.impl.model;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.ModelTypeEnum;
import prerna.om.Insight;
import prerna.util.DIHelper;
import prerna.util.Settings;
import prerna.util.Utility;

public class EmbeddedModelEngine extends AbstractModelEngine {
	// starts a embedded model in the same environment
	private static Logger logger = LogManager.getLogger(EmbeddedModelEngine.class);
	
	//public EmbeddedModelEngine() {};
	
	@Override
	public String askQuestion(String question, String context, Insight insight, Map <String, Object> parameters) 
	{
		// TODO Auto-generated method stub
		//if(this.pyt == null)
		//	return null;
		if(this.socketClient == null || !this.socketClient.isConnected()) {
			this.startServer();
		}
		
		String varName = (String) smssProp.get(Settings.VAR_NAME);
		
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
				if(value instanceof String)
				{
					callMaker.append("'").append(value+"").append("'");
				}
				else
				{
					callMaker.append(PyUtils.determineStringType(value));
				}
			}
		}
		// also set the prefix if one exists
		if(this.prefix != null)
			callMaker.append(", prefix='").append(prefix).append("'");
		
		callMaker.append(")");
		System.err.println("call maker.. " + callMaker);
		
		Object output = pyt.runScript(callMaker.toString(), insight);
		return output+"";
	}
	
	@Override
	public Object model(String question, Insight insight, Map <String, Object> parameters) 
	{
		// TODO Auto-generated method stub
		//if(this.pyt == null)
		//	return null;
		if(this.socketClient == null || !this.socketClient.isConnected()) {
			this.startServer();
		}
		
		String varName = (String) smssProp.get(Settings.VAR_NAME);
		
		StringBuilder callMaker = new StringBuilder().append(varName).append(".ask(");
		callMaker.append("question=\"\"\"").append(question.replace("\"", "\\\"")).append("\"\"\"");
		
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
		// also set the prefix if one exists
		if(this.prefix != null)
			callMaker.append(", prefix='").append(prefix).append("'");
		
		callMaker.append(")");
		System.err.println("call maker.. " + callMaker);
		
		Object output = pyt.runScript(callMaker.toString(), insight);
		return output;
	}

	
	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.EMBEDDED;
	}
	
	
	public static void main(String [] args) throws Exception
	{
		String smssFilePath = "c:/users/pkapaleeswaran/workspacej3/SemossDev/db/PolicyBot.smss";
		
		DIHelper.getInstance().loadCoreProp("c:/users/pkapaleeswaran/workspacej3/SemossDev/RDF_MAP.prop");
		
		IModelEngine eng = new EmbeddedModelEngine();
		eng.open(smssFilePath);
		eng.startServer();
		
		Map <String, Object> params = new HashMap<String, Object>();
		params.put("max_new_tokens", 2000);
		params.put("temperature", 0.01);
		
		Map<String, String> output = eng.ask("What is the capital of India ?", null, null, params);
		
		//PyTranslator pyt = eng.getClient();
		
		//System.err.println(output);
		
		//Object output = pyt.runScript("i.ask(question='What is the capital of India ?')");
		
		//System.err.println(output);
	}
}
