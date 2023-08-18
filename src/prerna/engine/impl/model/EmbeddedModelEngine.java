package prerna.engine.impl.model;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IModelEngine;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
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
		if(!this.socketClient.isConnected())
			this.startServer();
		
		String varName = (String) generalEngineProp.get(Settings.VAR_NAME);
		
		StringBuilder callMaker = new StringBuilder().append(varName).append(".ask(");
		callMaker.append("question=\"").append(question.replace("\"", "\\\"")).append("\"");
		if(context != null)
			callMaker.append(",").append("context=\"").append(context.replace("\"", "\\\"")).append("\"");	
		
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
		System.err.println("call maker.. " + callMaker);
		
		Object output = pyt.runScript(callMaker.toString(), insight);
		return output+"";
	}
	
	public static void main(String [] args)
	{
		String propFile = "c:/users/pkapaleeswaran/workspacej3/SemossDev/db/PolicyBot.smss";
		
		DIHelper.getInstance().loadCoreProp("c:/users/pkapaleeswaran/workspacej3/SemossDev/RDF_MAP.prop");
		
		IModelEngine eng = new EmbeddedModelEngine();
		eng.loadModel(propFile);
		eng.startServer();
		
		Map <String, Object> params = new HashMap<String, Object>();
		params.put("max_new_tokens", 200);
		params.put("temperature", 0.01);
		
		Map<String, String> output = eng.ask("What is the capital of India ?", null, null, params);
		
		//PyTranslator pyt = eng.getClient();
		
		System.err.println(output);
		
		//Object output = pyt.runScript("i.ask(question='What is the capital of India ?')");
		
		//System.err.println(output);
	}

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.EMBEDDED;
	}
}
