package prerna.engine.impl.model;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyUtils;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.responses.NerModelEngineResponse;
import prerna.om.Insight;


public class NamedEntityRecognitionEngine extends AbstractPythonModelEngine {
	
	private static final Logger classLogger = LogManager.getLogger(NamedEntityRecognitionEngine.class);
	
	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.NAMED_ENTITY_RECOGNITION;
	}
	
	/**
	 * Passes a text string a list of labels to perform entity prediction from a NER model.
	 * @param text			The text to perform predictions on
	 * @param labels		The labels used for entity recognition
	 * @param insight		The insight from where the call is being made. The insight holds user credentials, project information and conversation history tied to the insightId
	 * @param parameters    Additional parameters such as temperature, top_k, max_new_tokens etc
	 * @return				A List of objects including the result text ("text"), the associated label ("label"), the confidence score ("score") and the start and end indexs of the entity in the text ("start") ("end")
	 */
	public NerModelEngineResponse predict(String text, List<String> labels, Insight insight, Map <String, Object> parameters) {
		checkSocketStatus();
		
		StringBuilder callMaker = new StringBuilder(this.varName + ".predict(");
		
		callMaker.append("text=\"\"\"").append(text.replace("\"", "\\\"")).append("\"\"\"");
		callMaker.append(",").append("labels=").append(PyUtils.determineStringType(labels));
		
		if(parameters != null) {
			Iterator <String> paramKeys = parameters.keySet().iterator();
			while(paramKeys.hasNext()) {
				String key = paramKeys.next();
				Object value = parameters.get(key);
				callMaker.append(",")
				         .append(key)
				         .append("=")
						 .append(PyUtils.determineStringType(value));
			}
		}

		if(this.prefix != null) {
			callMaker.append(", prefix='")
			 		 .append(prefix)
			 		 .append("'");
		}

		callMaker.append(")");
		
		classLogger.debug("Running >>>" + callMaker.toString());

		Object output = pyt.runScript(callMaker.toString(), insight);
		
		NerModelEngineResponse response = NerModelEngineResponse.fromObject(output);
		
		return response;
	}

}
