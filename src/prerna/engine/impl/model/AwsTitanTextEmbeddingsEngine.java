package prerna.engine.impl.model;

import java.util.Map;

import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.om.Insight;

public class AwsTitanTextEmbeddingsEngine extends AbstractPythonModelEngine {

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.AWS_TITAN_TEXT_EMBEDDINGS;
	}
	
	@Override
	public AskModelEngineResponse askCall(String question, Object fullPrompt, String context, Insight insight, Map<String, Object> parameters) {
		return new AskModelEngineResponse("This model does not support text generation.", 0, 0);
	}

	@Override
	protected Object modelCall(Object input, Insight insight, Map<String, Object> parameters) {
		return "This model does have an model method defined.";
	}
		
}
