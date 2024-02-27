package prerna.engine.impl.model;

import prerna.engine.api.ModelTypeEnum;

public class OpenAiEngine extends PythonModelEngine {
	

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.OPEN_AI;
	}
}
