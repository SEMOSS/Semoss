package prerna.engine.impl.model;


import prerna.engine.api.ModelTypeEnum;

public class BedrockEngine extends PythonModelEngine {
	

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.BEDROCK;
	}
}
