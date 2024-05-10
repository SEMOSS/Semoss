package prerna.engine.impl.model;


import prerna.engine.api.ModelTypeEnum;

public class BedrockEngine extends AbstractPythonModelEngine {

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.BEDROCK;
	}
}
