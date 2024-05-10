package prerna.engine.impl.model;

import prerna.engine.api.ModelTypeEnum;

public class TextGenerationEngine  extends AbstractPythonModelEngine {

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.TEXT_GENERATION;
	}
}
