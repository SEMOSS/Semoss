package prerna.engine.impl.model;

import prerna.engine.api.ModelTypeEnum;

public class TextGenerationEngine  extends PythonModelEngine {

	//private static final Logger classLogger = LogManager.getLogger(TextGenerationEngine.class);

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.TEXT_GENERATION;
	}
}
