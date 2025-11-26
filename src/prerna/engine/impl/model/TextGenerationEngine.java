package prerna.engine.impl.model;

import prerna.engine.api.ModelTypeEnum;
import prerna.logging.IgnoreEngineLogging;

public class TextGenerationEngine extends AbstractPythonModelEngine {

	@Override
	@IgnoreEngineLogging
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.TEXT_GENERATION;
	}
}
