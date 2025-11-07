package prerna.engine.impl.model;

import prerna.engine.api.ModelTypeEnum;
import prerna.logging.IgnoreEngineLogging;

public class VertexEngine extends AbstractPythonModelEngine {

	@Override
	@IgnoreEngineLogging
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.VERTEX;
	}
}
