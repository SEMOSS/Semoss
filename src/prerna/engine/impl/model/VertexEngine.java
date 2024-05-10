package prerna.engine.impl.model;

import prerna.engine.api.ModelTypeEnum;

public class VertexEngine extends AbstractPythonModelEngine {

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.VERTEX;
	}
}
