package prerna.engine.impl.model;


import prerna.engine.api.ModelTypeEnum;

public class VertexEngine extends PythonModelEngine {

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.VERTEX;
	}
}
