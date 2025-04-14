package prerna.engine.impl.model;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import prerna.ds.py.PyUtils;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker;
import prerna.om.Insight;

public class EmbeddedModelEngine extends AbstractPythonModelEngine {
	
	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.EMBEDDED;
	}
	
}
