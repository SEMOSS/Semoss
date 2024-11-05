package prerna.engine.impl.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.engine.api.RemoteModelStateEnum;

import prerna.engine.api.ModelTypeEnum;

public class NEREngine extends AbstractRemoteModelEngine {
	
	private static final Logger classLogger = LogManager.getLogger(AbstractRemoteModelEngine.class);
	
	public String predict() {
		String modelStatus = getModelStatus();
		return modelStatus;
	}
	
	public String getModelStatus() {
		try {
		classLogger.info("Checking status for engineId: {}", this.engineId);
		RemoteModelStateEnum currentState = getCurrentModelState();
        classLogger.info("Current state for engineId {} is: {}", this.engineId, currentState);
		String modelState = currentState.name();
        return modelState;
		} catch (Exception e) {
			classLogger.error("Error getting model state", e);
			return "ERROR";
		}
		
	}
	
	
	
	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.NER;
	}

}
