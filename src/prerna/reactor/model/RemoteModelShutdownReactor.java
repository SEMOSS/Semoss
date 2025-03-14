package prerna.reactor.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.engine.impl.model.AbstractRemoteModelEngine;

public class RemoteModelShutdownReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(RemoteModelShutdownReactor.class);

	public RemoteModelShutdownReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey()};
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
	    organizeKeys();
	    String engineId = this.keyValue.get(this.keysToGet[0]);
	    
	    if(!SecurityEngineUtils.userIsOwner(this.insight.getUser(), engineId)) {
	        throw new IllegalArgumentException("Only the owner can shutdown the model");
	    }
	    
	    try {
	    	IModelEngine targetModel = Utility.getModel(engineId);
	    	AbstractRemoteModelEngine targetEngine = (AbstractRemoteModelEngine) targetModel;

	        String shutdownResult = targetEngine.shutdownModelRequest();
	        
	        return new NounMetadata(shutdownResult, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	    } catch (Exception e) {
	        classLogger.error("Error shutting down model: " + engineId, e);
	        throw new RuntimeException("Failed to shutdown model: " + e.getMessage());
	    }
	}
}
