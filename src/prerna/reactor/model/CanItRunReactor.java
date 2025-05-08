package prerna.reactor.model;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.engine.impl.model.KubernetesModelScaler;

public class CanItRunReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(MyRemoteModelsStatus.class);
	
	public CanItRunReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.HF_MODEL_ID.getKey()};
		this.keyRequired = new int[] { 1 };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String hfModelId = this.keyValue.get(this.keysToGet[0]);
		
		final KubernetesModelScaler kmsServer;
		kmsServer = KubernetesModelScaler.getInstance();
		
		try {
			Map<String, Object> canRun = kmsServer.canItRun(hfModelId);
			return new NounMetadata(canRun, PixelDataType.MAP, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Error checking if model can run: " + hfModelId, e);
			throw new RuntimeException("Failed to check if model can run: " + e.getMessage());
		}
		
	}

}
