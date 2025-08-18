package prerna.reactor.model;

import java.util.Map;
import java.util.List;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.GenRowStruct;
import prerna.engine.impl.model.KubernetesModelScaler;
import prerna.auth.utils.SecurityAdminUtils;



public class GetRemoteModelDeployConfigsReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GetRemoteModelDeployConfigsReactor.class);

	public GetRemoteModelDeployConfigsReactor() {
		this.keysToGet = new String[] {"refresh"};
		this.keyRequired = new int [] { 0 };
	}
	
	@Override
	public NounMetadata execute() {
		if (!SecurityAdminUtils.userIsAdmin(this.insight.getUser())) {
			throw new IllegalArgumentException("User does not have permission to query this endpoint.");
		}
		
		this.organizeKeys();
		Boolean refresh = this.getRefreshBool();
		
		final KubernetesModelScaler kmsServer;
		kmsServer = KubernetesModelScaler.getInstance();
		
		try {
			Map<String, Object> nodePoolsInfo = kmsServer.getModelDeploymentConfigs(refresh);
			return new NounMetadata(nodePoolsInfo, PixelDataType.MAP, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Error connecting to the Kubernetes Model Scaler endpoint for model deployment configurations..");
			throw new RuntimeException("Failed to connect to Kubernetes Model Scaler endpoint: " + e.getMessage());
		}
	}
	
	private boolean getRefreshBool() {
		GenRowStruct boolGrs = this.store.getNoun("refresh");
		if (boolGrs != null) {
			if (boolGrs.size() > 0) {
				List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
				return (boolean) val.get(0);
			}
		}
		return false;
	}

}
