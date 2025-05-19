package prerna.reactor.model;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.engine.impl.model.KubernetesModelScaler;
import prerna.auth.utils.SecurityAdminUtils;

public class GetNodePoolsInfoReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GetNodePoolsInfoReactor.class);

	@Override
	public NounMetadata execute() {
		if (!SecurityAdminUtils.userIsAdmin(this.insight.getUser())) {
			throw new IllegalArgumentException("User does not have permission to query this endpoint.");
		}
		
		final KubernetesModelScaler kmsServer;
		kmsServer = KubernetesModelScaler.getInstance();
		
		try {
			Map<String, Object> nodePoolsInfo = kmsServer.getNodePoolsInfo();
			return new NounMetadata(nodePoolsInfo, PixelDataType.MAP, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Error connecting to the Kubernetes Model Scaler endpoint for Nodepool information..");
			throw new RuntimeException("Failed to connect to Kubernetes Model Scaler endpoint: " + e.getMessage());
		}
	}
}
