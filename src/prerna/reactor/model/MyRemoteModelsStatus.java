package prerna.reactor.model;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.cluster.util.IRemoteClientServer;
import prerna.cluster.util.RemoteModelInfo;
import prerna.cluster.util.ZKClientFactory;


public class MyRemoteModelsStatus extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(MyRemoteModelsStatus.class);
	
	@Override
	public NounMetadata execute() {
		final IRemoteClientServer zkClient = ZKClientFactory.getZKClient();
		
		List<RemoteModelInfo> activeModels = zkClient.getActiveModels();
	    List<RemoteModelInfo> warmingModels = zkClient.getWarmingModels();
	    
	    List<RemoteModelInfo> myActiveModels = new ArrayList<>();
	    List<RemoteModelInfo> myWarmingModels = new ArrayList<>();
	    
		for (RemoteModelInfo model : activeModels) {
			if (SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), model.getId())) {
				myActiveModels.add(model);
			}
		}
		
		for (RemoteModelInfo model : warmingModels) {
			if (SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), model.getId())) {
				myWarmingModels.add(model);
			}
		}
		
        Map<String, List<RemoteModelInfo>> modelsMap = new HashMap<>();
        modelsMap.put("activeModels", myActiveModels);
        modelsMap.put("warmingModels", myWarmingModels);

		return new NounMetadata(modelsMap, PixelDataType.MAP, PixelOperationType.OPERATION);
	}
}