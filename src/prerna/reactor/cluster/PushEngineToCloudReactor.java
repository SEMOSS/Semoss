package prerna.reactor.cluster;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PushEngineToCloudReactor extends AbstractReactor {
	
	public PushEngineToCloudReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		
		if(engineId == null || engineId.isEmpty()) {
			throw new IllegalArgumentException("Must input an engine id");
		}
		
		// make sure valid id for user
		if(!SecurityEngineUtils.userIsOwner(this.insight.getUser(), engineId)) {
			// you dont have access
			throw new IllegalArgumentException("Engine does not exist or user is not an owner to force pushing to cloud storage");
		}
		
		ClusterUtil.pushEngine(engineId);
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}

