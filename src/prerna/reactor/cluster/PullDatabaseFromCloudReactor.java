package prerna.reactor.cluster;

import prerna.sablecc2.om.ReactorKeysEnum;

@Deprecated
public class PullDatabaseFromCloudReactor extends PullEngineFromCloudReactor {
	
	public PullDatabaseFromCloudReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}

}

