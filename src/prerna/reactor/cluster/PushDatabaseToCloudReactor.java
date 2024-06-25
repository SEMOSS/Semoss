package prerna.reactor.cluster;

import prerna.sablecc2.om.ReactorKeysEnum;

@Deprecated
public class PushDatabaseToCloudReactor extends PushEngineToCloudReactor {
	
	public PushDatabaseToCloudReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}

}

