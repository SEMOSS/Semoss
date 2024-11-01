package prerna.reactor;

import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ExecuteAppAsAPI extends AbstractReactor {
	
	public ExecuteAppAsAPI() {
		this.keysToGet = new String[] {ReactorKeysEnum.APP_ID.getKey(), ReactorKeysEnum.MAP.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		return null;
	}

}
