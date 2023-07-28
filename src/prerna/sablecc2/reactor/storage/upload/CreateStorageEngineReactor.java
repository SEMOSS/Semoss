package prerna.sablecc2.reactor.storage.upload;

import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class CreateStorageEngineReactor extends AbstractReactor {

	public CreateStorageEngineReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.STORAGE.getKey(), ReactorKeysEnum.STORAGE_DETAILS.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		return null;
	}

}
