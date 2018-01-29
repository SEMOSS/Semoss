package prerna.sablecc2.reactor.database;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;

public class DeleteEngineReactor extends AbstractReactor {

	public DeleteEngineReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineToDelete = this.keyValue.get(this.keysToGet[0]);
		
		
		
		// TODO Auto-generated method stub
		return null;
	}

}
