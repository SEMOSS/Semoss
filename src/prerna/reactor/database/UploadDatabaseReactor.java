package prerna.reactor.database;

import prerna.reactor.engine.UploadEngineReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

@Deprecated
public class UploadDatabaseReactor extends UploadEngineReactor {
	
	public UploadDatabaseReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		return super.execute();
	}
	
}
