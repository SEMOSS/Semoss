package prerna.sablecc2.reactor.database;

import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.engine.UploadEngineReactor;

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
