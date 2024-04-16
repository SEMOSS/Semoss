package prerna.reactor.database;

import prerna.reactor.engine.UploadEngineReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Deprecating this file as the functionality has been divided into two new classes,
 * UploadDatabaseReactor and UploadProjectReactor
 *
 */

@Deprecated
public class UploadAppReactor extends UploadEngineReactor {
	
	@Override
	public NounMetadata execute() {
		return super.execute();
	}
	
}
