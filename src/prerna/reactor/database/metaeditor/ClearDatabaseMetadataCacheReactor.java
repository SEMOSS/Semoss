package prerna.reactor.database.metaeditor;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineSyncUtility;

public class ClearDatabaseMetadataCacheReactor extends AbstractReactor {

	public ClearDatabaseMetadataCacheReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		EngineSyncUtility.clearEngineCache(databaseId);
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
