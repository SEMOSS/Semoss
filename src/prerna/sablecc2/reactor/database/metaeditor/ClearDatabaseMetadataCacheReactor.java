package prerna.sablecc2.reactor.database.metaeditor;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.EngineSyncUtility;

public class ClearDatabaseMetadataCacheReactor extends AbstractReactor {

	public ClearDatabaseMetadataCacheReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		EngineSyncUtility.clearEngineCache(databaseId);
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
