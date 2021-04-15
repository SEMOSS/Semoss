package prerna.sablecc2.reactor.app.metaeditor;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.EngineSyncUtility;

public class ClearEngineMetadataCacheReactor extends AbstractReactor {

	public ClearEngineMetadataCacheReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		String appId = this.keyValue.get(this.keysToGet[0]);
		EngineSyncUtility.clearEngineCache(appId);
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
