package prerna.sablecc2.reactor.app.metaeditor;

import java.util.Map;

import prerna.engine.api.IEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetOwlMetamodelReactor extends AbstractMetaEditorReactor {

	public GetOwlMetamodelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		// we may have the alias
		appId = getAppId(appId, false);
		
		IEngine app = Utility.getEngine(appId);
		Map<String, Object[]> metamodelObject = app.getMetamodel();
		return new NounMetadata(metamodelObject, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_METAMODEL);
	}

}
