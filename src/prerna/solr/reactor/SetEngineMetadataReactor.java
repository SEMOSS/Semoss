package prerna.solr.reactor;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.upload.UploadInputUtility;

public class SetEngineMetadataReactor extends AbstractSetMetadataReactor {
	
	public SetEngineMetadataReactor() {
		this.keysToGet = new String[]{
				ReactorKeysEnum.ENGINE.getKey(), META, 
				ReactorKeysEnum.ENCODED.getKey(), ReactorKeysEnum.JSON_CLEANUP.getKey()
			};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = UploadInputUtility.getEngineNameOrId(this.store);
		engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
		if(!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Engine does not exist or user does not have access to edit");
		}
		
		Map<String, Object> metadata = getMetaMap();
		// check for invalid metakeys
		List<String> validMetakeys = SecurityEngineUtils.getAllMetakeys();
		if(!validMetakeys.containsAll(metadata.keySet())) {
	    	throw new IllegalArgumentException("Unallowed metakeys. Can only use: "+String.join(", ", validMetakeys));
		}
		
		SecurityEngineUtils.updateDatabaseMetadata(engineId, metadata);
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully set the new metadata values for the engine"));
		return noun;
	}
	
	@Override
	public String getReactorDescription() {
		return "Define metadata on a datasource";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(META)) {
			return "Map containing {'metaKey':['value1','value2', etc.]} containing the list of metadata values to define on the engine. The list of values will determine the order that is defined for field";
		}
		return super.getDescriptionForKey(key);
	}

}
