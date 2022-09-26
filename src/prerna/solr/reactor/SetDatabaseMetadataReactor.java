package prerna.solr.reactor;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityDatabaseUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.upload.UploadInputUtility;

public class SetDatabaseMetadataReactor extends AbstractSetMetadataReactor {
	
	public SetDatabaseMetadataReactor() {
		this.keysToGet = new String[]{
				ReactorKeysEnum.DATABASE.getKey(), META, 
				ReactorKeysEnum.ENCODED.getKey(), ReactorKeysEnum.JSON_CLEANUP.getKey()
			};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = UploadInputUtility.getDatabaseNameOrId(this.store);
		
		if(AbstractSecurityUtils.securityEnabled()) {
			databaseId = SecurityQueryUtils.testUserDatabaseIdForAlias(this.insight.getUser(), databaseId);
			if(!SecurityDatabaseUtils.userCanEditDatabase(this.insight.getUser(), databaseId)) {
				throw new IllegalArgumentException("Database does not exist or user does not have access to edit");
			}
		} else {
			databaseId = MasterDatabaseUtility.testDatabaseIdIfAlias(databaseId);
		}
		
		Map<String, Object> metadata = getMetaMap();
		// check for invalid metakeys
		List<String> validMetakeys = SecurityDatabaseUtils.getAllMetakeys();
		if(!validMetakeys.containsAll(metadata.keySet())) {
	    	throw new IllegalArgumentException("Unallowed metakeys. Can only use: "+String.join(", ", validMetakeys));
		}
		
		SecurityDatabaseUtils.updateDatabaseMetadata(databaseId, metadata);
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully set the new metadata values for the database"));
		return noun;
	}
	
	@Override
	public String getReactorDescription() {
		return "Define metadata on a datasource";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(META)) {
			return "Map containing {'metaKey':['value1','value2', etc.]} containing the list of metadata values to define on the database. The list of values will determine the order that is defined for field";
		}
		return super.getDescriptionForKey(key);
	}

}
