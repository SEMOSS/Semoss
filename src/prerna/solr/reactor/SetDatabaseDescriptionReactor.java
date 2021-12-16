package prerna.solr.reactor;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.SecurityUserDatabaseUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;

public class SetDatabaseDescriptionReactor extends AbstractReactor {
	
	public static final String DESCRIPTIONS = "description";

	public SetDatabaseDescriptionReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.DESCRIPTION.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = UploadInputUtility.getDatabaseNameOrId(this.store);
		
		if(AbstractSecurityUtils.securityEnabled()) {
			appId = SecurityQueryUtils.testUserDatabaseIdForAlias(this.insight.getUser(), appId);
			if(!SecurityUserDatabaseUtils.userCanEditDatabase(this.insight.getUser(), appId)) {
				throw new IllegalArgumentException("App does not exist or user does not have access to edit database");
			}
		} else {
			appId = MasterDatabaseUtility.testDatabaseIdIfAlias(appId);
		}
		
		String description = this.keyValue.get(this.keysToGet[1]);
		SecurityUserDatabaseUtils.updateDatabaseDescription(appId, description);

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully saved new description for app"));
		return noun;
	}
}
