package prerna.solr.reactor;

import java.util.List;
import java.util.Vector;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.SecurityUserDatabaseUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;

public class SetDatabaseTagsReactor extends AbstractReactor {
	
	public SetDatabaseTagsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.TAGS.getKey()};
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
		
		List<String> tags = getTags();
		SecurityUserDatabaseUtils.updateDatabaseTags(appId, tags);
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully saved new tags for app"));
		return noun;
	}
	
	/**
	 * Get the tags to set for the insight
	 * @return
	 */
	protected List<String> getTags() {
		List<String> tags = new Vector<String>();
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.TAGS.getKey());
		if(grs != null && !grs.isEmpty()) {
			for(int i = 0; i < grs.size(); i++) {
				tags.add(grs.get(i).toString());
			}
		}
		
		return tags;
	}

}
