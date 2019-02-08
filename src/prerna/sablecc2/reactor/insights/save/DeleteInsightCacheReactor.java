package prerna.sablecc2.reactor.insights.save;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.om.InsightCacheUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class DeleteInsightCacheReactor extends AbstractReactor {

	public DeleteInsightCacheReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.ID.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		String rdbmsId = this.keyValue.get(this.keysToGet[1]);
		
		if(AbstractSecurityUtils.securityEnabled()) {
			appId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), appId);
			if(!SecurityInsightUtils.userCanEditInsight(this.insight.getUser(), appId, rdbmsId)) {
				throw new IllegalArgumentException("App does not exist or user does not have permission to edit this insight");
			}
		} else {
			appId = MasterDatabaseUtility.testEngineIdIfAlias(appId);
		}
		
		if(!SecurityQueryUtils.getEngineIds().contains(appId)) {
			throw new IllegalArgumentException("App id does not exist");
		}
		
		String appName = MasterDatabaseUtility.getEngineAliasForId(appId);
		
		try {
			InsightCacheUtility.deleteCache(appId, appName, rdbmsId);
			return new NounMetadata(true, PixelDataType.BOOLEAN);
		} catch(Exception e) {
			return new NounMetadata(false, PixelDataType.BOOLEAN);
		}
	}

}
