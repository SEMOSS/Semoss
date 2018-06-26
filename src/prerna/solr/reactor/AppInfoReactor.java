package prerna.solr.reactor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.SecurityQueryUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class AppInfoReactor extends AbstractReactor {
	
	public AppInfoReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		
		if(appId == null || appId.isEmpty()) {
			throw new IllegalArgumentException("Must input an app id");
		}
		
		List<Map<String, String>> baseInfo = null;
		if(this.securityEnabled()) {
			// make sure valid id for user
			if(!this.getUserAppFilters().contains(appId)) {
				// you dont have access
				throw new IllegalArgumentException("App does not exist or user does not have access to database");
			}
			// user has access!i
			baseInfo = SecurityQueryUtils.getUserDatabaseList(this.insight.getUserId(), appId);
		} else {
			// just grab the info
			baseInfo = SecurityQueryUtils.getUserDatabaseList(appId);
		}
		
		if(baseInfo.isEmpty()) {
			throw new IllegalArgumentException("Could not find any app data");
		}
		
		Map<String, String> appInfo = baseInfo.get(0);
		Map<String, List<String>> additionalMeta = SecurityQueryUtils.getAggregateEngineMetadata(appId);

		// combine into return object
		Map<String, Object> data = new HashMap<String, Object>();
		data.putAll(appInfo);
		if(additionalMeta.containsKey("description")) {
			data.put("app_description", additionalMeta.get("description").get(0));
		}
		data.put("app_tags", additionalMeta.get("tags"));
		return new NounMetadata(data, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.APP_INFO);
	}

}