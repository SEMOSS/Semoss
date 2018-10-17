package prerna.sablecc2.reactor.cluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class OpenAppReactor extends AbstractReactor {
	
	public OpenAppReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		
		if(appId == null || appId.isEmpty()) {
			throw new IllegalArgumentException("Must input an app id");
		}
		
		List<Map<String, Object>> baseInfo = null;
		if(AbstractSecurityUtils.securityEnabled()) {
			// make sure valid id for user
			if(!SecurityQueryUtils.getUserEngineIds(this.insight.getUser()).contains(appId)) {
				// you dont have access
				throw new IllegalArgumentException("App does not exist or user does not have access to database");
			}
			// user has access!
			baseInfo = SecurityQueryUtils.getUserDatabaseList(this.insight.getUser(), appId);
		} else {
			// just grab the info
			baseInfo = SecurityQueryUtils.getAllDatabaseList();
		}
		
		if(baseInfo.isEmpty()) {
			throw new IllegalArgumentException("Could not find any app data");
		}
		
		Map<String, Object> returnMap = new HashMap<String, Object>();
		if(appId.equals("NEWSEMOSSAPP")) {
			returnMap.put("app_name", "NEWSEMOSSAPP");
			returnMap.put("app_id", appId);
			returnMap.put("app_type", IEngine.ENGINE_TYPE.APP.toString());	
		} else {
			IEngine engine = Utility.getEngine(appId);
			returnMap.put("app_name", engine.getEngineName());
			returnMap.put("app_id", engine.getEngineId());
			returnMap.put("app_type", engine.getEngineType().toString());	
		}
		
		return new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPEN_APP);
	}

}