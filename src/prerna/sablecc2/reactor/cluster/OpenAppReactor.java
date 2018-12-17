package prerna.sablecc2.reactor.cluster;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.SecurityUpdateUtils;
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
		
		if(appId.equals("NEWSEMOSSAPP")) {
			Map<String, Object> returnMap = new HashMap<String, Object>();
			returnMap.put("app_name", "NEWSEMOSSAPP");
			returnMap.put("app_id", appId);
			returnMap.put("app_type", IEngine.ENGINE_TYPE.APP.toString());
			returnMap.put("app_cost", "");	
			return new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPEN_APP);
		}
		
		if(AbstractSecurityUtils.securityEnabled()) {
			// make sure valid id for user
			if(!SecurityQueryUtils.userCanViewEngine(this.insight.getUser(), appId)) {
				// you dont have access
				throw new IllegalArgumentException("App does not exist or user does not have access to database");
			}
		}
		
		IEngine engine = Utility.getEngine(appId);
		if(engine == null) {
			throw new IllegalArgumentException("Could not find or load app = " + appId);
		}

		Map<String, Object> returnMap = new HashMap<String, Object>();
		returnMap.put("app_name", engine.getEngineName());
		returnMap.put("app_id", engine.getEngineId());
		String[] typeAndCost = SecurityUpdateUtils.getAppTypeAndCost(engine.getProp());
		returnMap.put("app_type", typeAndCost[0]);	
		returnMap.put("app_cost", typeAndCost[1]);	

		return new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPEN_APP);
	}

}