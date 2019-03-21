package prerna.sablecc2.reactor.insights.save;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;
import prerna.util.Utility;

public class SetInsightCacheableReactor extends AbstractInsightReactor {

	private static final String CLASS_NAME = SetInsightCacheableReactor.class.getName();

	public SetInsightCacheableReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.ID.getKey(), CACHEABLE};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);

		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		String existingId = this.keyValue.get(this.keysToGet[1]);
		boolean cache = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[2]));

		// we may have the alias
		if(AbstractSecurityUtils.securityEnabled()) {
			appId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), appId);
			if(!SecurityInsightUtils.userCanEditInsight(this.insight.getUser(), appId, existingId)) {
				throw new IllegalArgumentException("App does not exist or user does not have permission to edit this insight");
			}
		} else {
			appId = MasterDatabaseUtility.testEngineIdIfAlias(appId);
			if(!MasterDatabaseUtility.getAllEngineIds().contains(appId)) {
				throw new IllegalArgumentException("App " + appId + " does not exist");
			}
		}
		
		logger.info("1) Updating insight in rdbms");
		IEngine engine = Utility.getEngine(appId);
		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
		admin.updateInsightCache(existingId, cache);
		logger.info("1) Done");

		logger.info("2) Updating insight in index");
		SecurityInsightUtils.updateInsightCache(appId, existingId, cache);
		logger.info("2) Done");
		
		Map<String, Object> returnMap = new HashMap<String, Object>();
		returnMap.put("app_insight_id", existingId);
		returnMap.put("app_name", engine.getEngineName());
		returnMap.put("app_id", engine.getEngineId());
		returnMap.put("cacheable", cache);
		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.SAVE_INSIGHT);
		return noun;
	}

}
