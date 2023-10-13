package prerna.reactor.insights.save;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.quartz.CronExpression;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cache.InsightCacheUtility;
import prerna.cluster.util.ClusterUtil;
import prerna.date.SemossDate;
import prerna.engine.impl.InsightAdministrator;
import prerna.io.connector.secrets.SecretsFactory;
import prerna.project.api.IProject;
import prerna.reactor.insights.AbstractInsightReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class SetInsightCacheableReactor extends AbstractInsightReactor {

	private static final String CLASS_NAME = SetInsightCacheableReactor.class.getName();

	public SetInsightCacheableReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey(), 
				CACHEABLE, CACHE_MINUTES, CACHE_CRON, CACHE_ENCRYPT};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);

		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String existingId = this.keyValue.get(this.keysToGet[1]);
		
		// we may have the alias
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if(!SecurityInsightUtils.userCanEditInsight(this.insight.getUser(), projectId, existingId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have permission to edit this insight");
		}
		
		Map<String, Object> currentInsightDetails = SecurityInsightUtils.getSpecificInsightCacheDetails(projectId, existingId);
		
		boolean cache = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[2]));
		
		int cacheMinutes = -1;
		if(this.keyValue.containsKey(this.keysToGet[3])) {
			cacheMinutes = Integer.parseInt(this.keyValue.get(this.keysToGet[3]));
		} else if(currentInsightDetails.containsKey("cacheMinutes")){
			cacheMinutes = (int) currentInsightDetails.get("cacheMinutes");
		} else {
			cacheMinutes = Utility.getApplicationCacheInsightMinutes();
		}
		
		String cacheCron = null;
		if(this.keyValue.containsKey(this.keysToGet[4])) {
			cacheCron = this.keyValue.get(this.keysToGet[4]);
			if(cacheCron != null && !cacheCron.isEmpty() && !CronExpression.isValidExpression(cacheCron)) {
				throw new IllegalArgumentException("The cache cron expression = '" + cacheCron + "' is invalid");
			}
		} else if(currentInsightDetails.containsKey("cacheCron")){
			cacheCron = (String) currentInsightDetails.get("cacheCron");
		} else {
			cacheCron = Utility.getApplicationCacheCron();
		}
		
		boolean cacheEncrypt = false;
		if(this.keyValue.containsKey(this.keysToGet[5])) {
			cacheEncrypt = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[5]));
		} else if(currentInsightDetails.containsKey("cacheEncrypt")){
			cacheEncrypt = (Boolean) currentInsightDetails.get("cacheEncrypt");
		} else {
			cacheEncrypt = Utility.getApplicationCacheEncrypt();
		}
		List<NounMetadata> additionalMetas = null;
		if(cacheEncrypt && SecretsFactory.getSecretConnector() == null) {
			additionalMetas = new ArrayList<>();
			additionalMetas.add(NounMetadata.getWarningNounMessage("Encryption services have not been enabled on this instance. Encryption cannot be enabled"));
			cacheEncrypt = false;
		}
		
		LocalDateTime cachedOn = null;
		SemossDate cachedOnDate = (SemossDate) currentInsightDetails.get("cachedOn");
		if(cachedOnDate != null) {
			cachedOn = cachedOnDate.getLocalDateTime();
		}
		
		logger.info("1) Updating insight in rdbms");
		IProject project = Utility.getProject(projectId);
		
		InsightAdministrator admin = new InsightAdministrator(project.getInsightDatabase());
		admin.updateInsightCache(existingId, cache, cacheMinutes, cacheCron, cachedOn, cacheEncrypt);
		logger.info("1) Done");

		logger.info("2) Updating insight in index");
		SecurityInsightUtils.updateInsightCache(projectId, existingId, cache, cacheMinutes, cacheCron, cachedOn, cacheEncrypt);
		logger.info("2) Done");
		
		Map<String, Object> returnMap = new HashMap<String, Object>();
		// TODO: delete and switch to only project_
		returnMap.put("app_insight_id", existingId);
		returnMap.put("app_name", project.getProjectName());
		returnMap.put("app_id", project.getProjectId());
		
		returnMap.put("project_insight_id", existingId);
		returnMap.put("project_name", project.getProjectName());
		returnMap.put("project_id", project.getProjectId());
		returnMap.put("cacheable", cache);
		returnMap.put("cacheMinutes", cacheMinutes);
		returnMap.put("cacheEncrypt", cacheEncrypt);
		returnMap.put("cacheCron", cacheCron);

		//push insight db
//		ClusterUtil.reactorPushInsightDB(projectId);
		if(cacheEncrypt != Boolean.parseBoolean(currentInsightDetails.get("cacheEncrypt")+"")) {
			// delete the current cache
			InsightCacheUtility.deleteCache(projectId, project.getProjectName(), existingId, null, false);
			ClusterUtil.pushProjectFolder(project, AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId));
		}
		
		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.SAVE_INSIGHT);
		if(additionalMetas != null && !additionalMetas.isEmpty()) {
			noun.addAllAdditionalReturn(additionalMetas);
		}
		return noun;
	}

}
