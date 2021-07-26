package prerna.sablecc2.reactor.insights.save;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.InsightAdministrator;
import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;
import prerna.util.Utility;

public class SetInsightCacheableReactor extends AbstractInsightReactor {

	private static final String CLASS_NAME = SetInsightCacheableReactor.class.getName();

	public SetInsightCacheableReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey(), CACHEABLE};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);

		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String existingId = this.keyValue.get(this.keysToGet[1]);
		boolean cache = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[2]));

		// we may have the alias
		if(AbstractSecurityUtils.securityEnabled()) {
			projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
			if(!SecurityInsightUtils.userCanEditInsight(this.insight.getUser(), projectId, existingId)) {
				throw new IllegalArgumentException("Project does not exist or user does not have permission to edit this insight");
			}
		} 
//		else {
//			projectId = MasterDatabaseUtility.testEngineIdIfAlias(projectId);
//			if(!MasterDatabaseUtility.getAllEngineIds().contains(projectId)) {
//				throw new IllegalArgumentException("App " + projectId + " does not exist");
//			}
//		}
		
		logger.info("1) Updating insight in rdbms");
		IProject project = Utility.getProject(projectId);
		InsightAdministrator admin = new InsightAdministrator(project.getInsightDatabase());
		admin.updateInsightCache(existingId, cache);
		logger.info("1) Done");

		logger.info("2) Updating insight in index");
		SecurityInsightUtils.updateInsightCache(projectId, existingId, cache);
		logger.info("2) Done");
		
		Map<String, Object> returnMap = new HashMap<String, Object>();
		returnMap.put("app_insight_id", existingId);
		returnMap.put("app_name", project.getProjectName());
		returnMap.put("app_id", project.getProjectId());
		returnMap.put("cacheable", cache);
		
		//push insight db
		ClusterUtil.reactorPushInsightDB(projectId);

		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.SAVE_INSIGHT);
		return noun;
	}

}
