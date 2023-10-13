package prerna.reactor.insights.save;

import java.util.Map;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cache.InsightCacheUtility;
import prerna.reactor.insights.AbstractInsightReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DeleteInsightCacheReactor extends AbstractInsightReactor {

	public DeleteInsightCacheReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String rdbmsId = this.keyValue.get(this.keysToGet[1]);
		Map<String, Object> parameterValues = getInsightParamValueMap();
		
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if(!SecurityInsightUtils.userCanEditInsight(this.insight.getUser(), projectId, rdbmsId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have permission to edit this insight");
		}
		
		String projectName = SecurityProjectUtils.getProjectAliasForId(projectId);
		try {
			InsightCacheUtility.deleteCache(projectId, projectName, rdbmsId, parameterValues, true);
			return new NounMetadata(true, PixelDataType.BOOLEAN);
		} catch(Exception e) {
			return new NounMetadata(false, PixelDataType.BOOLEAN);
		}
	}

}
