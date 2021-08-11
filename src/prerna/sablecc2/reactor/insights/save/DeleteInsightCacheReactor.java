package prerna.sablecc2.reactor.insights.save;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cache.InsightCacheUtility;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class DeleteInsightCacheReactor extends AbstractReactor {

	public DeleteInsightCacheReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String rdbmsId = this.keyValue.get(this.keysToGet[1]);
		
		if(AbstractSecurityUtils.securityEnabled()) {
			projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
			if(!SecurityInsightUtils.userCanEditInsight(this.insight.getUser(), projectId, rdbmsId)) {
				throw new IllegalArgumentException("Project does not exist or user does not have permission to edit this insight");
			}
		} else {
			projectId = MasterDatabaseUtility.testDatabaseIdIfAlias(projectId);
		}
		
		String projectName = SecurityProjectUtils.getProjectAliasForId(projectId);
		
		try {
			InsightCacheUtility.deleteCache(projectId, projectName, rdbmsId, true);
			return new NounMetadata(true, PixelDataType.BOOLEAN);
		} catch(Exception e) {
			return new NounMetadata(false, PixelDataType.BOOLEAN);
		}
	}

}
