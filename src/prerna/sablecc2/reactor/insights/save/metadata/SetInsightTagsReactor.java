package prerna.sablecc2.reactor.insights.save.metadata;

import java.util.List;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.InsightAdministrator;
import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;
import prerna.util.Utility;

public class SetInsightTagsReactor extends AbstractInsightReactor {

	public SetInsightTagsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey(), ReactorKeysEnum.TAGS.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		String projectId = getProject();
		// need to know what we are updating
		String existingId = getRdbmsId();
		
		// security
		if(AbstractSecurityUtils.securityEnabled()) {
			if(AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
			
			if(!SecurityInsightUtils.userCanEditInsight(this.insight.getUser(), projectId, existingId)) {
				throw new IllegalArgumentException("User does not have permission to edit this insight");
			}
		}

		List<String> tags = getTags();
		IProject project = Utility.getProject(projectId);
		InsightAdministrator admin = new InsightAdministrator(project.getInsightDatabase());
		admin.updateInsightTags(existingId, tags);
		SecurityInsightUtils.updateInsightTags(projectId, existingId, tags);
		
		//need to push insight db for this
		ClusterUtil.reactorPushInsightDB(projectId);

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully saved new tags for insight"));
		return noun;
	}

}
