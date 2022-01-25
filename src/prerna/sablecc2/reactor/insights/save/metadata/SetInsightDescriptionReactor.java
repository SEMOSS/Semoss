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

public class SetInsightDescriptionReactor extends AbstractInsightReactor {

	public SetInsightDescriptionReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey(), ReactorKeysEnum.DESCRIPTION.getKey()};
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
		
		String description = getDescription();
		IProject project = Utility.getProject(projectId);
		InsightAdministrator admin = new InsightAdministrator(project.getInsightDatabase());
		admin.updateInsightDescription(existingId, description);
		SecurityInsightUtils.updateInsightDescription(projectId, existingId, description);
		
		ClusterUtil.reactorPushInsightDB(projectId);

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully saved new description for insight"));
		return noun;
	}

	/**
	 * Get the description for the insight
	 * Assume it is passed by the key or it is the last string passed into the curRow
	 * @return
	 */
	protected String getDescription() {
		String desc = super.getDescription();
		if(desc == null) {
			// just return the last input
			List<String> strInputs = this.curRow.getAllStrValues();
			if(!strInputs.isEmpty()) {
				return strInputs.get(strInputs.size()-1);
			}
		}
		return desc;
	}
}
