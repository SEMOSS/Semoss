package prerna.sablecc2.reactor.insights.save.metadata;

import java.util.List;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;

public class SetInsightTagsReactor extends AbstractInsightReactor {

	public SetInsightTagsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.ID.getKey(), ReactorKeysEnum.TAGS.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		String appId = getApp();
		// need to know what we are updating
		String existingId = getRdbmsId();
		
		// security
		if(AbstractSecurityUtils.securityEnabled()) {
			if(AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
			
			if(!SecurityInsightUtils.userCanEditInsight(this.insight.getUser(), appId, existingId)) {
				throw new IllegalArgumentException("User does not have permission to edit this insight");
			}
		}

		List<String> tags = getTags();
		SecurityInsightUtils.updateInsightTags(appId, existingId, tags);
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully saved new description for insight"));
		return noun;
	}

}
