package prerna.reactor.insights;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetInsightUserAccessRequestReactor extends AbstractReactor {
	
	public GetInsightUserAccessRequestReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String insightId = this.keyValue.get(this.keysToGet[1]);
		if(projectId == null) {
			throw new IllegalArgumentException("Please define the project id.");
		}
		if(insightId == null) {
			throw new IllegalArgumentException("Please define the insight id.");
		}
		// check user permission for the database
		User user = this.insight.getUser();
		if(!SecurityAdminUtils.userIsAdmin(user) && 
				!SecurityInsightUtils.userCanEditInsight(user, projectId, insightId)) {
			throw new IllegalArgumentException("User does not have permission to view access requests for this insight");
		}
		List<Map<String, Object>> requests = SecurityInsightUtils.getUserAccessRequestsByInsight(projectId, insightId);;
		return new NounMetadata(requests, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PROJECT_INFO);
	}
	
}
