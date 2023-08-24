package prerna.sablecc2.reactor.project;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetProjectUserAccessRequestReactor extends AbstractReactor {
	public GetProjectUserAccessRequestReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		if(projectId == null) {
			throw new IllegalArgumentException("Please define the project id.");
		}
		// check user permission for the database
		User user = this.insight.getUser();
		if(!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException("User does not have permission to view access requests for this project");
		}
		
		List<Map<String, Object>> requests = SecurityProjectUtils.getUserAccessRequestsByProject(projectId);
		return new NounMetadata(requests, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PROJECT_INFO);
	}
	
}
