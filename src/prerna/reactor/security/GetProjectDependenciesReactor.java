package prerna.reactor.security;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadInputUtility;

public class GetProjectDependenciesReactor extends AbstractSetMetadataReactor {
	
	public GetProjectDependenciesReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.PROJECT.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String userId = this.insight.getUserId();
		String projectId = UploadInputUtility.getProjectNameOrId(this.store);
		if(!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException("The user does not have access to view this project or project id is invalid");
		}
		
		return new NounMetadata(SecurityProjectUtils.getProjectDependencyDetails(projectId, userId), PixelDataType.MAP);
	}
	
	@Override
	public String getReactorDescription() {
		return "Set the engine dependencies for a project";
	}
	
}
