package prerna.solr.reactor;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.upload.UploadInputUtility;

public class GetProjectDependenciesReactor extends AbstractSetMetadataReactor {
	
	public GetProjectDependenciesReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.PROJECT.getKey(), "details" };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String projectId = UploadInputUtility.getProjectNameOrId(this.store);
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);

		if(!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException("The user does not have access to view this project or project id is invalid");
		}
		
		boolean details = Boolean.parseBoolean(this.keyValue.get("details")+"");
		if(details) {
			return new NounMetadata(SecurityProjectUtils.getProjectDependencyDetails(projectId), PixelDataType.MAP);
		} 
		
		return new NounMetadata(SecurityProjectUtils.getProjectDependencies(projectId), PixelDataType.CONST_STRING);
	}
	
	@Override
	public String getReactorDescription() {
		return "Set the engine dependencies for a project";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals("details")) {
			return "true/false flags to get additional information beyond the engine id. This will result in a list of maps to be returned instead of a list of strings";
		}
		return super.getDescriptionForKey(key);
	}

}
