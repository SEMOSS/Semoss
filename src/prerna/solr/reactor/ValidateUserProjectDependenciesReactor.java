package prerna.solr.reactor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.upload.UploadInputUtility;

public class ValidateUserProjectDependenciesReactor extends AbstractSetMetadataReactor {
	
	public ValidateUserProjectDependenciesReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.PROJECT.getKey() };
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
		
		Map<String, Boolean> hasAccess = new HashMap<>();
		
		List<String> dependentEngineIds = SecurityProjectUtils.getProjectDependencies(projectId);
		for(String depEngineId : dependentEngineIds) {
			boolean canView = SecurityEngineUtils.userCanViewEngine(user, depEngineId);
			hasAccess.put(depEngineId, canView);
		}
		
		NounMetadata noun = new NounMetadata(hasAccess, PixelDataType.BOOLEAN);
		return noun;
	}
	
	@Override
	public String getReactorDescription() {
		return "Return a map {'engineid':true/false} for the users access to each engine dependency listed in this project";
	}
	
}
