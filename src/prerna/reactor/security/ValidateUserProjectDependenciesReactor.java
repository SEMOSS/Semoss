package prerna.reactor.security;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadInputUtility;

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
		
		List<Map<String, Object>> dependentEngines = SecurityProjectUtils.getProjectDependencies(projectId);
		for(Map<String, Object> depEngine : dependentEngines) {
			String depEngineId = (String) depEngine.get("engine_id");
			String depEngineType = (String) depEngine.get("engine_type");
			
			if (depEngineType == null || IEngine.CATALOG_TYPE.valueOf(depEngineType) != IEngine.CATALOG_TYPE.PROJECT) {
				boolean canView = SecurityEngineUtils.userCanViewEngine(user, depEngineId);
				hasAccess.put(depEngineId, canView);
			} else {
				boolean canView = SecurityProjectUtils.userCanViewProject(user, depEngineId);
				hasAccess.put(depEngineId, canView);
			}
		}
		
		NounMetadata noun = new NounMetadata(hasAccess, PixelDataType.MAP);
		return noun;
	}
	
	@Override
	public String getReactorDescription() {
		return "Return a map {'engineid':true/false} for the users access to each engine dependency listed in this project";
	}
	
}
