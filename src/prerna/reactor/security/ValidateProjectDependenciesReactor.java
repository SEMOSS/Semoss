package prerna.reactor.security;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ValidateProjectDependenciesReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(ValidateProjectDependenciesReactor.class);
	
	public ValidateProjectDependenciesReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] {1};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			// you don't have access
			throw new IllegalArgumentException("Project/App does not exist or user does not have access to the project");
		}
		
		IProject project = Utility.getProject(projectId);
		IProject.PROJECT_TYPE ptype = project.getProjectType();
		if (!ptype.equals(IProject.PROJECT_TYPE.BLOCKS)) {
			throw new SemossPixelException("App " + projectId + " is not a blocks project.");
		}
		
		Map<String, String> engineMap = project.getEngineDependencies();
		
		Map<String, Boolean> varToAccess = new HashMap<>();
		Map<String, Boolean> eIdToAccess = new HashMap<>();
		for (String varName : engineMap.keySet()) {
			String engineId = engineMap.get(varName);
			boolean canView = SecurityEngineUtils.userCanViewEngine(user, engineId);
			
			varToAccess.put(varName, canView);
			eIdToAccess.put(engineId, canView);
		}
		
		Map<String, Map<String, Boolean>> validationMap = new HashMap<>();
		validationMap.put("engine", eIdToAccess);
		validationMap.put("vars", varToAccess);
		NounMetadata noun = new NounMetadata(validationMap, PixelDataType.MAP);
		return noun;
	}
	
	@Override
	public String getReactorDescription() {
		return "Return true if the user has access to all engine dependencies listed in this project";
	}
	
}
