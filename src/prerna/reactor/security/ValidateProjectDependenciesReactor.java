package prerna.reactor.security;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.project.api.IProject;
import prerna.project.impl.notebook.INotebookHelper;
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
		
		List<Map<String, Object>> projectDependencies = SecurityProjectUtils.getProjectDependencies(projectId);
		Map<String, Object> dependencyMap = new HashMap<>();
		for (Map<String, Object> dep : projectDependencies) {
			IEngine.CATALOG_TYPE type = IEngine.CATALOG_TYPE.valueOf(((String) dep.get("engine_type")).toUpperCase());
			String engineId = (String) dep.get("engine_id");
			boolean canView = false;
			if (type == IEngine.CATALOG_TYPE.PROJECT) {
				canView = SecurityProjectUtils.userCanViewProject(user, engineId);
			} else {
				canView = SecurityEngineUtils.userCanViewEngine(user, engineId);
			}
			dependencyMap.put(engineId, canView);
		}
		
		NounMetadata noun = new NounMetadata(dependencyMap, PixelDataType.MAP);
		return noun;
	}
	
	@Override
	public String getReactorDescription() {
		return "Return true if the user has access to all engine dependencies listed in this project";
	}
	
}
