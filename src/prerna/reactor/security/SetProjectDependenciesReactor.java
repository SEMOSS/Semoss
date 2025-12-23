package prerna.reactor.security;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadInputUtility;

public class SetProjectDependenciesReactor extends AbstractSetMetadataReactor {
	
	public SetProjectDependenciesReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.PROJECT.getKey(), "dependencies" };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String projectId = UploadInputUtility.getProjectNameOrId(this.store);
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);

		if(!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException("The user does not have access to edit this project or project id is invalid");
		}
		
		List<Map<String, Object>> depEngines = getDependentEnginesList();
		List<Map<String, Object>> dependencyList = new ArrayList<>();
		for(Map<String, Object> depEngine : depEngines) {
			if (depEngine.containsKey("id") && depEngine.containsKey("type")) {
				String eType = ((String) depEngine.get("type")).toUpperCase();
				String eId = (String) depEngine.get("id");
				if(IEngine.CATALOG_TYPE.valueOf(eType) != IEngine.CATALOG_TYPE.PROJECT) {
					if (!SecurityEngineUtils.containsEngineId(eId)) {
						throw new IllegalArgumentException("Engine id = '" + eId + "' does not exist");
					}
				} else {
					if (!SecurityProjectUtils.containsProjectId(eId)) {
						throw new IllegalArgumentException("Project id = '" + eId + "' does not exist");
					}
				}
				Map<String, Object> dependencyEntry = new HashMap<>();
				dependencyEntry.put("ENGINEID", eId);
				dependencyEntry.put("ENGINETYPE", eType);
				dependencyList.add(dependencyEntry);
			} else {
				throw new IllegalArgumentException("Engine is missing id or type");
			}
			
		}
		SecurityProjectUtils.updateEngineDependencies(user, projectId, IEngine.CATALOG_TYPE.PROJECT.name(),
				dependencyList);

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully set the new dependencies"));
		return noun;
	}
	
	@Override
	public String getReactorDescription() {
		return "Set the engine dependencies for a project";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals("dependencies")) {
			return "The list of engineid's that this project depends on for full functionality";
		}
		return super.getDescriptionForKey(key);
	}
	
	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getDependentEnginesList() {
		List<Map<String, Object>> dependencyList = new ArrayList<>();
		GenRowStruct grs = this.store.getGenRowStruct("dependencies");
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				dependencyList.add((Map<String, Object>) grs.get(i));
			}
		}
		return dependencyList;
	}

}
