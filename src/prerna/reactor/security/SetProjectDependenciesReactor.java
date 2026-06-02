/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.security;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.agent.AppBuilderHarnessConfiguration;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadInputUtility;

public class SetProjectDependenciesReactor extends AbstractSetMetadataReactor {

	private static final Logger logger = LogManager.getLogger(SetProjectDependenciesReactor.class);

	public SetProjectDependenciesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), "dependencies" };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String projectId = UploadInputUtility.getProjectNameOrId(this.store);
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);

		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException(
					"The user does not have access to edit this project or project id is invalid");
		}

		List<Map<String, Object>> depEngines = getDependentEnginesList();
		List<Map<String, Object>> dependencyList = new ArrayList<>();
		for (Map<String, Object> depEngine : depEngines) {
			if (depEngine.containsKey("id") && depEngine.containsKey("type")) {
				String eType = ((String) depEngine.get("type")).toUpperCase();
				String eId = (String) depEngine.get("id");
				if (IEngine.CATALOG_TYPE.valueOf(eType) != IEngine.CATALOG_TYPE.PROJECT) {
					if (!SecurityEngineUtils.containsEngineId(eId)) {
						throw new IllegalArgumentException("Engine id = '" + eId + "' does not exist");
					}
				} else {
					if (!SecurityProjectUtils.containsProjectId(eId)) {
						throw new IllegalArgumentException("Project id = '" + eId + "' does not exist");
					}
				}
				if (eId.equals(projectId)) {
					throw new IllegalArgumentException("Cannot add current project as a dependency");
				}
				Map<String, Object> dependencyEntry = new HashMap<>();
				dependencyEntry.put("ENGINEID", eId);
				dependencyEntry.put("ENGINETYPE", eType);
				dependencyList.add(dependencyEntry);
			} else {
				throw new IllegalArgumentException("Engine is missing id or type");
			}
		}

		SecurityProjectUtils.updateProjectDependencies(user, projectId, dependencyList);

		// Regenerate the agent-facing selected-engines skill so Claude sees the
		// updated engine connections. Reads back the full dependency details
		// (with engine names) — required by the skill writer. Failures are
		// logged inside the helper; we don't let them surface as user-visible
		// errors because the dependency write itself already succeeded.
		try {
			List<Map<String, Object>> details = SecurityProjectUtils.getProjectDependencyDetails(projectId);
			AppBuilderHarnessConfiguration.regenerateSelectedEnginesSkillFromDependencies(projectId, details);
		} catch (Exception e) {
			logger.error("Failed to regenerate selected-engines skill for project: {}", projectId, e);
		}

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
		if (key.equals("dependencies")) {
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
