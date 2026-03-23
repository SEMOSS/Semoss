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
package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class EditWorkspaceReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(EditWorkspaceReactor.class);

	public static final String NAME = "name";
	public static final String DESCRIPTION = "description";
	public static final String SYSTEM_PROMPT = "systemPrompt";
	public static final String IS_ACTIVE = "isActive";
	public static final String PROMPT_LIBRARY_TAG = "promptLibraryTag";

	public EditWorkspaceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.WORKSPACE_ID.getKey(), NAME, DESCRIPTION, SYSTEM_PROMPT,
				IS_ACTIVE, ReactorKeysEnum.MCP.getKey(), PROMPT_LIBRARY_TAG };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();

		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());
		String workspaceName = this.keyValue.get(NAME);
		String workspaceDescription = Utility.decodeURIComponent(this.keyValue.get(DESCRIPTION));
		String workspaceSystemPrompt = Utility.decodeURIComponent(this.keyValue.get(SYSTEM_PROMPT));
		boolean isActive = !"false".equalsIgnoreCase(this.keyValue.get(IS_ACTIVE));
		String workspacePromptLibraryTag = this.keyValue.get(PROMPT_LIBRARY_TAG);
		
		Map<String, Object> current = ModelInferenceLogsUtils.getWorkspaceEntry(workspaceId);
		if (current == null) {
			throw new IllegalArgumentException("Workspace not found");
		}

		Object currentlyIsActive = current.get("is_active");
		Boolean currentlyActive = (Boolean) currentlyIsActive;

		if (!SecurityProjectUtils.userCanEditProject(user, workspaceId)) {
			throw new IllegalArgumentException(
					"Workspace " + workspaceId + " does not exist or user does not have access to the workspace");
		}

		if (!currentlyActive && isActive) {
			if (SecurityProjectUtils.userIsOwner(user, workspaceId)) {
				ModelInferenceLogsUtils.doSetWorksapceToActive(workspaceId);
			} else {
				throw new IllegalArgumentException("User must be an owner to set the workspace to active");
			}
		}

		if (currentlyActive && !isActive) {
			if (SecurityProjectUtils.userIsOwner(user, workspaceId)) {
				ModelInferenceLogsUtils.doSetWorksapceToActive(workspaceId);
			} else {
				throw new IllegalArgumentException("User must be an owner to set the workspace to inactive");
			}
		}
		
		List<Map<String, Object>> currProjectDependencies = SecurityProjectUtils.getProjectDependencies(workspaceId);
		Set<String> curDepList = currProjectDependencies.stream()
			    .map(map -> (String) map.get("engine_id"))
			    .collect(Collectors.toSet());
		
		List<Map<String, Object>> mcpMapList = getMcpMapList();
		Set<String> engines = new HashSet<>();
		Set<String> projectDependencies = new HashSet<>();

		List<Map<String, Object>> dependencyList = new ArrayList<>();
		if (!mcpMapList.isEmpty()) {
			for (Map<String, Object> mcpMap : mcpMapList) {
				if (mcpMap.containsKey("type") && mcpMap.containsKey("id")) {
					String type = (String) mcpMap.get("type");
					String id = (String) mcpMap.get("id");
					CATALOG_TYPE catalogType = CATALOG_TYPE.valueOf(type);
					switch (catalogType) {
					case PROJECT:
						projectDependencies.add(id);
						break;
					default:
						engines.add(id);
					}
					Map<String, Object> dependencyEntry = new HashMap<>();
					dependencyEntry.put("ENGINEID", id);
					dependencyEntry.put("ENGINETYPE", type);
					dependencyList.add(dependencyEntry);
				} else {
					return getError("Tool map must contain both type and id");
				}
			}
		}
		SecurityProjectUtils.updateProjectDependencies(user, workspaceId, dependencyList);

		List<Map<String, String>> workspaceResources = new ArrayList<>();
		for (String engine : engines) {
			if (!SecurityEngineUtils.userCanViewEngine(user, engine) && !curDepList.contains(engine)) {
				return getError("User lacks permission to one of the given engines: " + engine);
			}
			workspaceResources.add(makeResourceEntryMap(workspaceId, engine));
		}

		for (String project : projectDependencies) {
			if (!SecurityProjectUtils.userCanViewProject(user, project) && !curDepList.contains(project)) {
				return getError("User lacks permission to one of the mcp tools/projects: " + project);
			}
			workspaceResources.add(makeProjectResourceEntryMap(workspaceId, project));
		}

		try {
			ModelInferenceLogsUtils.updateWorkspaceEntry(workspaceId, workspaceName, workspaceDescription,
					workspaceSystemPrompt, isActive, workspaceResources, workspacePromptLibraryTag);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return getError("Error during workspace update: " + e.getMessage());
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	/**
	 * 
	 * @param workspaceId
	 * @param engineId
	 * @return
	 */
	private Map<String, String> makeResourceEntryMap(String workspaceId, String engineId) {
		Map<String, String> resource = new HashMap<>();
		Object[] typeAndSubtype = SecurityEngineUtils.getEngineTypeAndSubtype(engineId);
		resource.put("workspace_resource_id", UUID.randomUUID().toString());
		resource.put("workspace_id", workspaceId);
		resource.put("resource_id", engineId);
		resource.put("resource_type", typeAndSubtype[0].toString());
		resource.put("resource_subtype", typeAndSubtype[1].toString());
		return resource;
	}

	/**
	 * 
	 * @param workspaceId
	 * @param projectId
	 * @return
	 */
	private Map<String, String> makeProjectResourceEntryMap(String workspaceId, String projectId) {
		Map<String, String> resource = new HashMap<>();
		IProject projectObj = Utility.getProject(projectId);
		resource.put("workspace_resource_id", UUID.randomUUID().toString());
		resource.put("workspace_id", workspaceId);
		resource.put("resource_id", projectId);
		resource.put("resource_type", CATALOG_TYPE.PROJECT.name());
		resource.put("resource_subtype", projectObj.getProjectType().name());
		return resource;
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getMcpMapList() {
		List<Map<String, Object>> mcpMapList = new ArrayList<>();
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.MCP.getKey());
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				mcpMapList.add((Map<String, Object>) grs.get(i));
			}
		}
		return mcpMapList;
	}

}
