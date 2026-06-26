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
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.prompt.PromptUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.skill.PlatformSkills;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

public class GetWorkspaceReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetWorkspaceReactor.class);

	private static final String CLASS_NAME = GetWorkspaceReactor.class.getName();

	// To get workspaces without resources, call MyProjects w/ type as workspace
	public GetWorkspaceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.WORKSPACE_ID.getKey() };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);

		organizeKeys();

		User user = this.insight.getUser();

		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());

		Map<String, Object> current = ModelInferenceLogsUtils.getWorkspaceEntry(workspaceId);
		if (current == null) {
			throw new IllegalArgumentException("Workspace not found");
		}

		// convert legacy workspaces into projects
		if (!AbstractSecurityUtils.containsProjectId(workspaceId)) {
			String workspaceName = (String) current.get("name");
			if (!Utility.validateName(workspaceName)) {
				workspaceName = cleanWorkspaceName(workspaceName);
			}

			ProjectHelper.createWorkspaceProject(workspaceId, workspaceName, IProject.PROJECT_TYPE.WORKSPACE, false,
					false, null, null, null, user, logger);
		}

		String permission = null;
		long userCount = 1;

		Object currentlyIsActive = current.get("is_active");
		Boolean currentlyActive = (Boolean) currentlyIsActive;

		if (!currentlyActive || !SecurityProjectUtils.userCanViewProject(user, workspaceId)) {
			throw new IllegalArgumentException("User unauthorized to perform this operation");
		}

		try {
			permission = SecurityProjectUtils.getActualUserProjectPermission(user, workspaceId);
			userCount = SecurityProjectUtils.getProjectUsersCount(user, workspaceId, null, null);
		} catch (IllegalAccessException e) {
			classLogger.error("Failed to fetch workspace permission/collaborator info for workspace '{}'.", workspaceId,
					e);
		}

		List<Map<String, Object>> resources = ModelInferenceLogsUtils.getWorkspaceResourcesByType(workspaceId, null);

		List<Map<String, String>> mcps = new ArrayList<>();
		List<Map<String, String>> prompts = new ArrayList<>();
		List<Map<String, String>> skills = new ArrayList<>();
		for (Map<String, Object> r : resources) {
			String resourceId = (String) r.get("resource_id");
			String rType = (String) r.get("resource_type");

			// Handle prompt resources separately because prompt is not catalog type
			if (AbstractWorkspaceReactor.PROMPT_RESOURCE_TYPE.equalsIgnoreCase(rType)) {
				Map<String, String> promptMap = new HashMap<>();
				promptMap.put("id", resourceId);
				promptMap.put("type", rType);
				if (SystemEngineRegistry.isPromptDbLoaded()) {
					Map<String, Object> promptDetail = PromptUtils.getPrompt(resourceId, user);
					String promptTitle = promptDetail != null && !promptDetail.isEmpty()
							? (String) promptDetail.get("title")
							: null;
					promptMap.put("name", promptTitle);
				}
				prompts.add(promptMap);
			} else if (AbstractWorkspaceReactor.SKILL_RESOURCE_TYPE.equalsIgnoreCase(rType)) {

				Map<String, String> skillMap = new HashMap<>();
				skillMap.put("id", resourceId);
				skillMap.put("type", rType);
				skillMap.put("name", SecurityProjectUtils.getProjectAliasForId(resourceId));
				Map<String, Object> skillRow = ModelInferenceLogsUtils.getSkillEntry(resourceId);
				if (skillRow != null) {
					Object slug = skillRow.get("slug");
					if (slug != null) {
						skillMap.put("slug", slug.toString());
					}
					Object description = skillRow.get("description");
					if (description != null) {
						skillMap.put("description", description.toString());
					}
				}
				skills.add(skillMap);
			} else {
				CATALOG_TYPE resourceType = CATALOG_TYPE.valueOf(rType.toUpperCase());
				Map<String, String> mcpMap = new HashMap<>();
				mcpMap.put("id", resourceId);
				if (resourceType == CATALOG_TYPE.PROJECT) {
					String rName = SecurityProjectUtils.getProjectAliasForId(resourceId);
					mcpMap.put("name", rName);
				} else {
					String rName = SecurityEngineUtils.getEngineAliasForId(resourceId);
					mcpMap.put("name", rName);
				}
				mcpMap.put("type", rType);
				mcps.add(mcpMap);
			}
		}
		mergePlatformSkills(workspaceId, skills);

		current.put("mcp", mcps);
		current.put("prompts", prompts);
		current.put("skills", skills);
		current.put("permission", permission);
		current.put("number_collaborators", userCount);

		return new NounMetadata(current, PixelDataType.MAP);
	}

	/**
	 * Appends the workspace's platform skills (disk-backed built-ins referenced by
	 * slug in {@code CONFIG_JSON.platform_skills[]}) to {@code skills}. Each entry
	 * carries {@code id} (= slug), {@code slug}, {@code type=PLATFORM_SKILL}, and
	 * {@code name}/{@code description} resolved from the on-disk catalog - falling
	 * back to the slug for name when a slug is no longer present on disk so it stays
	 * visible (and detachable) in the UI.
	 */
	private void mergePlatformSkills(String workspaceId, List<Map<String, String>> skills) {
		try {
			JSONObject cfg = ModelInferenceLogsUtils.getWorkspaceConfigJson(workspaceId);
			JSONArray platformSlugs = cfg != null ? cfg.optJSONArray("platform_skills") : null;
			if (platformSlugs == null || platformSlugs.length() == 0) {
				return;
			}
			Map<String, Map<String, Object>> catalog = new HashMap<>();
			for (Map<String, Object> ps : PlatformSkills.list()) {
				Object slug = ps.get("slug");
				if (slug != null) {
					catalog.put(slug.toString(), ps);
				}
			}
			for (int i = 0; i < platformSlugs.length(); i++) {
				String slug = platformSlugs.optString(i, null);
				if (slug == null || slug.trim().isEmpty()) {
					continue;
				}
				slug = slug.trim();
				Map<String, Object> meta = catalog.get(slug);
				Map<String, String> skillMap = new HashMap<>();
				skillMap.put("id", slug);
				skillMap.put("slug", slug);
				skillMap.put("type", PlatformSkills.PLATFORM_SKILL_TYPE);
				Object name = meta != null ? meta.get("name") : null;
				skillMap.put("name", name != null ? name.toString() : slug);
				Object description = meta != null ? meta.get("description") : null;
				if (description != null) {
					skillMap.put("description", description.toString());
				}
				skills.add(skillMap);
			}
		} catch (Exception e) {
			classLogger.warn("Failed to merge platform skills for workspace '{}': {}", workspaceId, e.getMessage());
		}
	}

	/**
	 * 
	 * @param workspaceName
	 * @return
	 */
	public static String cleanWorkspaceName(String workspaceName) {
		if (workspaceName == null || workspaceName.isEmpty()) {
			return "Unnamed Workspace";
		}

		// Remove all invalid characters
		String cleaned = workspaceName.replaceAll("[^a-zA-Z0-9 _-]", "");

		// Remove leading non-letters
		cleaned = cleaned.replaceAll("^[^a-zA-Z]*", "");

		// If string is empty after cleaning, provide a default
		if (cleaned.isEmpty()) {
			return "Unnamed Workspace";
		}

		return cleaned;
	}

}
