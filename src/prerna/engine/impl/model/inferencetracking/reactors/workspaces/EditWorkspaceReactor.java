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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.prompt.PromptUtils;
import prerna.reactor.agent.skill.PlatformSkills;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.SystemEngineRegistry;

public class EditWorkspaceReactor extends AbstractWorkspaceReactor {

	private static final Logger classLogger = LogManager.getLogger(EditWorkspaceReactor.class);

	public EditWorkspaceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.WORKSPACE_ID.getKey(), NAME, DESCRIPTION, SYSTEM_PROMPT,
				IS_ACTIVE, ReactorKeysEnum.MCP.getKey(), PROMPTS, SKILLS, PLATFORM_SKILLS, MODEL_ID };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();

		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());
		String workspaceName = this.keyValue.get(NAME);
		String workspaceDescription = this.keyValue.get(DESCRIPTION);
		String workspaceSystemPrompt = this.keyValue.get(SYSTEM_PROMPT);
		boolean isActive = !"false".equalsIgnoreCase(this.keyValue.get(IS_ACTIVE));

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

		List<Map<String, Object>> currProjectDependencies = SecurityProjectUtils.getProjectDependencies(workspaceId,
				false);
		Set<String> curDepList = currProjectDependencies.stream().map(map -> (String) map.get("engine_id"))
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

		// linked to workspaces via WORKSPACE_RESOURCE with RESOURCE_TYPE = "PROMPT"
		List<String> promptIds = getNounAsStringList(PROMPTS);
		if (!promptIds.isEmpty()) {
			if (!SystemEngineRegistry.isPromptDbLoaded()) {
				return getError("Prompt database is not enabled");
			}
			for (String promptId : promptIds) {
				Map<String, Object> prompt = PromptUtils.getPrompt(promptId, user);
				if (prompt == null || prompt.isEmpty()) {
					return getError("Prompt not found or user lacks access: " + promptId);
				}
				workspaceResources.add(makePromptResourceEntryMap(workspaceId, promptId));
			}
		}


		Set<String> skillIds = new HashSet<>(getNounAsStringList(SKILLS));
		if (!skillIds.isEmpty()) {
			Set<String> curSkillList = ModelInferenceLogsUtils
					.getWorkspaceResources(workspaceId, SKILL_RESOURCE_TYPE, null).stream()
					.map(map -> map.get("resource_id")).collect(Collectors.toSet());
			for (String skillId : skillIds) {
				if (ModelInferenceLogsUtils.getSkillEntry(skillId) == null) {
					return getError("Skill not found: " + skillId);
				}
				if (!SecurityProjectUtils.userCanViewProject(user, skillId) && !curSkillList.contains(skillId)) {
					return getError("User lacks permission to one of the given skills: " + skillId);
				}
				workspaceResources.add(makeSkillResourceEntryMap(workspaceId, skillId));
			}
		}

		Set<String> platformSkills = null;
		if (getGenRowStruct(PLATFORM_SKILLS) != null) {
			platformSkills = new LinkedHashSet<>();
			for (String slug : getNounAsStringList(PLATFORM_SKILLS)) {
				if (slug == null) {
					continue;
				}
				String trimmed = slug.trim();
				if (trimmed.isEmpty()) {
					continue;
				}
				if (!PlatformSkills.exists(trimmed)) {
					return getError("Platform skill not found: " + trimmed);
				}
				platformSkills.add(trimmed);
			}
		}

		// Default/fallback model engine for the agent. Presence-detected so omitting
		// the key leaves any existing CONFIG_JSON.model_id untouched; passing it blank
		// clears it (falling back to room MODEL_ID / options at run time).
		boolean modelIdProvided = getGenRowStruct(MODEL_ID) != null;
		String workspaceModelId = modelIdProvided ? this.keyValue.get(MODEL_ID) : null;

		try {
			ModelInferenceLogsUtils.updateWorkspaceEntry(workspaceId, workspaceName, workspaceDescription,
					workspaceSystemPrompt, isActive, workspaceResources);
		} catch (Exception e) {
			classLogger.error("Failed to update workspace '{}' (ID: {}).", workspaceName, workspaceId, e);
			return getError("Error during workspace update: " + e.getMessage());
		}

		// Dual-write CONFIG_JSON: mirror system_prompt + MCP engine/project refs +
		// skill refs into WORKSPACE.CONFIG_JSON. Preserve any other CONFIG_JSON fields
		// (e.g. hooks) the workspace already has. Best-effort - a failure here is logged
		// but does not fail the reactor, since the legacy SYSTEM_PROMPT column +
		// WORKSPACE_RESOURCE rows already landed and AgentConfigLoader still resolves
		// correctly from those.
		try {
			mirrorCoreFieldsIntoConfigJson(workspaceId, workspaceSystemPrompt, engines, projectDependencies, skillIds,
					platformSkills, modelIdProvided, workspaceModelId);
		} catch (Exception e) {
			classLogger.warn(
					"Failed to mirror system_prompt/mcps/skills into CONFIG_JSON for workspaceId '{}' (legacy writes already succeeded)",
					workspaceId, e);
			Map<String, Object> partial = new HashMap<>();
			partial.put("success", true);
			partial.put("warning", "Workspace saved but CONFIG_JSON sync failed: " + e.getMessage());
			return new NounMetadata(partial, PixelDataType.MAP);
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	/**
	 * Persist {@code system_prompt}, the engine/project MCP refs, and the skill
	 * refs into {@code WORKSPACE.CONFIG_JSON}, preserving any other fields already
	 * there.
	 *
	 * <p>
	 * Empty {@code engines} + empty {@code projects} writes an empty {@code mcps}
	 * array, and empty {@code skills} writes an empty {@code skills} array - both
	 * intentional, since the user may be removing all MCPs/skills. Null
	 * {@code systemPrompt} omits the key (vs. writing JSON null), so the loader
	 * falls through to the legacy SYSTEM_PROMPT column read for that field.
	 *
	 * <p>
	 * The {@code skills} entry shape - {@code { "skill_id": <id> }} - matches what
	 * {@code AgentConfigLoader.resolveSkills} and
	 * {@code ModelInferenceLogsUtils.addSkillToWorkspaceConfigJson} read/write. No
	 * {@code pinned_version} is emitted because the edit input carries only ids.
	 *
	 * <p>
	 * {@code platformSkills} differs from {@code skills}/{@code mcps}: a {@code null}
	 * value leaves any existing {@code platform_skills} array untouched (callers that
	 * omit the input never clobber it), while a non-null value is a full replace (an
	 * empty set clears it). Entries are plain slug strings, matching
	 * {@code CONFIG_JSON.platform_skills[]}.
	 */
	private static void mirrorCoreFieldsIntoConfigJson(String workspaceId, String systemPrompt, Set<String> engines,
			Set<String> projects, Set<String> skills, Set<String> platformSkills, boolean modelIdProvided,
			String modelId) throws Exception {
		JSONObject cfg = ModelInferenceLogsUtils.getWorkspaceConfigJson(workspaceId);
		if (cfg == null) {
			cfg = new JSONObject();
			cfg.put("schema_version", 1);
		}
		if (systemPrompt != null && !systemPrompt.isEmpty()) {
			cfg.put("system_prompt", systemPrompt);
		} else {
			cfg.remove("system_prompt");
		}

		// model_id mirrors the platform_skills semantics: omitted -> leave untouched;
		// provided-blank -> clear; provided -> set as the agent's default model.
		if (modelIdProvided) {
			if (modelId != null && !modelId.trim().isEmpty()) {
				cfg.put("model_id", modelId.trim());
			} else {
				cfg.remove("model_id");
			}
		}

		JSONArray mcpsJson = new JSONArray();
		for (String id : engines) {
			JSONObject entry = new JSONObject();
			entry.put("id", id);
			entry.put("name", id);
			mcpsJson.put(entry);
		}
		for (String id : projects) {
			JSONObject entry = new JSONObject();
			entry.put("id", id);
			entry.put("name", id);
			mcpsJson.put(entry);
		}
		cfg.put("mcps", mcpsJson);

		JSONArray skillsJson = new JSONArray();
		for (String id : skills) {
			JSONObject entry = new JSONObject();
			entry.put("skill_id", id);
			skillsJson.put(entry);
		}
		cfg.put("skills", skillsJson);

		if (platformSkills != null) {
			JSONArray platformSkillsJson = new JSONArray();
			for (String slug : platformSkills) {
				platformSkillsJson.put(slug);
			}
			cfg.put("platform_skills", platformSkillsJson);
		}

		ModelInferenceLogsUtils.updateWorkspaceConfigJson(workspaceId, cfg);
	}

}
