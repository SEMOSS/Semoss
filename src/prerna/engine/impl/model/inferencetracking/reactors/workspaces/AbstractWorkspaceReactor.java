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

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.project.api.IProject;
import prerna.prompt.PromptUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.skill.PlatformSkills;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

/**
 * Base reactor for workspace operations that need to assemble workspace-resource mappings.
 * <p>
 * Provides common helper methods to construct normalized resource rows for engines, projects,
 * and prompts so concrete workspace reactors can reuse consistent payload structures.
 */
public abstract class AbstractWorkspaceReactor extends AbstractReactor {

	/** Resource type identifier used when a workspace resource points to a prompt. */
	public static final String PROMPT_RESOURCE_TYPE = "PROMPT";

	/** Resource type identifier used when a workspace resource points to a skill in the skill registry. */
	public static final String SKILL_RESOURCE_TYPE = "SKILL";

	/** Request key for workspace name. */
	static final String NAME = "name";
	/** Request key for workspace description. */
	static final String DESCRIPTION = "description";
	/** Request key for workspace-level system prompt. */
	static final String SYSTEM_PROMPT = "systemPrompt";
	/** Request key for prompt collection input. */
	static final String PROMPTS = "prompts";
	/** Request key for skill collection input. */
	static final String SKILLS = "skills";
	/** Request key for platform-skill (slug) collection input. */
	static final String PLATFORM_SKILLS = "platformSkills";
	/** Request key for active/inactive workspace state. */
	static final String IS_ACTIVE = "isActive";

	/**
	 * Builds a workspace resource row for an engine, including engine type metadata.
	 *
	 * @param workspaceId workspace identifier that owns the resource
	 * @param engineId engine identifier being linked to the workspace
	 * @return map representing a row for workspace resource persistence
	 */
	Map<String, String> makeResourceEntryMap(String workspaceId, String engineId) {
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
	 * Builds a workspace resource row for a project.
	 *
	 * @param workspaceId workspace identifier that owns the resource
	 * @param projectId project identifier being linked to the workspace
	 * @return map representing a row for workspace resource persistence
	 */
	Map<String, String> makeProjectResourceEntryMap(String workspaceId, String projectId) {
		Map<String, String> resource = new HashMap<>();
		IProject projectObj = Utility.getProject(projectId);
		resource.put("workspace_resource_id", UUID.randomUUID().toString());
		resource.put("workspace_id", workspaceId);
		resource.put("resource_id", projectId);
		resource.put("resource_type", CATALOG_TYPE.PROJECT.name());
		resource.put("resource_subtype", projectObj.getProjectType().name());
		return resource;
	}

	/**
	 * Builds a workspace resource row for a prompt.
	 *
	 * @param workspaceId workspace identifier that owns the resource
	 * @param promptId prompt identifier being linked to the workspace
	 * @return map representing a row for workspace resource persistence
	 */
	Map<String, String> makePromptResourceEntryMap(String workspaceId, String promptId) {
		Map<String, String> resource = new HashMap<>();
		resource.put("workspace_resource_id", UUID.randomUUID().toString());
		resource.put("workspace_id", workspaceId);
		resource.put("resource_id", promptId);
		resource.put("resource_type", PROMPT_RESOURCE_TYPE);
		resource.put("resource_subtype", null);
		return resource;
	}

	/**
	 * Builds a workspace resource row for a skill.
	 * <p>
	 * The {@code resource_subtype} carries the optional pinned skill version
	 * ({@code AgentConfigLoader.resolveSkills} reads it back as {@code pinned_version}).
	 * It is left {@code null} here because the workspace edit/add inputs only carry
	 * skill ids - matching {@link prerna.reactor.agent.skill.AttachSkillToWorkspaceReactor},
	 * which also attaches with no pinned version.
	 *
	 * @param workspaceId workspace identifier that owns the resource
	 * @param skillId skill identifier being linked to the workspace
	 * @return map representing a row for workspace resource persistence
	 */
	Map<String, String> makeSkillResourceEntryMap(String workspaceId, String skillId) {
		Map<String, String> resource = new HashMap<>();
		resource.put("workspace_resource_id", UUID.randomUUID().toString());
		resource.put("workspace_id", workspaceId);
		resource.put("resource_id", skillId);
		resource.put("resource_type", SKILL_RESOURCE_TYPE);
		resource.put("resource_subtype", null);
		return resource;
	}

	/**
	 * Returns MCP configuration entries from the incoming reactor request payload.
	 *
	 * @return list of MCP maps; each map represents one MCP configuration object
	 */
	List<Map<String, Object>> getMcpMapList() {
		return getList(ReactorKeysEnum.MCP.getKey(), List.of());
	}

	/**
	 * Mirrors the workspace's {@code system_prompt}, MCP refs, skill refs, and
	 * (optionally) platform-skill refs into {@code WORKSPACE.CONFIG_JSON},
	 * preserving any other fields already present (hooks, subagents, budgets,
	 * etc.).
	 *
	 * <p>Empty {@code engines} + empty {@code projects} writes an empty
	 * {@code mcps} array, and empty {@code skills} writes an empty {@code skills}
	 * array - both intentional, since the caller may be removing all of them.
	 * Null {@code systemPrompt} omits the key (vs. writing JSON null), so the
	 * loader falls through to the legacy SYSTEM_PROMPT column for that field.
	 *
	 * <p>The {@code skills} entry shape - {@code { "skill_id": <id> }} - matches
	 * what {@code AgentConfigLoader.resolveSkills} and
	 * {@code ModelInferenceLogsUtils.addSkillToWorkspaceConfigJson} read/write.
	 * No {@code pinned_version} is emitted because the edit/add inputs carry only
	 * ids.
	 *
	 * <p>{@code platformSkills} differs from {@code skills}/{@code mcps}: a
	 * {@code null} value leaves any existing {@code platform_skills} array
	 * untouched (callers that omit the input never clobber it), while a non-null
	 * value is a full replace (an empty set clears it). Entries are plain slug
	 * strings, matching {@code CONFIG_JSON.platform_skills[]}.
	 */
	protected static void mirrorCoreFieldsIntoConfigJson(String workspaceId, String systemPrompt, Set<String> engines,
			Set<String> projects, Set<String> skills, Set<String> platformSkills) throws Exception {
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

	/**
	 * Reads the MCP / prompt / skill nouns from the request, validates them, and
	 * populates the caller-owned accumulators in place. Throws
	 * {@link IllegalArgumentException} with a human-readable message on
	 * validation failure (callers catch and convert to {@code getError(...)}).
	 *
	 * <p>Platform skills are handled separately by
	 * {@link #collectPlatformSkillsInput()} because the null-vs-empty distinction
	 * (input absent vs. input supplied empty) matters for the CONFIG_JSON mirror
	 * and doesn't fit a simple out-param.
	 *
	 * <p>The allowlist parameters carry the "existing attachment" escape hatch
	 * Edit uses: an id already attached to the workspace passes the permission
	 * check even if the caller has lost view rights since. Add passes
	 * {@code null} for both, since on create there are no prior attachments to
	 * preserve.
	 */
	protected void validateWorkspaceInputs(User user, String workspaceId,
			Set<String> existingDepAllowlist, Set<String> existingSkillAllowlist,
			Set<String> engines, Set<String> projectDependencies,
			List<Map<String, Object>> dependencyList,
			List<Map<String, String>> workspaceResources,
			Set<String> skillIds) {
		boolean hasDepAllowlist = existingDepAllowlist != null;
		boolean hasSkillAllowlist = existingSkillAllowlist != null;
		List<Map<String, Object>> mcpMapList = getMcpMapList();
		for (Map<String, Object> mcpMap : mcpMapList) {
			if (!mcpMap.containsKey("type") || !mcpMap.containsKey("id")) {
				throw new IllegalArgumentException("Tool map must contain both type and id");
			}
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
		}

		for (String engine : engines) {
			if (!SecurityEngineUtils.userCanViewEngine(user, engine)
					&& !(hasDepAllowlist && existingDepAllowlist.contains(engine))) {
				throw new IllegalArgumentException("User lacks permission to one of the given engines: " + engine);
			}
			workspaceResources.add(makeResourceEntryMap(workspaceId, engine));
		}

		for (String project : projectDependencies) {
			if (!SecurityProjectUtils.userCanViewProject(user, project)
					&& !(hasDepAllowlist && existingDepAllowlist.contains(project))) {
				throw new IllegalArgumentException(
						"User lacks permission to one of the mcp tools/projects: " + project);
			}
			workspaceResources.add(makeProjectResourceEntryMap(workspaceId, project));
		}

		// linked to workspaces via WORKSPACE_RESOURCE with RESOURCE_TYPE = "PROMPT"
		List<String> promptIds = getNounAsStringList(PROMPTS);
		if (!promptIds.isEmpty()) {
			if (!SystemEngineRegistry.isPromptDbLoaded()) {
				throw new IllegalArgumentException("Prompt database is not enabled");
			}
			for (String promptId : promptIds) {
				Map<String, Object> prompt = PromptUtils.getPrompt(promptId, user);
				if (prompt == null || prompt.isEmpty()) {
					throw new IllegalArgumentException("Prompt not found or user lacks access: " + promptId);
				}
				workspaceResources.add(makePromptResourceEntryMap(workspaceId, promptId));
			}
		}

		skillIds.addAll(getNounAsStringList(SKILLS));
		for (String skillId : skillIds) {
			if (ModelInferenceLogsUtils.getSkillEntry(skillId) == null) {
				throw new IllegalArgumentException("Skill not found: " + skillId);
			}
			if (!SecurityProjectUtils.userCanViewProject(user, skillId)
					&& !(hasSkillAllowlist && existingSkillAllowlist.contains(skillId))) {
				throw new IllegalArgumentException("User lacks permission to one of the given skills: " + skillId);
			}
			workspaceResources.add(makeSkillResourceEntryMap(workspaceId, skillId));
		}
	}

	/**
	 * Reads and validates the {@code PLATFORM_SKILLS} noun. Returns {@code null}
	 * when the noun was not supplied (signal to leave existing CONFIG_JSON
	 * {@code platform_skills} untouched), a populated set when slugs were
	 * supplied (possibly empty after trimming), and throws
	 * {@link IllegalArgumentException} when a slug fails validation. Callers
	 * catch and convert to {@code getError(...)} when they want the existing
	 * partial-success response shape.
	 */
	protected Set<String> collectPlatformSkillsInput() {
		if (getGenRowStruct(PLATFORM_SKILLS) == null) {
			return null;
		}
		Set<String> out = new LinkedHashSet<>();
		for (String slug : getNounAsStringList(PLATFORM_SKILLS)) {
			if (slug == null) {
				continue;
			}
			String trimmed = slug.trim();
			if (trimmed.isEmpty()) {
				continue;
			}
			if (!PlatformSkills.exists(trimmed)) {
				throw new IllegalArgumentException("Platform skill not found: " + trimmed);
			}
			out.add(trimmed);
		}
		return out;
	}

}
