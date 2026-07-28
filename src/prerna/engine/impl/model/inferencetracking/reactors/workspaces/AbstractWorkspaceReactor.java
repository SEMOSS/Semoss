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
import prerna.reactor.agent.hooks.AgentHookRegistry;
import prerna.reactor.agent.skill.SkillProjects;
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
	/** Request key for the workspace/agent default model engine id (CONFIG_JSON.model_id). */
	static final String MODEL_ID = "modelId";
	/** Request key for prompt collection input. */
	static final String PROMPTS = "prompts";
	/** Request key for skill collection input. */
	static final String SKILLS = "skills";
	/** Request key for active/inactive workspace state. */
	static final String IS_ACTIVE = "isActive";
	/** Request key for the agent tool-loop turn cap (CONFIG_JSON.budgets.max_turns). */
	static final String MAX_TURNS = "maxTurns";
	/** Request key for the agent self-critique round cap (CONFIG_JSON.budgets.max_reflections). */
	static final String MAX_REFLECTIONS = "maxReflections";
	/** Request key for the subagent delegation depth cap (CONFIG_JSON.spawn_policy.max_subagent_depth). */
	static final String MAX_SUBAGENT_DEPTH = "maxSubagentDepth";
	/** Request key for the per-run subagent spawn budget (CONFIG_JSON.spawn_policy.max_subagents_per_run). */
	static final String MAX_SUBAGENTS_PER_RUN = "maxSubagentsPerRun";
	/** Request key for the per-turn subagent spawn cap (CONFIG_JSON.spawn_policy.max_spawns_per_turn). */
	static final String MAX_SPAWNS_PER_TURN = "maxSpawnsPerTurn";
	/** Request key for named subagent slots (CONFIG_JSON.subagents[]). */
	static final String SUBAGENTS = "subagents";
	/** Request key for agent lifecycle hooks (CONFIG_JSON.hooks[]). */
	static final String HOOKS = "hooks";

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
	 * Returns named subagent spec entries ({@code {workspaceId}}) from the
	 * incoming reactor request payload.
	 *
	 * @return list of subagent maps; each map represents one requested subagent slot
	 */
	List<Map<String, Object>> getSubagentMapList() {
		return getList(SUBAGENTS, List.of());
	}

	/**
	 * Validates and normalizes {@code rawSubagents} into the internal shape
	 * {@link #mirrorCoreFieldsIntoConfigJson} writes to {@code CONFIG_JSON.subagents[]}.
	 * The request and persisted config only identify a target {@code workspaceId}; alias and
	 * description are authoritative target-agent metadata resolved by {@code AgentConfigLoader}
	 * for each run.
	 *
	 * <p>Rejects a subagent workspaceId equal to {@code workspaceId} itself (trivial
	 * self-delegation loop - {@code spawn_policy.max_subagent_depth} bounds it at run time
	 * regardless, but there's no legitimate reason to author one), duplicate targets, inactive
	 * or missing target agents, and any workspaceId the user lacks view permission on.
	 *
	 * @throws IllegalArgumentException with a human-readable message on validation failure
	 *                                   (callers catch and convert to {@code getError(...)})
	 */
	protected static List<Map<String, Object>> validateAndNormalizeSubagents(User user, String workspaceId,
			List<Map<String, Object>> rawSubagents) {
		List<Map<String, Object>> normalized = new ArrayList<>(rawSubagents.size());
		Set<String> seenWorkspaceIds = new LinkedHashSet<>();
		for (Map<String, Object> raw : rawSubagents) {
			if (raw == null) {
				throw new IllegalArgumentException("Subagent entry cannot be null");
			}
			Object workspaceIdObj = raw.get("workspaceId");
			String targetWorkspaceId = workspaceIdObj == null ? null : workspaceIdObj.toString().trim();
			if (targetWorkspaceId == null || targetWorkspaceId.isEmpty()) {
				throw new IllegalArgumentException("Subagent entry is missing a target workspaceId");
			}
			if (targetWorkspaceId.equals(workspaceId)) {
				throw new IllegalArgumentException("A subagent cannot target its own workspace");
			}
			if (!seenWorkspaceIds.add(targetWorkspaceId)) {
				throw new IllegalArgumentException("Duplicate subagent workspace: " + targetWorkspaceId);
			}
			if (!SecurityProjectUtils.userCanViewProject(user, targetWorkspaceId)) {
				throw new IllegalArgumentException(
						"User lacks permission to one of the given subagent workspaces: " + targetWorkspaceId);
			}

			Map<String, Object> targetWorkspace = ModelInferenceLogsUtils.getWorkspaceEntry(targetWorkspaceId);
			if (targetWorkspace == null) {
				throw new IllegalArgumentException("Subagent workspace not found: " + targetWorkspaceId);
			}
			if (!Boolean.TRUE.equals(targetWorkspace.get("is_active"))) {
				throw new IllegalArgumentException("Subagent workspace is inactive: " + targetWorkspaceId);
			}
			String targetName = valueAsTrimmedString(targetWorkspace.get("name"));
			if (targetName == null) {
				throw new IllegalArgumentException("Subagent workspace has no name: " + targetWorkspaceId);
			}

			Map<String, Object> entry = new HashMap<>();
			entry.put("workspaceId", targetWorkspaceId);
			normalized.add(entry);
		}
		return normalized;
	}

	private static String valueAsTrimmedString(Object value) {
		if (value == null) {
			return null;
		}
		String stringValue = value.toString().trim();
		return stringValue.isEmpty() ? null : stringValue;
	}

	/**
	 * Returns agent lifecycle hook entries ({@code {kind, ...kind-specific fields}}) from
	 * the incoming reactor request payload.
	 *
	 * @return list of hook maps; each map represents one requested hook entry
	 */
	List<Map<String, Object>> getHookMapList() {
		return getList(HOOKS, List.of());
	}

	/**
	 * Validates each hook entry has a known {@code kind}, plus the one kind-specific
	 * required field known today ({@code kind="pixel"} requires a non-empty {@code pixel}
	 * field). Mirrors the validation the standalone {@code SetAgentHooksReactor} used to
	 * perform before hooks were folded into this same write path as every other agent-config
	 * field.
	 *
	 * <p>Unlike {@link #validateAndNormalizeSubagents}, this does not reconstruct entries
	 * field-by-field - hook schemas are open-ended per kind (e.g. {@code pixel}'s optional
	 * {@code events} array, and future kinds may carry their own fields this reactor doesn't
	 * need to know about), so validated entries are persisted as-is.
	 *
	 * @throws IllegalArgumentException with a human-readable message on validation failure
	 *                                   (callers catch and convert to {@code getError(...)})
	 */
	protected static void validateHooks(List<Map<String, Object>> rawHooks) {
		for (int i = 0; i < rawHooks.size(); i++) {
			Map<String, Object> entry = rawHooks.get(i);
			if (entry == null) {
				throw new IllegalArgumentException("hooks[" + i + "] is null");
			}
			Object kindObj = entry.get("kind");
			if (kindObj == null) {
				throw new IllegalArgumentException("hooks[" + i + "] missing required 'kind'");
			}
			String kind = String.valueOf(kindObj);
			if (!AgentHookRegistry.isKnown(kind)) {
				throw new IllegalArgumentException(
						"hooks[" + i + "] unknown kind '" + kind + "'. Known kinds: " + AgentHookRegistry.knownKinds());
			}
			if (AgentHookRegistry.PIXEL.equals(kind)) {
				Object pixelObj = entry.get("pixel");
				if (pixelObj == null || String.valueOf(pixelObj).trim().isEmpty()) {
					throw new IllegalArgumentException(
							"hooks[" + i + "] kind='pixel' requires a non-empty 'pixel' field");
				}
			}
		}
	}

	/**
	 * Mirrors the workspace's {@code system_prompt}, MCP refs, and skill refs
	 * into {@code WORKSPACE.CONFIG_JSON}, preserving any other fields already
	 * present (hooks, subagents, budgets, etc.).
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
	 * <p>Any legacy {@code platform_skills} array is dropped on write: platform
	 * skills are ordinary SKILL-type projects now and live in {@code skills[]}.
	 */
	protected static void mirrorCoreFieldsIntoConfigJson(String workspaceId, String systemPrompt, Set<String> engines,
			Set<String> projects, Set<String> skills) throws Exception {
		mirrorCoreFieldsIntoConfigJson(workspaceId, systemPrompt, engines, projects, skills, false, null, null, null,
				false, null, false, null);
	}

	protected static void mirrorCoreFieldsIntoConfigJson(String workspaceId, String systemPrompt, Set<String> engines,
			Set<String> projects, Set<String> skills, boolean modelIdProvided, String modelId) throws Exception {
		mirrorCoreFieldsIntoConfigJson(workspaceId, systemPrompt, engines, projects, skills, modelIdProvided, modelId,
				null, null, false, null, false, null);
	}

	/**
	 * @param budgetUpdates      {@code CONFIG_JSON.budgets} keys (e.g. {@code max_turns})
	 *                           to add/overwrite; a {@code null} value removes that key
	 *                           (falls back to the caller/session-supplied value at run
	 *                           time). {@code null} or empty leaves {@code budgets}
	 *                           untouched entirely.
	 * @param spawnPolicyUpdates same as {@code budgetUpdates}, but for
	 *                           {@code CONFIG_JSON.spawn_policy} keys (e.g.
	 *                           {@code max_subagent_depth}); a removed key falls back to
	 *                           the platform default for that field at run time.
	 * @param subagentsProvided  whether the caller passed a {@code subagents} key at all;
	 *                           when {@code false}, {@code CONFIG_JSON.subagents} is left
	 *                           untouched regardless of {@code subagents}.
	 * @param subagents          validated target workspace references (see
	 *                           {@link #validateAndNormalizeSubagents}) to fully replace
	 *                           {@code CONFIG_JSON.subagents[]} with; an empty list clears
	 *                           it. Ignored when {@code subagentsProvided} is {@code false}.
	 * @param hooksProvided      whether the caller passed a {@code hooks} key at all; when
	 *                           {@code false}, {@code CONFIG_JSON.hooks} is left untouched
	 *                           regardless of {@code hooks}.
	 * @param hooks              validated entries (see {@link #validateHooks}) to fully
	 *                           replace {@code CONFIG_JSON.hooks[]} with, persisted as-is;
	 *                           an empty list clears it. Ignored when {@code hooksProvided}
	 *                           is {@code false}.
	 */
	protected static void mirrorCoreFieldsIntoConfigJson(String workspaceId, String systemPrompt, Set<String> engines,
			Set<String> projects, Set<String> skills, boolean modelIdProvided, String modelId,
			Map<String, Integer> budgetUpdates, Map<String, Integer> spawnPolicyUpdates, boolean subagentsProvided,
			List<Map<String, Object>> subagents, boolean hooksProvided, List<Map<String, Object>> hooks)
			throws Exception {
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

		// legacy key from when platform skills were disk-backed built-ins; never
		// honored anymore, so scrub it whenever the config is rewritten
		cfg.remove("platform_skills");

		applyIntegerUpdates(cfg, "budgets", budgetUpdates);
		applyIntegerUpdates(cfg, "spawn_policy", spawnPolicyUpdates);

		if (subagentsProvided) {
			JSONArray subagentsJson = new JSONArray();
			for (Map<String, Object> entry : subagents) {
				JSONObject entryJson = new JSONObject();
				entryJson.put("workspaceId", entry.get("workspaceId"));
				subagentsJson.put(entryJson);
			}
			cfg.put("subagents", subagentsJson);
		}

		if (hooksProvided) {
			JSONArray hooksJson = new JSONArray();
			for (Map<String, Object> entry : hooks) {
				hooksJson.put(new JSONObject(entry));
			}
			cfg.put("hooks", hooksJson);
		}

		ModelInferenceLogsUtils.updateWorkspaceConfigJson(workspaceId, cfg);
	}

	/**
	 * Merges caller-provided integer fields into the {@code cfg.<subObjectKey>} sub-object,
	 * creating it if absent. A {@code null} value in {@code updates} removes that field from
	 * the sub-object (rather than writing JSON null) so the run-time resolver falls back to
	 * its own default/cap for that specific field. Leaves the sub-object alone entirely when
	 * {@code updates} is {@code null} or empty, preserving fields set by other means (e.g. a
	 * future hooks-style reactor writing {@code max_seconds}).
	 */
	private static void applyIntegerUpdates(JSONObject cfg, String subObjectKey, Map<String, Integer> updates) {
		if (updates == null || updates.isEmpty()) {
			return;
		}
		JSONObject sub = cfg.optJSONObject(subObjectKey);
		if (sub == null) {
			sub = new JSONObject();
		}
		for (Map.Entry<String, Integer> entry : updates.entrySet()) {
			if (entry.getValue() == null) {
				sub.remove(entry.getKey());
			} else {
				sub.put(entry.getKey(), entry.getValue().intValue());
			}
		}
		cfg.put(subObjectKey, sub);
	}

	/**
	 * Reads the MCP / prompt / skill nouns from the request, validates them, and
	 * populates the caller-owned accumulators in place. Throws
	 * {@link IllegalArgumentException} with a human-readable message on
	 * validation failure (callers catch and convert to {@code getError(...)}).
	 *
	 * <p>{@code existingDependencies} and {@code existingSkills} are the
	 * workspace's pre-existing attachments - ids already in
	 * {@code PROJECTDEPENDENCIES} and skill ids already in
	 * {@code WORKSPACE_RESOURCE} respectively. They carry the "existing
	 * attachment" escape hatch Edit uses: an id already attached passes the
	 * permission check even if the caller has lost view rights since. Add
	 * passes {@code null} for both, since on create there are no prior
	 * attachments to preserve.
	 */
	protected void validateWorkspaceInputs(User user, String workspaceId,
			Set<String> existingDependencies, Set<String> existingSkills,
			Set<String> engines, Set<String> projectDependencies,
			List<Map<String, Object>> dependencyList,
			List<Map<String, String>> workspaceResources,
			Set<String> skillIds) {
		boolean hasExistingDeps = existingDependencies != null;
		boolean hasExistingSkills = existingSkills != null;
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
					&& !(hasExistingDeps && existingDependencies.contains(engine))) {
				throw new IllegalArgumentException("User lacks permission to one of the given engines: " + engine);
			}
			workspaceResources.add(makeResourceEntryMap(workspaceId, engine));
		}

		for (String project : projectDependencies) {
			if (!SecurityProjectUtils.userCanViewProject(user, project)
					&& !(hasExistingDeps && existingDependencies.contains(project))) {
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
			if (!SkillProjects.isSkillProject(skillId)) {
				throw new IllegalArgumentException("Skill not found: " + skillId);
			}
			if (!SecurityProjectUtils.userCanViewProject(user, skillId)
					&& !(hasExistingSkills && existingSkills.contains(skillId))) {
				throw new IllegalArgumentException("User lacks permission to one of the given skills: " + skillId);
			}
			workspaceResources.add(makeSkillResourceEntryMap(workspaceId, skillId));
			// Skills are projects (type=SKILL), so they belong in PROJECTDEPENDENCIES
			// alongside MCP engines/projects. ENGINETYPE = "PROJECT" because the
			// catalog stores skills as projects.
			Map<String, Object> skillDep = new HashMap<>();
			skillDep.put("ENGINEID", skillId);
			skillDep.put("ENGINETYPE", CATALOG_TYPE.PROJECT.name());
			dependencyList.add(skillDep);
		}
	}

}
