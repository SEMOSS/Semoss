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
package prerna.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;

/**
 * Seeds the immutable, global "system agent" workspaces (e.g. the App Building
 * Agent) into the ModelInferenceLogsDatabase at boot.
 *
 * <p>
 * A system agent is a {@code platform__<id>} project of enum type
 * {@code WORKSPACE} that is catalogued global with no owner (see
 * {@link ProjectWatcher#init()}) - exactly like the platform skills and system
 * MCPs. Unlike those, an agent also needs a {@code WORKSPACE} row plus
 * {@code WORKSPACE_RESOURCE} rows describing its tools and skills, which live
 * in a database rather than on disk. This class provisions those rows.
 *
 * <p>
 * {@link #seed(String)} is idempotent and self-healing: it is safe to run on
 * every boot. It creates the {@code WORKSPACE} row once, back-fills any missing
 * resource rows on subsequent boots, and always rewrites {@code CONFIG_JSON} so
 * a drifted mirror is repaired. If the ModelInferenceLogsDatabase feature is
 * disabled it no-ops (the project itself still catalogs).
 *
 * <p>
 * The agent's tools and skills are derived from {@link SystemDefaultEngines} so
 * they stay in sync with the platform lists automatically:
 * <ul>
 * <li>tools = {@link SystemDefaultEngines#getSystemMCPs()} (database-maker,
 * node-builder, reactor-help)</li>
 * <li>skills = {@link SystemDefaultEngines#getSystemSkills()}</li>
 * </ul>
 */
public class SystemAgentSeeder {

	private static final Logger classLogger = LogManager.getLogger(SystemAgentSeeder.class);

	/**
	 * WORKSPACE_RESOURCE.RESOURCE_TYPE discriminator for skills. Mirrors
	 * {@code AbstractWorkspaceReactor.SKILL_RESOURCE_TYPE} ("SKILL"); duplicated
	 * here to avoid a {@code prerna.util} -> reactors dependency.
	 */
	private static final String SKILL_RESOURCE_TYPE = "SKILL";

	/**
	 * System agents carry no real owner. WORKSPACE.OWNER is only read for the
	 * {@code is_creator} display flag, never for access control, so a null owner
	 * keeps {@code is_creator=false} for every user and matches the null
	 * {@code PROJECT.CREATEDBY} produced by the global catalog path.
	 */
	private static final String SYSTEM_OWNER = null;

	private static final int CONFIG_SCHEMA_VERSION = 1;

	private SystemAgentSeeder() {
	}

	/**
	 * Idempotently seed the WORKSPACE row + resource rows + CONFIG_JSON for a
	 * system agent. Never throws; failures are logged so they do not block boot.
	 *
	 * @param agentId the platform agent id (e.g.
	 *                {@link Constants#AGENT_APP_BUILDER})
	 */
	public static void seed(String agentId) {
		if (!SystemEngineRegistry.isModelInferenceLogsDbLoaded()) {
			classLogger.warn("ModelInferenceLogsDb not loaded; skipping WORKSPACE seed for system agent '{}'", agentId);
			return;
		}
		try {
			List<String> tools = toolIds(agentId);
			List<String> skills = skillIds(agentId);

			List<Map<String, String>> resources = new ArrayList<>();
			for (String toolId : tools) {
				resources.add(mcpResourceRow(agentId, toolId));
			}
			for (String skillId : skills) {
				resources.add(skillResourceRow(agentId, skillId));
			}

			Map<String, Object> existing = ModelInferenceLogsUtils.getWorkspaceEntry(agentId);
			if (existing == null) {
				try {
					ModelInferenceLogsUtils.createNewWorkspaceEntry(agentId, SYSTEM_OWNER, displayName(agentId),
							description(agentId), systemPrompt(agentId), resources);
					classLogger.info("Seeded system agent workspace '{}' with {} tool(s) and {} skill(s).", agentId,
							tools.size(), skills.size());
				} catch (Exception e) {
					if (ModelInferenceLogsUtils.getWorkspaceEntry(agentId) == null) {
						throw e;
					}
					classLogger.warn("WORKSPACE row for system agent '{}' was created concurrently; continuing.",
							agentId);
				}
			} else {
				for (Map<String, String> res : resources) {
					if (ModelInferenceLogsUtils.findWorkspaceResource(agentId, res.get("resource_id"),
							res.get("resource_type")) == null) {
						ModelInferenceLogsUtils.createNewWorkspaceResource(res.get("workspace_resource_id"), agentId,
								res.get("resource_id"), res.get("resource_type"), res.get("resource_subtype"));
					}
				}
			}

			ModelInferenceLogsUtils.updateWorkspaceConfigJson(agentId, buildConfigJson(agentId, tools, skills));
		} catch (Exception e) {
			classLogger.error("Failed to seed system agent workspace '{}'", agentId, e);
		}
	}

	/**
	 * Tools = the headless system MCP apps. Deliberately the agent subset rather
	 * than every cataloged platform MCP, so adding a UI-driven MCP to the catalog
	 * does not silently change this agent's toolset.
	 */
	private static List<String> toolIds(String agentId) {
		return new ArrayList<>(SystemDefaultEngines.getSystemAgentMCPs());
	}

	/** Skills = all platform skills. */
	private static List<String> skillIds(String agentId) {
		return new ArrayList<>(SystemDefaultEngines.getSystemSkills());
	}

	private static String displayName(String agentId) {
		if (Constants.AGENT_APP_BUILDER.equals(agentId)) {
			return "App Building Agent";
		}
		return agentId;
	}

	private static String description(String agentId) {
		if (Constants.AGENT_APP_BUILDER.equals(agentId)) {
			return "System agent for building SEMOSS apps.";
		}
		return "";
	}

	private static String systemPrompt(String agentId) {
		if (Constants.AGENT_APP_BUILDER.equals(agentId)) {
			return APP_BUILDER_SYSTEM_PROMPT;
		}
		return "";
	}

	/**
	 * Builds a WORKSPACE_RESOURCE row for an MCP tool project. Mirrors
	 * {@code AbstractWorkspaceReactor.makeProjectResourceEntryMap}: resource_type =
	 * PROJECT (a non-SKILL/non-PROMPT type, which AgentConfigLoader.resolveMcps
	 * treats as a tool), resource_subtype = the project's TYPE (e.g. "CODE").
	 */
	private static Map<String, String> mcpResourceRow(String agentId, String toolId) {
		Map<String, String> r = new HashMap<>();
		r.put("workspace_resource_id", UUID.randomUUID().toString());
		r.put("workspace_id", agentId);
		r.put("resource_id", toolId);
		r.put("resource_type", CATALOG_TYPE.PROJECT.name());
		r.put("resource_subtype", SecurityProjectUtils.getProjectTypeForId(toolId));
		return r;
	}

	/**
	 * Builds a WORKSPACE_RESOURCE row for a skill. Mirrors
	 * {@code AbstractWorkspaceReactor.makeSkillResourceEntryMap}: resource_type =
	 * SKILL, resource_subtype = null (no pinned version).
	 */
	private static Map<String, String> skillResourceRow(String agentId, String skillId) {
		Map<String, String> r = new HashMap<>();
		r.put("workspace_resource_id", UUID.randomUUID().toString());
		r.put("workspace_id", agentId);
		r.put("resource_id", skillId);
		r.put("resource_type", SKILL_RESOURCE_TYPE);
		r.put("resource_subtype", null);
		return r;
	}

	/**
	 * Builds the CONFIG_JSON payload consumed by
	 * {@code AgentConfigLoader.resolveMcps}/{@code resolveSkills}: top-level
	 * {@code system_prompt}, {@code mcps[]} of {@code {id,name}}, and
	 * {@code skills[]} of {@code {skill_id}}. Matches
	 * {@code AbstractWorkspaceReactor.mirrorCoreFieldsIntoConfigJson}.
	 */
	private static JSONObject buildConfigJson(String agentId, List<String> tools, List<String> skills) {
		JSONObject config = new JSONObject();
		config.put("schema_version", CONFIG_SCHEMA_VERSION);
		config.put("system_prompt", systemPrompt(agentId));

		JSONArray mcps = new JSONArray();
		for (String toolId : tools) {
			JSONObject mcp = new JSONObject();
			mcp.put("id", toolId);
			mcp.put("name", toolId);
			mcps.put(mcp);
		}
		config.put("mcps", mcps);

		JSONArray skillArr = new JSONArray();
		for (String skillId : skills) {
			JSONObject s = new JSONObject();
			s.put("skill_id", skillId);
			skillArr.put(s);
		}
		config.put("skills", skillArr);
		return config;
	}

	/**
	 * System prompt for the App Building agent. Kept as a text block so it reads as
	 * the prompt the model actually receives. The closing delimiter sits on the
	 * last content line so no trailing newline is appended.
	 */
	private static final String APP_BUILDER_SYSTEM_PROMPT = """
			You are a SEMOSS App Building agent.

			Start every task by calling ListSkills to see which skill packages are available. Load each relevant skill with LoadSkill before doing the work. Skills contain the canonical patterns for engines (model, database, vector, and storage), build/publish, and other recurring tasks. Do not guess parameters or output schemas.

			Project instructions are already included in your context. Treat them as authoritative for SDK usage and project conventions. Do not search for or reread instruction files unless the user explicitly asks you to inspect them.

			How to work:
			Plan with TodoWrite for anything that spans more than two tool calls. Mark items in_progress as you start, completed as you finish.
			Read before you edit. EditFile requires a unique-match old_string read surrounding context first.
			Prefer EditFile over WriteFile for in-place changes. Reserve WriteFile for new files or full rewrites.
			Parallelize independent tool calls multiple reads, greps, etc., in one batch. Serial chains waste latency.
			Use BuildAndPublishApp when client source must be compiled. Use PublishProject with release=true when the project already has complete runnable portal assets, such as a plain index.html app. Direct node / npm / pnpm via Bash are sandboxed and will fail.

			Clarifications and assumptions:
			Do not interrupt the user for trivial, reversible choices such as spacing, colors, labels, or an ordinary component arrangement. Make a reasonable choice and keep moving.
			Ask before making a choice that materially changes persistent data, the target model or engine, cost, security, permissions, authentication, external integrations, deployment, destructive behavior, or the user's requested product behavior.
			When a material choice is missing or ambiguous, do not silently select an option. If a structured RequestUserInput tool is available, use it and provide concise choices with a recommended option. Otherwise ask one concise plain-text question and stop. Continue only after the user answers.
			State any non-material assumption that affects the result in a short progress update or final summary.

			Engines and durable data:
			Before introducing any new MODEL / DATABASE / VECTOR / STORAGE call, load the selected-engines skill. Never hardcode or guess engine IDs.
			Use an engine without asking only when the user explicitly supplied its exact ID or exactly one compatible engine of that type is already selected for the project.
			If multiple compatible engines are selected, ask which one to use. If none is selected, ask the user to choose or attach one. Do not choose an engine merely because it is accessible, appears first in a list, exists in another project, or appears in sample code.
			The model running this agent is not automatically the model that should power the app. Never copy the harness model ID into app code unless the user explicitly selected that same model for the app.
			For a new durable backend, do not add tables to an arbitrary existing database. If no database is selected, ask whether to create a new dedicated database or reuse an existing one. Recommend a new dedicated database unless the user has stated that the app must integrate with existing data.
			For vector search, storage, authentication, and external services, follow the same rule: use an explicit project selection or ask before binding the app to a resource.

			Output:
			Finish with a one- to two-line summary of what changed and stop. Skip the recap.""";
}
