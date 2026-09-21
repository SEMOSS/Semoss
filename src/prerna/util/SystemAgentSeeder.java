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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 * {@code WORKSPACE} that is cataloged global with no owner (see
 * {@link ProjectWatcher#init()}) - exactly like the platform skills and system
 * MCPs. Unlike those, an agent also needs a {@code WORKSPACE} row plus
 * {@code WORKSPACE_RESOURCE} rows describing its tools and skills, which live
 * in a database rather than on disk. This class provisions those rows.
 *
 * <p>
 * {@link #seed(String)} is idempotent and self-healing in both directions: it
 * is safe to run on every boot. It creates the {@code WORKSPACE} row once,
 * back-fills any missing resource rows on subsequent boots, prunes resource
 * rows that are no longer in the seeded set, and always rewrites both
 * {@code CONFIG_JSON} (the runtime source read by {@code AgentConfigLoader})
 * and the legacy NAME/DESCRIPTION/SYSTEM_PROMPT columns (the display source
 * surfaced to the Agent UI by GetWorkspace/ListWorkspaces) so a drifted mirror
 * on either side is repaired. Hand-edits to a system agent's prompt are
 * therefore intentionally clobbered on the next boot. If the
 * ModelInferenceLogsDatabase feature is disabled it no-ops (the project itself
 * still catalogs).
 *
 * <p>
 * The App Building Agent's tools and skills are derived from
 * {@link SystemDefaultEngines} so they stay in sync with the platform lists:
 * <ul>
 * <li>tools = {@link SystemDefaultEngines#getSystemAgentMCPs(String)}</li>
 * <li>skills = {@link SystemDefaultEngines#getSystemAgentSkills(String)}</li>
 * </ul>
 * The PPTX Reviewer uses only built-in tools, with file mutations and further
 * delegation disabled. Its InspectPptx result ends the run directly. The PPTX
 * Agent uses the platform pptx skill and the managed BuildPptx workflow, with
 * the system PPTX Reviewer attached for visual inspection.
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

	/** Mirrors AgentHookRegistry.GIT_COMMIT without adding a util-to-reactor dependency. */
	private static final String GIT_COMMIT_HOOK_KIND = "git_commit";

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
				pruneStaleResources(agentId, tools, skills);
			}

			ModelInferenceLogsUtils.updateWorkspaceCoreFields(agentId, displayName(agentId), description(agentId),
					systemPrompt(agentId));
			ModelInferenceLogsUtils.updateWorkspaceConfigJson(agentId, buildConfigJson(agentId, tools, skills));
		} catch (Exception e) {
			classLogger.error("Failed to seed system agent workspace '{}'", agentId, e);
		}
	}

	/**
	 * Drop WORKSPACE_RESOURCE rows no longer in the desired set, so removing an
	 * entry from {@link SystemDefaultEngines} takes effect on an already-seeded
	 * install.
	 *
	 * <p>
	 * Only the two resource types this seeder creates are considered (PROJECT for
	 * tools, SKILL for skills), leaving PROMPT and any other type untouched. System
	 * agents are immutable, so every row of those types is seeder-owned.
	 *
	 * @param agentId the platform agent id whose workspace is being reconciled
	 * @param tools   the desired tool project ids
	 * @param skills  the desired skill project ids
	 */
	private static void pruneStaleResources(String agentId, List<String> tools, List<String> skills) {
		Map<String, Set<String>> desiredByType = new HashMap<>();
		desiredByType.put(CATALOG_TYPE.PROJECT.name(), new HashSet<>(tools));
		desiredByType.put(SKILL_RESOURCE_TYPE, new HashSet<>(skills));

		for (Map.Entry<String, Set<String>> entry : desiredByType.entrySet()) {
			String resourceType = entry.getKey();
			Set<String> desired = entry.getValue();
			try {
				List<Map<String, Object>> existing = ModelInferenceLogsUtils.getWorkspaceResourcesByType(agentId,
						Arrays.asList(resourceType));
				if (existing == null) {
					continue;
				}
				for (Map<String, Object> row : existing) {
					Object rawId = row.get("resource_id");
					if (rawId == null) {
						continue;
					}
					String resourceId = rawId.toString();
					if (desired.contains(resourceId)) {
						continue;
					}
					int removed = ModelInferenceLogsUtils.deleteWorkspaceResource(agentId, resourceId, resourceType);
					if (removed > 0) {
						classLogger.info(
								"Pruned stale {} resource '{}' from system agent workspace '{}'; it is no longer in the seeded set.",
								resourceType, resourceId, agentId);
					}
				}
			} catch (Exception e) {
				// never block boot over reconciliation
				classLogger.warn("Failed to prune stale {} resources for system agent '{}': {}", resourceType, agentId,
						e.getMessage());
			}
		}
	}

	/**
	 * The App Building Agent uses the headless system MCP apps. The PPTX agents
	 * need only built-in tools; installation-specific MCPs are not seeded.
	 */
	private static List<String> toolIds(String agentId) {
		return new ArrayList<>(SystemDefaultEngines.getSystemAgentMCPs(agentId));
	}

	/** Skills = the platform skills assigned to this system agent. */
	private static List<String> skillIds(String agentId) {
		return new ArrayList<>(SystemDefaultEngines.getSystemAgentSkills(agentId));
	}

	private static String displayName(String agentId) {
		if (Constants.AGENT_APP_BUILDER.equals(agentId)) {
			return "App Building Agent";
		}
		if (Constants.AGENT_WORKFLOW_AUTOMATION_BUILDER.equals(agentId)) {
			return "Automation Building Agent";
		}
		if (Constants.AGENT_PPTX.equals(agentId)) {
			return "PPTX Agent";
		}
		if (Constants.AGENT_PPTX_REVIEWER.equals(agentId)) {
			return "PPTX Reviewer";
		}
		return agentId;
	}

	private static String description(String agentId) {
		if (Constants.AGENT_APP_BUILDER.equals(agentId)) {
			return "System agent for building platform apps.";
		}
		if (Constants.AGENT_WORKFLOW_AUTOMATION_BUILDER.equals(agentId)) {
			return "System agent for authoring and troubleshooting Automation workflows.";
		}
		if (Constants.AGENT_PPTX.equals(agentId)) {
			return "System agent for creating and editing PowerPoint presentations with validation and visual review.";
		}
		if (Constants.AGENT_PPTX_REVIEWER.equals(agentId)) {
			return "System agent for visually reviewing saved PowerPoint presentations.";
		}
		return "";
	}

	private static String systemPrompt(String agentId) {
		if (Constants.AGENT_APP_BUILDER.equals(agentId)) {
			return APP_BUILDER_SYSTEM_PROMPT;
		}
		if (Constants.AGENT_WORKFLOW_AUTOMATION_BUILDER.equals(agentId)) {
			return WORKFLOW_AUTOMATION_BUILDER_SYSTEM_PROMPT;
		}
		if (Constants.AGENT_PPTX.equals(agentId)) {
			return PPTX_SYSTEM_PROMPT;
		}
		if (Constants.AGENT_PPTX_REVIEWER.equals(agentId)) {
			return PPTX_REVIEWER_SYSTEM_PROMPT;
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

		// Preserve every file-changing App Building Agent run as a local project
		// commit. The hook skips the commit when the run leaves the tree unchanged.
		if (Constants.AGENT_APP_BUILDER.equals(agentId)) {
			JSONArray hooks = new JSONArray();
			hooks.put(new JSONObject().put("kind", GIT_COMMIT_HOOK_KIND));
			config.put("hooks", hooks);
		}

		if (Constants.AGENT_PPTX.equals(agentId)) {
			config.put("use_default_agent_tools", true);
			config.put("greeting_enabled", false);
			config.put("hooks", new JSONArray());
			config.put("subagents",
					new JSONArray().put(new JSONObject().put("workspaceId", Constants.AGENT_PPTX_REVIEWER)));
			config.put("budgets", new JSONObject().put("finishing_turns", 6));
			config.put("spawn_policy", new JSONObject().put("max_subagents_per_run", 2).put("max_spawns_per_turn", 1));
			config.put("tool_policy",
					new JSONObject()
							.put("default_tools",
									new JSONObject().put("disabled",
											new JSONArray(List.of("InspectPptx", "ExecuteNodeCode"))))
							.put("read_only_paths",
									new JSONArray(List.of(".claude/skills/pptx", ".semoss/pptx-workflow"))));
			config.put("pptx_workflow",
					new JSONObject().put("enabled", true).put("reviewer_alias", "agent_pptx_reviewer")
							.put("repair_turns", 6).put("review_timeout_seconds", 600));
		}

		if (Constants.AGENT_PPTX_REVIEWER.equals(agentId)) {
			config.put("use_default_agent_tools", true);
			config.put("greeting_enabled", false);
			config.put("subagents", new JSONArray());
			config.put("hooks", new JSONArray());
			config.put("budgets", new JSONObject().put("max_turns", 3).put("max_reflections", 0).put("max_seconds", 900)
					.put("finishing_turns", 1));
			config.put("spawn_policy", new JSONObject().put("max_subagent_depth", 0).put("max_subagents_per_run", 0)
					.put("max_spawns_per_turn", 0));
			config.put("tool_policy",
					new JSONObject()
							.put("default_tools",
									new JSONObject().put("disabled",
											new JSONArray(List.of("WriteFile", "EditFile", "MultiEdit", "MoveFile",
													"DeleteFile", "BashCommand", "ExecuteNodeCode", "TodoWrite"))))
							.put("result_tool", "InspectPptx"));
			// Model ids are deployment-specific. Use the selected/inherited agent model
			// and an explicit InspectPptx.engine or the deployment's PPTX_VISION_MODEL_ID.
		}
		return config;
	}

	/**
	 * Keep docs/agents/pptx-author-workflow-prompt.txt in sync with this prompt.
	 */
	private static final String PPTX_SYSTEM_PROMPT = """
			You are the PPTX authoring agent. Create or edit the requested PowerPoint in the SEMOSS working directory. Preserve the user's content, filename, requested slide count, template, branding and visual direction. Make purposeful, editable slides with audience-appropriate language.

			Load the pptx skill with LoadSkill. For a new deck, read one relevant example and pptx/references/generation.md. Use pptx/references/components.md for component options. For an existing deck, read pptx/references/editing.md and preserve its design and unrelated content. Read only references needed for the task; continue at the supplied offset if a read is truncated.

			For an existing presentation, FIRST call PreparePptxEdit alone with its exact current filename and only the requested original slide numbers. Use editType="text" for wording changes; it preserves formatting and objects even on selected slides. Read the editing reference, then use the returned protected inputSnapshot, inspected text indexes and scripts/edit.js with JSZip. Do not reconstruct existing slides with PptxGenJS or rerun an old creation generator. Change only what the user requested. Use editType="slides" and exact additionalParts only for requested layout/object/chart/media changes. The scope cannot be broadened during repairs.

			Save the complete authoring or editing program as build-deck.js using WriteFile. It must be one (async () => { ... })() with all declarations inside it and all asynchronous work awaited. ROOT is the working directory; save the exact requested filename with path.join(ROOT, filename). For NEW decks use the curated pptxgenjs package and packaged deck helper, as shown in the creation examples. Existing-deck programs edit the protected original package with JSZip; the creation API cannot import it. Native objects and components may be freely combined; the examples do not impose a fixed layout or slide count. Replace example content and imagery to suit the request; never invent data for a chart.

			Call BuildPptx alone with generator="build-deck.js", the exact filePath, the requested expectedSlides, and instructions describing the review criteria and design constraints. Include engine only when the caller supplied a vision model ID, preserving that exact ID. SEMOSS executes the saved program, independently validates the output, enforces the prepared edit scope against the original package, and invokes the system PPTX Reviewer automatically. A rejected edit restores the previous validated file or original. For existing decks review only the requested edits; pre-existing warnings do not authorize unrelated redesign. No human approval is needed between these stages.

			If BuildPptx returns repair_required, address its structural error or significant visual findings in one batch of generator edits. Then call BuildPptx with the same generator, filename and slide count before the stated repair budget expires. Prefer MultiEdit for several known changes. Read the affected lines after an exact-text edit fails. Prioritize saving and checking the repair over optional refinement. Advisory structural warnings are passed to the reviewer automatically. Provider failures and incomplete reviews end with an accurate disclosure; they do not authorize redesign or repeated reviewer calls.

			Packaged files under .claude/skills/pptx are read-only. Use documented component options or native editable objects for layout fixes. Top-level x,y,w,h and nested geometry:{x,y,w,h} are supported for components. Do not run local rendering commands, install packages, or modify the helper implementation.

			BuildPptx owns the review cycle and final delivery. ExecuteNodeCode and manual reviewer delegation are unavailable in this managed workflow. A source-code edit is not part of the delivered presentation until BuildPptx saves and checks it. SEMOSS reports the actual saved file, check coverage, and any unresolved findings.""";

	/**
	 * Keep the documented reviewer prompt in docs/agents/pptx-reviewer-prompt.txt
	 * in sync.
	 */
	private static final String PPTX_REVIEWER_SYSTEM_PROMPT = """
			You are the PPTX Reviewer. Inspect the saved PowerPoint against the caller's review brief. The author owns presentation edits.

			Read the task for filePath, optional original 1-based slides, review instructions, context, and optional vision engine ID. Call InspectPptx once. Omit slides for the whole deck. When the caller supplies engine or explicitly identifies a vision model ID, pass that exact ID as InspectPptx.engine. Any accessible image-capable SEMOSS text-generation model may be selected; do not replace the requested ID with your agent model or a preferred provider. When no ID is supplied, omit engine and use the configured tool/deployment default. Your agent model operates tools; the engine argument controls the separate image requests.

			InspectPptx renders through UnoServer, sends actual images to the selected engine, validates its output, and retries individual transient/format failures within a small tool-owned budget. Call it alone. Its configured result-tool behavior returns the report directly, ending this reviewer run without a summarization or retry loop.

			Preserve the requested scope and slide IDs. Review visible layout, clipping, overlap, text/chart-label readability and consistency. Distinguish major usability defects from advisory style preferences. Do not invent facts or ask the author to rebuild the deck because of a model/provider failure. Never edit files, run authoring commands, or delegate further.

			Treat report status, verdict, coverage, sourceHash/sourceChanged, issues, limitations, errors, engine and artifacts as authoritative. Partial or inconclusive output is not a pass. A targeted review does not establish whole-deck coverage. Return setup/input errors without silently switching models or retrying identical requests. Images are seen by InspectPptx's vision engine; you receive its structured text report.""";

	/**
	 * System prompt for the App Building agent. Kept as a text block so it reads as
	 * the prompt the model actually receives. The closing delimiter sits on the
	 * last content line so no trailing newline is appended.
	 */
	private static final String APP_BUILDER_SYSTEM_PROMPT = """
			You are an App Building agent for this platform.

			The available_skills block in this prompt lists every skill you can load. Before starting work that one of them covers, call LoadSkill(skill_name="<name>") and follow it. Skills contain the canonical patterns for engines (model, database, vector, and storage), build/publish, plain index.html apps, and other recurring tasks. Do not guess parameters or output schemas, and do not work from memory when a skill covers the task. ListSkill rescans the working directory, which you only need after a skill is created or attached mid-run.

			Project instructions are already included in your context. Treat them as authoritative for SDK usage and project conventions. Do not search for or reread instruction files unless the user explicitly asks you to inspect them.

			How to work:
			Plan with TodoWrite for anything that spans more than two tool calls. Mark items in_progress as you start, completed as you finish.
			Read before you edit. EditFile requires a unique-match old_string read surrounding context first.
			Prefer EditFile over WriteFile for in-place changes. Reserve WriteFile for new files or full rewrites.
			Parallelize independent tool calls multiple reads, greps, etc., in one batch. Serial chains waste latency.
			Use BuildAndPublishApp when client source must be compiled. Use PublishProject with release=true when the project already has complete runnable portal assets, such as a plain index.html app. Direct node / npm / pnpm via Bash are sandboxed and will fail.

			Load the app-bootstrap skill before writing any app code, including a single-file index.html. It carries the Insight lifecycle, how the SDK import resolves in a no-build app, and the tags publishing injects. Getting these wrong produces an app that loads to a blank screen, which is not something you can tell from reading your own output.

			Clarifications and assumptions:
			Do not interrupt the user for trivial, reversible choices such as spacing, colors, labels, or an ordinary component arrangement. Make a reasonable choice and keep moving.
			Ask before making a choice that materially changes persistent data, the target model or engine, cost, security, permissions, authentication, external integrations, deployment, destructive behavior, or the user's requested product behavior.
			When a material choice is missing or ambiguous, do not silently select an option. If a structured RequestUserInput tool is available, use it and provide concise choices with a recommended option. Otherwise ask one concise plain-text question and stop. Continue only after the user answers.
			State any non-material assumption that affects the result in a short progress update or final summary.

			Engines and durable data:
			Your system prompt contains a "Selected Engines" section listing the engines the user selected for this project. Before introducing any new MODEL / DATABASE / VECTOR / STORAGE call, pick the engine from that section. Never hardcode or guess engine IDs.
			Use an engine without asking only when the user explicitly supplied its exact ID or exactly one compatible engine of that type is already selected for the project.
			If multiple compatible engines are selected, ask which one to use. If none is selected, ask the user to choose or attach one. Do not choose an engine merely because it is accessible, appears first in a list, exists in another project, or appears in sample code.
			The model running this agent is not automatically the model that should power the app. Never copy the harness model ID into app code unless the user explicitly selected that same model for the app.
			For a new durable backend, do not add tables to an arbitrary existing database. If no database is selected, ask whether to create a new dedicated database or reuse an existing one. Recommend a new dedicated database unless the user has stated that the app must integrate with existing data.
			For vector search, storage, authentication, and external services, follow the same rule: use an explicit project selection or ask before binding the app to a resource.

			Output:
			Finish with a one- to two-line summary of what changed and stop. Skip the recap.""";

	/** System prompt for the Automation Building agent. */
	private static final String WORKFLOW_AUTOMATION_BUILDER_SYSTEM_PROMPT = """
			You are the Automation Building agent for this platform.

			Before authoring, changing, or troubleshooting a workflow, call LoadSkill(skill_name="workflow-automation") and follow it. The skill defines the canonical authoring sequence, graph semantics, runtime variable rules, and safe node-selection guidance.

			The active automation project is supplied at run time. Its project MCP is the only authority for reading, changing, or running that automation. Never edit automation-workflow.json or automation-nodes directly with file tools, and never claim an operation succeeded unless the corresponding automation tool confirms it.

			Ask only for required values that cannot be discovered from the available catalog tools. Do not invent engine, app, reactor, function, workspace, node, or output-variable identifiers. Prefer supported generated nodes; use developer.python only for custom computation or an integration that no supported node provides.

			Finish with a concise summary of the confirmed result and whether the automation was run.""";
}
