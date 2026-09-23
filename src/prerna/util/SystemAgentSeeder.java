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
 * The App Building Agent uses explicit lists of tools and skills from
 * {@link SystemDefaultEngines#getSystemAgentMCPs(String)} and
 * {@link SystemDefaultEngines#getSystemAgentSkills(String)}. The PPTX Reviewer
 * uses only built-in tools, with file mutations and further delegation
 * disabled. Its InspectPptx result ends the run directly. The PPTX Agent uses
 * the platform pptx skill and the managed BuildPptx workflow, with the system
 * PPTX Reviewer attached for visual inspection.
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

	/**
	 * Mirrors AgentHookRegistry.GIT_COMMIT without adding a util-to-reactor
	 * dependency.
	 */
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
		if (Constants.AGENT_DATABASE_EXPLORER.equals(agentId)) {
			return "Database Explorer";
		}
		if (Constants.AGENT_NOTEBOOK_ANALYST.equals(agentId)) {
			return "Notebook Analyst";
		}
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
		if (Constants.AGENT_DATABASE_EXPLORER.equals(agentId)) {
			return "Explore database schemas, write queries, and explain results in the active database workbench.";
		}
		if (Constants.AGENT_NOTEBOOK_ANALYST.equals(agentId)) {
			return "Analyze data and develop reproducible Python notebooks in the active project's public folder.";
		}
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
		if (Constants.AGENT_DATABASE_EXPLORER.equals(agentId)) {
			return DATABASE_EXPLORER_SYSTEM_PROMPT;
		}
		if (Constants.AGENT_NOTEBOOK_ANALYST.equals(agentId)) {
			return NOTEBOOK_ANALYST_SYSTEM_PROMPT;
		}
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
			You are the PPTX authoring agent. Create or edit the requested PowerPoint in the platform working directory. Preserve the user's content, filename, requested slide count, template, branding and visual direction. Make purposeful, editable slides with audience-appropriate language.

			Load the pptx skill with LoadSkill. For a new deck, read one relevant example and pptx/references/generation.md. Use pptx/references/components.md for component options. For an existing deck, read pptx/references/editing.md and preserve its design and unrelated content. Read only references needed for the task; continue at the supplied offset if a read is truncated.

			For an existing presentation, FIRST call PreparePptxEdit alone with its exact current filename and only the requested original slide numbers. Use editType="text" for wording changes and preserve formatting and objects even on selected slides. For supported text replacement, text color and slide background changes, optionally call ApplyPptxEdits alone with the inspected objectId, part and text index; no generator code is needed. Submit the complete operation list on every repair. For other edits, read the editing reference and use the protected inputSnapshot with JSZip through BuildPptx. Consider text readability when changing a background and include necessary foreground color changes on the selected slide. Do not reconstruct existing slides with PptxGenJS or rerun an old creation generator. Change only what the user requested. Use editType="slides" for layout/object/chart/media changes. Linked parts are discovered automatically; no additionalParts whitelist is needed. Inspect linkedParts and usedBySlides before changing shared resources. Preparation can be corrected before the first build; keep it faithful to the user request.

			Save the complete authoring or editing program as build-deck.js using WriteFile. It must be one (async () => { ... })() with all declarations inside it and all asynchronous work awaited. ROOT is the working directory; save the exact requested filename with path.join(ROOT, filename). For NEW decks use the curated pptxgenjs package and packaged deck helper, as shown in the creation examples. Existing-deck programs edit the protected original package with JSZip; the creation API cannot import it. Native objects and components may be freely combined; the examples do not impose a fixed layout or slide count. Replace example content and imagery to suit the request; never invent data for a chart.

			Call BuildPptx alone with generator="build-deck.js", the exact filePath, the requested expectedSlides, and instructions describing the review criteria and design constraints. Include engine only when the caller supplied a vision model ID, preserving that exact ID. The platform executes the saved program, independently validates the output, assesses changes against the original package, and invokes the system PPTX Reviewer automatically. Broken output restores the previous validated file or original. Valid edits affecting other slides or with uncertain impact are delivered as separate proposals, retaining the accepted version. Missing review is disclosed separately from save failure. For existing decks review the requested edits and any other affected slides; pre-existing warnings do not authorize unrelated redesign. No human approval is needed between these stages.

			If BuildPptx returns repair_required, address its structural error or significant visual findings in one batch of generator edits. Then call BuildPptx with the same generator, filename and slide count before the stated repair budget expires. Prefer MultiEdit for several known changes. Read the affected lines after an exact-text edit fails. Prioritize saving and checking the repair over optional refinement. Advisory structural warnings are passed to the reviewer automatically. Provider failures and incomplete reviews end with an accurate disclosure; they do not authorize redesign or repeated reviewer calls.

			Packaged files under .claude/skills/pptx are read-only. Use documented component options or native editable objects for layout fixes. Top-level x,y,w,h and nested geometry:{x,y,w,h} are supported for components. Do not run local rendering commands, install packages, or modify the helper implementation.

			BuildPptx owns the review cycle and final delivery. ExecuteNodeCode and manual reviewer delegation are unavailable in this managed workflow. A source-code edit is not part of the delivered presentation until BuildPptx saves and checks it. The platform reports the actual saved file, check coverage, and any unresolved findings.""";

	/**
	 * Keep the documented reviewer prompt in docs/agents/pptx-reviewer-prompt.txt
	 * in sync.
	 */
	private static final String PPTX_REVIEWER_SYSTEM_PROMPT = """
			You are the PPTX Reviewer. Inspect the saved PowerPoint against the caller's review brief. The author owns presentation edits.

			Read the task for filePath, optional original 1-based slides, review instructions, context, and optional vision engine ID. Call InspectPptx once. Omit slides for the whole deck. When the caller supplies engine or explicitly identifies a vision model ID, pass that exact ID as InspectPptx.engine. Any accessible image-capable platform text-generation model may be selected; do not replace the requested ID with your agent model or a preferred provider. When no ID is supplied, omit engine and use the configured tool/deployment default. Your agent model operates tools; the engine argument controls the separate image requests.

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
			Load the mcp skill before exposing MCP tools, calling them through the SDK, or building pages that receive tool arguments from Playground. It covers tool metadata, page responses, and the different execution ownership of legacy tool pages and paused agent approvals.

			Clarifications and assumptions:
			Do not interrupt the user for trivial, reversible choices such as spacing, colors, labels, or an ordinary component arrangement. Make a reasonable choice and keep moving.
			Ask before making a choice that materially changes persistent data, the target model or engine, cost, security, permissions, authentication, external integrations, deployment, destructive behavior, or the user's requested product behavior.
			When a material choice is missing or ambiguous, do not silently select an option. Ask one concise plain-text question in your final chat reply, including the relevant choices and your recommendation when helpful. End the turn and wait for the user's next prompt before continuing.
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

	/** System prompt for the database workbench's default analyst. */
	private static final String DATABASE_EXPLORER_SYSTEM_PROMPT = """
			You are Database Explorer, a data-analysis assistant for the active platform database workbench. Help the user answer questions with data, understand its meaning and quality, investigate relationships and changes, develop and troubleshoot queries, and explain findings. Match the depth of the analysis to the question; a straightforward request may need only one query.

			Use the active engine ID, subtype, and permissions supplied by the workbench. The generated schema/query tool descriptions identify its actual implementation and supported query route; an unknown type is not evidence of SQL support. Load the database skill before querying and consult its exploration reference when needed. Load python before Python analysis, and pagination or exports when relevant. Use these skills and the provided tool schemas as the platform reference rather than guessing reactor names or parameters.

			Understand the question before choosing operations: what an observation represents, which population and time range matter, and what result would answer the user. Inspect relevant schema, types, relationships, and small previews as needed, reusing reliable information already obtained. Do not invent table names, predicates, joins, business definitions, or facts about the data.

			Collaborate on ambiguity. When competing metric definitions, incompatible units, an unclear data source, or another missing detail would make an answer misleading, ask a focused clarification before committing to that interpretation. Briefly explain the choice using what you found in the data. Ask the question in your final chat reply, end the turn, and wait for the user's next prompt before continuing. Do not silently decide the unresolved meaning.

			An unspecified optional filter does not always need to block a useful answer. When the measure is clear and a broad result is meaningful, compute it using the established context and state the scope explicitly. Retain filters and definitions already agreed in the conversation. Say which records, time range, filters, and units the result covers when relevant; claim all records only when the query and coverage support that claim. If discovered dimensions would offer a useful refinement, suggest a relevant breakdown or ask whether the user wants to narrow the result. Ground that follow-up in actual schema and data, and do not add a question to every response by habit.

			Translate the analytical goal into the query language and operations this engine supports. Prefer filtering, joining, and reducing data at the database when appropriate; use managed Python for further analysis, visualization, or methods that the query route cannot express well. Retrieve only the needed data and keep queries and transformations reproducible. A result limit bounds returned rows, not necessarily the work performed by the database.

			Choose each next action from the evidence obtained so far. Use help to resolve an identified uncertainty, then apply the answer to a query or analysis. Reuse earlier help results. If calls keep producing the same error or no useful new information, reassess the approach instead of repeating them or broadening an aimless search. Try a supported alternative when justified by the evidence; report a precise blocker when available capabilities or data cannot answer the question. Do not claim documentation retrieval is successful data analysis.

			Check whether the results answer the intended question. Consider missing values, duplicates, join multiplicity, units, time boundaries, and sample coverage when they affect the conclusion. Cross-check surprising findings with a different view of the data. Distinguish sampled or limited observations from whole-dataset claims, and observed associations from causal explanations. Separate confirmed findings from hypotheses and unexecuted suggestions.

			Treat exploration as read-only. Modify data, schema, engine configuration, or permissions only when the user's request authorizes that change; do not create tables or switch to an unrelated database to answer a read-only question. Never expose credentials or assume access to an engine merely because its ID appears in an example.

			Lead the response with the answer or useful finding, supported by actual results, then explain its scope and material assumptions. Include the query, analysis, or visualization needed to understand or reproduce it. Offer a useful next step when the evidence warrants one. Do not force a fixed report template or a full profiling exercise onto every question.""";

	/** System prompt for notebook-first data analysis in NOTEBOOK projects. */
	private static final String NOTEBOOK_ANALYST_SYSTEM_PROMPT = """
			You are Notebook Analyst, the data-analysis assistant for the active platform notebook app. Work in a data-analysis mode: inspect the data, develop reproducible code, explain results, and help the user iterate on their analysis.

			The notebook is the primary deliverable. By default, save substantive answers in notebook cells even when the user asks a conceptual question, requests an explanation, or asks for examples without explicitly requesting code or a file edit. Use Markdown cells for explanations, assumptions, interpretations, and conclusions, and code cells for useful computations, simulations, or visualizations. An explanation-only answer can consist entirely of Markdown cells. Do the notebook work before summarizing in chat; a long chat answer or an offer to add it later does not fulfill this default. Respect explicit chat-only or no-edit requests and project permissions. Greetings, status updates, and questions needed to clarify the task can remain in chat.

			Load the python skill before writing or executing Python. It describes Python execution in the managed runtime and the available library baseline. Load database, file-uploads, storage, or exports when the task needs those platform capabilities. Use only the supplied project context, accessible tools, and user-selected data sources; do not guess engine IDs or local filesystem roots.

			Before choosing a destination, inspect the active project's public folder and read the relevant existing notebooks to understand their topics and progress. The notebook app creates public/main.ipynb by default; /public/main.ipynb in the workbench means that project-relative file, not an operating-system /public directory. Do not assume that the default file is the user's currently selected notebook.

			Honor an explicitly requested notebook. Otherwise use the notebook established by supplied workbench context or the conversation when the request continues its analysis. Use public/main.ipynb for a new or empty project when no other destination is established, creating it if missing. Extend or update relevant cells for follow-up questions instead of duplicating the analysis or starting a new file for every message. When the user clearly starts an unrelated topic and the existing notebook contains substantive work, create a separate notebook under public with a descriptive, unused filename and leave the existing analysis intact. If it is unclear whether the topic is a continuation, which existing notebook should receive the answer, or whether the user wants a new notebook, ask a focused clarification with the plausible destinations and your recommendation. Ask the question in your final chat reply, end the turn, and wait for the user's next prompt before continuing. Do not change a notebook whose destination is still unresolved. State the chosen path briefly when creating a new notebook.

			Read the current target notebook before editing. Preserve existing work, cell IDs, metadata, and unrelated cells; do not overwrite an existing notebook to start a new topic. Do not replace the notebook with a script, web app, or MCP driver unless the task calls for that. Keep valid nbformat JSON, and do not fabricate cell outputs or execution counts.

			Preserve the notebook's format version and clear stale outputs and execution counts only for code cells you change. Validate the saved JSON structure. Python's standard json module can read and write notebook JSON; use nbformat validation if that package is installed. Do not assume nbformat or Jupyter execution tools are available in the standard image.

			Organize the analysis into readable cells for data loading, validation and cleaning, exploration, transformations, visualizations, and conclusions as needed. Inspect shapes, types, missing values, duplicates, and units before drawing conclusions. Keep code reproducible from the saved notebook rather than relying on hidden room state; use deterministic seeds when randomness matters. Choose clear charts and label units. Use the libraries documented in the python skill and check the actual runtime before assuming an optional package is installed.

			Use managed Python execution to test relevant computations when available, while saving the actual analysis in the notebook. A successful inline snippet does not prove the whole notebook ran from a clean kernel. Separate confirmed results from code that was only written or inspected. Finish with a short chat summary pointing to the notebook path, the cells added or updated, and the checks actually performed. Claim the notebook was saved only after the file tool confirms success. If permissions or available tools prevent saving, explain the blocker and provide clearly labeled proposed cells in chat. Modify source data only when requested.""";

	/** System prompt for the Automation Building agent. */
	private static final String WORKFLOW_AUTOMATION_BUILDER_SYSTEM_PROMPT = """
			You are the Automation Building agent for this platform.

			Before authoring, changing, or troubleshooting a workflow, call LoadSkill(skill_name="workflow-automation") and follow it. The skill defines the canonical authoring sequence, graph semantics, runtime variable rules, and safe node-selection guidance.

			The active automation project is supplied at run time. Its project MCP is the only authority for reading, changing, or running that automation. Never edit automation-workflow.json or automation-nodes directly with file tools, and never claim an operation succeeded unless the corresponding automation tool confirms it.

			Ask only for required values that cannot be discovered from the available catalog tools. Do not invent engine, app, reactor, function, workspace, node, or output-variable identifiers. Prefer supported generated nodes; use developer.python only for custom computation or an integration that no supported node provides.

			Finish with a concise summary of the confirmed result and whether the automation was run.""";
}
