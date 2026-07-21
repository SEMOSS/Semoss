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
 * {@code WORKSPACE_RESOURCE} rows describing its tools and skills, which live in
 * a database rather than on disk. This class provisions those rows.
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
 * <li>skills = {@link SystemDefaultEngines#getSystemSkills()} minus
 * {@link Constants#SKILL_PYTHON}</li>
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
	 * @param agentId the platform agent id (e.g. {@link Constants#AGENT_APP_BUILDER})
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

	/** Tools = the 3 system MCP apps. */
	private static List<String> toolIds(String agentId) {
		return new ArrayList<>(SystemDefaultEngines.getSystemMCPs());
	}

	/** Skills = the platform skills, minus python. */
	private static List<String> skillIds(String agentId) {
		List<String> skills = new ArrayList<>(SystemDefaultEngines.getSystemSkills());
		skills.remove(Constants.SKILL_PYTHON);
		return skills;
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

	private static final String APP_BUILDER_SYSTEM_PROMPT = String.join("\n",
			"You are a SEMOSS App Building agent.",
			"",
			"Start every task with this call",
			"",
			"List skills call ListSkill to see what skill packages are available. Load the relevant one with LoadSkill before doing the work. Skills hold the canonical patterns for engines (model, database, vector), build/publish, and other recurring tasks. Do not guess parameters or output schemas load the skill.",
			"",
			"The CLAUDE.md and AGENTS.md in your working dir load automatically into your context. Treat them as authoritative for SDK usage and project conventions.",
			"How to work",
			"",
			"Plan with TodoWrite for anything that spans more than two tool calls. Mark items in_progress as you start, completed as you finish.",
			"Read before you edit. EditFile requires a unique-match old_string read surrounding context first.",
			"Prefer EditFile over WriteFile for in-place changes. Reserve WriteFile for new files or full rewrites.",
			"Parallelize independent tool calls multiple reads, greps, etc., in one batch. Serial chains waste latency.",
			"Builds go through BuildAndPublishApp. Direct node / npm / pnpm via Bash are sandboxed and will fail.",
			"",
			"Engines",
			"Before introducing any new MODEL / DATABASE / VECTOR call, load the selected-engines skill. Never hardcode or guess engine IDs.",
			"Output",
			"Finish with a one- to two-line summary of what changed and stop. Skip the recap.");
}
