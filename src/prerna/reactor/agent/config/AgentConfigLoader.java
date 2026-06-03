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
package prerna.reactor.agent.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.inferencetracking.reactors.workspaces.AbstractWorkspaceReactor;
import prerna.reactor.agent.IAgentHook;
import prerna.reactor.agent.IAgentRunHook;
import prerna.reactor.agent.IToolHook;
import prerna.reactor.agent.hooks.AgentHookRegistry;
import prerna.reactor.agent.runtime.AgentsMdLoader;

/**
 * Builds the resolved {@link AgentConfig} for one run.
 *
 * <p>This is the single place where workspace, room, config-json, prompt,
 * MCP, skill, budget, hook, and subagent resolution are composed.
 */
public final class AgentConfigLoader {

    private static final Logger logger = LogManager.getLogger(AgentConfigLoader.class);

    /** paramMap key for the optional wall-clock run-time limit in seconds (0 = no limit). */
    public static final String PARAM_MAX_SECONDS = "max_seconds";

    private AgentConfigLoader() {}

    /**
     * Resolve and freeze the {@link AgentConfig} for this run.
     *
     * @param room                 loaded room (required)
     * @param workingDir           optional working directory; also used to walk for {@code AGENTS.md}
     * @param modelId              resolved model engine id (or {@code null})
     * @param paramMap             extra model parameters; also scanned for budget overrides
     *                             like {@code max_seconds}. May be {@code null}.
     * @param maxTurns             turn cap for this run (must be {@code > 0})
     * @param maxReflections       reflection cap for this run (must be {@code >= 0})
     * @param explicitWorkspaceId  optional override - wins over {@code room.options.workspace.workspace_id}.
     *                             {@code null} = fall back to room option.
     * @return non-null {@link AgentConfig}
     */
    public static AgentConfig load(
            Room room,
            String workingDir,
            String modelId,
            Map<String, Object> paramMap,
            Map<String, Object> agentParams,
            int maxTurns,
            int maxReflections,
            String explicitWorkspaceId) {

        if (room == null) {
            throw new IllegalArgumentException("room is required");
        }

        AgentConfig.Builder b = AgentConfig.builder();

        // Resolve the workspace id: explicit param first, then room options.
        String workspaceId = explicitWorkspaceId != null
                ? explicitWorkspaceId
                : extractWorkspaceId(room);
        if (explicitWorkspaceId != null) {
            logger.info("AgentConfigLoader: workspace_id override applied (explicit='{}' wins over room.options)",
                    explicitWorkspaceId);
        }

        // Workspace lookup is best-effort because ad-hoc rooms may have no workspace.
        Map<String, Object> workspaceRow = null;
        if (workspaceId != null) {
            b.workspaceId(workspaceId);
            try {
                workspaceRow = ModelInferenceLogsUtils.getWorkspaceEntry(workspaceId);
                if (workspaceRow != null) {
                    Object nameVal = workspaceRow.get("name");
                    Object descVal = workspaceRow.get("description");
                    if (nameVal != null) b.name(String.valueOf(nameVal));
                    if (descVal != null) b.description(String.valueOf(descVal));
                }
            } catch (Exception e) {
                logger.warn("AgentConfigLoader: workspace metadata lookup failed for id={}: {}",
                        workspaceId, e.getMessage());
            }
        }

        if (workspaceId != null && workspaceRow != null) {
            User user = room.getInsight() != null ? room.getInsight().getUser() : null;
            if (user != null && !SecurityProjectUtils.userCanViewProject(user, workspaceId)) {
                throw new IllegalArgumentException(
                        "Workspace " + workspaceId + " does not exist or user does not have access to the workspace");
            }
            Object isActive = workspaceRow.get("is_active");
            if (Boolean.FALSE.equals(isActive)) {
                throw new IllegalArgumentException("Workspace is disabled by the owner");
            }
        }

        // Per-workspace config blob; null means fall back to legacy sources.
        JSONObject cfgJson = loadWorkspaceConfigJson(workspaceId);

        // Room instructions win, then CONFIG_JSON.system_prompt, then workspace.system_prompt.
        b.authoredPrompt(resolveAuthoredPrompt(room, workspaceId, workspaceRow, cfgJson));

        // Workdir AGENTS.md / CLAUDE.md exact-dir lookup only (no walk-up). Loader
        // tolerates null workingDir.
        b.workdirAgentsMd(AgentsMdLoader.discover(workingDir));

        // 5. Model
        b.modelId(StringUtils.trimToNull(modelId));
        b.modelParams(paramMap);
        b.agentParams(agentParams);

        // 6. Working directory
        b.workingDir(StringUtils.trimToNull(workingDir));

        // CONFIG_JSON budgets override per field; remaining values come from caller args.
        b.budgets(resolveBudgets(cfgJson, paramMap, maxTurns, maxReflections));
        b.spawnPolicy(resolveSpawnPolicy(cfgJson));

        // MCP refs come from workspace resources, CONFIG_JSON, and room options.
        b.mcps(resolveMcps(workspaceId, room, cfgJson));

        // Skills follow the same layered merge and are staged later by AgentRunner.
        b.skills(resolveSkills(workspaceId, room, cfgJson));

        // Hooks and subagents currently come from CONFIG_JSON only. resolveHooks classifies
        // each entry by interface so a hook can land on the run-hook list, tool-hook list,
        // or both.
        ResolvedHooks rh = resolveHooks(cfgJson);
        b.runHooks(rh.runHooks);
        b.toolHooks(rh.toolHooks);
        b.subagents(resolveSubagents(cfgJson));

        AgentConfig cfg = b.build();
        logger.info(
                "AgentConfigLoader: resolved room={} workspaceId={} name={} modelId={} workingDir={} mcps={} skills={} runHooks={} toolHooks={} subagents={} budgets(turns={},refl={},secs={}) authoredChars={} workdirAgentsMdChars={} cfgJson={}",
                room.getId(), cfg.getWorkspaceId(), cfg.getName(), cfg.getModelId(), cfg.getWorkingDir(),
                cfg.getMcps().size(), cfg.getSkills().size(), cfg.getRunHooks().size(), cfg.getToolHooks().size(), cfg.getSubagents().size(),
                cfg.getBudgets().getMaxTurns(), cfg.getBudgets().getMaxReflections(), cfg.getBudgets().getMaxSeconds(),
                lengthOrZero(cfg.getAuthoredPrompt()), lengthOrZero(cfg.getWorkdirAgentsMd()),
                cfgJson == null ? "absent" : "present");
        return cfg;
    }

    /**
     * Best-effort load of {@code WORKSPACE.CONFIG_JSON}. Returns {@code null} when
     * the workspace has no row, the column is empty/unparseable, or the lookup
     * fails - caller resolvers must tolerate null and fall back to legacy paths.
     */
    private static JSONObject loadWorkspaceConfigJson(String workspaceId) {
        if (workspaceId == null) {
            return null;
        }
        try {
            return ModelInferenceLogsUtils.getWorkspaceConfigJson(workspaceId);
        } catch (Exception e) {
            logger.warn("AgentConfigLoader: CONFIG_JSON load failed for workspaceId={}: {}",
                    workspaceId, e.getMessage());
            return null;
        }
    }

    /**
     * Builds the MCP project-ref list from workspace resources, CONFIG_JSON,
     * and room options, deduped by UUID portion.
     */
    private static List<Map<String, String>> resolveMcps(String workspaceId, Room room, JSONObject cfgJson) {
        List<Map<String, String>> out = new ArrayList<>();
        Set<String> seenUuids = new LinkedHashSet<>();

        // Legacy WORKSPACE_RESOURCE rows.
        if (workspaceId != null) {
            try {
                List<Map<String, Object>> rows = ModelInferenceLogsUtils.getWorkspaceResourcesIgnoringType(
                        workspaceId, List.of(AbstractWorkspaceReactor.PROMPT_RESOURCE_TYPE,
                                AbstractWorkspaceReactor.SKILL_RESOURCE_TYPE));
                for (Map<String, Object> row : rows) {
                    Object idObj = row.get("resource_id");
                    if (idObj == null) continue;
                    String id = String.valueOf(idObj).trim();
                    if (id.isEmpty()) continue;
                    String uuidKey = extractUuidPortion(id);
                    if (!seenUuids.add(uuidKey)) continue;
                    Map<String, String> entry = new HashMap<>();
                    entry.put("id", id);
                    entry.put("name", id);
                    out.add(entry);
                }
            } catch (Exception e) {
                logger.warn("AgentConfigLoader: workspace_resource lookup failed for workspaceId={}: {}",
                        workspaceId, e.getMessage());
            }
        }

        // CONFIG_JSON.mcps[] additions.
        if (cfgJson != null && cfgJson.has("mcps")) {
            JSONArray arr = cfgJson.optJSONArray("mcps");
            if (arr != null) {
                for (int i = 0; i < arr.length(); i++) {
                    JSONObject mcp = arr.optJSONObject(i);
                    if (mcp == null) continue;
                    String id = StringUtils.trimToNull(mcp.optString("id", null));
                    if (id == null) continue;
                    String uuidKey = extractUuidPortion(id);
                    if (!seenUuids.add(uuidKey)) continue;
                    String name = StringUtils.trimToNull(mcp.optString("name", null));
                    Map<String, String> entry = new HashMap<>();
                    entry.put("id", id);
                    entry.put("name", name != null ? name : id);
                    out.add(entry);
                }
            }
        }

        // room.options.mcp[] additions.
        List<Map<String, String>> roomMcps = extractRoomMcps(room);
        for (Map<String, String> entry : roomMcps) {
            String id = entry.get("id");
            if (id == null || id.isEmpty()) continue;
            String uuidKey = extractUuidPortion(id);
            if (!seenUuids.add(uuidKey)) continue;
            out.add(entry);
        }

        return out;
    }

    /**
     * Builds the skill-ref list from workspace resources, CONFIG_JSON, and
     * room options, deduped by {@code skill_id}.
     */
    private static List<Map<String, String>> resolveSkills(String workspaceId, Room room, JSONObject cfgJson) {
        List<Map<String, String>> out = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();

        // Use the SQL-based helper because the SelectQueryStruct path filters incorrectly here.
        if (workspaceId != null) {
            try {
                List<Map<String, String>> rows = ModelInferenceLogsUtils.getWorkspaceResources(
                        workspaceId, AbstractWorkspaceReactor.SKILL_RESOURCE_TYPE, null);
                if (rows != null) {
                    for (Map<String, String> row : rows) {
                        String skillId = StringUtils.trimToNull(row.get("resource_id"));
                        if (skillId == null) continue;
                        if (!seen.add(skillId)) continue;
                        String pinned = StringUtils.trimToNull(row.get("resource_subtype"));
                        out.add(skillRef(skillId, pinned));
                    }
                }
            } catch (Exception e) {
                logger.warn("AgentConfigLoader: workspace_resource SKILL lookup failed for workspaceId={}: {}",
                        workspaceId, e.getMessage());
            }
        }

        // CONFIG_JSON.skills[] additions.
        if (cfgJson != null && cfgJson.has("skills")) {
            JSONArray arr = cfgJson.optJSONArray("skills");
            if (arr != null) {
                for (int i = 0; i < arr.length(); i++) {
                    JSONObject s = arr.optJSONObject(i);
                    if (s == null) continue;
                    String skillId = StringUtils.trimToNull(s.optString("skill_id", null));
                    if (skillId == null) continue;
                    if (!seen.add(skillId)) continue;
                    String pinned = StringUtils.trimToNull(s.optString("pinned_version", null));
                    out.add(skillRef(skillId, pinned));
                }
            }
        }

        // room.options.skills[] additions.
        for (Map<String, String> entry : extractRoomSkills(room)) {
            String skillId = entry.get("skill_id");
            if (skillId == null || skillId.isEmpty()) continue;
            if (!seen.add(skillId)) continue;
            out.add(entry);
        }

        return out;
    }

    /** Parse {@code room.options.skills[]} into a list of skill refs. */
    private static List<Map<String, String>> extractRoomSkills(Room room) {
        List<Map<String, String>> result = new ArrayList<>();
        String opts = room.getOptions();
        if (opts == null || opts.trim().isEmpty()) {
            return result;
        }
        try {
            JsonObject obj = JsonParser.parseString(opts).getAsJsonObject();
            JsonElement el = obj.get("skills");
            if (el == null || !el.isJsonArray()) {
                return result;
            }
            JsonArray arr = el.getAsJsonArray();
            for (JsonElement e : arr) {
                if (!e.isJsonObject()) continue;
                JsonObject m = e.getAsJsonObject();
                JsonElement idEl = m.get("skill_id");
                if (idEl == null || !idEl.isJsonPrimitive()) continue;
                String skillId = StringUtils.trimToNull(idEl.getAsString());
                if (skillId == null) continue;
                String pinned = null;
                JsonElement pinEl = m.get("pinned_version");
                if (pinEl != null && pinEl.isJsonPrimitive()) {
                    pinned = StringUtils.trimToNull(pinEl.getAsString());
                }
                result.add(skillRef(skillId, pinned));
            }
        } catch (Exception ignore) {
            // best-effort parse; bad options blob -> no room skills
        }
        return result;
    }

    private static Map<String, String> skillRef(String skillId, String pinnedVersion) {
        Map<String, String> entry = new HashMap<>();
        entry.put("skill_id", skillId);
        if (pinnedVersion != null && !pinnedVersion.isEmpty()) {
            entry.put("pinned_version", pinnedVersion);
        }
        return entry;
    }

    /** Parse {@code room.options.mcp[]} into a list of {@code {id, name}} maps. */
    private static List<Map<String, String>> extractRoomMcps(Room room) {
        List<Map<String, String>> result = new ArrayList<>();
        String opts = room.getOptions();
        if (opts == null || opts.trim().isEmpty()) {
            return result;
        }
        try {
            JsonObject obj = JsonParser.parseString(opts).getAsJsonObject();
            JsonElement mcpElem = obj.get("mcp");
            if (mcpElem == null || !mcpElem.isJsonArray()) {
                return result;
            }
            JsonArray arr = mcpElem.getAsJsonArray();
            for (JsonElement e : arr) {
                if (!e.isJsonObject()) continue;
                JsonObject m = e.getAsJsonObject();
                JsonElement idEl = m.get("id");
                if (idEl == null || !idEl.isJsonPrimitive()) continue;
                String id = StringUtils.trimToNull(idEl.getAsString());
                if (id == null) continue;
                String name = id;
                JsonElement nameEl = m.get("name");
                if (nameEl != null && nameEl.isJsonPrimitive()) {
                    String n = StringUtils.trimToNull(nameEl.getAsString());
                    if (n != null) name = n;
                }
                Map<String, String> entry = new HashMap<>();
                entry.put("id", id);
                entry.put("name", name);
                result.add(entry);
            }
        } catch (Exception ignore) {
            // best-effort parse; bad options blob -> no room mcps
        }
        return result;
    }

    /**
     * For dedup: extract the UUID portion of an MCP id.
     * Accepts both raw UUIDs ({@code "2bd5857e-..."}) and prefixed forms
     * ({@code "FDA Application Explorer__2bd5857e-..."}). Returns the substring
     * after the last {@code "__"} occurrence, or the input as-is when no delimiter is present.
     */
    private static String extractUuidPortion(String id) {
        if (id == null) return "";
        int idx = id.lastIndexOf("__");
        return (idx >= 0 && idx + 2 < id.length()) ? id.substring(idx + 2) : id;
    }

    /**
     * Resolves the authored prompt for this run.
     *
     * <p>Priority is room instructions, then {@code CONFIG_JSON.system_prompt},
     * then the legacy workspace column. Workspace-backed reads still enforce
     * ACL and active-state checks.
     */
    private static String resolveAuthoredPrompt(Room room, String workspaceId, Map<String, Object> workspaceRow,
            JSONObject cfgJson) {
        // (a) room.options.instructions
        String roomInstructions = extractRoomInstructions(room);
        if (roomInstructions != null) {
            return roomInstructions;
        }
        // (b) + (c) need workspace context
        if (workspaceId == null || workspaceRow == null) {
            return null;
        }
        // (b) CONFIG_JSON.system_prompt - new source, preferred
        if (cfgJson != null && cfgJson.has("system_prompt")) {
            String fromJson = StringUtils.trimToNull(cfgJson.optString("system_prompt", null));
            if (fromJson != null) {
                return fromJson;
            }
        }
        // (c) legacy column fallback
        return StringUtils.trimToNull((String) workspaceRow.get("system_prompt"));
    }

    /** Parse {@code room.options.instructions} as a non-blank string, or {@code null}. */
    private static String extractRoomInstructions(Room room) {
        String opts = room.getOptions();
        if (opts == null || opts.trim().isEmpty()) {
            return null;
        }
        try {
            JsonObject obj = JsonParser.parseString(opts).getAsJsonObject();
            JsonElement el = obj.get("instructions");
            if (el != null && el.isJsonPrimitive()) {
                return StringUtils.trimToNull(el.getAsString());
            }
        } catch (Exception ignore) {
            // fall through
        }
        return null;
    }

    /**
     * Extract {@code workspace_id} from {@code room.options.workspace}, mirroring
     * {@link Room#getRoomOrWorkspaceSystemPrompt()}'s parsing.
     */
    private static String extractWorkspaceId(Room room) {
        String opts = room.getOptions();
        if (opts == null || opts.trim().isEmpty()) {
            return null;
        }
        JsonObject obj;
        try {
            obj = JsonParser.parseString(opts).getAsJsonObject();
        } catch (Exception ignore) {
            return null;
        }
        JsonElement wsElem = obj.get("workspace");
        if (wsElem == null) {
            return null;
        }
        if (wsElem.isJsonPrimitive()) {
            return StringUtils.trimToNull(wsElem.getAsString());
        }
        if (wsElem.isJsonObject()) {
            JsonElement idElem = wsElem.getAsJsonObject().get("workspace_id");
            if (idElem != null && idElem.isJsonPrimitive()) {
                return StringUtils.trimToNull(idElem.getAsString());
            }
        }
        return null;
    }

    private static int resolveMaxSeconds(Map<String, Object> paramMap) {
        if (paramMap == null) {
            return 0;
        }
        Object val = paramMap.get(PARAM_MAX_SECONDS);
        if (val == null) {
            return 0;
        }
        if (val instanceof Number) {
            return ((Number) val).intValue();
        }
        if (val instanceof String) {
            try {
                return Integer.parseInt(((String) val).trim());
            } catch (NumberFormatException ignored) {
                // fall through
            }
        }
        return 0;
    }

    /**
     * Build the run budgets.
     *
     * <p>{@code CONFIG_JSON.budgets} sets hard caps per field. The runtime
     * values (caller args and {@code paramMap}) are honored as-is when no cap
     * is configured, but are clamped to the cap when one is present — callers
     * can always request less, never more.
     *
     * <p>Recognized {@code CONFIG_JSON.budgets} keys: {@code max_turns},
     * {@code max_reflections}, {@code max_seconds}.
     */
    private static AgentConfig.Budgets resolveBudgets(JSONObject cfgJson, Map<String, Object> paramMap,
            int callerMaxTurns, int callerMaxReflections) {
        int maxTurns       = callerMaxTurns;
        int maxReflections = callerMaxReflections;
        int maxSeconds     = resolveMaxSeconds(paramMap);

        if (cfgJson != null && cfgJson.has("budgets")) {
            JSONObject bj = cfgJson.optJSONObject("budgets");
            if (bj != null) {
                int capTurns   = bj.optInt("max_turns",       -1);
                int capRefl    = bj.optInt("max_reflections", -1);
                int capSeconds = bj.optInt("max_seconds",     -1);
                // Clamp runtime values to the configured caps — runtime can go lower, never higher.
                if (capTurns   > 0)  maxTurns       = Math.min(maxTurns, capTurns);
                if (capRefl    >= 0) maxReflections = Math.min(maxReflections, capRefl);
                if (capSeconds >= 0) maxSeconds     = (maxSeconds <= 0) ? capSeconds
                                                        : Math.min(maxSeconds, capSeconds);
            }
        }
        return AgentConfig.Budgets.of(maxTurns, maxReflections, maxSeconds);
    }

    /** CONFIG_JSON.spawn_policy keys: max_subagent_depth, max_subagents_per_run, max_spawns_per_turn. */
    private static AgentConfig.SubAgentSpawnPolicy resolveSpawnPolicy(JSONObject cfgJson) {
        int maxDepth     = AgentConfig.SubAgentSpawnPolicy.DEFAULT_MAX_SUBAGENT_DEPTH;
        int maxPerRun    = AgentConfig.SubAgentSpawnPolicy.DEFAULT_MAX_SUBAGENTS_PER_RUN;
        int maxPerTurn   = AgentConfig.SubAgentSpawnPolicy.DEFAULT_MAX_SPAWNS_PER_TURN;

        if (cfgJson != null && cfgJson.has("spawn_policy")) {
            JSONObject pj = cfgJson.optJSONObject("spawn_policy");
            if (pj != null) {
                int d = pj.optInt("max_subagent_depth",    -1);
                int r = pj.optInt("max_subagents_per_run", -1);
                int t = pj.optInt("max_spawns_per_turn",   -1);
                if (d >= 0) maxDepth   = d;
                if (r >= 0) maxPerRun  = r;
                if (t >= 0) maxPerTurn = t;
            }
        }
        return AgentConfig.SubAgentSpawnPolicy.of(maxDepth, maxPerRun, maxPerTurn);
    }

    // Pair of resolved hook lists, returned in one pass over CONFIG_JSON.hooks[].
    private static final class ResolvedHooks {
        final List<IAgentRunHook> runHooks;
        final List<IToolHook>    toolHooks;
        ResolvedHooks(List<IAgentRunHook> m, List<IToolHook> t) {
            this.runHooks = m;
            this.toolHooks    = t;
        }
        static ResolvedHooks empty() {
            return new ResolvedHooks(Collections.emptyList(), Collections.emptyList());
        }
    }

    /**
     * Resolve the configured hook list from {@code CONFIG_JSON.hooks[]}. Each entry's
     * {@code kind} is mapped to a concrete {@link IAgentHook} via {@link #resolveHook};
     * the result is then classified by interface - implementations of
     * {@link IAgentRunHook} land in the run-hook list, implementations of
     * {@link IToolHook} land in the tool-hook list. A single hook may implement both
     * and land in both lists. Unknown kinds are logged and skipped.
     */
    private static ResolvedHooks resolveHooks(JSONObject cfgJson) {
        if (cfgJson == null || !cfgJson.has("hooks")) {
            return ResolvedHooks.empty();
        }
        JSONArray arr = cfgJson.optJSONArray("hooks");
        if (arr == null || arr.length() == 0) {
            return ResolvedHooks.empty();
        }
        List<IAgentRunHook> runHooks = new ArrayList<>(arr.length());
        List<IToolHook>    toolHooks    = new ArrayList<>(arr.length());
        for (int i = 0; i < arr.length(); i++) {
            JSONObject spec = arr.optJSONObject(i);
            if (spec == null) continue;
            IAgentHook h = resolveHook(spec);
            if (h == null) continue;
            boolean classified = false;
            if (h instanceof IAgentRunHook) {
                runHooks.add((IAgentRunHook) h);
                classified = true;
            }
            if (h instanceof IToolHook) {
                toolHooks.add((IToolHook) h);
                classified = true;
            }
            if (!classified) {
                logger.warn("AgentConfigLoader: hook kind '{}' resolved to class {} which implements neither IAgentRunHook nor IToolHook - skipping",
                        spec.optString("kind", null), h.getClass().getName());
            }
        }
        return new ResolvedHooks(
                Collections.unmodifiableList(runHooks),
                Collections.unmodifiableList(toolHooks));
    }

    /**
     * Resolve named subagent slots from {@code CONFIG_JSON.subagents[]}. Each entry must
     * have non-blank {@code alias} and {@code workspaceId}; {@code description} is optional.
     * Duplicate aliases within the same list are dropped with a warn log (first wins).
     *
     * <p>The semoss harness synthesizes one MCP tool per returned spec; CLI harnesses
     * ignore the list.
     *
     * @return unmodifiable list, never {@code null}
     */
    private static List<SubAgentSpec> resolveSubagents(JSONObject cfgJson) {
        if (cfgJson == null || !cfgJson.has("subagents")) {
            return Collections.emptyList();
        }
        JSONArray arr = cfgJson.optJSONArray("subagents");
        if (arr == null || arr.length() == 0) {
            return Collections.emptyList();
        }
        List<SubAgentSpec> out = new ArrayList<>(arr.length());
        Set<String> seenAliases = new LinkedHashSet<>();
        for (int i = 0; i < arr.length(); i++) {
            JSONObject spec = arr.optJSONObject(i);
            if (spec == null) continue;
            String alias       = StringUtils.trimToNull(spec.optString("alias",       null));
            String workspaceId = StringUtils.trimToNull(spec.optString("workspaceId", null));
            String description = StringUtils.trimToNull(spec.optString("description", null));
            if (alias == null || workspaceId == null) {
                logger.warn("AgentConfigLoader: subagent entry missing alias or workspaceId - skipping (index={})", i);
                continue;
            }
            if (!seenAliases.add(alias)) {
                logger.warn("AgentConfigLoader: duplicate subagent alias '{}' - keeping first, skipping later entry", alias);
                continue;
            }
            try {
                out.add(new SubAgentSpec(alias, workspaceId, description));
            } catch (IllegalArgumentException e) {
                logger.warn("AgentConfigLoader: invalid subagent entry alias='{}' workspaceId='{}': {}",
                        alias, workspaceId, e.getMessage());
            }
        }
        return Collections.unmodifiableList(out);
    }

    /**
     * Map one {@code {"kind": ..., "params": {...}}} spec to a concrete
     * {@link IAgentHook} instance via {@link AgentHookRegistry}. The
     * {@code params} sub-object is loader-internal today - each hook reads
     * what it needs from {@link prerna.reactor.agent.AgentRunContext} at run
     * time.
     *
     * <p>Adding a new hook: implement {@code IAgentRunHook}, {@code IToolHook}, or
     * both, and register it via
     * {@link AgentHookRegistry#register(String, java.util.function.Supplier)}
     * (built-ins live in the registry's static init block). The registry is
     * the same source of truth {@code SetAgentHooksReactor} validates
     * against on write.
     *
     * @return new hook instance, or {@code null} for an unknown kind (logged warn)
     */
    private static IAgentHook resolveHook(JSONObject spec) {
        String kind = spec.optString("kind", null);
        if (kind == null || kind.isEmpty()) {
            logger.warn("AgentConfigLoader: hook spec missing 'kind' - skipping");
            return null;
        }
        IAgentHook hook = AgentHookRegistry.resolve(kind);
        if (hook == null) {
            logger.warn("AgentConfigLoader: unknown hook kind '{}' - skipping", kind);
            return null;
        }
        // Pass the full spec so config-bearing hooks (e.g. PixelReactorHook)
        // can extract their kind-specific fields. Stateless hooks ignore
        // it via the no-op default. A configure() failure skips this hook
        // but doesn't poison the rest of the run.
        try {
            hook.configure(spec);
        } catch (Exception e) {
            logger.warn("AgentConfigLoader: hook kind '{}' configure() threw — skipping. cause: {}",
                    kind, e.getMessage(), e);
            return null;
        }
        return hook;
    }

    private static int lengthOrZero(String s) {
        return s == null ? 0 : s.length();
    }
}
