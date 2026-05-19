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
import prerna.reactor.agent.IMessageHook;
import prerna.reactor.agent.hooks.AgentHookRegistry;
import prerna.reactor.agent.runtime.AgentsMdLoader;

/**
 * Single resolver that builds an {@link AgentConfig} for one agent run.
 *
 * <p>Every harness reads its agent state from {@code ctx.getAgentConfig()} -
 * no harness re-implements room/workspace resolution. The composition rule
 * lives here, in one place.
 *
 * <h2>v1 sources (in resolution order)</h2>
 * <ol>
 *   <li><b>Workspace metadata</b> - {@code name}, {@code description} fetched via
 *       {@link ModelInferenceLogsUtils#getWorkspaceEntry(String)} when a
 *       {@code workspace_id} can be parsed from {@code room.options.workspace}
 *       (or supplied directly on {@code RunAgent}).</li>
 *   <li><b>CONFIG_JSON</b> - {@link ModelInferenceLogsUtils#getWorkspaceConfigJson(String)}
 *       returns the per-workspace agent config blob. Consumed by the resolvers
 *       below; layered on top of the legacy column / WORKSPACE_RESOURCE reads
 *       so workspaces without CONFIG_JSON keep working byte-identically.</li>
 *   <li><b>Authored prompt</b> - {@code room.options.instructions} (room-level
 *       override) → {@code CONFIG_JSON.system_prompt} → {@code workspace.system_prompt}
 *       (legacy column), with ACL + active-state checks against the resolved
 *       workspace.</li>
 *   <li><b>Workdir AGENTS.md</b> - discovered by walking up from {@code workingDir}
 *       via {@link AgentsMdLoader#discover(String)}.</li>
 *   <li><b>MCP tool projects</b> - union of {@code WORKSPACE_RESOURCE} rows
 *       (legacy source), {@code CONFIG_JSON.mcps} entries (new source, additive),
 *       and {@code room.options.mcp[]} per-room additions, deduped by UUID portion.</li>
 *   <li><b>Budgets</b> - {@code CONFIG_JSON.budgets} when present, else caller
 *       args ({@code maxTurns} / {@code maxReflections}) and {@code max_seconds}
 *       from {@code paramMap} (0 = no limit).</li>
 *   <li><b>Hooks</b> - {@code CONFIG_JSON.hooks[]}; each entry's {@code kind}
 *       resolves to a concrete {@link IMessageHook} via {@link #resolveHook}.
 *       Unknown kinds are logged and dropped.</li>
 *   <li><b>Agent's own AGENTS.md</b> - reserved; will load from the workspace's
 *       project assets folder once that wiring lands.</li>
 * </ol>
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
            int maxTurns,
            int maxReflections,
            String explicitWorkspaceId) {

        if (room == null) {
            throw new IllegalArgumentException("room is required");
        }

        AgentConfig.Builder b = AgentConfig.builder();

        // 1. Resolve the workspace id.
        //    Precedence: explicit param > room.options.workspace.workspace_id.
        String workspaceId = explicitWorkspaceId != null
                ? explicitWorkspaceId
                : extractWorkspaceId(room);
        if (explicitWorkspaceId != null) {
            logger.info("AgentConfigLoader: workspace_id override applied (explicit='{}' wins over room.options)",
                    explicitWorkspaceId);
        }

        // 2. Workspace lookup + metadata (best-effort - ad-hoc rooms have no workspace).
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

        // 2.5. CONFIG_JSON - per-workspace agent config blob. Resolvers layer this
        //      on top of legacy column / WORKSPACE_RESOURCE reads; null/empty means
        //      everything falls back to the pre-CONFIG_JSON paths.
        JSONObject cfgJson = loadWorkspaceConfigJson(workspaceId);

        // 3. Authored prompt - resolution rule, in priority order:
        //    (a) room.options.instructions   (room-level override always wins)
        //    (b) CONFIG_JSON.system_prompt   (new; preferred over the legacy column)
        //    (c) workspace.system_prompt     (legacy column, kept as back-compat fallback)
        b.authoredPrompt(resolveAuthoredPrompt(room, workspaceId, workspaceRow, cfgJson));

        // 4. Workdir AGENTS.md / CLAUDE.md auto-discovery.
        //    DISABLED — the walk-up was leaking unrelated repo-level instructions
        //    (e.g. Semoss/CLAUDE.md) into every run whenever SEMOSS_BASE_FOLDER lived
        //    inside the source tree. Per-workspace / per-room behavior should be
        //    expressed explicitly via:
        //      - CONFIG_JSON.system_prompt   (workspace-level authored prompt), or
        //      - room.options.instructions   (room-level override),
        //    both of which are picked up by resolveAuthoredPrompt above.
        //    To re-enable, restore: b.workdirAgentsMd(AgentsMdLoader.discover(workingDir)).

        // 5. Model
        b.modelId(StringUtils.trimToNull(modelId));
        b.modelParams(paramMap);

        // 6. Working directory
        b.workingDir(StringUtils.trimToNull(workingDir));

        // 7. Budgets - CONFIG_JSON.budgets wins per-field, else caller args / paramMap.
        b.budgets(resolveBudgets(cfgJson, paramMap, maxTurns, maxReflections));

        // 8. MCP tool projects - WORKSPACE_RESOURCE rows AND CONFIG_JSON.mcps both
        //    contribute (layered, not switched), plus room.options.mcp[] additions.
        b.mcps(resolveMcps(workspaceId, room, cfgJson));

        // 9. Hooks - CONFIG_JSON.hooks[] only. No legacy fallback (hooks didn't
        //    previously have a persistence layer).
        b.hooks(resolveHooks(cfgJson));

        // 10. Subagents - CONFIG_JSON.subagents[] only. Semoss harness synthesizes one
        //     MCP tool per spec; CLI harnesses read but ignore.
        b.subagents(resolveSubagents(cfgJson));

        // 11. Agent's own AGENTS.md - reserved for follow-up PR.

        AgentConfig cfg = b.build();
        logger.info(
                "AgentConfigLoader: resolved room={} workspaceId={} name={} modelId={} workingDir={} mcps={} hooks={} subagents={} budgets(turns={},refl={},secs={}) authoredChars={} workdirAgentsMdChars={} cfgJson={}",
                room.getId(), cfg.getWorkspaceId(), cfg.getName(), cfg.getModelId(), cfg.getWorkingDir(),
                cfg.getMcps().size(), cfg.getHooks().size(), cfg.getSubagents().size(),
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
     * Build the agent's MCP tool-project list - a union of three sources, deduped
     * by UUID portion of the id:
     *
     * <ol>
     *   <li><b>{@code WORKSPACE_RESOURCE} rows</b> (legacy source) for the resolved
     *       {@code workspaceId}, excluding {@code PROMPT} rows. Mirrors what
     *       {@link Room#getAllToolsJsonForRoom(int)} reads for the SEMOSS harness path.</li>
     *   <li><b>{@code CONFIG_JSON.mcps[]} entries</b> (new source, additive). When
     *       a workspace migrates to dual-write or CONFIG_JSON-only, this is where
     *       new entries land. Older workspaces with only WORKSPACE_RESOURCE rows
     *       keep working byte-identically.</li>
     *   <li><b>{@code room.options.mcp[]} entries</b> - per-room additions / overrides.</li>
     * </ol>
     *
     * <p>Dedupe uses the UUID portion ({@link #extractUuidPortion}) so the same
     * engine appearing as {@code Name__uuid} in one source and bare {@code uuid}
     * in another counts as one entry.
     *
     * <p>Returns a list of immutable {@code {id, name}} maps; empty when no source
     * contributes.
     */
    private static List<Map<String, String>> resolveMcps(String workspaceId, Room room, JSONObject cfgJson) {
        List<Map<String, String>> out = new ArrayList<>();
        Set<String> seenUuids = new LinkedHashSet<>();

        // 1. Legacy WORKSPACE_RESOURCE rows (existing read path - unchanged).
        if (workspaceId != null) {
            try {
                List<Map<String, Object>> rows = ModelInferenceLogsUtils.getWorkspaceResourcesIgnoringType(
                        workspaceId, Collections.singletonList(AbstractWorkspaceReactor.PROMPT_RESOURCE_TYPE));
                for (Map<String, Object> row : rows) {
                    Object idObj = row.get("resource_id");
                    if (idObj == null) continue;
                    String id = String.valueOf(idObj).trim();
                    if (id.isEmpty()) continue;
                    String uuidKey = extractUuidPortion(id);
                    if (!seenUuids.add(uuidKey)) continue;
                    Map<String, String> entry = new HashMap<>();
                    entry.put("id", id);
                    entry.put("name", id);   // name defaults to id; harness consumers may resolve a friendlier name
                    out.add(entry);
                }
            } catch (Exception e) {
                logger.warn("AgentConfigLoader: workspace_resource lookup failed for workspaceId={}: {}",
                        workspaceId, e.getMessage());
            }
        }

        // 2. CONFIG_JSON.mcps[] (new source, layered on top - additive only).
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

        // 3. room.options.mcp[] additions.
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
     * Resolve the authored prompt for this run.
     *
     * <ol>
     *   <li>{@code room.options.instructions} - room-level override always wins.</li>
     *   <li>{@code CONFIG_JSON.system_prompt} via the resolved {@code workspaceId}
     *       - preferred over the legacy column when present.</li>
     *   <li>{@code workspace.system_prompt} (legacy column) - back-compat fallback;
     *       still written by {@code EditWorkspaceReactor} as a dual-write until a
     *       follow-up PR removes it.</li>
     * </ol>
     *
     * <p>ACL + active-state checks against the resolved workspace apply once
     * either workspace-scoped source (CONFIG_JSON or column) is consulted.
     *
     * @throws IllegalArgumentException when the user lacks access or the workspace is disabled
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
        User user = room.getInsight() != null ? room.getInsight().getUser() : null;
        if (user != null && !SecurityProjectUtils.userCanViewProject(user, workspaceId)) {
            throw new IllegalArgumentException(
                    "Workspace " + workspaceId + " does not exist or user does not have access to the workspace");
        }
        Object isActive = workspaceRow.get("is_active");
        if (Boolean.FALSE.equals(isActive)) {
            throw new IllegalArgumentException("Workspace is disabled by the owner");
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
     * Build the run budgets. {@code CONFIG_JSON.budgets} wins per-field when
     * present; missing fields fall through to caller args and {@code paramMap}.
     *
     * <p>Recognized {@code CONFIG_JSON.budgets} keys: {@code max_turns},
     * {@code max_reflections}, {@code max_seconds}. Any zero / negative value
     * (except {@code max_turns} which must be {@code > 0}) is ignored.
     */
    private static AgentConfig.Budgets resolveBudgets(JSONObject cfgJson, Map<String, Object> paramMap,
            int callerMaxTurns, int callerMaxReflections) {
        int maxTurns       = callerMaxTurns;
        int maxReflections = callerMaxReflections;
        int maxSeconds     = resolveMaxSeconds(paramMap);

        if (cfgJson != null && cfgJson.has("budgets")) {
            JSONObject bj = cfgJson.optJSONObject("budgets");
            if (bj != null) {
                int t = bj.optInt("max_turns",       -1);
                int r = bj.optInt("max_reflections", -1);
                int s = bj.optInt("max_seconds",     -1);
                if (t > 0)  maxTurns       = t;
                if (r >= 0) maxReflections = r;
                if (s >= 0) maxSeconds     = s;
            }
        }
        return AgentConfig.Budgets.of(maxTurns, maxReflections, maxSeconds);
    }

    /**
     * Resolve the configured hook list from {@code CONFIG_JSON.hooks[]}. Each
     * entry's {@code kind} is mapped to a concrete {@link IMessageHook} via
     * {@link #resolveHook}; unknown kinds are logged and skipped.
     *
     * @return unmodifiable list, never {@code null}
     */
    private static List<IMessageHook> resolveHooks(JSONObject cfgJson) {
        if (cfgJson == null || !cfgJson.has("hooks")) {
            return Collections.emptyList();
        }
        JSONArray arr = cfgJson.optJSONArray("hooks");
        if (arr == null || arr.length() == 0) {
            return Collections.emptyList();
        }
        List<IMessageHook> out = new ArrayList<>(arr.length());
        for (int i = 0; i < arr.length(); i++) {
            JSONObject spec = arr.optJSONObject(i);
            if (spec == null) continue;
            IMessageHook h = resolveHook(spec);
            if (h != null) out.add(h);
        }
        return Collections.unmodifiableList(out);
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
     * {@link IMessageHook} instance via {@link AgentHookRegistry}. The
     * {@code params} sub-object is loader-internal today - each hook reads
     * what it needs from {@link prerna.reactor.agent.AgentRunContext} at run
     * time.
     *
     * <p>Adding a new hook: implement {@code IMessageHook} and register it via
     * {@link AgentHookRegistry#register(String, java.util.function.Supplier)}
     * (built-ins live in the registry's static init block). The registry is
     * the same source of truth {@code SetWorkspaceHooksReactor} validates
     * against on write.
     *
     * @return new hook instance, or {@code null} for an unknown kind (logged warn)
     */
    private static IMessageHook resolveHook(JSONObject spec) {
        String kind = spec.optString("kind", null);
        if (kind == null || kind.isEmpty()) {
            logger.warn("AgentConfigLoader: hook spec missing 'kind' - skipping");
            return null;
        }
        IMessageHook hook = AgentHookRegistry.resolve(kind);
        if (hook == null) {
            logger.warn("AgentConfigLoader: unknown hook kind '{}' - skipping", kind);
        }
        return hook;
    }

    private static int lengthOrZero(String s) {
        return s == null ? 0 : s.length();
    }
}
