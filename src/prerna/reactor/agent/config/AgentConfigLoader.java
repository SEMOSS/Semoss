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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.inferencetracking.reactors.workspaces.AbstractWorkspaceReactor;
import prerna.reactor.agent.runtime.AgentsMdLoader;

/**
 * Single resolver that builds an {@link AgentConfig} for one agent run.
 *
 * <p>Every harness reads its agent state from {@code ctx.getAgentConfig()} —
 * no harness re-implements room/workspace resolution. The composition rule
 * lives here, in one place.
 *
 * <h2>v1 sources (in resolution order)</h2>
 * <ol>
 *   <li><b>Authored prompt</b> — delegates to
 *       {@link Room#getRoomOrWorkspaceSystemPrompt()} which enforces the
 *       {@code room.options.instructions} → {@code workspace.system_prompt}
 *       precedence and access checks.</li>
 *   <li><b>Workspace metadata</b> — {@code name}, {@code description} fetched via
 *       {@link ModelInferenceLogsUtils#getWorkspaceEntry(String)} when a
 *       {@code workspace_id} can be parsed from {@code room.options.workspace}.</li>
 *   <li><b>Workdir AGENTS.md</b> — discovered by walking up from {@code workingDir}
 *       via {@link AgentsMdLoader#discover(String)}.</li>
 *   <li><b>Budgets</b> — caller supplies {@code maxTurns} / {@code maxReflections};
 *       {@code max_seconds} extracted from {@code paramMap} (0 = no limit).</li>
 *   <li><b>Agent's own AGENTS.md</b> — reserved; will load from the workspace's
 *       project assets folder once that wiring lands.</li>
 * </ol>
 *
 * <p>Subsequent PRs add more fields (tool selection, hooks, ...) from
 * {@code WORKSPACE.CONFIG_JSON} when the DDL ships.
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
     * @param explicitWorkspaceId  optional override — wins over {@code room.options.workspace.workspace_id}.
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

        // 2. Workspace lookup + metadata (best-effort — ad-hoc rooms have no workspace).
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

        // 3. Authored prompt — resolution rule, in priority order:
        //    (a) room.options.instructions  (room-level override always wins)
        //    (b) workspace.system_prompt    (looked up against the resolved workspaceId,
        //                                    honoring the explicit override; with ACL +
        //                                    active-state checks)
        b.authoredPrompt(resolveAuthoredPrompt(room, workspaceId, workspaceRow));

        // 3. Workdir AGENTS.md / CLAUDE.md (walked from working_dir).
        if (workingDir != null && !workingDir.trim().isEmpty()) {
            b.workdirAgentsMd(AgentsMdLoader.discover(workingDir));
        }

        // 4. Model
        b.modelId(StringUtils.trimToNull(modelId));
        b.modelParams(paramMap);

        // 5. Working directory
        b.workingDir(StringUtils.trimToNull(workingDir));

        // 6. Budgets
        int maxSeconds = resolveMaxSeconds(paramMap);
        b.budgets(AgentConfig.Budgets.of(maxTurns, maxReflections, maxSeconds));

        // 7. MCP tool projects — workspace-driven, with room.options.mcp[] additions.
        b.mcps(resolveMcps(workspaceId, room));

        // 8. Agent's own AGENTS.md — reserved for follow-up PR.

        AgentConfig cfg = b.build();
        logger.info(
                "AgentConfigLoader: resolved room={} workspaceId={} name={} modelId={} workingDir={} mcps={} budgets(turns={},refl={},secs={}) authoredChars={} workdirAgentsMdChars={}",
                room.getId(), cfg.getWorkspaceId(), cfg.getName(), cfg.getModelId(), cfg.getWorkingDir(),
                cfg.getMcps().size(),
                cfg.getBudgets().getMaxTurns(), cfg.getBudgets().getMaxReflections(), cfg.getBudgets().getMaxSeconds(),
                lengthOrZero(cfg.getAuthoredPrompt()), lengthOrZero(cfg.getWorkdirAgentsMd()));
        return cfg;
    }

    /**
     * Build the agent's MCP tool-project list, in this order:
     * <ol>
     *   <li><b>Workspace-resource entries</b> for the resolved {@code workspaceId} —
     *       this is the source of truth. Reads {@code WORKSPACE_RESOURCE} via
     *       {@link ModelInferenceLogsUtils#getWorkspaceResourcesIgnoringType} excluding
     *       {@code PROMPT} rows (mirrors what {@link Room#getAllToolsJsonForRoom(int)}
     *       already does for the SEMOSS harness path).</li>
     *   <li><b>{@code room.options.mcp[]} entries</b> appended as per-room additions /
     *       overrides. Deduped against the workspace list by the UUID portion of the id
     *       (the last {@code __}-segment when the id is in {@code <name>__<uuid>} form).</li>
     * </ol>
     *
     * <p>Returns a list of immutable {@code {id, name}} maps; empty when neither source
     * contributes.
     */
    private static List<Map<String, String>> resolveMcps(String workspaceId, Room room) {
        List<Map<String, String>> out = new ArrayList<>();
        Set<String> seenUuids = new LinkedHashSet<>();

        // 1. Workspace tools (source of truth).
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

        // 2. room.options.mcp[] additions.
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
     *   <li>{@code room.options.instructions} — room-level override always wins.</li>
     *   <li>{@code workspace.system_prompt} via the resolved {@code workspaceId}, with
     *       ACL ({@link SecurityProjectUtils#userCanViewProject}) and active-state checks.</li>
     * </ol>
     *
     * @throws IllegalArgumentException when the user lacks access or the workspace is disabled
     */
    private static String resolveAuthoredPrompt(Room room, String workspaceId, Map<String, Object> workspaceRow) {
        // (a) room.options.instructions
        String roomInstructions = extractRoomInstructions(room);
        if (roomInstructions != null) {
            return roomInstructions;
        }
        // (b) workspace fallback
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

    private static int lengthOrZero(String s) {
        return s == null ? 0 : s.length();
    }
}
