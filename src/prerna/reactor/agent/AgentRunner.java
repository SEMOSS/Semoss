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
package prerna.reactor.agent;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;
import prerna.reactor.agent.config.AgentConfig;
import prerna.reactor.agent.config.AgentConfigLoader;
import prerna.reactor.agent.sandbox.EnforcementMode;
import prerna.reactor.agent.sandbox.SandboxPolicy;
import prerna.reactor.agent.sandbox.SandboxPolicyBuilder;
import prerna.reactor.agent.skill.SkillStager;
import prerna.util.AssetUtility;
import prerna.util.Utility;

/**
 * High-level orchestrator for the generic agent loop.
 *
 */
public final class AgentRunner {

    private static final Logger logger = LogManager.getLogger(AgentRunner.class);

    /** Key under which the resolved working directory is injected into {@code paramMap}. */
    public static final String FILE_PATH_PARAM_KEY = "file_path";

    /**
     * paramMap key for an explicit workspace id (agent identity) that overrides
     * whatever {@code room.options.workspace.workspace_id} carries. Stripped from
     * paramMap by {@link #extractExplicitWorkspaceId(Map)} before the model engine call.
     */
    public static final String PARAM_WORKSPACE_ID = "workspace_id";

    /**
     * paramMap key: target SEMOSS project (workspace) the agent should operate inside.
     * Resolves to that project's {@code assets/} folder. Mutually preferred over the
     * default (current room's folder).
     */
    public static final String PARAM_PROJECT = "project";

    /**
     * paramMap key: relative subfolder inside the resolved container (room or project).
     * Must be relative (no leading {@code /}, {@code \}, or {@code ~}) and must not
     * contain {@code ..} segments. The resolved path is canonical-checked to stay
     * under the container.
     */
    public static final String PARAM_SUBDIR = "subdir";

    /**
     * paramMap key: legacy absolute working-dir path. <b>Deprecated.</b> Callers
     * should use {@link #PARAM_PROJECT} + {@link #PARAM_SUBDIR}. Logged + ignored.
     */
    public static final String PARAM_FILE_PATH_LEGACY = "filePath";

    /** Options-map keys checked (in order) when room.getModelId() is not set. */
    private static final String[] MODEL_ID_OPTION_KEYS = {"engine", "model", "modelId", "engineId"};

    // paramMap keys that let the caller extend the default sandbox policy.
    /** List of absolute paths to add as read-only to the sandbox policy. */
    public static final String PARAM_SANDBOX_READS   = "sandbox_reads";
    /** List of absolute paths to add as read-write to the sandbox policy. */
    public static final String PARAM_SANDBOX_WRITES  = "sandbox_writes";
    /** Override enforcement mode per-run: {@code ENFORCE} | {@code DISABLED}. */
    public static final String PARAM_SANDBOX_ENFORCE = "sandbox_enforce";

    private AgentRunner() { /* static utility */ }

    /**
     * Run the agent loop.
     *
     * @param roomId          Required. ROOM table ID that provides model, history, and tools.
     * @param input           Required. Initial user message.
     * @param engineIdFallback Optional. Engine/model ID to use if the room has no MODEL_ID set.
     * @param harnessType     Optional. Registry key for the harness; defaults to {@code "room_loop"}.
     * @param maxTurns        Optional. Maximum SEMOSS harness tool-call rounds.
     * @param maxReflections  Optional. Maximum SEMOSS harness self-critique rounds.
     * @param paramMap        Optional. Extra model parameters (temperature, max_tokens, etc.).
     * @param insight         Required. Current insight context (user, project, etc.).
     * @return Rich result containing final text, iteration count, and tool-call trace.
     * @throws Exception on unrecoverable errors during execution.
     */
    public static AgentHarnessResult run(
            String roomId,
            String input,
            String engineIdFallback,
            String harnessType,
            int maxTurns,
            int maxReflections,
            Map<String, Object> paramMap,
            Insight insight
            ) throws Exception {

        if (roomId == null || roomId.trim().isEmpty()) {
            throw new IllegalArgumentException("roomId is required");
        }
        if (input == null || input.trim().isEmpty()) {
            throw new IllegalArgumentException("input is required");
        }

        Room room = RoomUtils.getOrLoadRoom(roomId, insight);

        String modelId = resolveModelId(room, engineIdFallback);
        if (modelId == null || modelId.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "No model engine found for room '" + roomId + "'. "
                    + "Set MODEL_ID on the room or pass engine= to the reactor.");
        }
        logger.debug("AgentRunner: room={} resolved modelId={}", roomId, modelId);

        IModelEngine modelEngine = Utility.getModel(modelId);
        if (modelEngine == null) {
            throw new IllegalArgumentException(
                    "Could not load model engine '" + modelId + "' for room '" + roomId + "'");
        }
        room.setModelId(modelId);

        insight.setRoomForInsight(room);

        Map<String, Object> params = paramMap != null ? new HashMap<>(paramMap) : new HashMap<>();

        // Explicit workspace_id override (from the RunAgent `workspaceId` named arg
        // or paramMap key). Wins over room.options.workspace.workspace_id when set.
        // Stripped here so it doesn't leak into the model engine call.
        // Resolved BEFORE resolveWorkingDir so the working-dir code can fall back to
        // CONFIG_JSON.subdir for the per-workspace runtime convention.
        String explicitWorkspaceId = extractExplicitWorkspaceId(params);
        String effectiveWorkspaceId = explicitWorkspaceId != null
                ? explicitWorkspaceId
                : extractWorkspaceIdFromOptionField(room.getOptionsMap().get("workspace"));

        String filePath = resolveWorkingDir(room, params, effectiveWorkspaceId);
        if (filePath != null && !filePath.trim().isEmpty()) {
            params.put(FILE_PATH_PARAM_KEY, filePath);
        }

        SandboxPolicy sandboxPolicy = buildSandboxPolicyFromParams(params);

        // Resolve the agent config once. Every harness reads from ctx.getAgentConfig()
        // - no harness re-implements room/workspace prompt resolution. All "what is this
        // agent" fields (working_dir, model_id, model_params, budgets, prompt layers) live
        // on AgentConfig; AgentRunContext just carries the per-call live state.
        AgentConfig agentConfig = AgentConfigLoader.load(
                room, filePath, modelId, params, maxTurns, maxReflections, explicitWorkspaceId);

        // Materialize attached skills into <workingDir>/.claude/skills/<slug>/ so Claude
        // Code's skill discovery picks them up. Best-effort — individual failures are
        // logged inside the stager and do not abort the run.
        try {
            SkillStager.stage(filePath, agentConfig.getSkills());
        } catch (Exception e) {
            logger.warn("AgentRunner: skill staging failed for room='{}': {}", roomId, e.getMessage(), e);
        }

        AgentRunContext ctx = AgentRunContext.builder()
                .room(room)
                .modelEngine(modelEngine)
                .insight(insight)
                .userId(room.getUserId())
                .input(input)
                .sandboxPolicy(sandboxPolicy)
                .agentConfig(agentConfig)
                .build();

        IAgentHarness harness = AgentHarnessRegistry.getOrDefault(harnessType);
        logger.info("AgentRunner: using harness '{}' for room={}", harness.getName(), roomId);

        // Overlay room.options.workspace.workspace_id with the explicit override
        // for the duration of the run. This ensures every code path that reads
        // workspace_id (Room.getRoomOrWorkspaceSystemPrompt, Room.getAllToolsJsonForRoom,
        // any harness reading room.options directly) sees the same value as AgentConfig.
        // Restored in finally below - in-memory only, no DB write.
        //
        // Hook chain runs INSIDE the overlay (beforeMessage -> harness.execute ->
        // afterMessage), so hooks see the per-call workspace_id too. If any hook
        // throws, the chain short-circuits and the exception propagates; the
        // overlay restore still runs in the finally.
        List<IMessageHook> hooks = ctx.getAgentConfig().getHooks();
        WorkspaceOverlay wsOverlay = applyWorkspaceOverlay(room, explicitWorkspaceId);
        AgentHarnessResult result;
        try {
            for (IMessageHook h : hooks) {
                h.beforeMessage(ctx);
            }
            result = harness.execute(ctx);
            for (IMessageHook h : hooks) {
                h.afterMessage(ctx, result);
            }
        } finally {
            restoreWorkspaceOverlay(room, wsOverlay);
        }

        if (ClusterUtil.IS_CLUSTER) {
            try {
                ClusterUtil.pushRoom(roomId);
            } catch (Exception e) {
                logger.warn("AgentRunner: post-agent room push to cloud failed for room='{}'", roomId, e);
            }
        }

        return result;
    }

    // ============================================================
    // workspace_id overlay - keeps Room and AgentConfig in agreement
    // for the duration of one harness run
    // ============================================================

    /**
     * Captured state needed to restore {@code room.options.workspace} to its
     * pre-overlay shape. Returned from {@link #applyWorkspaceOverlay} and
     * consumed by {@link #restoreWorkspaceOverlay}. Immutable.
     */
    private static final class WorkspaceOverlay {
        private final Room    room;
        private final boolean hadField;
        private final Object  originalWorkspace;

        WorkspaceOverlay(Room room, boolean hadField, Object originalWorkspace) {
            this.room              = room;
            this.hadField          = hadField;
            this.originalWorkspace = originalWorkspace;
        }
    }

    /**
     * If {@code explicitWorkspaceId} is set and differs from the room's current
     * {@code options.workspace.workspace_id}, overlay it for the run. Returns
     * {@code null} when no overlay is needed (no override, or already matching).
     *
     * <p>In-memory mutation only - no DB write. Always pair with
     * {@link #restoreWorkspaceOverlay} in a {@code finally} block.
     */
    private static WorkspaceOverlay applyWorkspaceOverlay(Room room, String explicitWorkspaceId) {
        if (explicitWorkspaceId == null || explicitWorkspaceId.trim().isEmpty()) {
            return null;
        }
        Map<String, Object> opts = room.getOptionsMap();
        boolean hadField = opts.containsKey("workspace");
        Object originalWorkspace = opts.get("workspace");

        // No-op when the room already points at this workspace_id (avoids
        // touching state we don't need to).
        String currentId = extractWorkspaceIdFromOptionField(originalWorkspace);
        if (explicitWorkspaceId.equals(currentId)) {
            return null;
        }

        Map<String, Object> newWorkspace = new HashMap<>();
        newWorkspace.put("workspace_id", explicitWorkspaceId);
        // Best-effort name lookup - not load-bearing for downstream reads (which only
        // care about workspace_id), but keeps the field's shape consistent with how
        // callers usually populate it.
        try {
            Map<String, Object> ws = ModelInferenceLogsUtils.getWorkspaceEntry(explicitWorkspaceId);
            if (ws != null && ws.get("name") != null) {
                newWorkspace.put("name", String.valueOf(ws.get("name")));
            }
        } catch (Exception ignore) {
            // best-effort; absence of name doesn't affect resolution
        }
        opts.put("workspace", newWorkspace);
        room.setOptionsMap(opts);

        logger.info("AgentRunner: workspace overlay applied (explicit='{}' for run; original={})",
                explicitWorkspaceId, currentId == null ? "<unset>" : currentId);
        return new WorkspaceOverlay(room, hadField, originalWorkspace);
    }

    /**
     * Restore the room's {@code options.workspace} field to the value captured
     * before the overlay was applied. No-op when {@code overlay} is null.
     */
    private static void restoreWorkspaceOverlay(Room room, WorkspaceOverlay overlay) {
        if (overlay == null) {
            return;
        }
        Map<String, Object> opts = overlay.room.getOptionsMap();
        if (overlay.hadField) {
            opts.put("workspace", overlay.originalWorkspace);
        } else {
            opts.remove("workspace");
        }
        overlay.room.setOptionsMap(opts);
        logger.debug("AgentRunner: workspace overlay restored");
    }

    /**
     * Extract the {@code workspace_id} from an {@code options.workspace} field
     * which may be (a) absent, (b) a primitive id string, or
     * (c) a {@code {workspace_id, name}} map.
     */
    @SuppressWarnings("unchecked")
    private static String extractWorkspaceIdFromOptionField(Object workspaceField) {
        if (workspaceField == null) return null;
        if (workspaceField instanceof String) {
            String s = ((String) workspaceField).trim();
            return s.isEmpty() ? null : s;
        }
        if (workspaceField instanceof Map) {
            Object id = ((Map<String, Object>) workspaceField).get("workspace_id");
            if (id == null) return null;
            String s = String.valueOf(id).trim();
            return s.isEmpty() ? null : s;
        }
        return null;
    }

    /**
     * Extract and strip the {@link #PARAM_WORKSPACE_ID} key from {@code params}.
     * Returns {@code null} when absent or blank.
     */
    private static String extractExplicitWorkspaceId(Map<String, Object> params) {
        Object raw = params.remove(PARAM_WORKSPACE_ID);
        if (raw == null) {
            return null;
        }
        String s = String.valueOf(raw).trim();
        return s.isEmpty() ? null : s;
    }

    /**
     * Resolve the agent's working directory from paramMap, in this priority:
     *
     * <ol>
     *   <li><b>Container</b>: {@code project=<uuid>} resolves to that project's assets
     *       folder. Otherwise the current room's folder (created if missing) is used.</li>
     *   <li><b>Subdir</b>: {@code subdir=<relative-path>} on paramMap joins under the
     *       container. When absent, falls back to {@code CONFIG_JSON.subdir} on the
     *       effective workspace (per-workspace runtime convention — e.g. app-builder
     *       workspaces pin {@code "client"}). Must be relative, must not escape via
     *       {@code ..}; checked by canonical-path containment.</li>
     *   <li><b>Legacy {@code filePath}</b>: deprecated. Logged at WARN and ignored -
     *       callers must use {@code project} + {@code subdir}.</li>
     * </ol>
     *
     * <p>Consumed keys: {@code subdir}, legacy {@code filePath} are removed from
     * {@code params} (runner-internal only). {@code project} is read but NOT
     * removed — downstream hooks (notably {@code GitCommitAgentHook}) consume
     * it from the same paramMap to resolve the project's git folder.
     *
     * @param effectiveWorkspaceId resolved workspace id (explicit override or
     *                             {@code room.options.workspace.workspace_id});
     *                             {@code null} when the room has no workspace binding.
     *                             Used only for the {@code CONFIG_JSON.subdir} fallback.
     * @throws IllegalArgumentException for unresolvable project, illegal subdir, or
     *                                  containment failure
     */
    private static String resolveWorkingDir(Room room, Map<String, Object> params, String effectiveWorkspaceId) {
        // Legacy filePath - strip + warn, never honor.
        Object legacyFilePath = params.remove(PARAM_FILE_PATH_LEGACY);
        if (legacyFilePath != null && !String.valueOf(legacyFilePath).trim().isEmpty()) {
            logger.warn("AgentRunner: '{}' is deprecated and ignored - use '{}' + '{}' instead. value='{}'",
                    PARAM_FILE_PATH_LEGACY, PARAM_PROJECT, PARAM_SUBDIR, legacyFilePath);
        }

        // 1. Container.
        //    Peek at PARAM_PROJECT (don't remove) — downstream consumers including
        //    GitCommitAgentHook.afterMessage rely on params["project"] to know
        //    which project's git folder to commit against. The model engine
        //    treats unknown keys as no-ops, so leaving the project id in the
        //    map is safe.
        String container;
        String containerLabel;
        Object projectObj = params.get(PARAM_PROJECT);
        if (projectObj != null && !String.valueOf(projectObj).trim().isEmpty()) {
            String projectId = String.valueOf(projectObj).trim();
            container = AssetUtility.getProjectAssetsFolder(projectId);
            if (container == null || container.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "AgentRunner: could not resolve assets folder for project='" + projectId + "'");
            }
            containerLabel = "project=" + projectId;
            logger.info("AgentRunner: container resolved from project='{}' -> '{}'", projectId, container);
        } else {
            container = room.getRoomFolderPath();
            File roomFolder = new File(container);
            if (!roomFolder.exists()) {
                roomFolder.mkdirs();
            }
            containerLabel = "room=" + room.getId();
            logger.info("AgentRunner: container defaulted to room folder='{}' (room={})",
                    container, room.getId());
        }

        // 2. Subdir: paramMap override first, then CONFIG_JSON.subdir for the workspace.
        Object subdirObj = params.remove(PARAM_SUBDIR);
        String subdir = subdirObj == null ? null : String.valueOf(subdirObj).trim();
        if (subdir == null || subdir.isEmpty()) {
            subdir = resolveSubdirFromConfigJson(effectiveWorkspaceId);
            if (subdir != null) {
                logger.info("AgentRunner: subdir='{}' resolved from CONFIG_JSON for workspaceId='{}'",
                        subdir, effectiveWorkspaceId);
            }
        }
        if (subdir == null || subdir.isEmpty()) {
            return container;
        }
        return joinSubdir(container, subdir, containerLabel);
    }

    /**
     * Pull {@code CONFIG_JSON.subdir} for the given workspace, or {@code null} when
     * the workspace has no row / no CONFIG_JSON / no {@code subdir} key. Errors
     * are logged warn and treated as null so a CONFIG_JSON outage never blocks a run.
     */
    private static String resolveSubdirFromConfigJson(String workspaceId) {
        if (workspaceId == null || workspaceId.trim().isEmpty()) {
            return null;
        }
        try {
            org.json.JSONObject cfg = ModelInferenceLogsUtils.getWorkspaceConfigJson(workspaceId);
            if (cfg == null) {
                return null;
            }
            String v = cfg.optString("subdir", null);
            if (v == null) {
                return null;
            }
            v = v.trim();
            return v.isEmpty() ? null : v;
        } catch (Exception e) {
            logger.warn("AgentRunner: CONFIG_JSON.subdir read failed for workspaceId='{}': {}",
                    workspaceId, e.getMessage());
            return null;
        }
    }

    /**
     * Join {@code subdir} under {@code container}, validating that the result stays
     * inside the container after canonicalisation. Rejects absolute paths and
     * {@code ..} escape.
     */
    private static String joinSubdir(String container, String subdir, String containerLabel) {
        if (subdir.startsWith("/") || subdir.startsWith("\\") || subdir.startsWith("~")) {
            throw new IllegalArgumentException(
                    "subdir must be relative (no leading '/', '\\', or '~'); got '" + subdir
                    + "' under " + containerLabel);
        }
        if (subdir.contains("..")) {
            throw new IllegalArgumentException(
                    "subdir must not contain '..' segments; got '" + subdir + "' under " + containerLabel);
        }
        File containerFile = new File(container);
        File joined = new File(containerFile, subdir);
        String containerCanonical;
        String joinedCanonical;
        try {
            containerCanonical = containerFile.getCanonicalPath();
            joinedCanonical    = joined.getCanonicalPath();
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Could not canonicalize working dir for " + containerLabel + " + '" + subdir + "': "
                    + e.getMessage(), e);
        }
        // Allow equality (subdir resolves to container itself) or strict-subdir relationship.
        if (!joinedCanonical.equals(containerCanonical)
                && !joinedCanonical.startsWith(containerCanonical + File.separator)) {
            throw new IllegalArgumentException(
                    "subdir '" + subdir + "' escapes container (" + containerLabel + "): "
                    + joinedCanonical + " is outside " + containerCanonical);
        }
        logger.info("AgentRunner: subdir='{}' joined to container -> '{}'", subdir, joinedCanonical);
        return joinedCanonical;
    }

    /**
     * Build a {@link SandboxPolicy} from pixel-level overrides in {@code paramMap}
     * when any of {@link #PARAM_SANDBOX_READS}, {@link #PARAM_SANDBOX_WRITES}, or
     * {@link #PARAM_SANDBOX_ENFORCE} is present. Consumed keys are removed so
     * they don't bleed into model engine params.
     *
     * <p>Returns {@code null} when no overrides are supplied; harnesses will
     * then build a DIHelper-backed default via
     * {@code AgentSandboxConfig.defaultPolicy(...)}.
     */
    @SuppressWarnings("unchecked")
    private static SandboxPolicy buildSandboxPolicyFromParams(Map<String, Object> params) {
        Object readsObj   = params.remove(PARAM_SANDBOX_READS);
        Object writesObj  = params.remove(PARAM_SANDBOX_WRITES);
        Object enforceObj = params.remove(PARAM_SANDBOX_ENFORCE);

        if (readsObj == null && writesObj == null && enforceObj == null) {
            return null;
        }

        SandboxPolicyBuilder b = SandboxPolicy.builder();
        if (readsObj instanceof java.util.List) {
            for (Object p : (java.util.List<Object>) readsObj) {
                if (p != null) b.withRead(String.valueOf(p));
            }
        }
        if (writesObj instanceof java.util.List) {
            for (Object p : (java.util.List<Object>) writesObj) {
                if (p != null) b.withReadWrite(String.valueOf(p));
            }
        }
        if (enforceObj instanceof String) {
            try {
                b.withEnforcement(EnforcementMode.valueOf(((String) enforceObj).trim().toUpperCase()));
            } catch (IllegalArgumentException e) {
                logger.warn("Invalid sandbox_enforce value '{}' - keeping default", enforceObj);
            }
        }
        return b.build();
    }

    /**
     * Resolves the model/engine ID using a three-tier priority:
     */
    @SuppressWarnings("unchecked")
    private static String resolveModelId(Room room, String fallback) {
        // Tier 1: direct column
        String modelId = room.getModelId();
        if (modelId != null && !modelId.trim().isEmpty()) {
            return modelId.trim();
        }

        // Tier 2: options map some older rooms stored it under "engine"
        Map<String, Object> opts = room.getOptionsMap();
        if (opts != null) {
            for (String key : MODEL_ID_OPTION_KEYS) {
                Object val = opts.get(key);
                if (val instanceof String && !((String) val).trim().isEmpty()) {
                    logger.info("AgentRunner: resolved modelId from options['{}']={}", key, val);
                    return ((String) val).trim();
                }
            }
        }

        // Tier 3: caller-supplied fallback (e.g. engine= param from reactor)
        if (fallback != null && !fallback.trim().isEmpty()) {
            logger.info("AgentRunner: using caller-supplied engine fallback={}", fallback);
            return fallback.trim();
        }

        return null;
    }
}
