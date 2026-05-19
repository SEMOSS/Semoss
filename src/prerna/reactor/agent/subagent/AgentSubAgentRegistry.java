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
package prerna.reactor.agent.subagent;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;
import prerna.reactor.agent.AgentRunner;
import prerna.sablecc2.comm.PixelJobManager;
import prerna.sablecc2.comm.PixelJobRunner;

/**
 * Process-wide registry for SEMOSS subagent runs.
 *
 * <p>Heavy lifting (thread, status, stream queue, interrupt) lives in
 * {@link PixelJobManager}; this registry holds the metadata that lets a parent
 * agent address its children — alias, parent jobId, child room id, target
 * workspace — and emits a {@code subagent-spawned} envelope into the parent
 * stream when a child is launched.
 *
 * <p>Each spawn becomes a normal async {@code RunAgent(...)} pixel job. The
 * returned {@link SpawnResult#getJobId() jobId} is the model-facing handle the
 * orchestrator passes to {@code wait_subagent} / {@code check_subagent}.
 */
public final class AgentSubAgentRegistry {

    private static final Logger logger = LogManager.getLogger(AgentSubAgentRegistry.class);

    private static final String DEFAULT_HARNESS_TYPE = "semoss";

    private static final AgentSubAgentRegistry INSTANCE = new AgentSubAgentRegistry();

    /** jobId -> meta */
    private final Map<String, SubAgentMeta> byJobId = new ConcurrentHashMap<>();

    /** parentJobId -> list of child jobIds (kept in spawn order for predictable tree walks). */
    private final Map<String, List<String>> childrenByParent = new ConcurrentHashMap<>();

    private AgentSubAgentRegistry() {}

    public static AgentSubAgentRegistry getManager() {
        return INSTANCE;
    }

    /**
     * Spawn a subagent run and return its handle.
     *
     * <p>Steps:
     * <ol>
     *   <li>Clone the parent room's options into a freshly created child room
     *       (with {@code PARENT_ROOM_ID = parentRoomId}); for named spawns
     *       (alias + workspaceId), seed the child's {@code options.workspace}
     *       so {@code AgentConfigLoader} loads the child's own CONFIG_JSON.</li>
     *   <li>Build a fresh {@link Insight} owned by the parent's user, registered
     *       in {@link InsightStore} so the async runner can resolve it.</li>
     *   <li>Submit a {@code RunAgent(...)} pixel via {@link PixelJobManager} +
     *       a virtual thread — same path the {@code /runPixelAsync} HTTP entry uses.</li>
     *   <li>Stash {@link SubAgentMeta} keyed by the new jobId and append the
     *       jobId to {@code childrenByParent[parentJobId]}.</li>
     *   <li>Emit a {@code subagent-spawned} envelope into the parent's stream
     *       queue so a frontend can mount a new sub-pane.</li>
     * </ol>
     *
     * @return immutable handle the caller (and the LLM, via tool result) uses
     *         to wait/check the child
     */
    public SpawnResult spawn(SpawnRequest req) {
        if (req == null) {
            throw new IllegalArgumentException("SpawnRequest is required");
        }
        if (req.callerInsight == null) {
            throw new IllegalArgumentException("callerInsight is required");
        }
        if (req.prompt == null || req.prompt.trim().isEmpty()) {
            throw new IllegalArgumentException("prompt is required");
        }
        if (req.parentRoomId == null || req.parentRoomId.trim().isEmpty()) {
            throw new IllegalArgumentException("parentRoomId is required");
        }

        Insight callerInsight = req.callerInsight;
        Room parentRoom       = RoomUtils.getOrLoadRoom(req.parentRoomId, callerInsight);

        // 1. Build child room options. Start from parent's options (carries MCP refs,
        //    vectorDbs, etc.) so anonymous spawns inherit the orchestrator's setup.
        Map<String, Object> clonedOptions = parentRoom.getOptionsMap().isEmpty()
                ? new HashMap<>()
                : new HashMap<>(parentRoom.getOptionsMap());

        // Named spawn: re-seed options.workspace so the child loads its own CONFIG_JSON.
        // Anonymous spawn: leave options.workspace inherited from the parent.
        if (req.workspaceId != null && !req.workspaceId.trim().isEmpty()) {
            Map<String, Object> wsMap = new HashMap<>();
            wsMap.put("workspace_id", req.workspaceId.trim());
            clonedOptions.put("workspace", wsMap);
            // A named subagent has its own MCP set per its workspace CONFIG_JSON.
            // Drop inherited mcp[] so the parent's per-room additions don't bleed in.
            clonedOptions.remove("mcp");
        }

        // Shared-filesystem mode. When the caller asked for inherit_parent_workdir,
        // we record the override on the CHILD ROOM's own options. AgentRunner reads
        // it from there on every run — RunAgent itself stays oblivious. The child
        // keeps its own roomId / jobId / stream / history; only the on-disk
        // working dir is shared with the parent.
        if (req.workingDirOverride != null && !req.workingDirOverride.trim().isEmpty()) {
            clonedOptions.put(AgentRunner.ROOM_OPTION_WORKING_DIR, req.workingDirOverride.trim());
        } else {
            // Isolated mode (default). Strip any inherited override from the parent's
            // options so a clone of a parent that itself has a working_dir set doesn't
            // accidentally propagate that to children that asked for fresh isolation.
            clonedOptions.remove(AgentRunner.ROOM_OPTION_WORKING_DIR);
        }

        // 2. Determine the effective system prompt for the child room.
        //    Override > parent's resolved system prompt.
        String effectiveContext = (req.additionalContext != null && !req.additionalContext.trim().isEmpty())
                ? req.additionalContext
                : parentRoom.getEffectiveSystemPrompt();

        // 3. Create child room. Uses caller's insight for ownership/security.
        String childRoomId = UUID.randomUUID().toString();
        String resolvedEngine = (req.engine != null && !req.engine.trim().isEmpty())
                ? req.engine.trim()
                : parentRoom.getModelId();
        Room childRoom = RoomUtils.createRoomIfNotExists(
                childRoomId,
                callerInsight,
                null,                         // modelEngine - resolved by AgentRunner from room/options/fallback
                req.prompt,                   // question (used for default room name)
                req.workspaceId,
                clonedOptions,
                effectiveContext,
                parentRoom.getProjectId(),
                req.parentRoomId);

        if (childRoom != null && resolvedEngine != null && childRoom.getModelId() == null) {
            childRoom.setModelId(resolvedEngine);
        }

        // 4. Build a fresh Insight for the child. Sharing the parent's Insight would
        //    break thread-safety: the PixelJobRunner mutates ThreadStore + Insight state
        //    (room binding, working dir) and we want subagents to be independent.
        Insight childInsight = new Insight();
        childInsight.setUser(callerInsight.getUser());
        String baseURL = callerInsight.getBaseURL();
        if (baseURL != null) {
            childInsight.setBaseURL(baseURL);
        }
        InsightStore.getInstance().put(childInsight);

        // 5. Build the RunAgent(...) pixel. Use <encode> + URL-encoded prompt so any
        //    quote/special-char content survives the sablecc2 lexer intact.
        String encodedPrompt = URLEncoder.encode(req.prompt, StandardCharsets.UTF_8);
        String harnessType = (req.harnessType != null && !req.harnessType.trim().isEmpty())
                ? req.harnessType.trim()
                : DEFAULT_HARNESS_TYPE;
        StringBuilder pixel = new StringBuilder();
        pixel.append("RunAgent(roomId='").append(escapeSingle(childRoomId)).append("'")
             .append(", command='<encode>").append(encodedPrompt).append("</encode>'")
             .append(", harnessType=\"").append(harnessType).append("\"");
        if (resolvedEngine != null && !resolvedEngine.trim().isEmpty()) {
            pixel.append(", engine='").append(escapeSingle(resolvedEngine)).append("'");
        }
        if (req.workspaceId != null && !req.workspaceId.trim().isEmpty()) {
            pixel.append(", workspaceId='").append(escapeSingle(req.workspaceId)).append("'");
        }
        // Note: working-dir sharing is intentionally NOT a RunAgent pixel arg.
        // It lives on the child room's options and is picked up by
        // AgentRunner.resolveWorkingDir on every run (see clonedOptions wiring above).
        pixel.append(");");

        // 6. Submit the job. Mirrors the path NameServer.runPixelAsync uses for HTTP-triggered
        //    async pixels — same PixelJobManager, same virtual-thread launch.
        String sessionId = ThreadStore.getSessionId();
        String routeId   = ThreadStore.getRouteId();
        PixelJobManager manager = PixelJobManager.getManager();
        PixelJobRunner runner = manager.makeJob(childInsight, sessionId, routeId);
        runner.addPixel(pixel.toString());
        Thread.ofVirtual().start(runner);
        String childJobId = runner.getJobId();

        // 7. Record metadata.
        SubAgentMeta meta = new SubAgentMeta(
                childJobId, req.parentJobId, req.alias, req.workspaceId, childRoomId,
                System.currentTimeMillis());
        byJobId.put(childJobId, meta);
        if (req.parentJobId != null && !req.parentJobId.isBlank()) {
            childrenByParent
                    .computeIfAbsent(req.parentJobId, k -> Collections.synchronizedList(new ArrayList<>()))
                    .add(childJobId);
        }

        // 8. Notify the parent's stream so a frontend can mount a child pane.
        if (req.parentJobId != null && !req.parentJobId.isBlank()) {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("kind", "subagent-spawned");
            data.put("jobId", childJobId);
            data.put("alias", req.alias);
            data.put("workspaceId", req.workspaceId);
            data.put("roomId", childRoomId);
            data.put("spawnedAt", meta.getSpawnedAt());
            Map<String, Object> envelope = new LinkedHashMap<>();
            envelope.put("stream_type", "subagent-spawned");
            envelope.put("data", data);
            logger.info("AgentSubAgentRegistry: emitting subagent-spawned envelope to parentJobId={} childJobId={} alias={}",
                    req.parentJobId, childJobId, req.alias);
            manager.addStreamOut(req.parentJobId, envelope);
            logger.info("AgentSubAgentRegistry: emitted subagent-spawned envelope (parentJobId={} childJobId={})",
                    req.parentJobId, childJobId);
        } else {
            logger.warn("AgentSubAgentRegistry: SKIPPING subagent-spawned envelope — parentJobId is null/blank (childJobId={} alias={})",
                    childJobId, req.alias);
        }

        logger.info("AgentSubAgentRegistry: spawned subagent jobId={} parentJobId={} alias={} workspaceId={} roomId={}",
                childJobId, req.parentJobId, req.alias, req.workspaceId, childRoomId);
        return new SpawnResult(childJobId, childRoomId, req.alias);
    }

    /** Look up the metadata for a spawned subagent. {@code null} when the jobId is unknown. */
    public SubAgentMeta lookup(String jobId) {
        if (jobId == null) return null;
        return byJobId.get(jobId);
    }

    /**
     * List the child jobIds spawned by {@code parentJobId}, in spawn order. Returns an
     * empty list when the parent has no recorded children.
     */
    public List<String> childrenOf(String parentJobId) {
        if (parentJobId == null) return Collections.emptyList();
        List<String> kids = childrenByParent.get(parentJobId);
        if (kids == null) return Collections.emptyList();
        synchronized (kids) {
            return new ArrayList<>(kids);
        }
    }

    /** Cooperatively cancel every subagent (recursively) spawned by {@code parentJobId}. Returns the count signaled. */
    public int cascadeCancel(String parentJobId) {
        if (parentJobId == null || parentJobId.isBlank()) return 0;
        List<String> kids = childrenOf(parentJobId);
        if (kids.isEmpty()) return 0;
        int count = 0;
        for (String childJobId : kids) {
            // depth-first so grandchildren get signaled before their parent finishes
            count += cascadeCancel(childJobId);
            try {
                prerna.sablecc2.comm.JobStreamEnvelopes.jobCancelled(childJobId, "parent-cancelled");
            } catch (Exception streamErr) {
                logger.warn("cascadeCancel: stream emit failed childJobId={}: {}", childJobId, streamErr.toString());
            }
            try {
                PixelJobManager.InterruptResult ir = PixelJobManager.getManager().interruptThread(childJobId);
                logger.info("cascadeCancel: interruptThread(childJobId={}) -> {}", childJobId, ir);
                count++;
            } catch (Exception interruptErr) {
                logger.warn("cascadeCancel: interrupt failed childJobId={}: {}", childJobId, interruptErr.toString());
            }
        }
        return count;
    }

    private static String escapeSingle(String s) {
        return s == null ? "" : s.replace("'", "\\'");
    }
}
