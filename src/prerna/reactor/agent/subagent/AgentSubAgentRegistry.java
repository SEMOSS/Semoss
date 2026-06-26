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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.f4b6a3.uuid.alt.GUID;

import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.AgentHarnessRegistry;
import prerna.reactor.agent.AgentRunner;
import prerna.reactor.agent.config.AgentConfig;
import prerna.reactor.agent.exceptions.AgentMaxSpawnDepthException;
import prerna.reactor.agent.exceptions.AgentSpawnBudgetExhaustedException;
import prerna.reactor.agent.run.AgentRuntimeManager;
import prerna.reactor.agent.run.RunAgentRequest;
import prerna.reactor.agent.run.RunAgentResult;
import prerna.sablecc2.comm.PixelJobManager;

/**
 * Process-wide registry for SEMOSS subagent runs.
 *
 * <p>Heavy lifting (queueing, status, execution, interrupt) lives in
 * {@link AgentRuntimeManager}; this registry holds the metadata that lets a parent
 * agent address its children - alias, parent jobId, child room id, target
 * workspace - and emits a {@code subagent-spawned} envelope into the parent
 * stream when a child is launched.
 *
 * <p>Each spawn becomes a normal async {@code AgentRun}. The
 * returned {@link SpawnResult#getJobId() jobId} is the model-facing handle the
 * orchestrator passes to {@code WaitForSubAgent} / {@code CheckSubAgentStatus}.
 */
public final class AgentSubAgentRegistry {

    private static final Logger logger = LogManager.getLogger(AgentSubAgentRegistry.class);

    private static final String DEFAULT_HARNESS_TYPE = "semoss";

    private static final AgentSubAgentRegistry INSTANCE = new AgentSubAgentRegistry();

    /** jobId -> meta */
    private final Map<String, SubAgentMeta> byJobId = new ConcurrentHashMap<>();

    /** parentJobId -> list of child jobIds (kept in spawn order for predictable tree walks). */
    private final Map<String, List<String>> childrenByParent = new ConcurrentHashMap<>();

    // rootJobId -> spawn policy + remaining-budget counter shared by the whole tree.
    private final Map<String, RootSpawnContext> rootContextByJobId = new ConcurrentHashMap<>();

    // childJobId -> root context snapshot for that child's tree. Populated on spawn so
    // that descendants can find the shared budget counter even after the root harness has
    // called unregisterRoot (e.g. "spawn now, wait later" flows where the root run returns
    // before all children finish their own spawning).
    private final Map<String, RootSpawnContext> rootCtxByChildJobId = new ConcurrentHashMap<>();

    private AgentSubAgentRegistry() {}

    // Shared by every descendant in the tree so the total stays bounded.
    private static final class RootSpawnContext {
        final AgentConfig.SubAgentSpawnPolicy policy;
        final AtomicInteger                   spawnBudgetRemaining;
        RootSpawnContext(AgentConfig.SubAgentSpawnPolicy policy) {
            this.policy = policy;
            this.spawnBudgetRemaining = new AtomicInteger(policy.getMaxSubagentsPerRun());
        }
    }

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
     *   <li>Build a fresh {@link Insight} owned by the parent's user.</li>
     *   <li>Submit an async child {@code AgentRun} via {@link AgentRuntimeManager}.</li>
     *   <li>Stash {@link SubAgentMeta} keyed by the new runId/jobId and append the
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

        // Spawn-policy enforcement — depth + per-root budget.
        SubAgentMeta parentMeta = req.parentJobId == null ? null : byJobId.get(req.parentJobId);
        int parentDepth = parentMeta == null ? 0 : parentMeta.getSpawnDepth();
        int childDepth  = parentDepth + 1;
        RootSpawnContext rootCtx = lookupRootContextForJob(req.parentJobId);
        AgentConfig.SubAgentSpawnPolicy policy = rootCtx != null
                ? rootCtx.policy
                : AgentConfig.SubAgentSpawnPolicy.defaults();

        if (parentMeta != null && rootCtx == null) {
            throw new IllegalStateException(
                    "Missing root spawn context for subagent tree parentJobId=" + req.parentJobId);
        }

        // Pixel-initiated spawns (parentMeta == null) bypass SemossAgentHarness and never
        // call registerRoot. Auto-register defaults keyed by the caller's job ID so
        // maxSubagentsPerRun is tracked for these callers too.
        if (rootCtx == null && req.parentJobId != null && !req.parentJobId.isBlank()) {
            registerRoot(req.parentJobId, policy);
            rootCtx = lookupRootContextForJob(req.parentJobId);
        }

        if (childDepth > policy.getMaxSubagentDepth()) {
            logger.warn(
                "AgentSubAgentRegistry: spawn REJECTED — childDepth={} > maxSubagentDepth={} (parentJobId={})",
                childDepth, policy.getMaxSubagentDepth(), req.parentJobId);
            throw new AgentMaxSpawnDepthException(childDepth, policy.getMaxSubagentDepth());
        }
        boolean rootBudgetClaimed = false;
        if (rootCtx != null) {
            int remaining = rootCtx.spawnBudgetRemaining.decrementAndGet();
            if (remaining < 0) {
                rootCtx.spawnBudgetRemaining.incrementAndGet();   // restore — we did not spawn
                logger.warn(
                    "AgentSubAgentRegistry: spawn REJECTED — per-root budget exhausted (max={}, parentJobId={})",
                    policy.getMaxSubagentsPerRun(), req.parentJobId);
                throw new AgentSpawnBudgetExhaustedException(policy.getMaxSubagentsPerRun());
            }
            rootBudgetClaimed = true;
        }

        String childJobIdForCleanup = null;
        boolean childStarted = false;
        try {
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
            // it from there on every run - RunAgent itself stays oblivious. The child
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

            // 3. Create child room. It is always separate from the parent room, while
            //    optional working-dir inheritance is handled through child room options.
            String childRunId = GUID.v7().toUUID().toString();
            String childRoomId = childRunId;
            childJobIdForCleanup = childRunId;
            clonedOptions.put("subagent_parent_room_id", req.parentRoomId);
            if (req.parentJobId != null && !req.parentJobId.isBlank()) {
                clonedOptions.put("subagent_parent_run_id", req.parentJobId);
            }
            if (req.alias != null && !req.alias.isBlank()) {
                clonedOptions.put("subagent_alias", req.alias);
            }
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
            //    break thread-safety because the worker mutates ThreadStore + Insight state.
            Insight childInsight = new Insight();
            childInsight.setUser(callerInsight.getUser());
            String baseURL = callerInsight.getBaseURL();
            if (baseURL != null) {
                childInsight.setBaseURL(baseURL);
            }
            childInsight.setProjectId(callerInsight.getProjectId());
            childInsight.setContextProjectId(callerInsight.getContextProjectId());

            // 5. Submit the child as a durable AgentRun. The run id is pre-generated so
            //    subagent depth metadata is visible before the worker can start executing.
            String harnessType = (req.harnessType != null && !req.harnessType.trim().isEmpty())
                    ? req.harnessType.trim()
                    : DEFAULT_HARNESS_TYPE;
            if (AgentHarnessRegistry.get(harnessType) == null) {
                throw new IllegalArgumentException("Unknown harnessType: " + harnessType);
            }

            // 6. Record metadata before submitting the child run. AgentRunner resolves
            //    ctx.spawnDepth from this registry using ThreadStore.getJobId(), so the
            //    child must be able to find its metadata as soon as RunAgent begins.
            SubAgentMeta meta = new SubAgentMeta(
                    childRunId, req.parentJobId, req.alias, req.workspaceId, childRoomId,
                    System.currentTimeMillis(), childDepth);
            byJobId.put(childRunId, meta);
            if (rootCtx != null) {
                rootCtxByChildJobId.put(childRunId, rootCtx);
            }
            if (req.parentJobId != null && !req.parentJobId.isBlank()) {
                childrenByParent
                        .computeIfAbsent(req.parentJobId, k -> Collections.synchronizedList(new ArrayList<>()))
                        .add(childRunId);
            }
            RunAgentRequest runRequest = new RunAgentRequest(childRoomId, req.prompt, resolvedEngine, harnessType,
                    req.workspaceId, AgentRunContext.DEFAULT_MAX_TURNS, AgentRunContext.DEFAULT_MAX_REFLECTIONS,
                    null, null, null, null, childInsight);
            RunAgentResult runResult = AgentRuntimeManager.get().runWithId(childRunId, runRequest);
            childStarted = true;

            // 7. Notify the parent's stream so a frontend can mount a child pane.
            if (req.parentJobId != null && !req.parentJobId.isBlank()) {
                Map<String, Object> data = new LinkedHashMap<>();
                data.put("kind", "subagent-spawned");
                data.put("jobId", childRunId);
                data.put("runId", childRunId);
                data.put("alias", req.alias);
                data.put("workspaceId", req.workspaceId);
                data.put("roomId", childRoomId);
                data.put("spawnedAt", meta.getSpawnedAt());
                data.put("status", runResult.getStatus() == null ? null : runResult.getStatus().name());
                Map<String, Object> envelope = new LinkedHashMap<>();
                envelope.put("stream_type", "subagent-spawned");
                envelope.put("data", data);
                logger.info("AgentSubAgentRegistry: emitting subagent-spawned envelope to parentJobId={} childJobId={} alias={}",
                        req.parentJobId, childRunId, req.alias);
                PixelJobManager.getManager().addStreamOut(req.parentJobId, envelope);
                logger.info("AgentSubAgentRegistry: emitted subagent-spawned envelope (parentJobId={} childJobId={})",
                        req.parentJobId, childRunId);
            } else {
                logger.warn("AgentSubAgentRegistry: SKIPPING subagent-spawned envelope - parentJobId is null/blank (childJobId={} alias={})",
                        childRunId, req.alias);
            }

            logger.info("AgentSubAgentRegistry: spawned subagent jobId={} parentJobId={} alias={} workspaceId={} roomId={}",
                    childRunId, req.parentJobId, req.alias, req.workspaceId, childRoomId);
            return new SpawnResult(childRunId, childRoomId, req.alias, runResult.getStatus());
        } catch (RuntimeException | Error e) {
            if (!childStarted && childJobIdForCleanup != null) {
                byJobId.remove(childJobIdForCleanup);
                rootCtxByChildJobId.remove(childJobIdForCleanup);
                removeChildLink(req.parentJobId, childJobIdForCleanup);
            }
            if (rootBudgetClaimed && !childStarted) {
                rootCtx.spawnBudgetRemaining.incrementAndGet();
            }
            throw e;
        }
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

    private void removeChildLink(String parentJobId, String childJobId) {
        if (parentJobId == null || parentJobId.isBlank() || childJobId == null) return;
        List<String> kids = childrenByParent.get(parentJobId);
        if (kids == null) return;
        synchronized (kids) {
            kids.remove(childJobId);
            if (kids.isEmpty()) {
                childrenByParent.remove(parentJobId, kids);
            }
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
                SubAgentMeta meta = lookup(childJobId);
                boolean cancelled = AgentRuntimeManager.get().cancelRun(childJobId,
                        meta == null ? null : meta.getChildRoomId(), "parent-cancelled");
                logger.info("cascadeCancel: cancelRun(childJobId={}) -> {}", childJobId, cancelled);
                if (cancelled) {
                    count++;
                }
            } catch (Exception interruptErr) {
                logger.warn("cascadeCancel: interrupt failed childJobId={}: {}", childJobId, interruptErr.toString());
            }
        }
        return count;
    }

    // Root spawn-policy registry — called by the harness at run boundaries.

    /**
     * Harness calls this at the top of a root run.
     *
     * @return {@code true} only when this call created the root context; callers should
     *         unregister only in that case.
     */
    public boolean registerRoot(String rootJobId, AgentConfig.SubAgentSpawnPolicy policy) {
        if (rootJobId == null || rootJobId.isBlank() || policy == null) return false;
        return rootContextByJobId.putIfAbsent(rootJobId, new RootSpawnContext(policy)) == null;
    }

    /** No-op on unknown id. */
    public void unregisterRoot(String rootJobId) {
        if (rootJobId == null || rootJobId.isBlank()) return;
        rootContextByJobId.remove(rootJobId);
    }

    /**
     * Runtime/UI notification that a child finished. The canonical model-facing
     * result path remains WaitForSubAgent; this only wakes listeners watching the
     * parent job stream.
     */
    public void emitSubAgentCompleted(String childJobId, String finalText) {
        if (childJobId == null || childJobId.isBlank()) return;
        SubAgentMeta meta = byJobId.get(childJobId);
        if (meta == null || meta.getParentJobId() == null || meta.getParentJobId().isBlank()) return;

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("kind", "subagent-completed");
        data.put("jobId", childJobId);
        data.put("childJobId", childJobId);
        data.put("parentJobId", meta.getParentJobId());
        data.put("status", "succeeded");
        data.put("result", finalText);
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("stream_type", "subagent-completed");
        envelope.put("data", data);
        PixelJobManager.getManager().addStreamOut(meta.getParentJobId(), envelope);
        byJobId.remove(childJobId);
        rootCtxByChildJobId.remove(childJobId);
        removeChildLink(meta.getParentJobId(), childJobId);
    }

    /** Returns 0 for any jobId that has no recorded parent (root, or unregistered spawn). */
    public int getDepthForJob(String jobId) {
        if (jobId == null) return 0;
        SubAgentMeta meta = byJobId.get(jobId);
        return meta == null ? 0 : meta.getSpawnDepth();
    }

    /** {@code null} = no registered root context; caller should fall back to defaults. */
    public AgentConfig.SubAgentSpawnPolicy lookupSpawnPolicyForJob(String jobId) {
        RootSpawnContext ctx = lookupRootContextForJob(jobId);
        return ctx == null ? null : ctx.policy;
    }

    // Walk from jobId up to the root; null if the chain never reaches a registered root.
    private RootSpawnContext lookupRootContextForJob(String jobId) {
        String cursor = jobId;
        while (cursor != null && !cursor.isBlank()) {
            RootSpawnContext direct = rootContextByJobId.get(cursor);
            if (direct != null) return direct;
            // Child-to-root snapshot: persists after the root harness calls unregisterRoot,
            // so deferred spawns from still-running children resolve the shared budget.
            RootSpawnContext snapshot = rootCtxByChildJobId.get(cursor);
            if (snapshot != null) return snapshot;
            SubAgentMeta m = byJobId.get(cursor);
            if (m == null) return null;            // cursor is a root with no registered context
            cursor = m.getParentJobId();
        }
        return null;
    }
}
