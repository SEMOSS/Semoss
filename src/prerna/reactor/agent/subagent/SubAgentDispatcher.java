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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.engine.impl.model.Room;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.agent.config.SubAgentSpec;
import prerna.reactor.agent.runtime.AgentCancelledException;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.comm.PixelJobManager;
import prerna.sablecc2.comm.PixelJobStatus;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Tool-call dispatch for synthesized subagent tools.
 *
 * <p>Called from the semoss harness when a tool name matches either a named
 * {@link SubAgentSpec} alias or one of the {@link SubAgentToolSynthesizer}
 * built-ins ({@code spawn_subagent}, {@code check_subagent},
 * {@code wait_subagent}).
 *
 * <p>All four entry points return a string suitable for handing directly back
 * to the model as the tool result.
 */
public final class SubAgentDispatcher {

    private static final Logger logger = LogManager.getLogger(SubAgentDispatcher.class);

    private static final Gson GSON = new Gson();

    /** Default wait timeout when the LLM omits {@code timeoutSec}. */
    public static final int DEFAULT_WAIT_TIMEOUT_SEC = 300;

    /** Poll interval while blocking inside {@link #wait(String, int)}. */
    private static final long WAIT_POLL_MS = 250L;

    private SubAgentDispatcher() {}

    /**
     * Spawn a named subagent (alias resolved via {@code spec.workspaceId}). Returns a
     * JSON string {@code {"jobId":..., "roomId":..., "alias":...}} for the LLM.
     */
    public static String spawnNamed(SubAgentSpec spec, Map<String, Object> args,
            Room parentRoom, Insight callerInsight) {
        return spawnNamed(spec, args, parentRoom, callerInsight, null);
    }

    /**
     * Spawn a named subagent with an explicit parent jobId. Prefer this overload
     * when the call originates from inside the harness's parallel tool pool -
     * the worker threads have a fresh empty {@link ThreadStore} so a fallback to
     * {@code ThreadStore.getJobId()} would yield null and silently skip the
     * parent-stream {@code subagent-spawned} envelope.
     */
    public static String spawnNamed(SubAgentSpec spec, Map<String, Object> args,
            Room parentRoom, Insight callerInsight, String parentJobId) {
        String prompt  = stringArg(args, "prompt");
        String context = stringArg(args, "context");
        boolean inheritParentWorkdir = boolArg(args, "inherit_parent_workdir");
        if (prompt == null) {
            return GSON.toJson(error("Missing required argument 'prompt' for named subagent '"
                    + spec.getAlias() + "'"));
        }

        SpawnRequest req = new SpawnRequest();
        req.parentJobId       = resolveParentJobId(parentJobId);
        logger.info("SubAgentDispatcher.spawnNamed: alias={} parentJobId={} (explicit={}) parentRoomId={} inheritWorkdir={}",
                spec.getAlias(), req.parentJobId, parentJobId, parentRoom.getId(), inheritParentWorkdir);
        req.parentRoomId      = parentRoom.getId();
        req.alias             = spec.getAlias();
        req.workspaceId       = spec.getWorkspaceId();
        req.prompt            = prompt;
        req.additionalContext = context;
        req.callerInsight     = callerInsight;
        if (inheritParentWorkdir) {
            req.workingDirOverride = parentRoom.getRoomFolderPath();
        }

        SpawnResult result = AgentSubAgentRegistry.getManager().spawn(req);
        return GSON.toJson(toMap(result));
    }

    /**
     * Spawn an anonymous (cloned) subagent. Returns the same JSON shape as
     * {@link #spawnNamed} with {@code alias=null}.
     */
    public static String spawnAnonymous(Map<String, Object> args, Room parentRoom, Insight callerInsight) {
        return spawnAnonymous(args, parentRoom, callerInsight, null);
    }

    /**
     * Anonymous spawn with explicit parent jobId - same rationale as
     * {@link #spawnNamed(SubAgentSpec, Map, Room, Insight, String)}.
     */
    public static String spawnAnonymous(Map<String, Object> args, Room parentRoom, Insight callerInsight,
            String parentJobId) {
        String prompt  = stringArg(args, "prompt");
        String context = stringArg(args, "context");
        boolean inheritParentWorkdir = boolArg(args, "inherit_parent_workdir");
        if (prompt == null) {
            return GSON.toJson(error("Missing required argument 'prompt' for spawn_subagent"));
        }

        SpawnRequest req = new SpawnRequest();
        req.parentJobId       = resolveParentJobId(parentJobId);
        logger.info("SubAgentDispatcher.spawnAnonymous: parentJobId={} (explicit={}) parentRoomId={} inheritWorkdir={}",
                req.parentJobId, parentJobId, parentRoom.getId(), inheritParentWorkdir);
        req.parentRoomId      = parentRoom.getId();
        req.alias             = null;
        req.workspaceId       = null;
        req.prompt            = prompt;
        req.additionalContext = context;
        req.callerInsight     = callerInsight;
        if (inheritParentWorkdir) {
            req.workingDirOverride = parentRoom.getRoomFolderPath();
        }

        SpawnResult result = AgentSubAgentRegistry.getManager().spawn(req);
        return GSON.toJson(toMap(result));
    }

    /**
     * Prefer the explicit parent jobId (captured on the caller's thread, where
     * {@link ThreadStore} is valid). Fall back to {@code ThreadStore.getJobId()}
     * only when the caller didn't pass one - preserves callers that aren't yet
     * on the explicit-jobId path.
     */
    private static String resolveParentJobId(String explicitParentJobId) {
        if (explicitParentJobId != null && !explicitParentJobId.isBlank()) {
            return explicitParentJobId;
        }
        return ThreadStore.getJobId();
    }

    /**
     * Non-blocking status peek. Returns {@code {status, eventCount}} JSON; the
     * {@code lastEventTimestamp} field is omitted when no events have been emitted.
     */
    public static String check(String jobId) {
        if (jobId == null || jobId.isBlank()) {
            return GSON.toJson(error("Missing required argument 'jobId' for check_subagent"));
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("jobId", jobId);
        out.put("status", PixelJobManager.getManager().getStatus(jobId));
        SubAgentMeta meta = AgentSubAgentRegistry.getManager().lookup(jobId);
        if (meta != null) {
            out.put("alias", meta.getAlias());
            out.put("workspaceId", meta.getWorkspaceId());
            out.put("roomId", meta.getChildRoomId());
            out.put("spawnedAt", meta.getSpawnedAt());
        }
        return GSON.toJson(out);
    }

    /**
     * Block until {@code jobId} is in a terminal status (COMPLETE / PROGRESS_COMPLETE
     * / ERROR / CANCELED) or {@code timeoutSec} elapses. On success returns the
     * subagent's final-text string. On timeout returns a JSON error object.
     */
    public static String wait(String jobId, int timeoutSec) {
        if (jobId == null || jobId.isBlank()) {
            return GSON.toJson(error("Missing required argument 'jobId' for wait_subagent"));
        }
        if (timeoutSec <= 0) {
            timeoutSec = DEFAULT_WAIT_TIMEOUT_SEC;
        }
        long deadline = System.currentTimeMillis() + (timeoutSec * 1000L);
        PixelJobManager manager = PixelJobManager.getManager();
        while (true) {
            String status = manager.getStatus(jobId);
            if (isTerminal(status)) {
                if (PixelJobStatus.ERROR.getValue().equals(status)) {
                    Map<String, Object> errorOut = error("Subagent job ended in ERROR status");
                    errorOut.put("jobId", jobId);
                    return GSON.toJson(errorOut);
                }
                if (PixelJobStatus.CANCELED.getValue().equals(status)) {
                    Map<String, Object> cancelOut = error("Subagent job was canceled");
                    cancelOut.put("jobId", jobId);
                    return GSON.toJson(cancelOut);
                }
                return collectFinalText(jobId);
            }
            if (System.currentTimeMillis() >= deadline) {
                Map<String, Object> timeout = new LinkedHashMap<>();
                timeout.put("error", "timeout");
                timeout.put("jobId", jobId);
                timeout.put("timeoutSec", timeoutSec);
                return GSON.toJson(timeout);
            }
            try {
                Thread.sleep(WAIT_POLL_MS);
            } catch (InterruptedException ie) {
                // propagate as cancel rather than returning a phantom tool-result string
                Thread.currentThread().interrupt();
                throw new AgentCancelledException("wait_subagent interrupted while polling jobId=" + jobId);
            }
        }
    }

    private static String collectFinalText(String jobId) {
        try {
            PixelRunner runner = PixelJobManager.getManager().getOutput(jobId);
            if (runner == null) {
                Map<String, Object> err = error("Subagent output unavailable");
                err.put("jobId", jobId);
                return GSON.toJson(err);
            }
            // Mirror RunAgentReactor: the final text is the value of the last CONST_STRING
            // NounMetadata in the run's results list. RunAgent returns a single string result,
            // so the last value typically is the agent's final text.
            java.util.List<NounMetadata> results = runner.getResults();
            if (results != null && !results.isEmpty()) {
                for (int i = results.size() - 1; i >= 0; i--) {
                    Object val = results.get(i).getValue();
                    if (val instanceof CharSequence) {
                        return val.toString();
                    }
                }
                // No string result - return JSON of the last result's value.
                Object lastVal = results.get(results.size() - 1).getValue();
                return GSON.toJson(lastVal == null ? new LinkedHashMap<>() : lastVal);
            }
            return GSON.toJson(new LinkedHashMap<>());
        } catch (Exception e) {
            logger.warn("SubAgentDispatcher: failed to collect final text for jobId={}: {}", jobId, e.getMessage());
            Map<String, Object> err = error("Failed to collect subagent result: " + e.getMessage());
            err.put("jobId", jobId);
            return GSON.toJson(err);
        }
    }

    private static boolean isTerminal(String status) {
        return PixelJobStatus.COMPLETE.getValue().equals(status)
                || PixelJobStatus.PROGRESS_COMPLETE.getValue().equals(status)
                || PixelJobStatus.ERROR.getValue().equals(status)
                || PixelJobStatus.CANCELED.getValue().equals(status);
    }

    private static String stringArg(Map<String, Object> args, String key) {
        if (args == null) return null;
        Object v = args.get(key);
        if (v == null) return null;
        String s = String.valueOf(v).trim();
        return s.isEmpty() ? null : s;
    }

    /** Reads a boolean tool-call arg. Accepts JSON boolean true/false or string "true"/"false". */
    private static boolean boolArg(Map<String, Object> args, String key) {
        if (args == null) return false;
        Object v = args.get(key);
        if (v == null) return false;
        if (v instanceof Boolean) return (Boolean) v;
        return Boolean.parseBoolean(String.valueOf(v).trim());
    }

    private static Map<String, Object> error(String msg) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("error", msg);
        return out;
    }

    private static Map<String, Object> toMap(SpawnResult result) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("jobId",  result.getJobId());
        out.put("roomId", result.getRoomId());
        if (result.getAlias() != null) {
            out.put("alias", result.getAlias());
        }
        return out;
    }
}
