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
import prerna.reactor.agent.AgentRunTarget;
import prerna.reactor.agent.AgentRunner;
import prerna.reactor.agent.config.SubAgentSpec;
import prerna.reactor.agent.exceptions.AgentCancelledException;
import prerna.reactor.agent.run.AgentRunStatus;
import prerna.reactor.agent.run.AgentRuntimeManager;

/**
 * Tool-call dispatch for synthesized subagent tools.
 *
 * <p>Called from the semoss harness when a tool name matches either a named
 * {@link SubAgentSpec} alias or one of the {@link SubAgentToolSynthesizer}
 * built-ins ({@code SpawnSubAgent}, {@code CheckSubAgentStatus},
 * {@code WaitForSubAgent}).
 *
 * <p>All entry points return a string suitable for handing directly back
 * to the model as the tool result.
 */
public final class SubAgentDispatcher {

    private static final Logger logger = LogManager.getLogger(SubAgentDispatcher.class);

    private static final Gson GSON = new Gson();

    /** Default wait timeout when the LLM omits {@code timeoutSec}. */
    public static final int DEFAULT_WAIT_TIMEOUT_SEC = 300;

    private SubAgentDispatcher() {}

    /**
     * Spawn a named subagent (alias resolved via {@code spec.workspaceId}). Returns a
     * JSON string {@code {"jobId":..., "runId":..., "roomId":..., "status":..., "alias":...}} for the LLM.
     */
    public static String spawnNamed(SubAgentSpec spec, Map<String, Object> args,
            Room parentRoom, Insight callerInsight) {
        return spawnNamed(spec, args, parentRoom, callerInsight, null,
                parentRoom.getRoomOrWorkspaceSystemPrompt());
    }

    /**
     * Spawn a named subagent with an explicit parent jobId when the parent room's
     * instructions are stable. In-flight harness callers must use the overload that
     * also accepts {@code parentAuthoredSystemPrompt}.
     */
    public static String spawnNamed(SubAgentSpec spec, Map<String, Object> args,
            Room parentRoom, Insight callerInsight, String parentJobId) {
        return spawnNamed(spec, args, parentRoom, callerInsight, parentJobId,
                parentRoom.getRoomOrWorkspaceSystemPrompt());
    }

    /**
     * Harness-safe named spawn. The caller supplies the parent's authored system prompt
     * because the live room may temporarily contain a composed runtime prompt.
     */
    public static String spawnNamed(SubAgentSpec spec, Map<String, Object> args,
            Room parentRoom, Insight callerInsight, String parentJobId, String parentAuthoredSystemPrompt) {
        return spawnNamed(spec, args, parentRoom, callerInsight, parentJobId, parentAuthoredSystemPrompt, null);
    }

    /**
     * Harness-safe named spawn with the parent's already-resolved working directory.
     */
    public static String spawnNamed(SubAgentSpec spec, Map<String, Object> args,
            Room parentRoom, Insight callerInsight, String parentJobId, String parentAuthoredSystemPrompt,
            AgentRunTarget parentTarget) {
        String prompt  = stringArg(args, "prompt");
        boolean inheritParentWorkdir = boolArg(args, "inherit_parent_workdir");
        if (prompt == null) {
            return GSON.toJson(error("Missing required argument 'prompt' for named subagent '"
                    + spec.getAlias() + "'"));
        }
        // Named subagents own their system prompt through workspace configuration. Fold
        // legacy callers' context value into the ordinary task input instead of allowing
        // it to replace the named agent's authored system prompt.
        String legacyContext = stringArg(args, "context");
        if (legacyContext != null) {
            prompt = prompt + "\n\nAdditional task context:\n" + legacyContext;
        }

        SpawnRequest req = new SpawnRequest();
        req.parentJobId          = resolveParentJobId(parentJobId);
        logger.info("SubAgentDispatcher.spawnNamed: alias={} parentJobId={} (explicit={}) parentRoomId={} inheritWorkdir={}",
                spec.getAlias(), req.parentJobId, parentJobId, parentRoom.getId(), inheritParentWorkdir);
        req.parentRoomId         = parentRoom.getId();
        req.alias                = spec.getAlias();
        req.workspaceId          = spec.getWorkspaceId();
        req.prompt               = prompt;
        req.parentAuthoredSystemPrompt = parentAuthoredSystemPrompt;
        req.callerInsight        = callerInsight;
		if (inheritParentWorkdir) {
			req.inheritedTarget = parentTarget;
			req.workingDirOverride = resolveInheritedWorkingDir(parentTarget, parentRoom);
		}

        SpawnResult result = AgentSubAgentRegistry.getManager().spawn(req);
        return GSON.toJson(toMap(result));
    }

    /**
     * Spawn an anonymous (cloned) subagent. Returns the same JSON shape as
     * {@link #spawnNamed} with {@code alias=null}.
     */
    public static String spawnAnonymous(Map<String, Object> args, Room parentRoom, Insight callerInsight) {
        return spawnAnonymous(args, parentRoom, callerInsight, null,
                parentRoom.getRoomOrWorkspaceSystemPrompt());
    }

    /**
     * Anonymous spawn with an explicit parent jobId when the parent room's
     * instructions are stable. In-flight harness callers must use the overload that
     * also accepts {@code parentAuthoredSystemPrompt}.
     */
    public static String spawnAnonymous(Map<String, Object> args, Room parentRoom, Insight callerInsight,
            String parentJobId) {
        return spawnAnonymous(args, parentRoom, callerInsight, parentJobId,
                parentRoom.getRoomOrWorkspaceSystemPrompt());
    }

    /**
     * Harness-safe anonymous spawn. The caller supplies the parent's authored system prompt
     * because the live room may temporarily contain a composed runtime prompt.
     */
    public static String spawnAnonymous(Map<String, Object> args, Room parentRoom, Insight callerInsight,
            String parentJobId, String parentAuthoredSystemPrompt) {
        return spawnAnonymous(args, parentRoom, callerInsight, parentJobId, parentAuthoredSystemPrompt, null);
    }

    /**
     * Harness-safe anonymous spawn with the parent's already-resolved working directory.
     */
    public static String spawnAnonymous(Map<String, Object> args, Room parentRoom, Insight callerInsight,
            String parentJobId, String parentAuthoredSystemPrompt, AgentRunTarget parentTarget) {
        String prompt  = stringArg(args, "prompt");
        String context = stringArg(args, "context");
        boolean inheritParentWorkdir = boolArg(args, "inherit_parent_workdir");
        if (prompt == null) {
            return GSON.toJson(error("Missing required argument 'prompt' for SpawnSubAgent"));
        }

        SpawnRequest req = new SpawnRequest();
        req.parentJobId          = resolveParentJobId(parentJobId);
        logger.info("SubAgentDispatcher.spawnAnonymous: parentJobId={} (explicit={}) parentRoomId={} inheritWorkdir={}",
                req.parentJobId, parentJobId, parentRoom.getId(), inheritParentWorkdir);
        req.parentRoomId         = parentRoom.getId();
        req.alias                = null;
        req.workspaceId          = null;
        req.prompt               = prompt;
        req.additionalContext    = context;
        req.parentAuthoredSystemPrompt = parentAuthoredSystemPrompt;
        req.callerInsight        = callerInsight;
		if (inheritParentWorkdir) {
			req.inheritedTarget = parentTarget;
			req.workingDirOverride = resolveInheritedWorkingDir(parentTarget, parentRoom);
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
     * Prefer the active run's resolved target over room configuration. The latter
     * remains the fallback for direct Pixel callers that lack an agent context.
     */
    private static String resolveInheritedWorkingDir(AgentRunTarget parentTarget, Room parentRoom) {
        if (parentTarget != null && parentTarget.getWorkingDirectory() != null
                && !parentTarget.getWorkingDirectory().trim().isEmpty()) {
            return parentTarget.getWorkingDirectory().trim();
        }
        Object roomWorkingDir = parentRoom.getOptionsMap() != null
                ? parentRoom.getOptionsMap().get(AgentRunner.ROOM_OPTION_WORKING_DIR)
                : null;
        if (roomWorkingDir != null && !String.valueOf(roomWorkingDir).trim().isEmpty()) {
            return String.valueOf(roomWorkingDir).trim();
        }
        return parentRoom.getRoomFolderPath();
    }

    // Non-blocking status peek. Always returns the {jobId, status, result, error} envelope.
    // status=RUNNING when the child is still working; otherwise a terminal value with result/error filled in.
    public static String check(String jobId, Insight callerInsight) {
        if (jobId == null || jobId.isBlank()) {
            return GSON.toJson(error("Missing required argument 'jobId' for CheckSubAgentStatus"));
        }
        try {
            Map<String, Object> run = AgentRuntimeManager.get().getRun(jobId, callerInsight);
            return GSON.toJson(toSubAgentResult(jobId, run).toMap());
        } catch (Exception e) {
            logger.warn("SubAgentDispatcher.check: failed jobId={}: {}", jobId, e.getMessage());
            return GSON.toJson(error(e.getMessage()));
        }
    }

    // Block until the child AgentRun reaches a terminal status or wait-side timeoutSec elapses.
    // On wait-side timeout the envelope reports status=RUNNING; the child is unaffected.
    public static String wait(String jobId, Insight callerInsight, int timeoutSec) {
        if (jobId == null || jobId.isBlank()) {
            return GSON.toJson(error("Missing required argument 'jobId' for WaitForSubAgent"));
        }
        if (timeoutSec <= 0) {
            timeoutSec = DEFAULT_WAIT_TIMEOUT_SEC;
        }
        try {
            Map<String, Object> run = AgentRuntimeManager.get().waitForRun(jobId, callerInsight, timeoutSec * 1000L);
            if (Boolean.TRUE.equals(run.get("waitTimedOut"))) {
                return GSON.toJson(SubAgentResult.running(jobId).toMap());
            }
            return GSON.toJson(toSubAgentResult(jobId, run).toMap());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new AgentCancelledException("WaitForSubAgent interrupted while polling jobId=" + jobId);
        } catch (Exception e) {
            logger.warn("SubAgentDispatcher.wait: failed jobId={}: {}", jobId, e.getMessage());
            return GSON.toJson(error(e.getMessage()));
        }
    }

    private static SubAgentResult toSubAgentResult(String jobId, Map<String, Object> run) {
        String status = stringValue(run.get("status"));
        if (AgentRunStatus.COMPLETED.name().equals(status)) {
            String finalText = stringValue(run.get("finalText"));
            if (finalText == null) {
                return SubAgentResult.failed(jobId, "Subagent completed but produced no output");
            }
            return SubAgentResult.succeeded(jobId, finalText);
        }
        if (AgentRunStatus.FAILED.name().equals(status)) {
            String error = stringValue(run.get("errorMessage"));
            return SubAgentResult.failed(jobId, error == null ? "Subagent failed" : error);
        }
        if (AgentRunStatus.CANCELLED.name().equals(status)) {
            return SubAgentResult.cancelled(jobId);
        }
        if (AgentRunStatus.INPUT_REQUIRED.name().equals(status)) {
            return SubAgentResult.failed(jobId,
                    "Subagent requires input; parent-child clarification is not supported in this version.");
        }
        return SubAgentResult.running(jobId);
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
        out.put("runId",  result.getRunId());
        out.put("roomId", result.getRoomId());
        out.put("status", result.getStatus() == null ? null : result.getStatus().name());
        if (result.getAlias() != null) {
            out.put("alias", result.getAlias());
        }
        return out;
    }

    private static String stringValue(Object value) {
        if (value == null) {
            return null;
        }
        String str = String.valueOf(value);
        return str.trim().isEmpty() ? null : str;
    }
}
