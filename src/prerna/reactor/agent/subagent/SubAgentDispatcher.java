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
import prerna.reactor.agent.AgentRunner;
import prerna.reactor.agent.config.SubAgentSpec;
import prerna.reactor.agent.exceptions.AgentCancelledException;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.comm.PixelJobManager;
import prerna.sablecc2.comm.PixelJobStatus;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Tool-call dispatch for synthesized subagent tools.
 *
 * <p>Called from the semoss harness when a tool name matches either a named
 * {@link SubAgentSpec} alias or one of the {@link SubAgentToolSynthesizer}
 * built-ins ({@code SpawnSubAgent}, {@code CheckSubAgentStatus},
 * {@code WaitForSubAgent}, {@code AskParent}, {@code NotifyParent},
 * {@code ReplyToSubAgentAsk}).
 *
 * <p>All entry points return a string suitable for handing directly back
 * to the model as the tool result.
 */
public final class SubAgentDispatcher {

    private static final Logger logger = LogManager.getLogger(SubAgentDispatcher.class);

    private static final Gson GSON = new Gson();

    /** Default wait timeout when the LLM omits {@code timeoutSec}. */
    public static final int DEFAULT_WAIT_TIMEOUT_SEC = 300;

    /** Poll interval while blocking inside {@link #wait(String, int)}. */
    private static final long WAIT_POLL_MS = 250L;

    /** Poll interval while a child AskParent tool waits for a parent reply. */
    private static final long ASK_PARENT_POLL_MS = 250L;

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
            Object wd = parentRoom.getOptionsMap() != null
                    ? parentRoom.getOptionsMap().get(AgentRunner.ROOM_OPTION_WORKING_DIR)
                    : null;
            req.workingDirOverride = (wd != null && !String.valueOf(wd).trim().isEmpty())
                    ? String.valueOf(wd).trim()
                    : parentRoom.getRoomFolderPath();
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
            return GSON.toJson(error("Missing required argument 'prompt' for SpawnSubAgent"));
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
            Object wd = parentRoom.getOptionsMap() != null
                    ? parentRoom.getOptionsMap().get(AgentRunner.ROOM_OPTION_WORKING_DIR)
                    : null;
            req.workingDirOverride = (wd != null && !String.valueOf(wd).trim().isEmpty())
                    ? String.valueOf(wd).trim()
                    : parentRoom.getRoomFolderPath();
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

    // Non-blocking status peek. Always returns the {jobId, status, result, error} envelope.
    // status=RUNNING when the child is still working; otherwise a terminal value with result/error filled in.
    public static String check(String jobId) {
        if (jobId == null || jobId.isBlank()) {
            return GSON.toJson(error("Missing required argument 'jobId' for CheckSubAgentStatus"));
        }
        AgentSubAgentRegistry.PendingClarification pending =
                AgentSubAgentRegistry.getManager().pendingClarificationFor(jobId);
        if (pending != null) {
            return GSON.toJson(SubAgentResult.needsInput(
                    jobId, pending.getQuestion(), pending.getRequestId()).toMap());
        }
        String rawStatus = PixelJobManager.getManager().getStatus(jobId);
        SubAgentResult envelope = isTerminal(rawStatus)
                ? toTerminalEnvelope(jobId, rawStatus)
                : SubAgentResult.running(jobId);
        return GSON.toJson(envelope.toMap());
    }

    // Block until the child reaches a terminal status or wait-side timeoutSec elapses.
    // On wait-side timeout the envelope reports status=RUNNING (the child is unaffected); the
    // parent may call again. Terminal mappings: COMPLETE/PROGRESS_COMPLETE->SUCCEEDED,
    // ERROR->FAILED, CANCELED->CANCELLED. Child-side max_seconds exhaustion currently surfaces
    // as FAILED until PixelJobManager exposes the underlying exception kind.
    public static String wait(String jobId, int timeoutSec) {
        if (jobId == null || jobId.isBlank()) {
            return GSON.toJson(error("Missing required argument 'jobId' for WaitForSubAgent"));
        }
        if (timeoutSec <= 0) {
            timeoutSec = DEFAULT_WAIT_TIMEOUT_SEC;
        }
        long deadline = System.currentTimeMillis() + (timeoutSec * 1000L);
        PixelJobManager manager = PixelJobManager.getManager();
        AgentSubAgentRegistry registry = AgentSubAgentRegistry.getManager();
        while (true) {
            AgentSubAgentRegistry.PendingClarification pending = registry.pendingClarificationFor(jobId);
            if (pending != null) {
                return GSON.toJson(SubAgentResult.needsInput(
                        jobId, pending.getQuestion(), pending.getRequestId()).toMap());
            }
            String rawStatus = manager.getStatus(jobId);
            if (isTerminal(rawStatus)) {
                return GSON.toJson(toTerminalEnvelope(jobId, rawStatus).toMap());
            }
            if (System.currentTimeMillis() >= deadline) {
                // Wait-side timeout: child is still alive, parent gave up waiting.
                return GSON.toJson(SubAgentResult.running(jobId).toMap());
            }
            try {
                Thread.sleep(WAIT_POLL_MS);
            } catch (InterruptedException ie) {
                // propagate as cancel rather than returning a phantom tool-result string
                Thread.currentThread().interrupt();
                throw new AgentCancelledException("WaitForSubAgent interrupted while polling jobId=" + jobId);
            }
        }
    }

    /**
     * Child -> parent progress event. This is observability/status only; it does not
     * inject anything into the parent's model loop.
     */
    public static String notifyParent(String callerJobId, String message, String kind) {
        if (callerJobId == null || callerJobId.isBlank()) {
            return GSON.toJson(error("Caller has no jobId - cannot notify parent."));
        }
        if (message == null || message.isBlank()) {
            return GSON.toJson(error("Missing required argument 'message' for NotifyParent."));
        }
        AgentSubAgentRegistry registry = AgentSubAgentRegistry.getManager();
        SubAgentMeta callerMeta = registry.lookup(callerJobId);
        if (callerMeta == null || callerMeta.getParentJobId() == null
                || callerMeta.getParentJobId().isBlank()) {
            return GSON.toJson(error("This run has no recorded parent - NotifyParent is only valid for subagent runs."));
        }
        String normalizedKind = normalizeNotifyKind(kind);
        Map<String, Object> data = baseSubagentEvent("subagent-notification", callerJobId, callerMeta.getParentJobId());
        data.put("noticeKind", normalizedKind);
        data.put("message", message.trim());
        emitStreamEvent(callerMeta.getParentJobId(), "subagent-notification", data);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.put("status", "notified");
        out.put("kind", normalizedKind);
        return GSON.toJson(out);
    }

    /**
     * Child -> parent clarification request. The tool call blocks here until the
     * direct parent replies, then returns the reply as a normal tool result.
     */
    public static String askParent(String callerJobId, String question) {
        if (callerJobId == null || callerJobId.isBlank()) {
            throw new IllegalArgumentException("Caller has no jobId - cannot ask parent.");
        }
        if (question == null || question.isBlank()) {
            throw new IllegalArgumentException("Missing required argument 'question' for AskParent.");
        }

        AgentSubAgentRegistry registry = AgentSubAgentRegistry.getManager();
        SubAgentMeta callerMeta = registry.lookup(callerJobId);
        if (callerMeta == null || callerMeta.getParentJobId() == null
                || callerMeta.getParentJobId().isBlank()) {
            throw new IllegalStateException("This run has no recorded parent - AskParent is only valid for subagent runs.");
        }
        String parentJobId = callerMeta.getParentJobId();
        String parentStatus = PixelJobManager.getManager().getStatus(parentJobId);
        if (!isActive(parentStatus)) {
            throw new IllegalStateException("Parent run is not active; cannot wait for clarification reply.");
        }

        AgentSubAgentRegistry.PendingClarification pending =
                registry.openClarification(callerJobId, parentJobId, question);
        Map<String, Object> data = baseSubagentEvent("subagent-needs-input", callerJobId, parentJobId);
        data.put("question", pending.getQuestion());
        data.put("requestId", pending.getRequestId());
        data.put("createdAtMs", pending.getCreatedAtMs());
        emitStreamEvent(parentJobId, "subagent-needs-input", data);

        logger.info("SubAgentDispatcher.askParent: childJobId={} parentJobId={} requestId={}",
                callerJobId, parentJobId, pending.getRequestId());

        while (true) {
            if (Thread.currentThread().isInterrupted()) {
                registry.cleanupClarification(callerJobId);
                throw new AgentCancelledException(
                        "AskParent interrupted while waiting for parent reply requestId=" + pending.getRequestId());
            }
            if (pending.hasReply()) {
                Map<String, Object> out = new LinkedHashMap<>();
                out.put("ok", true);
                out.put("requestId", pending.getRequestId());
                out.put("reply", pending.getReply());
                return GSON.toJson(out);
            }
            if (pending.isClosed()) {
                throw new IllegalStateException("AskParent was closed before the parent replied.");
            }
            try {
                pending.awaitReply(ASK_PARENT_POLL_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Parent -> child reply to a pending AskParent request. */
    public static String replyToSubAgentAsk(String callerJobId, String childJobId,
            String requestId, String message) {
        AgentSubAgentRegistry.PendingClarification pending =
                AgentSubAgentRegistry.getManager().replyToClarification(
                        callerJobId, childJobId, requestId, message);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.put("status", "replied");
        out.put("jobId", pending.getChildJobId());
        out.put("requestId", pending.getRequestId());
        return GSON.toJson(out);
    }

    // Map a terminal PixelJobStatus value to a SubAgentResult envelope. SUCCEEDED carries
    // the child's final text in result; FAILED/CANCELLED carry a short message in error.
    private static SubAgentResult toTerminalEnvelope(String jobId, String rawStatus) {
        if (PixelJobStatus.ERROR.getValue().equals(rawStatus)) {
            return SubAgentResult.failed(jobId, "Subagent job ended in ERROR status");
        }
        if (PixelJobStatus.CANCELED.getValue().equals(rawStatus)) {
            return SubAgentResult.cancelled(jobId);
        }
        // COMPLETE / PROGRESS_COMPLETE
        String finalText = collectFinalText(jobId);
        if (finalText == null) {
            return SubAgentResult.failed(jobId, "Subagent completed but produced no output");
        }
        return SubAgentResult.succeeded(jobId, finalText);
    }

    // Returns the child's final text string, or null when the output is unavailable. Errors
    // are logged - the caller decides how to express them in the envelope.
    private static String collectFinalText(String jobId) {
        try {
            PixelRunner runner = PixelJobManager.getManager().getOutput(jobId);
            if (runner == null) return null;
            // Mirror RunAgentReactor: the final text is the value of the last CONST_STRING
            // NounMetadata in the run's results list. RunAgent returns a single string result,
            // so the last value typically is the agent's final text.
            java.util.List<NounMetadata> results = runner.getResults();
            if (results == null || results.isEmpty()) return null;
            for (int i = results.size() - 1; i >= 0; i--) {
                Object val = results.get(i).getValue();
                if (val instanceof CharSequence) {
                    return val.toString();
                }
            }
            // No string result - serialize the last non-null value as JSON.
            Object lastVal = results.get(results.size() - 1).getValue();
            return lastVal == null ? null : GSON.toJson(lastVal);
        } catch (Exception e) {
            logger.warn("SubAgentDispatcher: failed to collect final text for jobId={}: {}", jobId, e.getMessage());
            return null;
        }
    }

    private static boolean isTerminal(String status) {
        return PixelJobStatus.COMPLETE.getValue().equals(status)
                || PixelJobStatus.PROGRESS_COMPLETE.getValue().equals(status)
                || PixelJobStatus.ERROR.getValue().equals(status)
                || PixelJobStatus.CANCELED.getValue().equals(status);
    }

    private static boolean isActive(String status) {
        return PixelJobStatus.CREATED.getValue().equals(status)
                || PixelJobStatus.SUBMITTED.getValue().equals(status)
                || PixelJobStatus.IN_PROGRESS.getValue().equals(status)
                || PixelJobStatus.STREAMING.getValue().equals(status)
                || PixelJobStatus.PAUSED.getValue().equals(status);
    }

    private static String normalizeNotifyKind(String kind) {
        if (kind == null) return "progress";
        String k = kind.trim().toLowerCase();
        if ("progress".equals(k) || "completed".equals(k) || "blocked".equals(k)
                || "milestone".equals(k)) {
            return k;
        }
        return "progress";
    }

    private static Map<String, Object> baseSubagentEvent(String kind, String childJobId, String parentJobId) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("kind", kind);
        data.put("jobId", childJobId);
        data.put("childJobId", childJobId);
        data.put("parentJobId", parentJobId);
        return data;
    }

    private static void emitStreamEvent(String recipientJobId, String streamType, Map<String, Object> data) {
        try {
            Map<String, Object> envelope = new LinkedHashMap<>();
            envelope.put("stream_type", streamType);
            envelope.put("data", data);
            PixelJobManager.getManager().addStreamOut(recipientJobId, envelope);
        } catch (Exception streamErr) {
            logger.warn("SubAgentDispatcher: stream emit failed recipientJobId={} streamType={}: {}",
                    recipientJobId, streamType, streamErr.toString());
        }
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
