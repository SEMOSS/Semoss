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
package prerna.reactor.agent.runtime;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

import org.apache.logging.log4j.LogManager;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.run.AgentRunStore;
import prerna.reactor.agent.stream.AgentRunStreamService;

/** Measured run activity. Model time excludes tool execution, including parallel batches. */
public final class AgentRunProgress {
    private static final Map<String, AgentRunProgress> ACTIVE = new ConcurrentHashMap<>();
    private final LongSupplier clock;
    private final long started;
    private final int maxTurns;
    private final int finishingTurns;
    private final String runId;
    private final Map<String, Map<String, Object>> failures = new LinkedHashMap<>();
    private int turnsCompleted, modelCalls, toolCalls, toolFailures, activeTools;
    private long modelStarted = -1, toolsStarted = -1, modelNanos, toolNanos, toolTotalMs;
    private long stopped = -1, elapsedOffsetMs;
    private String phase = "starting", activity = "idle", currentTool;
    private boolean closed;
    private Map<String, Object> workflow;

    AgentRunProgress(int maxTurns, int finishingTurns, LongSupplier clock) {
        this(null, maxTurns, finishingTurns, clock);
    }

    private AgentRunProgress(String runId, int maxTurns, int finishingTurns, LongSupplier clock) {
        this.runId = runId;
        this.maxTurns = maxTurns;
        this.finishingTurns = finishingTurns;
        this.clock = clock;
        this.started = clock.getAsLong();
    }

    static AgentRunProgress start(AgentRunContext ctx) {
        var progress = new AgentRunProgress(ctx.getRunId(), ctx.getMaxTurns(),
                ctx.getAgentConfig().getFinishingTurns(), System::nanoTime);
        if (ctx.isResumeMode() && ctx.getRunId() != null) {
            var run = AgentRunStore.getRunMap(ctx.getRunId(), ctx.getInsight());
            if (run != null && run.get("progress") instanceof Map<?, ?> saved) progress.restore(saved);
        }
        if (ctx.getRunId() != null) ACTIVE.put(ctx.getRunId(), progress);
        progress.publish();
        return progress;
    }

    synchronized void restore(Map<?, ?> saved) {
        turnsCompleted = (int) numeric(saved, "turnsCompleted");
        modelCalls = (int) numeric(saved, "modelCalls");
        toolCalls = (int) numeric(saved, "toolCalls");
        toolFailures = (int) numeric(saved, "toolFailures");
        modelNanos = numeric(saved, "modelTimeMs") * 1_000_000;
        toolNanos = numeric(saved, "toolWallTimeMs") * 1_000_000;
        toolTotalMs = numeric(saved, "toolTimeMs");
        elapsedOffsetMs = numeric(saved, "elapsedMs");
        if (saved.get("repeatedFailures") instanceof Iterable<?> entries) {
            for (Object entry : entries) {
                if (!(entry instanceof Map<?, ?> f)) continue;
                String tool = String.valueOf(f.get("tool")), target = String.valueOf(f.get("target"));
                failures.put(tool + "\n" + target, Map.of("tool", tool, "target", target,
                        "count", (int) numeric(f, "count"), "error", String.valueOf(f.get("error"))));
            }
        }
    }

    synchronized int completedTurns() { return turnsCompleted; }

    private static long numeric(Map<?, ?> values, String key) {
        return values.get(key) instanceof Number n ? Math.max(0, n.longValue()) : 0;
    }

    /** Call only after authorizing access to the durable run record. */
    public static Map<String, Object> activeSnapshot(String runId) {
        var progress = runId == null ? null : ACTIVE.get(runId);
        return progress == null ? null : progress.snapshot();
    }

    synchronized void workflow(Map<String, Object> value) {
        workflow = new LinkedHashMap<>(value);
        phase = String.valueOf(value.get("phase"));
        publish();
    }

    synchronized void beginModel() {
        modelStarted = clock.getAsLong();
        modelCalls++;
        activity = "model";
        if (finishingTurns > 0 && remaining() <= finishingTurns) phase = "finishing";
        publish();
    }

    synchronized void endModel() {
        if (modelStarted >= 0) modelNanos += clock.getAsLong() - modelStarted;
        modelStarted = -1;
        activity = "idle";
        publish();
    }

    synchronized void beginTool(String name) {
        if (activeTools++ == 0) toolsStarted = clock.getAsLong();
        currentTool = name;
        activity = "tool";
        phase = switch (name) {
            case "LoadSkill" -> "loading_instructions";
            case "WriteFile", "EditFile", "MultiEdit" -> "editing";
            case "ExecuteNodeCode" -> "executing_code";
            case "InspectPptx" -> "visual_review";
            case "WaitForSubAgent" -> "waiting_for_subagent";
            default -> name.startsWith("agent_") ? "delegating" : "using_tools";
        };
        publish();
    }

    synchronized void endTool(String name, Map<String, Object> args, boolean success, String output, long elapsedMs) {
        toolCalls++;
        toolTotalMs += elapsedMs;
        if (--activeTools == 0) {
            toolNanos += clock.getAsLong() - toolsStarted;
            toolsStarted = -1;
            activity = "idle";
            currentTool = null;
        }
        String target = String.valueOf(args.getOrDefault("path", args.getOrDefault("command", "")));
        String key = name + "\n" + target;
        if (success) failures.remove(key);
        else {
            toolFailures++;
            var previous = failures.get(key);
            int count = previous == null ? 1 : ((Number) previous.get("count")).intValue() + 1;
            failures.put(key, Map.of("tool", name, "target", bounded(target), "count", count,
                    "error", bounded(output == null ? "Tool failed" : output)));
        }
        publish();
    }

    synchronized void completedTurn(int value) { turnsCompleted = value; }

    synchronized String guidance() {
        String text = "Run budget: " + turnsCompleted + " of " + maxTurns + " tool rounds used; "
                + remaining() + " remaining. Current phase: " + phase + ".";
        if (finishingTurns > 0 && remaining() <= finishingTurns)
            text += " Reserve remaining rounds for essential verification and delivery. End with the saved artifact and disclose unresolved work; do not start optional repairs.";
        if (remaining() == 0)
            text += " No tool rounds remain. Give the final response now, describing actual completion and any unresolved errors. Do not claim checks that did not run.";
        for (var failure : failures.values()) {
            if (((Number) failure.get("count")).intValue() >= 2)
                text += " Repeated failure: " + failure.get("tool") + " on " + failure.get("target")
                        + " (" + failure.get("count") + " attempts). Change approach instead of retrying the same failing edit or command.";
        }
        return text;
    }

    synchronized Map<String, Object> snapshot() {
        long now = stopped < 0 ? clock.getAsLong() : stopped;
        var result = new LinkedHashMap<String, Object>();
        result.put("phase", phase);
        if (workflow != null) result.put("workflow", new LinkedHashMap<>(workflow));
        result.put("activity", activity);
        result.put("currentTool", currentTool);
        result.put("maxTurns", maxTurns);
        result.put("turnsCompleted", turnsCompleted);
        result.put("turnsRemaining", remaining());
        result.put("modelCalls", modelCalls);
        result.put("modelTimeMs", (modelNanos + (modelStarted < 0 ? 0 : now - modelStarted)) / 1_000_000);
        result.put("toolTimeMs", toolTotalMs);
        result.put("toolWallTimeMs", (toolNanos + (toolsStarted < 0 ? 0 : now - toolsStarted)) / 1_000_000);
        result.put("elapsedMs", elapsedOffsetMs + (now - started) / 1_000_000);
        result.put("toolCalls", toolCalls);
        result.put("toolFailures", toolFailures);
        result.put("repeatedFailures", failures.values().stream()
                .filter(f -> ((Number) f.get("count")).intValue() >= 2).map(LinkedHashMap::new).toList());
        return result;
    }

    synchronized void close(String status) {
        if (closed) return;
        if (modelStarted >= 0) modelNanos += clock.getAsLong() - modelStarted;
        if (toolsStarted >= 0) toolNanos += clock.getAsLong() - toolsStarted;
        modelStarted = toolsStarted = -1;
        activity = "idle";
        currentTool = null;
        phase = status;
        stopped = clock.getAsLong();
        publish();
        closed = true;
        if (runId != null) ACTIVE.remove(runId, this);
    }

    private int remaining() { return Math.max(0, maxTurns - turnsCompleted); }
    private static String bounded(String text) { return text.length() <= 300 ? text : text.substring(0, 300); }

    private void publish() {
        if (runId == null || closed) return;
        var snapshot = snapshot();
        AgentRunStreamService.get().publishProgress(runId, snapshot);
        try { AgentRunStore.updateProgress(runId, snapshot); }
        catch (RuntimeException e) { LogManager.getLogger(AgentRunProgress.class).warn("Cannot persist progress for run {}", runId, e); }
    }
}
