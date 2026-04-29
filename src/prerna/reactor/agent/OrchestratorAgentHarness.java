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

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.AgentTraceLogsUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.om.Insight;
import prerna.om.InsightFork;
import prerna.om.ThreadStore;
import prerna.util.Utility;

/**
 * Wave-based multi-agent coordinator harness.
 *
 * <p>On each invocation:
 * <ol>
 *   <li><b>Plan</b> – calls the model to decompose the task into a wave plan: a JSON array
 *       of waves, each wave being an array of sub-agent specs.</li>
 *   <li><b>Execute waves</b> – for each wave, launches all sub-agents in parallel on a
 *       dedicated per-invocation thread pool, then barriers with
 *       {@code CompletableFuture.allOf().join()} before proceeding to the next wave.</li>
 *   <li><b>Synthesize</b> – calls the model with all child outputs and produces a final answer.</li>
 * </ol>
 *
 * <p>Each sub-agent gets:
 * <ul>
 *   <li>A forked {@link Insight} (fresh VarStore + insightId, shared User/Project)</li>
 *   <li>A fresh in-memory {@link Room} (no DB persistence, unique roomId, parent's room as parentRoomId)</li>
 *   <li>A per-invocation bounded thread pool (never the shared ForkJoinPool)</li>
 * </ul>
 *
 * <p>ThreadStore context (sessionId, user, hostname, etc.) is captured from the calling thread
 * and re-seeded on each sub-agent worker thread to preserve SEMOSS auth/session context.
 *
 * <p>Registered as {@code "orchestrator"} in {@link AgentHarnessRegistry}.
 */
public class OrchestratorAgentHarness implements IAgentHarness {

    private static final Logger classLogger = LogManager.getLogger(OrchestratorAgentHarness.class);

    public static final String NAME = "orchestrator";

    private static final int MAX_WAVE_THREADS = 16;
    private static final int MAX_PLAN_RETRIES  = 2;

    /** Maximum number of previous-wave output summaries injected into the next wave's context. */
    private static final int MAX_CONTEXT_SNIPPETS = 5;
    /** Max characters from a previous wave output used as context. */
    private static final int MAX_SNIPPET_CHARS    = 500;

    private static final String PLANNER_SYSTEM_PROMPT =
            "You are an expert AI task decomposer. Analyze the given task and break it into a "
            + "series of parallel waves of specialized sub-agents. "
            + "Respond ONLY with valid JSON (no markdown, no extra text) in this exact format:\n"
            + "{\n"
            + "  \"waves\": [\n"
            + "    [\n"
            + "      {\"harness\": \"room_loop\", \"task\": \"...\", \"systemPrompt\": \"...\", "
            + "\"engineId\": null}\n"
            + "    ]\n"
            + "  ]\n"
            + "}\n"
            + "Rules:\n"
            + "- Each wave is an array of sub-agent specs. Sub-agents in the same wave run in parallel.\n"
            + "- Use separate waves to express dependencies (wave N+1 agents see wave N outputs).\n"
            + "- Available harness types: \"room_loop\", \"claude_code\", \"github_copilot_py\".\n"
            + "- engineId: leave null to use the parent engine, or set an explicit engine UUID.\n"
            + "- systemPrompt: role and output format for the sub-agent.\n"
            + "- task: specific instruction for that sub-agent.\n"
            + "- Keep waves focused. 1-4 sub-agents per wave is ideal.";

    private static final String SYNTHESIZER_SYSTEM_PROMPT =
            "You are a synthesis agent. Combine the outputs from multiple specialized sub-agents "
            + "into a single coherent, complete final answer. Resolve conflicts, fill gaps, and "
            + "present the result clearly and concisely.";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public AgentHarnessResult execute(AgentRunContext ctx) throws Exception {
        Room   parentRoom = ctx.getRoom();
        Instant startTime = Instant.now();
        String  traceId   = UUID.randomUUID().toString();
        String  parentTraceId = ctx.getParentTraceId();
        String  terminationReason = "SUCCESS";
        String  userId = (ctx.getInsight() != null && ctx.getInsight().getUser() != null)
                ? ctx.getInsight().getUser().getPrimaryLoginToken().getId() : null;
        String  projectId = (ctx.getInsight() != null) ? ctx.getInsight().getProjectId() : null;

        AgentTraceLogsUtils.setActiveTraceId(ctx.getInsight().getInsightId(), traceId);

        // Capture ThreadStore context BEFORE spawning any threads so auth/session info
        // is available for propagation into child worker threads.
        // getTheadMapObject() may return null on REST/API threads with no prior ThreadStore setup.
        Map<String, Object> parentThreadContextRaw = ThreadStore.getTheadMapObject();
        Map<String, Object> parentThreadContext = parentThreadContextRaw != null
                ? new HashMap<>(parentThreadContextRaw)
                : new HashMap<>();

        AgentHarnessResult.Builder resultBuilder = AgentHarnessResult.builder()
                .parentTraceId(parentTraceId);

        List<AgentHarnessResult> allChildResults = new ArrayList<>();

        int waveThreads = Math.min(MAX_WAVE_THREADS, Runtime.getRuntime().availableProcessors() * 2);
        ExecutorService waveExecutor = Executors.newFixedThreadPool(waveThreads);

        try {
            // === Phase 1: Plan — LLM decomposes task into wave structure ===
            SubAgentPlan plan = planWaves(ctx, parentRoom);
            classLogger.info("OrchestratorAgentHarness: planned {} wave(s) for room={}",
                    plan.getWaves().size(), parentRoom.getId());

            // === Phase 2: Execute waves with barriers ===
            List<String> previousWaveOutputs = new ArrayList<>();

            for (int waveIdx = 0; waveIdx < plan.getWaves().size(); waveIdx++) {
                List<SubAgentSpec> wave = plan.getWaves().get(waveIdx);
                classLogger.info("OrchestratorAgentHarness: wave {}/{} — {} sub-agent(s)",
                        waveIdx + 1, plan.getWaves().size(), wave.size());

                List<CompletableFuture<SubAgentResult>> waveFutures = new ArrayList<>();
                final List<String> prevOutputsSnapshot = Collections.unmodifiableList(
                        new ArrayList<>(previousWaveOutputs));

                for (SubAgentSpec spec : wave) {
                    CompletableFuture<SubAgentResult> future = CompletableFuture.supplyAsync(
                            () -> executeSubAgent(spec, ctx, traceId, prevOutputsSnapshot, parentThreadContext),
                            waveExecutor);
                    waveFutures.add(future);
                }

                // BARRIER — all sub-agents in this wave must complete before next wave starts
                if (!waveFutures.isEmpty()) {
                    CompletableFuture.allOf(waveFutures.toArray(new CompletableFuture[0])).join();
                }

                // Collect wave results and build context for the next wave
                List<String> waveOutputs = new ArrayList<>();
                for (int i = 0; i < waveFutures.size(); i++) {
                    SubAgentResult r = waveFutures.get(i).getNow(null);
                    if (r == null) continue;

                    allChildResults.add(r.result);
                    resultBuilder.addChildResult(r.result);

                    SubAgentSpec spec = wave.get(i);
                    String taskLabel = spec.task.length() > 50
                            ? spec.task.substring(0, 50) + "..." : spec.task;

                    if (r.success && r.result.getFinalText() != null && !r.result.getFinalText().isEmpty()) {
                        String snippet = r.result.getFinalText().length() > MAX_SNIPPET_CHARS
                                ? r.result.getFinalText().substring(0, MAX_SNIPPET_CHARS) + "..."
                                : r.result.getFinalText();
                        waveOutputs.add("[Wave " + (waveIdx + 1) + "] " + taskLabel + ":\n" + snippet);
                    } else {
                        waveOutputs.add("[Wave " + (waveIdx + 1) + "] " + taskLabel
                                + " [FAILED]: " + r.errorMessage);
                    }
                }
                previousWaveOutputs.addAll(waveOutputs);
            }

            // === Phase 3: Synthesis ===
            String finalAnswer = synthesize(ctx, parentRoom, previousWaveOutputs);

            return resultBuilder
                    .finalText(finalAnswer)
                    .iterations(allChildResults.size())
                    .build();

        } catch (Exception e) {
            terminationReason = "ERROR: " + e.getClass().getSimpleName();
            classLogger.error("OrchestratorAgentHarness: error room={}", parentRoom.getId(), e);
            return resultBuilder
                    .finalText("Orchestration failed: " + e.getMessage())
                    .iterations(allChildResults.size())
                    .build();
        } finally {
            // Shut down per-invocation pool before emitting trace
            waveExecutor.shutdown();
            try {
                if (!waveExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    waveExecutor.shutdownNow();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                waveExecutor.shutdownNow();
            }

            AgentTraceLogsUtils.clearActiveTraceId(ctx.getInsight().getInsightId());
            AgentTraceLogsUtils.clearStepCounter(traceId);
            AgentTraceLogsUtils.logTrace(
                    traceId,
                    parentRoom.getId(),
                    userId,
                    projectId,
                    ctx.getModelEngine() != null ? ctx.getModelEngine().getEngineId() : null,
                    NAME,
                    startTime,
                    Instant.now(),
                    allChildResults.size(),
                    0,
                    terminationReason,
                    buildMetricsJson(allChildResults),
                    parentTraceId);
        }
    }

    /**
     * Executes a single sub-agent task.  Always returns a {@link SubAgentResult} — never throws.
     * Runs on a worker thread from the per-invocation executor.
     */
    private SubAgentResult executeSubAgent(SubAgentSpec spec, AgentRunContext parentCtx,
            String parentTraceId, List<String> prevWaveOutputs,
            Map<String, Object> parentThreadContext) {

        // Seed this worker thread's ThreadStore with parent thread's auth/session context.
        // IMPORTANT: setThreadMapObject() calls CURRENT.get() without null-checking, so we must
        // prime the ThreadLocal on this fresh worker thread first via any setter that uses
        // getThreadMap() (which auto-initializes). We then putAll into the initialized map.
        if (parentThreadContext != null && !parentThreadContext.isEmpty()) {
            // Use setInsightId("") to prime the ThreadLocal (getThreadMap initializes if null)
            // then overwrite with the actual parent context.
            ThreadStore.setInsightId(""); // initializes ThreadLocal on this worker thread
            ThreadStore.setThreadMapObject(new HashMap<>(parentThreadContext));
        } else {
            ThreadStore.setInsightId(""); // ensure ThreadLocal is always initialized
        }

        Insight childInsight = null;
        try {
            // Resolve engine (spec may override or inherit from parent)
            String engineId = (spec.engineId != null && !spec.engineId.trim().isEmpty())
                    ? spec.engineId : parentCtx.getModelEngine().getEngineId();
            IModelEngine childEngine = Utility.getModel(engineId);
            if (childEngine == null) {
                return SubAgentResult.failure(spec, "Could not resolve model engine: " + engineId);
            }

            // Fork a fresh Insight for isolation (new VarStore + insightId, shared User/Project)
            childInsight = InsightFork.forkForChildAgent(parentCtx.getInsight());
            ThreadStore.setInsightId(childInsight.getInsightId());

            // Build child room (in-memory, not persisted to DB)
            Room childRoom = createChildRoom(spec, parentCtx.getRoom(), engineId);
            childInsight.setRoomForInsight(childRoom);
            childRoom.setInsight(childInsight); // wire insight back onto the room

            // Inject context from previous waves (size-limited to avoid token blowout)
            String taskInput = spec.task;
            if (!prevWaveOutputs.isEmpty()) {
                int snippetCount = Math.min(prevWaveOutputs.size(), MAX_CONTEXT_SNIPPETS);
                List<String> snippets = prevWaveOutputs.subList(
                        prevWaveOutputs.size() - snippetCount, prevWaveOutputs.size());
                taskInput = taskInput
                        + "\n\n--- Context from earlier agents ---\n"
                        + String.join("\n\n", snippets);
            }

            AgentRunContext childCtx = AgentRunContext.builder()
                    .room(childRoom)
                    .modelEngine(childEngine)
                    .insight(childInsight)
                    .userId(childRoom.getUserId())
                    .input(taskInput)
                    .maxIterations(parentCtx.getMaxIterations())
                    .maxReflections(0)
                    .parentTraceId(parentTraceId)
                    .build();

            IAgentHarness harness = AgentHarnessRegistry.getOrDefault(spec.harness);
            classLogger.info("OrchestratorAgentHarness: running sub-agent harness={} task={}",
                    harness.getName(), spec.task.length() > 60 ? spec.task.substring(0, 60) + "..." : spec.task);

            AgentHarnessResult result = harness.execute(childCtx);
            return SubAgentResult.success(spec, result);

        } catch (Exception e) {
            classLogger.error("OrchestratorAgentHarness: sub-agent failed task='{}': {}",
                    spec.task, e.getMessage(), e);
            return SubAgentResult.failure(spec, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        } finally {
            // Always clean up the forked child insight and thread-local context
            if (childInsight != null) {
                InsightFork.cleanup(childInsight);
            }
            ThreadStore.remove();
        }
    }

    /**
     * Calls the LLM to decompose the task into a wave plan.
     * Retries up to {@value #MAX_PLAN_RETRIES} times. Falls back to a single-wave plan on failure.
     */
    private SubAgentPlan planWaves(AgentRunContext ctx, Room parentRoom) {
        String planningRoomId = UUID.randomUUID().toString();
        JsonObject planOpts = new JsonObject();
        planOpts.addProperty("instructions", PLANNER_SYSTEM_PROMPT);
        planOpts.addProperty("modelId", ctx.getModelEngine().getEngineId());

        Room planningRoom = new Room(
                planningRoomId,
                parentRoom.getUserId(),
                "orchestrator-planner",
                null, parentRoom.getProjectId(), null, true,
                new Timestamp(System.currentTimeMillis()),
                new Timestamp(System.currentTimeMillis()),
                "[]", false,
                planOpts.toString(),
                ctx.getModelEngine().getEngineId(),
                parentRoom.getId());

        for (int attempt = 1; attempt <= MAX_PLAN_RETRIES + 1; attempt++) {
            try {
                InputMessage planMsg = InputMessage.builder(planningRoom)
                        .withSystemPrompt(PLANNER_SYSTEM_PROMPT)
                        .withText("Task to decompose: " + ctx.getInput())
                        .withModelType(ctx.getModelEngine().getModelType())
                        .withParamMap(new HashMap<>())
                        .build();

                ResponseMessage planResponse = planningRoom.ask(planMsg, ctx.getModelEngine(), null);
                String planJson = planResponse != null ? planResponse.getContent() : null;
                if (planJson != null) {
                    // Strip markdown code fences that some models wrap JSON in
                    planJson = planJson.trim();
                    if (planJson.startsWith("```")) {
                        planJson = planJson.replaceAll("^```[a-zA-Z]*\\n?", "").replaceAll("```\\s*$", "").trim();
                    }
                    SubAgentPlan plan = SubAgentPlan.fromJson(planJson);
                    if (plan != null && !plan.getWaves().isEmpty()) {
                        return plan;
                    }
                }
                classLogger.warn("OrchestratorAgentHarness: plan attempt {}/{} returned empty plan",
                        attempt, MAX_PLAN_RETRIES + 1);
            } catch (Exception e) {
                classLogger.warn("OrchestratorAgentHarness: plan attempt {}/{} failed: {}",
                        attempt, MAX_PLAN_RETRIES + 1, e.getMessage());
            }
        }

        // Fallback: single wave, single sub-agent with the original task
        classLogger.warn("OrchestratorAgentHarness: using fallback single-agent plan");
        return SubAgentPlan.singleAgent(new SubAgentSpec("room_loop", ctx.getInput(), null, null));
    }

    /**
     * Calls the LLM to synthesize all wave outputs into a final answer.
     * If there is only one output, returns it directly without an extra LLM call.
     */
    private String synthesize(AgentRunContext ctx, Room parentRoom, List<String> outputs) {
        if (outputs.isEmpty()) return "No results produced by sub-agents.";
        if (outputs.size() == 1) return outputs.get(0);

        String planningRoomId = UUID.randomUUID().toString();
        JsonObject opts = new JsonObject();
        opts.addProperty("instructions", SYNTHESIZER_SYSTEM_PROMPT);
        opts.addProperty("modelId", ctx.getModelEngine().getEngineId());

        Room synthRoom = new Room(
                planningRoomId,
                parentRoom.getUserId(),
                "orchestrator-synthesizer",
                null, parentRoom.getProjectId(), null, true,
                new Timestamp(System.currentTimeMillis()),
                new Timestamp(System.currentTimeMillis()),
                "[]", false,
                opts.toString(),
                ctx.getModelEngine().getEngineId(),
                parentRoom.getId());

        String combinedOutputs = String.join("\n\n---\n\n", outputs);

        try {
            InputMessage synthMsg = InputMessage.builder(synthRoom)
                    .withSystemPrompt(SYNTHESIZER_SYSTEM_PROMPT)
                    .withText("Sub-agent outputs:\n\n" + combinedOutputs
                            + "\n\nSynthesize these into a single final answer.")
                    .withModelType(ctx.getModelEngine().getModelType())
                    .withParamMap(new HashMap<>())
                    .build();

            ResponseMessage synthResponse = synthRoom.ask(synthMsg, ctx.getModelEngine(), null);
            return synthResponse != null && synthResponse.getContent() != null
                    ? synthResponse.getContent() : combinedOutputs;
        } catch (Exception e) {
            classLogger.warn("OrchestratorAgentHarness: synthesis failed, returning combined output: {}",
                    e.getMessage());
            return combinedOutputs;
        }
    }

    /**
     * Creates an ephemeral in-memory child Room with the sub-agent's system prompt and engine.
     * The room is not persisted to the ROOM database table.
     */
    private Room createChildRoom(SubAgentSpec spec, Room parentRoom, String engineId) {
        String childRoomId  = UUID.randomUUID().toString();
        String systemPrompt = (spec.systemPrompt != null && !spec.systemPrompt.isEmpty())
                ? spec.systemPrompt : parentRoom.getEffectiveSystemPrompt();

        JsonObject optsJson = new JsonObject();
        if (systemPrompt != null) {
            optsJson.addProperty("instructions", systemPrompt);
        }
        optsJson.addProperty("modelId", engineId);

        return new Room(
                childRoomId,
                parentRoom.getUserId(),
                "child-agent",
                null,
                parentRoom.getProjectId(),
                null,
                true,
                new Timestamp(System.currentTimeMillis()),
                new Timestamp(System.currentTimeMillis()),
                "[]",
                false,
                optsJson.toString(),
                engineId,
                parentRoom.getId());
    }

    private String buildMetricsJson(List<AgentHarnessResult> childResults) {
        JsonObject metrics = new JsonObject();
        metrics.addProperty("total_sub_agents", childResults.size());
        return metrics.toString();
    }

    /** Dummy overload to satisfy the finally block's plan reference (plan is built inside execute). */
    private String plan(@SuppressWarnings("unused") String input) { return null; }

    // -------------------------------------------------------------------------
    // Inner types
    // -------------------------------------------------------------------------

    /** Specification for a single sub-agent task within a wave. */
    static final class SubAgentSpec {
        final String harness;
        final String task;
        final String systemPrompt;
        final String engineId;

        SubAgentSpec(String harness, String task, String systemPrompt, String engineId) {
            this.harness      = harness != null ? harness : "room_loop";
            this.task         = task != null ? task : "";
            this.systemPrompt = systemPrompt;
            this.engineId     = engineId;
        }
    }

    /** Ordered list of waves, each wave being a list of sub-agent specs to run in parallel. */
    static final class SubAgentPlan {
        private final List<List<SubAgentSpec>> waves;

        SubAgentPlan(List<List<SubAgentSpec>> waves) {
            this.waves = waves;
        }

        List<List<SubAgentSpec>> getWaves() { return waves; }

        static SubAgentPlan singleAgent(SubAgentSpec spec) {
            List<List<SubAgentSpec>> waves = new ArrayList<>();
            waves.add(Collections.singletonList(spec));
            return new SubAgentPlan(waves);
        }

        /**
         * Parses a wave plan from the LLM's JSON response.
         * Returns null if the JSON is invalid or produces no waves.
         */
        static SubAgentPlan fromJson(String json) {
            try {
                JsonObject root = JsonParser.parseString(json).getAsJsonObject();
                JsonArray wavesArray = root.getAsJsonArray("waves");
                if (wavesArray == null || wavesArray.size() == 0) return null;

                List<List<SubAgentSpec>> waves = new ArrayList<>();
                for (JsonElement waveElem : wavesArray) {
                    JsonArray waveArray = waveElem.getAsJsonArray();
                    List<SubAgentSpec> wave = new ArrayList<>();
                    for (JsonElement specElem : waveArray) {
                        JsonObject specObj = specElem.getAsJsonObject();
                        String harness    = specObj.has("harness")
                                ? specObj.get("harness").getAsString() : "room_loop";
                        String task       = specObj.has("task")
                                ? specObj.get("task").getAsString() : "";
                        String sysp       = specObj.has("systemPrompt")
                                && !specObj.get("systemPrompt").isJsonNull()
                                ? specObj.get("systemPrompt").getAsString() : null;
                        String eid        = specObj.has("engineId")
                                && !specObj.get("engineId").isJsonNull()
                                ? specObj.get("engineId").getAsString() : null;
                        wave.add(new SubAgentSpec(harness, task, sysp, eid));
                    }
                    if (!wave.isEmpty()) waves.add(wave);
                }
                return waves.isEmpty() ? null : new SubAgentPlan(waves);
            } catch (Exception e) {
                return null;
            }
        }
    }

    /** Result envelope for a single sub-agent execution. Never throws — always wraps success/failure. */
    static final class SubAgentResult {
        final boolean           success;
        final AgentHarnessResult result;
        final String            errorMessage;
        final SubAgentSpec      spec;

        private SubAgentResult(boolean success, AgentHarnessResult result, String errorMessage,
                SubAgentSpec spec) {
            this.success      = success;
            this.result       = result;
            this.errorMessage = errorMessage;
            this.spec         = spec;
        }

        static SubAgentResult success(SubAgentSpec spec, AgentHarnessResult result) {
            return new SubAgentResult(true, result, null, spec);
        }

        static SubAgentResult failure(SubAgentSpec spec, String errorMessage) {
            AgentHarnessResult errResult = AgentHarnessResult.builder()
                    .finalText("Sub-agent failed: " + errorMessage)
                    .build();
            return new SubAgentResult(false, errResult, errorMessage, spec);
        }
    }
}
