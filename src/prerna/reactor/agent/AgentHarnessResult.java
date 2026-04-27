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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import prerna.engine.impl.model.message.MessageType;

/**
 * Immutable result returned by {@link IAgentHarness#execute}.
 *
 * <p>Carries the final text response together with execution metadata useful for logging,
 * debugging, and audit:
 * <ul>
 *   <li>The total number of agentic tool-call rounds performed.
 *   <li>A per-tool-call trace ({@link ToolCallRecord}) with the tool name, call ID,
 *       result content, wall-clock duration, and success/failure status.
 *   <li>Optional full decision trace (traceId, steps, timing, terminationReason) populated
 *       by harnesses that support tracing. Use {@link #builder()} to construct with trace data.
 * </ul>
 */
public final class AgentHarnessResult {

    // ── Core fields (always populated) ────────────────────────────────────────

    /** Final {@code RESPONSE_TEXT} content produced by the model. Never null; may be empty. */
    private final String finalText;

    /** Total number of tool-call rounds completed (0 if the model responded without calling tools). */
    private final int iterations;

    /** Ordered record of every tool call made during the agentic loop. Immutable. */
    private final List<ToolCallRecord> toolCallRecords;

    /** Number of reflection rounds actually executed (0 if reflection was disabled). */
    private final int reflectionsUsed;

    // ── Trace fields (nullable — backward compatible) ─────────────────────────

    /** UUID assigned at the start of this agent run. Null if harness does not populate it. */
    private final String traceId;

    /** Room ID this run was associated with. Null if not populated. */
    private final String roomId;

    /** Engine ID of the model used. Null if not populated. */
    private final String modelEngineId;

    /** Wall-clock start time of the full agent run. Null if not populated. */
    private final Instant startTime;

    /** Wall-clock end time of the full agent run (set in finally block). Null if not populated. */
    private final Instant endTime;

    /**
     * Step-by-step trace of each tool-call round. Only observable artifacts — no provider-specific
     * fields like reasoning tokens. Null if harness does not populate it.
     */
    private final List<DecisionStep> steps;

    /** Aggregate metrics (cost, tokens, latency). Null if not populated. */
    private final Map<String, Object> metrics;

    /**
     * Why the agent loop ended: {@code "SUCCESS"}, {@code "MAX_ITERATIONS"}, or
     * {@code "EXCEPTION:<message>"}. Null if not populated.
     */
    private final String terminationReason;

    // ── Backward-compatible constructors ──────────────────────────────────────

    /** Backward-compatible constructor — sets {@code reflectionsUsed = 0} and all trace fields null. */
    public AgentHarnessResult(String finalText, int iterations, List<ToolCallRecord> toolCallRecords) {
        this(finalText, iterations, toolCallRecords, 0);
    }

    /** Backward-compatible constructor — sets all trace fields null. */
    public AgentHarnessResult(String finalText, int iterations, List<ToolCallRecord> toolCallRecords,
                              int reflectionsUsed) {
        this.finalText         = finalText != null ? finalText : "";
        this.iterations        = iterations;
        this.toolCallRecords   = Collections.unmodifiableList(toolCallRecords);
        this.reflectionsUsed   = reflectionsUsed;
        this.traceId           = null;
        this.roomId            = null;
        this.modelEngineId     = null;
        this.startTime         = null;
        this.endTime           = null;
        this.steps             = null;
        this.metrics           = null;
        this.terminationReason = null;
    }

    private AgentHarnessResult(Builder b) {
        this.finalText         = b.finalText != null ? b.finalText : "";
        this.iterations        = b.iterations;
        this.toolCallRecords   = Collections.unmodifiableList(b.toolCallRecords);
        this.reflectionsUsed   = b.reflectionsUsed;
        this.traceId           = b.traceId;
        this.roomId            = b.roomId;
        this.modelEngineId     = b.modelEngineId;
        this.startTime         = b.startTime;
        this.endTime           = b.endTime;
        this.steps             = b.steps != null
                ? Collections.unmodifiableList(b.steps) : null;
        this.metrics           = b.metrics != null
                ? Collections.unmodifiableMap(b.metrics) : null;
        this.terminationReason = b.terminationReason;
    }

    // ── Core accessors ────────────────────────────────────────────────────────

    /** Final text content of the model's {@code RESPONSE_TEXT} message. */
    public String getFinalText() { return finalText; }

    /** Number of tool-call rounds the harness completed before obtaining the final text. */
    public int getIterations() { return iterations; }

    /** Number of reflection rounds executed (0 if {@code maxReflections} was 0). */
    public int getReflectionsUsed() { return reflectionsUsed; }

    /** Ordered list of all tool calls made during this run. */
    public List<ToolCallRecord> getToolCallRecords() { return toolCallRecords; }

    // ── Trace accessors ───────────────────────────────────────────────────────

    /** UUID for this run. Null if this result was created via the legacy constructors. */
    public String getTraceId() { return traceId; }

    /** Room ID for this run. Null if not populated. */
    public String getRoomId() { return roomId; }

    /** Model engine ID used during this run. Null if not populated. */
    public String getModelEngineId() { return modelEngineId; }

    /** Run start time. Null if not populated. */
    public Instant getStartTime() { return startTime; }

    /** Run end time (always set, even on failure). Null if not populated. */
    public Instant getEndTime() { return endTime; }

    /** Per-step decision trace. Null if not populated. */
    public List<DecisionStep> getSteps() { return steps; }

    /** Aggregate metrics map. Null if not populated. */
    public Map<String, Object> getMetrics() { return metrics; }

    /**
     * Why the run ended. {@code "SUCCESS"}, {@code "MAX_ITERATIONS"}, or
     * {@code "EXCEPTION:<message>"}. Null if not populated.
     */
    public String getTerminationReason() { return terminationReason; }

    // ── Builder ───────────────────────────────────────────────────────────────

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private String               finalText         = "";
        private int                  iterations        = 0;
        private List<ToolCallRecord> toolCallRecords   = new ArrayList<>();
        private int                  reflectionsUsed   = 0;
        private String               traceId           = UUID.randomUUID().toString();
        private String               roomId;
        private String               modelEngineId;
        private Instant              startTime;
        private Instant              endTime;
        private List<DecisionStep>   steps             = new ArrayList<>();
        private Map<String, Object>  metrics           = new HashMap<>();
        private String               terminationReason;

        public Builder finalText(String v)               { this.finalText = v;         return this; }
        public Builder iterations(int v)                 { this.iterations = v;        return this; }
        public Builder toolCallRecords(List<ToolCallRecord> v) { this.toolCallRecords = v != null ? v : new ArrayList<>(); return this; }
        public Builder reflectionsUsed(int v)            { this.reflectionsUsed = v;   return this; }
        public Builder traceId(String v)                 { this.traceId = v;           return this; }
        public Builder roomId(String v)                  { this.roomId = v;            return this; }
        public Builder modelEngineId(String v)           { this.modelEngineId = v;     return this; }
        public Builder startTime(Instant v)              { this.startTime = v;         return this; }
        public Builder endTime(Instant v)                { this.endTime = v;           return this; }
        public Builder addStep(DecisionStep step)        { this.steps.add(step);       return this; }
        public Builder steps(List<DecisionStep> v)       { this.steps = v != null ? v : new ArrayList<>(); return this; }
        public Builder metric(String key, Object value)  { this.metrics.put(key, value); return this; }
        public Builder metrics(Map<String, Object> v)    { this.metrics = v != null ? v : new HashMap<>(); return this; }
        public Builder terminationReason(String v)       { this.terminationReason = v; return this; }

        public AgentHarnessResult build() { return new AgentHarnessResult(this); }
    }

    // ── DecisionStep ─────────────────────────────────────────────────────────

    /**
     * One iteration of the agent loop — observable artifacts only.
     *
     * <p>No provider-specific fields (e.g. no reasoning tokens — Anthropic-only and a data-leak
     * path). We trace message IDs (references, not content), tool call records, guardrails fired,
     * token counts, and timing — uniform across all {@link IAgentHarness} implementations.
     */
    public static final class DecisionStep {
        private final int                  stepIndex;
        private final MessageType          modelResponseType;
        private final String               inputMessageId;
        private final String               responseMessageId;
        private final List<ToolCallRecord> toolCalls;
        private final List<String>         guardrailsFired;
        private final long                 tokenCount;
        private final Instant              timestamp;

        private DecisionStep(StepBuilder b) {
            this.stepIndex         = b.stepIndex;
            this.modelResponseType = b.modelResponseType;
            this.inputMessageId    = b.inputMessageId;
            this.responseMessageId = b.responseMessageId;
            this.toolCalls         = b.toolCalls != null
                    ? Collections.unmodifiableList(b.toolCalls) : Collections.emptyList();
            this.guardrailsFired   = b.guardrailsFired != null
                    ? Collections.unmodifiableList(b.guardrailsFired) : Collections.emptyList();
            this.tokenCount        = b.tokenCount;
            this.timestamp         = b.timestamp != null ? b.timestamp : Instant.now();
        }

        public int getStepIndex()                { return stepIndex; }
        public MessageType getModelResponseType(){ return modelResponseType; }
        public String getInputMessageId()        { return inputMessageId; }
        public String getResponseMessageId()     { return responseMessageId; }
        public List<ToolCallRecord> getToolCalls(){ return toolCalls; }
        public List<String> getGuardrailsFired() { return guardrailsFired; }
        public long getTokenCount()              { return tokenCount; }
        public Instant getTimestamp()            { return timestamp; }

        public static StepBuilder builder()      { return new StepBuilder(); }

        public static final class StepBuilder {
            private int                  stepIndex;
            private MessageType          modelResponseType;
            private String               inputMessageId;
            private String               responseMessageId;
            private List<ToolCallRecord> toolCalls      = new ArrayList<>();
            private List<String>         guardrailsFired = new ArrayList<>();
            private long                 tokenCount;
            private Instant              timestamp;

            public StepBuilder stepIndex(int v)                    { this.stepIndex = v;           return this; }
            public StepBuilder modelResponseType(MessageType v)    { this.modelResponseType = v;   return this; }
            public StepBuilder inputMessageId(String v)            { this.inputMessageId = v;      return this; }
            public StepBuilder responseMessageId(String v)         { this.responseMessageId = v;   return this; }
            public StepBuilder toolCalls(List<ToolCallRecord> v)   { this.toolCalls = v;           return this; }
            public StepBuilder addGuardrailFired(String v)         { this.guardrailsFired.add(v);  return this; }
            public StepBuilder tokenCount(long v)                  { this.tokenCount = v;          return this; }
            public StepBuilder timestamp(Instant v)                { this.timestamp = v;           return this; }

            public DecisionStep build()                            { return new DecisionStep(this); }
        }
    }

    // ── ToolCallRecord ────────────────────────────────────────────────────────

    /**
     * Immutable record of a single MCP tool invocation inside the agentic loop.
     */
    public static final class ToolCallRecord {

        private final String  toolName;
        private final String  toolCallId;
        private final String  result;
        private final long    durationMs;
        private final boolean success;

        public ToolCallRecord(String toolName, String toolCallId, String result,
                              long durationMs, boolean success) {
            this.toolName   = toolName;
            this.toolCallId = toolCallId;
            this.result     = result;
            this.durationMs = durationMs;
            this.success    = success;
        }

        /** Raw (prefixed) tool name as sent by the model, e.g. {@code "a<UUID>_readFile"}. */
        public String getToolName()   { return toolName;   }
        /** Unique call ID assigned by the model for this specific invocation. */
        public String getToolCallId() { return toolCallId; }
        /** Full result string returned by the tool. */
        public String getResult()     { return result;     }
        /** Wall-clock time from tool start to result obtained. */
        public long   getDurationMs() { return durationMs; }
        /** {@code true} if the tool executed without error. */
        public boolean isSuccess()    { return success;    }

        @Override
        public String toString() {
            return "ToolCallRecord{toolName='" + toolName + "', toolCallId='" + toolCallId
                    + "', durationMs=" + durationMs + ", success=" + success + '}';
        }
    }
}
