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

import java.util.Collections;
import java.util.List;

/**
 * Immutable result returned by {@link IAgentHarness#execute}.
 *
 * <p>Carries the final text response together with execution metadata useful for logging,
 * debugging, and audit:
 * <ul>
 *   <li>The total number of agentic tool-call rounds performed.
 *   <li>A per-tool-call trace ({@link ToolCallRecord}) with the tool name, call ID,
 *       result content, wall-clock duration, and success/failure status.
 * </ul>
 */
public final class AgentHarnessResult {

    /** Final {@code RESPONSE_TEXT} content produced by the model. Never null; may be empty. */
    private final String finalText;

    /** Total number of tool-call rounds completed (0 if the model responded without calling tools). */
    private final int iterations;

    /** Ordered record of every tool call made during the agentic loop. Immutable. */
    private final List<ToolCallRecord> toolCallRecords;

    /** Number of reflection rounds actually executed (0 if reflection was disabled). */
    private final int reflectionsUsed;

    /** Backward-compatible constructor — sets {@code reflectionsUsed = 0}. */
    public AgentHarnessResult(String finalText, int iterations, List<ToolCallRecord> toolCallRecords) {
        this(finalText, iterations, toolCallRecords, 0);
    }

    public AgentHarnessResult(String finalText, int iterations, List<ToolCallRecord> toolCallRecords,
                              int reflectionsUsed) {
        this.finalText        = finalText != null ? finalText : "";
        this.iterations       = iterations;
        this.toolCallRecords  = Collections.unmodifiableList(toolCallRecords);
        this.reflectionsUsed  = reflectionsUsed;
    }

    /** Final text content of the model's {@code RESPONSE_TEXT} message. */
    public String getFinalText() {
        return finalText;
    }

    /** Number of tool-call rounds the harness completed before obtaining the final text. */
    public int getIterations() {
        return iterations;
    }

    /** Number of reflection rounds executed (0 if {@code maxReflections} was 0). */
    public int getReflectionsUsed() {
        return reflectionsUsed;
    }

    /** Ordered list of all tool calls made during this run. */
    public List<ToolCallRecord> getToolCallRecords() {
        return toolCallRecords;
    }

    // ToolCallRecord

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
