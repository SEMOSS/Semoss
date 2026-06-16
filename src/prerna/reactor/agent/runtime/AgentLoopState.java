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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import prerna.reactor.agent.AgentHarnessResult;

/**
 * Mutable state bag for one agent run inside {@link SemossAgentHarness}.
 *
 * <p>Tracks iteration progress, reflection rounds, elapsed time, and the accumulating
 * tool-call record. Future PRs will extend this with token counts (for compaction) and
 * hook/span references (for observability).
 *
 * <p>Not thread-safe - owned and mutated exclusively by the harness's main execution thread.
 * Tool call records are added from the main thread after all parallel futures complete.
 */
public final class AgentLoopState {

    /** Counts completed tool-call rounds (not reflection rounds). */
    private int iterations = 0;

    /** Reflection rounds consumed so far. */
    private int reflectionsUsed = 0;

    /** Set to {@code true} to exit the main loop. */
    private boolean terminal = false;

    /** Final text content once the model stops calling tools. {@code null} until terminal. */
    private String finalText = null;

    /** Monotonic start of this run. Used for elapsed-time budget checks. */
    private final long startNanos = System.nanoTime();

    /** Ordered record of every tool call executed. */
    private final List<AgentHarnessResult.ToolCallRecord> toolCallRecords = new ArrayList<>();

    AgentLoopState() {}

    // Iteration counter
    /** Number of tool-call rounds completed (excludes reflection rounds). */
    public int getIterations() {
        return iterations;
    }

    /** Increment after each full tool batch has been submitted and responded to. */
    public void incrementIterations() {
        iterations++;
    }

    // Reflection counter
    /** Number of reflection rounds fired so far. */
    public int getReflectionsUsed() {
        return reflectionsUsed;
    }

    /** Increment once before sending the reflection prompt. */
    public void incrementReflections() {
        reflectionsUsed++;
    }

    // Terminal flag
    /** {@code true} when the loop should exit on the next check. */
    public boolean isTerminal() {
        return terminal;
    }

    public void setTerminal(boolean terminal) {
        this.terminal = terminal;
    }

    // Final text
    /** Set when the loop exits normally with a final assistant response. */
    public String getFinalText() {
        return finalText;
    }

    public void setFinalText(String text) {
        this.finalText = text;
    }

    // Elapsed time
    /** Milliseconds elapsed since this state object was created (i.e., since run start). */
    public long getElapsedMs() {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }

    // Tool call records
    /** Append a completed tool call record. Called from the harness main thread only. */
    public void addToolCallRecord(AgentHarnessResult.ToolCallRecord record) {
        toolCallRecords.add(record);
    }

    /** All tool calls made so far, in order. Unmodifiable view. */
    public List<AgentHarnessResult.ToolCallRecord> getToolCallRecords() {
        return Collections.unmodifiableList(toolCallRecords);
    }

    /** Snapshot for constructing the final {@link AgentHarnessResult}. */
    List<AgentHarnessResult.ToolCallRecord> getToolCallRecordsSnapshot() {
        return new ArrayList<>(toolCallRecords);
    }
}
