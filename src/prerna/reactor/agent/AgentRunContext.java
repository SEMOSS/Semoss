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

import java.util.HashMap;
import java.util.Map;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.om.Insight;
import prerna.reactor.agent.config.AgentConfig;
import prerna.reactor.agent.sandbox.SandboxPolicy;

/**
 * Immutable per-call state handed to an {@link IAgentHarness}.
 *
 * <p>{@link AgentConfig} holds the resolved agent spec. This class holds the
 * live invocation state for one run and keeps a few delegating accessors for
 * older callers.
 */
public final class AgentRunContext {

    /** @deprecated read from {@code agentConfig.getBudgets()}. */
    @Deprecated
    public static final int DEFAULT_MAX_TURNS       = AgentConfig.Budgets.DEFAULT_MAX_TURNS;
    /** @deprecated read from {@code agentConfig.getBudgets()}. */
    @Deprecated
    public static final int DEFAULT_MAX_REFLECTIONS = AgentConfig.Budgets.DEFAULT_MAX_REFLECTIONS;

    // Root = not spawned by another agent. Each spawn increments by 1.
    public static final int ROOT_SPAWN_DEPTH = 0;

    // Live per-call state
    private final Room          room;
    private final IModelEngine  modelEngine;
    private final Insight       insight;
    private final String        userId;
    private final String        input;
    private final SandboxPolicy sandboxPolicy;
    private final String        runId;

    // 0 for a root run, parent.spawnDepth+1 for a subagent run.
    private final int           spawnDepth;

    // Resolved agent spec shared across harnesses.
    private final AgentConfig   agentConfig;

    private AgentRunContext(Builder b) {
        this.room          = b.room;
        this.modelEngine   = b.modelEngine;
        this.insight       = b.insight;
        this.userId        = b.userId;
        this.input         = b.input;
        this.sandboxPolicy = b.sandboxPolicy;
        this.runId         = b.runId;
        this.spawnDepth    = b.spawnDepth;
        this.agentConfig   = b.agentConfig;
    }

    // Live per-call state
    /** Pre-loaded SEMOSS Room supplying history and MCP tool plumbing. */
    public Room getRoom() {
        return room;
    }

    /** Model engine resolved from {@code agentConfig.getModelId()} (or fallback). */
    public IModelEngine getModelEngine() {
        return modelEngine;
    }

    /** Current insight context (for tool execution and file-system access). */
    public Insight getInsight() {
        return insight;
    }

    /** User id from {@code room.getUserId()} - used when persisting messages. */
    public String getUserId() {
        return userId;
    }

    /** Initial user message to start the agentic loop. */
    public String getInput() {
        return input;
    }

    /** Durable {@code AGENT_RUN.RUN_ID} for this invocation. May be {@code null} for legacy direct callers. */
    public String getRunId() {
        return runId;
    }

    /**
     * Filesystem allowlist applied to agent binaries before they {@code execvp}.
     * {@code null} when the caller did not build a policy; harnesses may construct
     * a default or skip sandboxing.
     */
    public SandboxPolicy getSandboxPolicy() {
        return sandboxPolicy;
    }

    /**
     * Resolved agent configuration - the single source of truth every harness
     * reads from. Built once by
     * {@link prerna.reactor.agent.config.AgentConfigLoader#load(Room, String, String, Map, int, int)}
     * at the top of {@link AgentRunner#run(String, String, String, String, int, int, Map, Insight)}.
     * Never {@code null}.
     */
    public AgentConfig getAgentConfig() {
        return agentConfig;
    }

    /** 0 = root run; checked against {@code agentConfig.getSpawnPolicy().getMaxSubagentDepth()}. */
    public int getSpawnDepth() {
        return spawnDepth;
    }

    // Compatibility accessors (delegate to AgentConfig)
    /**
     * @return working directory; equivalent to {@code getAgentConfig().getWorkingDir()}.
     * @deprecated read from {@link #getAgentConfig()}.
     */
    @Deprecated
    public String getFilePath() {
        return agentConfig.getWorkingDir();
    }

    /**
     * @return extra parameters forwarded to the model engine. Unmodifiable view.
     *         Equivalent to {@code getAgentConfig().getModelParams()}.
     * @deprecated read from {@link #getAgentConfig()}.
     */
    @Deprecated
    public Map<String, Object> getParamMap() {
        return agentConfig.getModelParams();
    }

    /**
     * @return tool-call round cap. Equivalent to
     *         {@code getAgentConfig().getBudgets().getMaxTurns()}.
     * @deprecated read from {@link #getAgentConfig()}.
     */
    @Deprecated
    public int getMaxTurns() {
        return agentConfig.getBudgets().getMaxTurns();
    }

    /**
     * @return reflection round cap. Equivalent to
     *         {@code getAgentConfig().getBudgets().getMaxReflections()}.
     * @deprecated read from {@link #getAgentConfig()}.
     */
    @Deprecated
    public int getMaxReflections() {
        return agentConfig.getBudgets().getMaxReflections();
    }

    // Builder
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builds an {@link AgentRunContext}.
     *
     * <p>New code should supply {@link #agentConfig(AgentConfig)} directly.
     * The legacy setters still synthesize a minimal {@link AgentConfig} so
     * older callers continue to work.
     */
    public static final class Builder {
        // Live state
        private Room          room;
        private IModelEngine  modelEngine;
        private Insight       insight;
        private String        userId;
        private String        input;
        private SandboxPolicy sandboxPolicy;
        private String        runId;

        private int           spawnDepth = ROOT_SPAWN_DEPTH;

        // Either supplied directly or assembled from the legacy setters below.
        private AgentConfig   agentConfig;

        // Legacy field holders (used only when agentConfig is not supplied directly)
        private String              legacyFilePath;
        private Map<String, Object> legacyParamMap;
        private int                 legacyMaxTurns       = AgentConfig.Budgets.DEFAULT_MAX_TURNS;
        private int                 legacyMaxReflections = AgentConfig.Budgets.DEFAULT_MAX_REFLECTIONS;

        public Builder room(Room room)                       { this.room = room;                   return this; }
        public Builder modelEngine(IModelEngine modelEngine) { this.modelEngine = modelEngine;     return this; }
        public Builder insight(Insight insight)              { this.insight = insight;             return this; }
        public Builder userId(String userId)                 { this.userId = userId;               return this; }
        public Builder input(String input)                   { this.input = input;                 return this; }
        public Builder sandboxPolicy(SandboxPolicy policy)   { this.sandboxPolicy = policy;        return this; }
        public Builder runId(String runId)                   { this.runId = runId;                 return this; }

        public Builder spawnDepth(int spawnDepth)            { this.spawnDepth = spawnDepth;       return this; }

        /** Sets the canonical agent spec. Preferred path. */
        public Builder agentConfig(AgentConfig agentConfig)  { this.agentConfig = agentConfig;     return this; }

        /** @deprecated set on {@link AgentConfig} via {@link #agentConfig(AgentConfig)}. */
        @Deprecated
        public Builder filePath(String filePath)             { this.legacyFilePath = filePath;     return this; }

        /** @deprecated set on {@link AgentConfig} via {@link #agentConfig(AgentConfig)}. */
        @Deprecated
        public Builder paramMap(Map<String, Object> paramMap) {
            this.legacyParamMap = paramMap != null ? paramMap : new HashMap<>();
            return this;
        }

        /** @deprecated set on {@link AgentConfig.Budgets} via {@link #agentConfig(AgentConfig)}. */
        @Deprecated
        public Builder maxTurns(int maxTurns)                { this.legacyMaxTurns = maxTurns;             return this; }

        /** @deprecated set on {@link AgentConfig.Budgets} via {@link #agentConfig(AgentConfig)}. */
        @Deprecated
        public Builder maxReflections(int maxReflections)    { this.legacyMaxReflections = maxReflections; return this; }

        public AgentRunContext build() {
            if (room == null)        throw new IllegalStateException("room is required");
            if (modelEngine == null) throw new IllegalStateException("modelEngine is required");
            if (insight == null)     throw new IllegalStateException("insight is required");
            if (input == null || input.trim().isEmpty())
                throw new IllegalStateException("input is required");

            if (agentConfig == null) {
                // Backward-compat path: synthesize from legacy setters.
                agentConfig = AgentConfig.builder()
                        .workingDir(legacyFilePath)
                        .modelParams(legacyParamMap)
                        .budgets(AgentConfig.Budgets.of(legacyMaxTurns, legacyMaxReflections,
                                AgentConfig.Budgets.DEFAULT_MAX_SECONDS))
                        .build();
            }
            return new AgentRunContext(this);
        }
    }
}
