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
import java.util.HashMap;
import java.util.Map;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.om.Insight;
import prerna.reactor.agent.sandbox.SandboxPolicy;

/**
 * Immutable value object carrying all context needed by an {@link IAgentHarness} implementation.
 *
 * <p>Built via {@link #builder()}.
 */
public final class AgentRunContext {

    /** Default safety cap for tool-call rounds (a.k.a. "turns"). */
    public static final int DEFAULT_MAX_TURNS       = 30;
    /** Default reflection rounds — 0 means no reflection (backward-compatible). */
    public static final int DEFAULT_MAX_REFLECTIONS = 0;

    private final Room                   room;
    private final IModelEngine           modelEngine;
    private final Insight                insight;
    private final String                 userId;
    private final String                 filePath;
    private final String                 input;
    private final Map<String, Object>    paramMap;
    private final int                    maxTurns;
    private final int                    maxReflections;
    private final SandboxPolicy          sandboxPolicy;

    private AgentRunContext(Builder b) {
        this.room           = b.room;
        this.modelEngine    = b.modelEngine;
        this.insight        = b.insight;
        this.userId         = b.userId;
        this.filePath       = b.filePath;
        this.input          = b.input;
        this.paramMap       = Collections.unmodifiableMap(b.paramMap);
        this.maxTurns       = b.maxTurns;
        this.maxReflections = b.maxReflections;
        this.sandboxPolicy  = b.sandboxPolicy;
    }

    // Accessors
    /** Pre-loaded SEMOSS Room supplying system prompt, history, and MCP tools. */
    public Room getRoom() {
        return room;
    }

    /** Model engine resolved from {@code room.getModelId()}. */
    public IModelEngine getModelEngine() {
        return modelEngine;
    }

    /** Current insight context (for tool execution and file-system access). */
    public Insight getInsight() {
        return insight;
    }

    /** User ID from {@code room.getUserId()} — used when persisting messages. */
    public String getUserId() {
        return userId;
    }

    /**
     * Optional working directory or project ID for file-system tools.
     * Used as {@code cwd} by {@link ClaudeCodeAgentHarness} and added to paramMap under
     * {@code "file_path"} for {@link RoomAgentHarness}.
     */
    public String getFilePath() {
        return filePath;
    }

    /** Initial user message to start the agentic loop. */
    public String getInput() {
        return input;
    }

    /** Extra parameters forwarded to the model engine. Immutable view. */
    public Map<String, Object> getParamMap() {
        return paramMap;
    }

    /** Maximum tool-call rounds before the harness should throw or abort. */
    public int getMaxTurns() {
        return maxTurns;
    }

    /** Maximum self-critique rounds after the first RESPONSE_TEXT. 0 disables reflection. */
    public int getMaxReflections() {
        return maxReflections;
    }

    /**
     * Filesystem allowlist applied to the agent binary (claude-code, copilot, …)
     * before it {@code execvp}s. {@code null} means the caller did not build
     * a policy and the harness should either construct a default one or skip
     * sandboxing — see {@link prerna.reactor.agent.sandbox.AgentSandboxConfig}.
     */
    public SandboxPolicy getSandboxPolicy() {
        return sandboxPolicy;
    }

    // Builder
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Room                room;
        private IModelEngine        modelEngine;
        private Insight             insight;
        private String              userId;
        private String              filePath;
        private String              input;
        private Map<String, Object> paramMap       = new HashMap<>();
        private int                 maxTurns       = DEFAULT_MAX_TURNS;
        private int                 maxReflections = DEFAULT_MAX_REFLECTIONS;
        private SandboxPolicy       sandboxPolicy;

        public Builder room(Room room)                       { this.room = room;                   return this; }
        public Builder modelEngine(IModelEngine modelEngine) { this.modelEngine = modelEngine;     return this; }
        public Builder insight(Insight insight)              { this.insight = insight;             return this; }
        public Builder userId(String userId)                 { this.userId = userId;               return this; }
        public Builder filePath(String filePath)             { this.filePath = filePath;           return this; }
        public Builder input(String input)                   { this.input = input;                 return this; }
        public Builder paramMap(Map<String, Object> paramMap){
            this.paramMap = paramMap != null ? paramMap : new HashMap<>();
            return this;
        }
        public Builder maxTurns(int maxTurns)                { this.maxTurns = maxTurns;             return this; }
        public Builder maxReflections(int maxReflections)    { this.maxReflections = maxReflections; return this; }
        public Builder sandboxPolicy(SandboxPolicy policy)   { this.sandboxPolicy = policy;         return this; }

        public AgentRunContext build() {
            if (room == null)        throw new IllegalStateException("room is required");
            if (modelEngine == null) throw new IllegalStateException("modelEngine is required");
            if (insight == null)     throw new IllegalStateException("insight is required");
            if (input == null || input.trim().isEmpty())
                throw new IllegalStateException("input is required");
            if (maxTurns <= 0)
                throw new IllegalArgumentException("maxTurns must be > 0");
            if (maxReflections < 0)
                throw new IllegalArgumentException("maxReflections must be >= 0");
            return new AgentRunContext(this);
        }
    }
}
