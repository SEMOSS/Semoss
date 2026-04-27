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
/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); ...
 *******************************************************************************/
package prerna.reactor.agent.eval;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import prerna.reactor.agent.AgentHarnessResult;

/**
 * Immutable context object passed to {@link prerna.engine.api.IEvalEngine#evaluate}.
 *
 * <p>Eval engines must NOT reach into globals or the database to get context — everything
 * they need is in this object. This makes eval engines testable in isolation and prevents
 * security bypasses.
 *
 * <p>Built via {@link #builder()}.
 */
public final class AgentEvalContext {

    private final String               traceId;
    private final String               roomId;
    private final String               modelEngineId;
    private final String               userId;
    private final String               originalInput;
    private final AgentHarnessResult   result;
    private final Map<String, Object>  taskSpec;
    private final List<AgentHarnessResult.DecisionStep> steps;

    private AgentEvalContext(Builder b) {
        this.traceId       = b.traceId;
        this.roomId        = b.roomId;
        this.modelEngineId = b.modelEngineId;
        this.userId        = b.userId;
        this.originalInput = b.originalInput;
        this.result        = b.result;
        this.taskSpec      = b.taskSpec != null
                ? Collections.unmodifiableMap(b.taskSpec) : Collections.emptyMap();
        this.steps         = b.steps != null
                ? Collections.unmodifiableList(b.steps) : Collections.emptyList();
    }

    public String getTraceId()       { return traceId; }
    public String getRoomId()        { return roomId; }
    public String getModelEngineId() { return modelEngineId; }
    public String getUserId()        { return userId; }
    /** The original user input that started the agent run. */
    public String getOriginalInput() { return originalInput; }
    /** The full agent run result including tool call records. */
    public AgentHarnessResult getResult() { return result; }
    /** Task spec from Room options, if provided. Empty map if not set. */
    public Map<String, Object> getTaskSpec() { return taskSpec; }
    /** Normalized step trace (same as result.getSteps(), for convenience). */
    public List<AgentHarnessResult.DecisionStep> getSteps() { return steps; }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private String               traceId;
        private String               roomId;
        private String               modelEngineId;
        private String               userId;
        private String               originalInput;
        private AgentHarnessResult   result;
        private Map<String, Object>  taskSpec;
        private List<AgentHarnessResult.DecisionStep> steps;

        public Builder traceId(String v)       { this.traceId = v;       return this; }
        public Builder roomId(String v)        { this.roomId = v;        return this; }
        public Builder modelEngineId(String v) { this.modelEngineId = v; return this; }
        public Builder userId(String v)        { this.userId = v;        return this; }
        public Builder originalInput(String v) { this.originalInput = v; return this; }
        public Builder result(AgentHarnessResult v) { this.result = v;   return this; }
        public Builder taskSpec(Map<String, Object> v) { this.taskSpec = v; return this; }
        public Builder steps(List<AgentHarnessResult.DecisionStep> v) { this.steps = v; return this; }

        public AgentEvalContext build() {
            if (traceId == null) throw new IllegalStateException("traceId is required");
            if (result == null)  throw new IllegalStateException("result is required");
            return new AgentEvalContext(this);
        }
    }

    /** Convenience factory — derives all fields directly from a traced result. */
    public static AgentEvalContext fromResult(AgentHarnessResult result, String originalInput,
            Map<String, Object> taskSpec) {
        return builder()
                .traceId(result.getTraceId())
                .roomId(result.getRoomId())
                .modelEngineId(result.getModelEngineId())
                .originalInput(originalInput)
                .result(result)
                .taskSpec(taskSpec)
                .steps(result.getSteps())
                .build();
    }
}
