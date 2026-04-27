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

/**
 * Describes what an {@link prerna.engine.api.IEvalEngine} checks and how it scores.
 * Loaded from .smss config in {@link prerna.engine.api.IEvalEngine#open}.
 */
public final class EvalSpec {

    private final String       evalEngineId;
    private final String       evalType;
    private final List<String> requiredOutcomes;
    private final List<String> forbiddenActions;
    private final int          maxAllowedIterations;
    private final long         maxAllowedDurationSeconds;

    private EvalSpec(Builder b) {
        this.evalEngineId             = b.evalEngineId;
        this.evalType                 = b.evalType;
        this.requiredOutcomes         = Collections.unmodifiableList(b.requiredOutcomes);
        this.forbiddenActions         = Collections.unmodifiableList(b.forbiddenActions);
        this.maxAllowedIterations     = b.maxAllowedIterations;
        this.maxAllowedDurationSeconds = b.maxAllowedDurationSeconds;
    }

    public String       getEvalEngineId()              { return evalEngineId; }
    public String       getEvalType()                  { return evalType; }
    public List<String> getRequiredOutcomes()          { return requiredOutcomes; }
    public List<String> getForbiddenActions()          { return forbiddenActions; }
    public int          getMaxAllowedIterations()      { return maxAllowedIterations; }
    public long         getMaxAllowedDurationSeconds() { return maxAllowedDurationSeconds; }

    public static Builder builder(String evalEngineId) { return new Builder(evalEngineId); }

    public static final class Builder {
        private final String   evalEngineId;
        private String         evalType                  = "TASK_SUCCESS";
        private List<String>   requiredOutcomes          = Collections.emptyList();
        private List<String>   forbiddenActions          = Collections.emptyList();
        private int            maxAllowedIterations      = 30;
        private long           maxAllowedDurationSeconds = 120;

        private Builder(String evalEngineId) { this.evalEngineId = evalEngineId; }

        public Builder evalType(String v)                       { this.evalType = v; return this; }
        public Builder requiredOutcomes(List<String> v)         { this.requiredOutcomes = v; return this; }
        public Builder forbiddenActions(List<String> v)         { this.forbiddenActions = v; return this; }
        public Builder maxAllowedIterations(int v)              { this.maxAllowedIterations = v; return this; }
        public Builder maxAllowedDurationSeconds(long v)        { this.maxAllowedDurationSeconds = v; return this; }

        public EvalSpec build() { return new EvalSpec(this); }
    }
}
