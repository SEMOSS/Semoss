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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Result of running an {@link prerna.engine.api.IEvalEngine} against a single agent trace.
 */
public final class EvalResult {

    public enum Status { PASS, FAIL, ERROR }

    /** A single failure finding from an eval engine. */
    public static final class Failure {
        private final String code;
        private final String message;
        public Failure(String code, String message) { this.code = code; this.message = message; }
        public String getCode()    { return code;    }
        public String getMessage() { return message; }
        @Override public String toString() { return code + ": " + message; }
    }

    private final String        traceId;
    private final String        evalEngineId;
    private final Status        status;
    private final List<Failure> failures;
    private final double        score;
    /** Non-null when {@code status == Status.ERROR}. */
    private final String        errorCause;

    private EvalResult(Builder b) {
        this.traceId      = b.traceId;
        this.evalEngineId = b.evalEngineId;
        this.failures     = Collections.unmodifiableList(b.failures);
        this.status       = b.errored ? Status.ERROR : (b.failures.isEmpty() ? Status.PASS : Status.FAIL);
        this.score        = b.score;
        this.errorCause   = b.errorCause;
    }

    public String getTraceId()        { return traceId; }
    public String getEvalEngineId()   { return evalEngineId; }
    public Status getStatus()         { return status; }
    public boolean isPassed()         { return status == Status.PASS; }
    public List<Failure> getFailures(){ return failures; }
    /** Normalized score 0.0–1.0. Meaning is eval-engine-specific. 1.0 = full pass. */
    public double getScore()          { return score; }
    /** Non-null when status is {@code ERROR}. Contains the exception or error message. */
    public String getErrorCause()     { return errorCause; }

    public static Builder builder(String traceId, String evalEngineId) {
        return new Builder(traceId, evalEngineId);
    }

    public static final class Builder {
        private final String        traceId;
        private final String        evalEngineId;
        private final List<Failure> failures = new ArrayList<>();
        private double              score    = 1.0;
        private boolean             errored  = false;
        private String              errorCause;

        private Builder(String traceId, String evalEngineId) {
            this.traceId = traceId;
            this.evalEngineId = evalEngineId;
        }

        public Builder addFailure(String code, String message) {
            this.failures.add(new Failure(code, message));
            this.score = 0.0;
            return this;
        }
        public Builder score(double v) { this.score = v; return this; }

        /** Marks this result as an evaluation error (e.g. the eval engine itself threw). */
        public Builder error(String cause) {
            this.errored    = true;
            this.errorCause = cause;
            this.score      = 0.0;
            return this;
        }

        public EvalResult build() { return new EvalResult(this); }
    }
}
