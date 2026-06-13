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
package prerna.reactor.agent.subagent;

import java.util.LinkedHashMap;
import java.util.Map;

// Minimal envelope returned to the parent agent by CheckSubAgentStatus / WaitForSubAgent.
// Intentionally small - context isolation is the point of subagents, so we do NOT propagate
// tool-call records, transcripts, token usage, or prompts. Those live in observability/logs.
//
// JSON shape on the wire:
//   { "jobId":..., "status": "...", "result": "...", "error": "..." }
public final class SubAgentResult {

    public enum Status {
        // CheckSubAgentStatus may return this; WaitForSubAgent returns it only when its own
        // timeoutSec elapsed while the child is still working (the child is unaffected).
        RUNNING("running"),
        // Terminal happy-path.
        SUCCEEDED("succeeded"),
        // Terminal error path. error field carries a short human/model-readable message.
        FAILED("failed"),
        // Terminal cancellation (cascade-cancel or explicit interrupt).
        CANCELLED("cancelled");

        private final String wire;
        Status(String wire) { this.wire = wire; }
        public String wire() { return wire; }
    }

    private final String jobId;
    private final Status status;
    private final String result;
    private final String error;

    private SubAgentResult(String jobId, Status status, String result, String error) {
        this.jobId     = jobId;
        this.status    = status;
        this.result    = result;
        this.error     = error;
    }

    public String getJobId()     { return jobId; }
    public Status getStatus()    { return status; }
    public String getResult()    { return result; }
    public String getError()     { return error; }

    public static SubAgentResult running(String jobId) {
        return new SubAgentResult(jobId, Status.RUNNING, null, null);
    }

    public static SubAgentResult succeeded(String jobId, String finalText) {
        return new SubAgentResult(jobId, Status.SUCCEEDED, finalText, null);
    }

    public static SubAgentResult failed(String jobId, String errorMessage) {
        return new SubAgentResult(jobId, Status.FAILED, null, errorMessage);
    }

    public static SubAgentResult cancelled(String jobId) {
        return new SubAgentResult(jobId, Status.CANCELLED, null, "Subagent was cancelled");
    }

    // Stable JSON-friendly Map view. Field order is fixed for log/diff readability.
    public Map<String, Object> toMap() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("jobId",  jobId);
        out.put("status", status.wire());
        out.put("result", result);
        out.put("error",  error);
        return out;
    }
}
