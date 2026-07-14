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

import prerna.reactor.agent.run.AgentRunStatus;

/**
 * Output of {@link AgentSubAgentRegistry#spawn(SpawnRequest)}.
 *
 * <p>The {@code jobId} is the model-facing handle the parent passes to
 * {@code WaitForSubAgent} / {@code CheckSubAgentStatus} to collect the child's final answer.
 * Immutable.
 */
public final class SpawnResult {

    private final String jobId;
    private final String roomId;
    private final String alias;
    private final String workspaceId;
    private final AgentRunStatus status;

    public SpawnResult(String jobId, String roomId, String alias, String workspaceId, AgentRunStatus status) {
        this.jobId  = jobId;
        this.roomId = roomId;
        this.alias  = alias;
        this.workspaceId = workspaceId;
        this.status = status;
    }

    /** Durable child AgentRun id, also exposed as the job handle for wait/check tools. */
    public String getJobId() {
        return jobId;
    }

    /** Durable AgentRun id; currently equal to {@link #getJobId()}. */
    public String getRunId() {
        return jobId;
    }

    /** Newly created child room id. */
    public String getRoomId() {
        return roomId;
    }

    /** Configured alias if the spawn was named; {@code null} for anonymous spawns. */
    public String getAlias() {
        return alias;
    }

    /** Workspace selected for the child, or {@code null} for an anonymous clone. */
    public String getWorkspaceId() {
        return workspaceId;
    }

    /** Initial AgentRun status after submission. */
    public AgentRunStatus getStatus() {
        return status;
    }
}
