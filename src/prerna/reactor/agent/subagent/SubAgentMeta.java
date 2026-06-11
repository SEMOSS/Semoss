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

/**
 * Per-spawn metadata recorded by {@link AgentSubAgentRegistry}.
 *
 * <p>Heavy state (thread, status, stream queue, interrupt) lives in
 * {@link prerna.sablecc2.comm.PixelJobManager}; this record is just the
 * parent/alias/workspace context needed to assemble a subagent tree and
 * address stream events.
 *
 * <p>Immutable.
 */
public final class SubAgentMeta {

    private final String jobId;
    private final String parentJobId;
    private final String alias;
    private final String workspaceId;
    private final String childRoomId;
    private final long   spawnedAt;
    // Root=0, direct children=1, etc.
    private final int    spawnDepth;

    public SubAgentMeta(String jobId, String parentJobId, String alias, String workspaceId,
            String childRoomId, long spawnedAt, int spawnDepth) {
        this.jobId       = jobId;
        this.parentJobId = parentJobId;
        this.alias       = alias;
        this.workspaceId = workspaceId;
        this.childRoomId = childRoomId;
        this.spawnedAt   = spawnedAt;
        this.spawnDepth  = spawnDepth;
    }

    /** Async pixel job id assigned to this subagent run; doubles as the model-facing handle. */
    public String getJobId() {
        return jobId;
    }

    /** Job id of the parent run that spawned this child; {@code null} when no parent context. */
    public String getParentJobId() {
        return parentJobId;
    }

    /** Configured alias from {@code CONFIG_JSON.subagents[]}; {@code null} for anonymous spawns. */
    public String getAlias() {
        return alias;
    }

    /** Target child workspace id; {@code null} for anonymous (clone) spawns. */
    public String getWorkspaceId() {
        return workspaceId;
    }

    /** Room id of the freshly created child room. */
    public String getChildRoomId() {
        return childRoomId;
    }

    /** Wall-clock spawn timestamp (epoch ms). */
    public long getSpawnedAt() {
        return spawnedAt;
    }

    public int getSpawnDepth() {
        return spawnDepth;
    }
}
