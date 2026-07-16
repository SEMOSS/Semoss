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

import prerna.om.Insight;

/**
 * Input to {@link AgentSubAgentRegistry#spawn(SpawnRequest)}.
 *
 * <p>One of {@link #alias} or {@link #workspaceId} should be non-null to designate a
 * named subagent. When both are null the spawn is anonymous (the child clones the
 * parent room's config and history).
 *
 * <p>Mutable for fluent construction at the call site; once handed to
 * {@link AgentSubAgentRegistry#spawn(SpawnRequest)} it should not be modified.
 */
public final class SpawnRequest {

    /** Async pixel job id of the caller; used to address {@code subagent-spawned} stream events. May be {@code null}. */
    public String parentJobId;

    /** Room id of the caller; sets {@code PARENT_ROOM_ID} on the freshly created child room. */
    public String parentRoomId;

    /** Configured alias from {@code CONFIG_JSON.subagents[]}; {@code null} for anonymous spawns. */
    public String alias;

    /** Target child workspace id; {@code null} for anonymous (parent-clone) spawns. */
    public String workspaceId;

    /** User prompt sent into the child agent. Required. */
    public String prompt;

    /**
     * Optional per-spawn system-prompt override. When absent, anonymous children
     * inherit {@link #parentAuthoredSystemPrompt}; named children use their own workspace
     * system prompt.
     */
    public String additionalContext;

    /**
     * Parent's clean, user-authored system prompt, captured before the harness
     * temporarily composes runtime instructions into {@code room.options.instructions}.
     * Used only as the default for anonymous children; named children load their own
     * workspace system prompt.
     */
    public String parentAuthoredSystemPrompt;

    /** Optional engine fallback when neither room nor workspace specifies a model. */
    public String engine;

    /** Optional harness type override; defaults to {@code "semoss"}. */
    public String harnessType;

    /**
     * Optional absolute working-directory override for the child agent. When set, the
     * child's {@code RunAgent} call receives this as {@code working_dir} and
     * {@link prerna.reactor.agent.AgentRunner#resolveWorkingDir} honors it (with a
     * containment check against the SEMOSS base folder) instead of defaulting to the
     * child room's own folder. Used by {@code inherit_parent_workdir=true} so the
     * child operates on the parent's room folder while still having its own roomId
     * for stream + history isolation. {@code null} = use the default child room folder.
     */
    public String workingDirOverride;

    /** Caller's live insight - used for user, projectId, base URL inheritance. Required. */
    public Insight callerInsight;
}
