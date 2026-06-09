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
package prerna.reactor.agent.config;

import java.util.Objects;

/**
 * Declarative spec for a named subagent the parent agent can delegate to.
 *
 * <p>Each spec surfaces to the LLM as a synthesized MCP tool named {@link #getAlias() alias};
 * invoking the tool spawns a child run against the configured {@link #getWorkspaceId() workspaceId}.
 * Specs are loaded from {@code WORKSPACE.CONFIG_JSON.subagents[]} by
 * {@link AgentConfigLoader#resolveSubagents(org.json.JSONObject)} and attached to
 * {@link AgentConfig#getSubagents()}.
 *
 * <p>Immutable.
 */
public final class SubAgentSpec {

    private final String alias;
    private final String workspaceId;
    private final String description;

    public SubAgentSpec(String alias, String workspaceId, String description) {
        if (alias == null || alias.trim().isEmpty()) {
            throw new IllegalArgumentException("alias is required");
        }
        if (workspaceId == null || workspaceId.trim().isEmpty()) {
            throw new IllegalArgumentException("workspaceId is required");
        }
        this.alias       = alias.trim();
        this.workspaceId = workspaceId.trim();
        this.description = description;
    }

    /** Tool name the LLM sees; must be unique within a workspace's subagent list. */
    public String getAlias() {
        return alias;
    }

    /** Target child workspace id. The child run loads its own CONFIG_JSON (prompt, MCPs, hooks). */
    public String getWorkspaceId() {
        return workspaceId;
    }

    /** Free-form description shown to the LLM as the tool description. May be {@code null}. */
    public String getDescription() {
        return description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SubAgentSpec)) return false;
        SubAgentSpec that = (SubAgentSpec) o;
        return alias.equals(that.alias)
                && workspaceId.equals(that.workspaceId)
                && Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(alias, workspaceId, description);
    }

    @Override
    public String toString() {
        return "SubAgentSpec{alias=" + alias + ", workspaceId=" + workspaceId
                + ", descriptionChars=" + (description == null ? 0 : description.length()) + "}";
    }
}
