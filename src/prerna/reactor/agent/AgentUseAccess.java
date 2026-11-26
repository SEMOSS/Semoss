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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.access.EngineGrantee;
import prerna.auth.utils.SecurityEngineAccessUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.pipeline.AgentRunOperation;
import prerna.project.api.IProject;

/**
 * Who may use an agent, and so have its runs use the permissions engines and
 * projects gave it: a person who can view the agent, or, for a sub-agent, one
 * who may use an agent above it that was given it. A sub-agent's room records
 * the agents it was started under ({@link #ROOM_OPTION_AGENT_CHAIN}), the first
 * agent first, when it is spawned; the check reads only that list and the
 * grants, so it never loads or changes another room. Only a project of type
 * WORKSPACE is an agent.
 */
public final class AgentUseAccess {

	/**
	 * The room option that lists the agents a sub-agent's room was started under,
	 * the first agent first and the agent that spawned it last
	 */
	public static final String ROOM_OPTION_AGENT_CHAIN = "subagent_agent_chain";
	/** How many agents a sub-agent's room records above it */
	private static final int MAX_CHAIN = 8;

	private AgentUseAccess() {
	}

	/**
	 * Whether a person may use an agent in a room: they can view it, or the room is
	 * a sub-agent's and, walking up the agents it was started under, each agent was
	 * given the one below it until one the person can view.
	 *
	 * @param user    the person
	 * @param room    the room the agent runs in, or null
	 * @param agentId the agent
	 * @return whether the person may use the agent
	 */
	public static boolean mayUseAgent(User user, Room room, String agentId) {
		if (user == null || agentId == null) {
			return false;
		}
		if (SecurityProjectUtils.userCanViewProject(user, agentId)) {
			return true;
		}
		List<String> chain = agentChainOf(room);
		String used = agentId;
		for (int i = chain.size() - 1; i >= 0; i--) {
			String using = chain.get(i);
			if (!isAgent(using)
					|| SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(using), used, true) == null) {
				return false;
			}
			if (SecurityProjectUtils.userCanViewProject(user, using)) {
				return true;
			}
			used = using;
		}
		return false;
	}

	/**
	 * The agents a sub-agent's room records above it, as it was spawned.
	 *
	 * @param room the room, or null
	 * @return the agents, the first agent first; empty for a room that is not a
	 *         sub-agent's, or whose list is longer than allowed or has a blank id
	 */
	public static List<String> agentChainOf(Room room) {
		Map<String, Object> options = room == null ? null : room.getOptionsMap();
		Object value = options == null ? null : options.get(ROOM_OPTION_AGENT_CHAIN);
		if (!(value instanceof List<?> ids) || ids.size() > MAX_CHAIN) {
			return List.of();
		}
		List<String> chain = new ArrayList<>();
		for (Object id : ids) {
			String text = id == null ? "" : String.valueOf(id).trim();
			if (text.isEmpty()) {
				return List.of();
			}
			chain.add(text);
		}
		return chain;
	}

	/**
	 * The agents a new sub-agent's room records: those its parent room records,
	 * then the agent whose run spawns it when the sub-agent is another agent. A
	 * sub-agent that runs the same agent, as an unnamed spawn does, records what
	 * its parent room records.
	 *
	 * @param parentRoom    the room of the run that spawns the sub-agent
	 * @param parentAgentId the agent whose run spawns it, or null when the run has
	 *                      no agent
	 * @param childAgentId  the agent the sub-agent runs, or null when it runs the
	 *                      parent's agent
	 * @return the agents, the first agent first; empty when there are none, or when
	 *         there would be more than allowed
	 */
	public static List<String> subAgentChain(Room parentRoom, String parentAgentId, String childAgentId) {
		List<String> chain = new ArrayList<>(agentChainOf(parentRoom));
		if (parentAgentId != null && !parentAgentId.isBlank() && childAgentId != null
				&& !childAgentId.trim().equals(parentAgentId.trim())) {
			chain.add(parentAgentId.trim());
		}
		return chain.size() > MAX_CHAIN ? List.of() : chain;
	}

	/**
	 * Begins the use of the permissions an agent was given, on this thread, for an
	 * agent the person may use ({@link #mayUseAgent}). No agent, a project that is
	 * not an agent, or an agent the person may not use, gives nothing, so the work
	 * runs on the person's own access only.
	 *
	 * @param user    the person
	 * @param room    the room the agent runs in, or null
	 * @param agentId the agent, or null
	 * @return the agent run to close when the work ends, or null for none
	 */
	public static AgentRunOperation beginAgentRun(User user, Room room, String agentId) {
		if (agentId == null || agentId.isBlank() || user == null || !isAgent(agentId)
				|| !mayUseAgent(user, room, agentId)) {
			return null;
		}
		return AgentRunOperation.begin(agentId);
	}

	/**
	 * Whether a project is an agent: a project of type WORKSPACE.
	 *
	 * @param projectId the project
	 * @return whether it is an agent
	 */
	public static boolean isAgent(String projectId) {
		return projectId != null
				&& IProject.PROJECT_TYPE.WORKSPACE.name().equals(SecurityProjectUtils.getProjectTypeForId(projectId));
	}

}
