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
package prerna.reactor.agent.transfer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.reactor.agent.mcp.MCPUtility;

/** Builds model tools for same-room ownership transfers. */
public final class AgentTransferToolSynthesizer {

	private AgentTransferToolSynthesizer() {
	}

	public static List<Map<String, Object>> tools(List<RoomAgentRoster.Target> targets) {
		List<Map<String, Object>> tools = new ArrayList<>();
		for (RoomAgentRoster.Target target : targets) {
			Map<String, Object> task = new LinkedHashMap<>();
			task.put("type", "string");
			task.put("description", "The complete task and all relevant context for the receiving agent. Include the "
					+ "user's complete request; do not replace it with a summary that loses constraints.");

			Map<String, Object> schema = new LinkedHashMap<>();
			schema.put("type", "object");
			schema.put("title", target.toolName() + "_Arguments");
			schema.put("properties", Map.of("task", task));
			schema.put("required", List.of("task"));

			Map<String, Object> tool = new LinkedHashMap<>();
			tool.put("name", target.toolName());
			String description = target.description() == null ? "" : target.description().trim() + " ";
			tool.put("description", description + "Transfer this room to " + target.name()
					+ " for the current task. The specialist responds directly in this conversation; ownership returns "
					+ "to you when its run completes.");
			tool.put("inputSchema", schema);
			tool.put("_meta", Map.of("SMSS_TOOL_KIND", "semoss_agent_transfer", "SMSS_AGENT_ID",
					target.workspaceId(), MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.AUTO.getValue()));
			tools.add(tool);
		}
		return tools;
	}

	public static RoomAgentRoster.Target find(List<RoomAgentRoster.Target> targets, String toolName) {
		if (toolName == null) {
			return null;
		}
		for (RoomAgentRoster.Target target : targets) {
			if (toolName.equals(target.toolName())) {
				return target;
			}
		}
		return null;
	}
}
