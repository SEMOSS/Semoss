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

import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.run.AgentRunHandle;
import prerna.reactor.agent.run.AgentRunRequest;
import prerna.reactor.agent.run.AgentRunService;
import prerna.reactor.agent.runtime.SemossAgentHarness;

/** Submits a same-room successor run for a validated transfer target. */
public final class AgentTransferService {

	private static final Gson GSON = new Gson();

	private AgentTransferService() {
	}

	public static String transfer(AgentRunContext context, RoomAgentRoster.Target requestedTarget,
			Map<String, Object> arguments) {
		List<RoomAgentRoster.Target> currentTargets = RoomAgentRoster.transferTargets(context.getRoom(),
				context.getAgentConfig());
		RoomAgentRoster.Target target = AgentTransferToolSynthesizer.find(currentTargets,
				requestedTarget == null ? null : requestedTarget.toolName());
		if (target == null) {
			throw new IllegalArgumentException("The requested agent is no longer available in this room");
		}
		String instructions = arguments == null ? null : stringValue(arguments.get("task"));
		if (instructions == null) {
			throw new IllegalArgumentException("task is required");
		}

		String transferInput = "Original user request:\n" + context.getInput() + "\n\nTransfer instructions:\n"
				+ instructions;
		AgentRunRequest successor = new AgentRunRequest(context.getRoom().getId(), transferInput,
				context.getModelEngine().getEngineId(), SemossAgentHarness.NAME, target.workspaceId(),
				context.getMaxTurns(), context.getMaxReflections(), context.getAgentConfig().getModelParams(),
				context.getAgentConfig().getAgentParams(), null, null, context.getInsight())
				.withTransferMetadata(context.getRunId(), context.getRunId());
		AgentRunHandle handle = AgentRunService.get().run(successor);
		return GSON.toJson(Map.of("runId", handle.runId(), "roomId", handle.roomId(), "status", handle.status().name(),
				"workspaceId", target.workspaceId(), "agentName", target.name()));
	}

	private static String stringValue(Object value) {
		if (value == null || String.valueOf(value).isBlank()) {
			return null;
		}
		return String.valueOf(value).trim();
	}
}
