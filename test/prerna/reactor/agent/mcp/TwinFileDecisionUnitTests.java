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
package prerna.reactor.agent.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;

import prerna.collaboration.CollaborationAgentTools;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.ToolExecutionResult;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.om.Insight;
import prerna.reactor.agent.run.AgentRunActionStore;
import prerna.reactor.agent.stream.AgentRunStreamService;
import prerna.util.Utility;

class TwinFileDecisionUnitTests {

	@ParameterizedTest(name = "{0} removal in {1}")
	@CsvSource({ "approve,twin-1", "reject,twin-1", "approve,changed-twin" })
	void anOwnerDecisionRunsOnlyTheApprovedChangeInItsOriginalChat(String decision, String roomTwin) {
		String tool = "RemoveFromDigitalTwin";
		Map<String, Object> file = Map.of("path", "agent:/files/plan.pdf");
		Map<String, Object> meta = Map.of("SMSS_TOOL_KIND", CollaborationAgentTools.TOOL_KIND,
				CollaborationAgentTools.TWIN_APPROVAL_PROJECT, "twin-1");
		Map<String, Object> action = Map.of("status", "PENDING", "userId", "owner", "roomId", "original-room",
				"runId", "run", "toolCallId", "call", "parentMessageId", "message", "toolName", tool,
				"toolArgs", file, "toolMeta", meta);
		Insight insight = mock(Insight.class);
		when(insight.getUserId()).thenReturn("owner");
		when(insight.getRoomId()).thenReturn("different-room");
		Room room = mock(Room.class);
		when(room.getOptionsMap()).thenReturn(Map.of("workspace", roomTwin));
		when(room.getModelId()).thenReturn("model");
		IModelEngine model = mock(IModelEngine.class);
		AgentRunStreamService stream = mock(AgentRunStreamService.class);
		AgentToolDecisionHandler handler = new AgentToolDecisionHandler(insight);

		try (MockedStatic<AgentRunActionStore> actions = mockStatic(AgentRunActionStore.class);
				MockedStatic<CollaborationAgentTools> tools = mockStatic(CollaborationAgentTools.class);
				MockedStatic<RoomUtils> rooms = mockStatic(RoomUtils.class);
				MockedStatic<Utility> models = mockStatic(Utility.class);
				MockedStatic<AgentRunStreamService> streams = mockStatic(AgentRunStreamService.class)) {
			actions.when(() -> AgentRunActionStore.getActionById("action", "owner")).thenReturn(action);
			actions.when(() -> AgentRunActionStore.claimForExecution("action", "run", "owner")).thenReturn(true);
			rooms.when(() -> RoomUtils.getOrLoadRoom("original-room", insight)).thenReturn(room);
			models.when(() -> Utility.getModel("model")).thenReturn(model);
			streams.when(AgentRunStreamService::get).thenReturn(stream);
			tools.when(() -> CollaborationAgentTools.isTool(tool)).thenReturn(true);
			tools.when(() -> CollaborationAgentTools.isTwinFileTool(tool)).thenReturn(true);
			tools.when(() -> CollaborationAgentTools.isToolMeta(meta)).thenReturn(true);
			tools.when(() -> CollaborationAgentTools.execute(tool, file, insight)).thenAnswer(invocation -> {
				verify(insight).setRoomForInsight(room);
				return ToolExecutionResult.success("Removed the copy.");
			});

			if (!"twin-1".equals(roomTwin)) {
				assertThrows(IllegalStateException.class,
						() -> handler.handleDecision("action", decision, null, null, null, Map.of()));
				actions.verify(() -> AgentRunActionStore.claimForExecution("action", "run", "owner"), never());
				tools.verify(() -> CollaborationAgentTools.execute(tool, file, insight), never());
				return;
			}
			boolean approved = "approve".equals(decision);
			String status = approved ? "APPROVED" : "REJECTED";
			String result = approved ? "Removed the copy." : "Tool call rejected by user.";
			String toolStatus = approved ? "success" : "cancelled";
			actions.when(() -> AgentRunActionStore.markDecided("action", "run", "owner", null, result, status, toolStatus))
					.thenReturn(true);
			assertEquals(result, handler.handleDecision("action", decision, null, null, null, Map.of()));
			actions.verify(() -> AgentRunActionStore.markDecided("action", "run", "owner", null, result, status, toolStatus));
			if (approved) {
				tools.verify(() -> CollaborationAgentTools.execute(tool, file, insight));
			} else {
				tools.verify(() -> CollaborationAgentTools.execute(tool, file, insight), never());
			}
		}
	}
}
