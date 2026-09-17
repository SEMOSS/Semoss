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
package prerna.reactor.agent.runtime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import prerna.engine.api.ToolExecutionResult;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.config.AgentConfig;

class ResultToolTest {
    @Test void resultToolReturnsPartialOrFailedReportVerbatimWithoutModelFollowup() {
        for (String status : List.of("partial", "failed", "complete")) {
            String report = "{\"status\":\"" + status + "\",\"verdict\":\"inconclusive\",\"consistencyReview\":\"incomplete\"}";
            Room room = mock(Room.class);
            var messages = new ArrayList<AbstractMessage>();
            when(room.getMessages()).thenReturn(messages);
            AgentConfig config = AgentConfig.builder().resultTool("InspectPptx").build();
            AgentRunContext ctx = mock(AgentRunContext.class);
            when(ctx.getRoom()).thenReturn(room);
            when(ctx.getAgentConfig()).thenReturn(config);
            when(room.addToolExecutionResult(anyString(), anyString(), anyString(), anyMap(), anyMap(), any(), any(), any(), anyString(), any(ResponseMessage.class)))
                .thenAnswer(call -> { messages.add(call.getArgument(9)); return null; });
            try (var tools = mockStatic(PlatformAgentTools.class)) {
                tools.when(() -> PlatformAgentTools.isDefaultTool("InspectPptx")).thenReturn(true);
                tools.when(() -> PlatformAgentTools.executeDefaultToolResult(eq("InspectPptx"), anyMap(), eq(ctx)))
                    .thenReturn(ToolExecutionResult.success(report));
                ResponseMessage call = ResponseMessage.toolResponses(List.of(Map.of("id", "call-1", "name", "InspectPptx", "input", Map.of())));
                AgentLoopState state = new AgentLoopState();
                ResponseMessage result = HarnessToolExecutor.executeToolBatch(call, state, new HashMap<>(), ctx);
                assertEquals(report, result.getContent());
                assertEquals(report, state.getFinalText());
                assertTrue(state.isTerminal());
                assertEquals(1, state.getToolCallRecords().size());
                verify(room, never()).addToolExecutionResult(any(), any(), any(), any(), any(), any(), any(), any(), any());
                verify(room, never()).ask(any(), any(), any());
            }
        }
    }

    @Test void resultToolCannotStartMultipleInspectionsInOneBatch() {
        AgentRunContext ctx = mock(AgentRunContext.class);
        when(ctx.getRoom()).thenReturn(mock(Room.class));
        when(ctx.getAgentConfig()).thenReturn(AgentConfig.builder().resultTool("InspectPptx").build());
        ResponseMessage call = ResponseMessage.toolResponses(List.of(
            Map.of("id", "one", "name", "InspectPptx", "input", Map.of()),
            Map.of("id", "two", "name", "InspectPptx", "input", Map.of())));
        assertThrows(IllegalArgumentException.class, () -> HarnessToolExecutor.executeToolBatch(call, new AgentLoopState(), new HashMap<>(), ctx));
        assertNull(AgentConfig.builder().build().getResultTool());
    }
}
