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
package prerna.collaboration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.reactor.agent.AgentHarnessResult;
import prerna.reactor.agent.AgentRunContext;
import prerna.om.Insight;

class InboxBriefHarnessUnitTests {
	@Test
	void returnsOneTextResponseWithoutExecutingAnyTools() {
		Room room = mock(Room.class);
		when(room.getId()).thenReturn("room-1");
		IModelEngine model = mock(IModelEngine.class);
		AgentRunContext context = AgentRunContext.builder().room(room).modelEngine(model).insight(mock(Insight.class)).runId("run-1").input("Inbox data").build();
		ResponseMessage response = ResponseMessage.builder().withText("A concise brief.").build();
		when(room.ask(any(InputMessage.class), same(model), isNull())).thenReturn(response);
		AgentHarnessResult result = new InboxBriefHarness().execute(context);
		assertEquals("A concise brief.", result.getFinalText());
		assertTrue(result.getToolCallRecords().isEmpty());
		verify(room, times(1)).ask(any(InputMessage.class), same(model), isNull());
	}

	@Test
	void refusesToolRequestsInsteadOfExecutingThem() {
		Room room = mock(Room.class);
		when(room.getId()).thenReturn("room-1");
		IModelEngine model = mock(IModelEngine.class);
		AgentRunContext context = AgentRunContext.builder().room(room).modelEngine(model).insight(mock(Insight.class)).runId("run-1").input("Inbox data").build();
		ResponseMessage response = mock(ResponseMessage.class);
		when(response.hasToolCallPart()).thenReturn(true);
		when(room.ask(any(InputMessage.class), same(model), isNull())).thenReturn(response);
		assertThrows(IllegalStateException.class, () -> new InboxBriefHarness().execute(context));
		verify(room, times(1)).ask(any(InputMessage.class), same(model), isNull());
	}
}
