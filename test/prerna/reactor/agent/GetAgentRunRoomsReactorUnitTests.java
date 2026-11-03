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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.om.Insight;
import prerna.reactor.agent.run.AgentRunStatus;
import prerna.reactor.agent.run.AgentRunStore;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;

/**
 * An agent's Activity tab lists only its user's own rooms, read by their
 * insight, with the room search and statuses given.
 */
class GetAgentRunRoomsReactorUnitTests {

	private GetAgentRunRoomsReactor reactor(Insight insight, String agentId, String search, String status) {
		NounStore store = new NounStore("get-agent-run-rooms-test");
		if (agentId != null) {
			store.makeGenRowStruct("agentId").add(agentId, PixelDataType.CONST_STRING);
		}
		if (search != null) {
			store.makeGenRowStruct("search").add(search, PixelDataType.CONST_STRING);
		}
		if (status != null) {
			store.makeGenRowStruct("status").add(status, PixelDataType.CONST_STRING);
		}
		GetAgentRunRoomsReactor reactor = new GetAgentRunRoomsReactor();
		reactor.setInsight(insight);
		reactor.setNounStore(store);
		return reactor;
	}

	@Test
	void listsTheCallersOwnRoomsWithTheAgent() {
		Insight insight = mock(Insight.class);
		try (var store = mockStatic(AgentRunStore.class)) {
			store.when(() -> AgentRunStore.getRoomsForAgent(eq(insight), eq("agent-a"), eq("report"), anySet(),
					anyLong(), anyLong())).thenReturn(List.of());
			store.when(() -> AgentRunStore.countRoomsForAgent(eq(insight), eq("agent-a"), eq("report"), anySet()))
					.thenReturn(0L);

			Object page = reactor(insight, "agent-a", " report ", "running").execute().getValue();

			store.verify(() -> AgentRunStore.getRoomsForAgent(insight, "agent-a", "report",
					EnumSet.of(AgentRunStatus.RUNNING), 25L, 0L));
			assertEquals(Map.of("rooms", List.of(), "total", 0L), page);
		}
	}

	@Test
	void anAgentIsRequired() {
		try (var store = mockStatic(AgentRunStore.class)) {
			assertThrows(IllegalArgumentException.class, reactor(mock(Insight.class), " ", null, null)::execute);
			store.verifyNoInteractions();
		}
	}
}
