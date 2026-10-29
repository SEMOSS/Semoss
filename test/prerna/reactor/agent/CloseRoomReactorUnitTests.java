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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.om.Insight;
import prerna.reactor.agent.run.AgentRunService;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;

/**
 * A user closes their own room through the service's owner path, with the
 * caller's insight.
 */
class CloseRoomReactorUnitTests {

	private CloseRoomReactor reactor(Insight insight, String roomId) {
		NounStore store = new NounStore("close-room-test");
		store.makeGenRowStruct("roomId").add(roomId, PixelDataType.CONST_STRING);
		CloseRoomReactor reactor = new CloseRoomReactor();
		reactor.setInsight(insight);
		reactor.setNounStore(store);
		return reactor;
	}

	@Test
	void closesTheCallersOwnRoomAndSaysWhatItStopped() {
		Insight insight = mock(Insight.class);
		AgentRunService service = mock(AgentRunService.class);
		when(service.closeRoom("r1", insight))
				.thenReturn(new AgentRunService.RoomClosure("agent-a", List.of("r1"), List.of("run2")));
		try (var services = mockStatic(AgentRunService.class)) {
			services.when(AgentRunService::get).thenReturn(service);

			Object result = reactor(insight, " r1 ").execute().getValue();

			assertEquals(Map.of("roomId", "r1", "closedRoomIds", List.of("r1"), "cancelledRunIds", List.of("run2")),
					result);
		}
	}

	@Test
	void aRoomIdIsRequired() {
		try (var services = mockStatic(AgentRunService.class)) {
			assertThrows(IllegalArgumentException.class, reactor(mock(Insight.class), " ")::execute);
			services.verifyNoInteractions();
		}
	}
}
