/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	you may obtain a copy of the License at
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
package prerna.playground.reactors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.om.Insight;
import prerna.playground.PlaygroundUtils;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

class CreatePlaygroundRoomReactorUnitTests {

	@Test
	void newRoomUsesOrchestratorWhenWorkspaceIsOmitted() {
		CreatePlaygroundRoomReactor reactor = new CreatePlaygroundRoomReactor();
		Insight insight = mock(Insight.class);
		User user = mock(User.class);
		Room room = mock(Room.class);
		when(insight.getUser()).thenReturn(user);
		when(room.getId()).thenReturn("room-id");
		reactor.setInsight(insight);
		reactor.setNounStore(new NounStore("test"));

		try (var security = mockStatic(SecurityProjectUtils.class);
				var rooms = mockStatic(RoomUtils.class);
				var utility = mockStatic(Utility.class)) {
			security.when(() -> SecurityProjectUtils.userCanViewProject(user, Constants.AGENT_ORCHESTRATOR))
					.thenReturn(true);
			rooms.when(() -> RoomUtils.createRoomIfNotExists(anyString(), eq(insight), isNull(), isNull(),
					eq(Constants.AGENT_ORCHESTRATOR), isNull(), isNull(), eq(PlaygroundUtils.PLAYGROUND_PROJECT_ID),
					isNull())).thenReturn(room);
			utility.when(() -> Utility.getDIHelperProperty(Constants.CHROOT_ENABLE)).thenReturn("false");

			NounMetadata result = reactor.execute();

			assertEquals("room-id", ((java.util.Map<?, ?>) result.getValue()).get("roomId"));
		}
	}
}
