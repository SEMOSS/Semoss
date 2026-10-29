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
package prerna.reactor.agent.admin;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.EnumSet;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.om.Insight;
import prerna.reactor.agent.run.AgentRunStatus;
import prerna.reactor.agent.run.AgentRunStore;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;

/**
 * Only an admin lists every user's agent run rooms, and the status filter takes
 * every value given.
 */
class AdminGetAgentRunRoomsReactorUnitTests {

	private AdminGetAgentRunRoomsReactor reactor(User user, String... statuses) {
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);
		NounStore store = new NounStore("admin-agent-run-rooms-test");
		for (String status : statuses) {
			store.makeGenRowStruct("status").add(status, PixelDataType.CONST_STRING);
		}
		AdminGetAgentRunRoomsReactor reactor = new AdminGetAgentRunRoomsReactor();
		reactor.setInsight(insight);
		reactor.setNounStore(store);
		return reactor;
	}

	@Test
	void nonAdminsAreRefused() {
		User user = mock(User.class);
		try (var admins = mockStatic(SecurityAdminUtils.class); var store = mockStatic(AgentRunStore.class)) {
			admins.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(false);
			assertThrows(SecurityException.class, reactor(user)::execute);
			store.verifyNoInteractions();
		}
	}

	@Test
	void adminsListRoomsHoldingAnyOfTheGivenStatuses() {
		User user = mock(User.class);
		AdminAgentRunReadService readService = mock(AdminAgentRunReadService.class);
		when(readService.getRooms(any(), anyLong(), anyLong())).thenReturn(Map.of());
		try (var reads = mockStatic(AdminAgentRunReadService.class)) {
			reads.when(() -> AdminAgentRunReadService.getInstance(user)).thenReturn(readService);

			reactor(user, "running", "INPUT_REQUIRED").execute();

			verify(readService)
					.getRooms(
							eq(new AgentRunStore.RoomFilter(null, null, null, null,
									EnumSet.of(AgentRunStatus.RUNNING, AgentRunStatus.INPUT_REQUIRED))),
							eq(25L), eq(0L));
		}
	}

	@Test
	void aUserFilterTakesTheUsersLoginType() {
		User user = mock(User.class);
		AdminAgentRunReadService readService = mock(AdminAgentRunReadService.class);
		when(readService.getRooms(any(), anyLong(), anyLong())).thenReturn(Map.of());
		try (var reads = mockStatic(AdminAgentRunReadService.class)) {
			reads.when(() -> AdminAgentRunReadService.getInstance(user)).thenReturn(readService);
			AdminGetAgentRunRoomsReactor reactor = reactor(user);
			NounStore store = reactor.getNounStore();
			store.makeGenRowStruct("userId").add("alice", PixelDataType.CONST_STRING);
			store.makeGenRowStruct("userType").add(" MICROSOFT ", PixelDataType.CONST_STRING);

			reactor.execute();

			verify(readService).getRooms(eq(new AgentRunStore.RoomFilter(null, "alice", "MICROSOFT", null,
					EnumSet.noneOf(AgentRunStatus.class))), eq(25L), eq(0L));
		}
	}

	@Test
	void anUnknownStatusIsRefused() {
		User user = mock(User.class);
		AdminAgentRunReadService readService = mock(AdminAgentRunReadService.class);
		try (var reads = mockStatic(AdminAgentRunReadService.class)) {
			reads.when(() -> AdminAgentRunReadService.getInstance(user)).thenReturn(readService);
			assertThrows(IllegalArgumentException.class, reactor(user, "DONE")::execute);
			verifyNoInteractions(readService);
		}
	}
}
