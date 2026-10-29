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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.run.AgentRunStore;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;

/**
 * Only an admin reads the agents and users the Agent Runs filters offer, with
 * the search and limit given.
 */
class AdminGetAgentRunFilterOptionsReactorUnitTests {

	private static <T extends AbstractReactor> T reactor(T reactor, User user, String search, String limit) {
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);
		NounStore store = new NounStore("admin-agent-run-filter-options-test");
		if (search != null) {
			store.makeGenRowStruct("search").add(search, PixelDataType.CONST_STRING);
		}
		if (limit != null) {
			store.makeGenRowStruct("limit").add(limit, PixelDataType.CONST_STRING);
		}
		reactor.setInsight(insight);
		reactor.setNounStore(store);
		return reactor;
	}

	@Test
	void nonAdminsAreRefused() {
		User user = mock(User.class);
		try (var admins = mockStatic(SecurityAdminUtils.class); var store = mockStatic(AgentRunStore.class)) {
			admins.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(false);

			assertThrows(SecurityException.class,
					reactor(new AdminGetAgentRunAgentsReactor(), user, null, null)::execute);
			assertThrows(SecurityException.class,
					reactor(new AdminGetAgentRunUsersReactor(), user, null, null)::execute);
			store.verifyNoInteractions();
		}
	}

	@Test
	void adminsSearchTheAgentsAndUsersWithTheLimitGiven() {
		User user = mock(User.class);
		AdminAgentRunReadService readService = mock(AdminAgentRunReadService.class);
		when(readService.getAgentOptions("report", 25L)).thenReturn(List.of());
		when(readService.getUserOptions("alice", 10L)).thenReturn(List.of());
		try (var reads = mockStatic(AdminAgentRunReadService.class)) {
			reads.when(() -> AdminAgentRunReadService.getInstance(user)).thenReturn(readService);

			reactor(new AdminGetAgentRunAgentsReactor(), user, " report ", null).execute();
			reactor(new AdminGetAgentRunUsersReactor(), user, "alice", "10").execute();

			verify(readService).getAgentOptions("report", 25L);
			verify(readService).getUserOptions("alice", 10L);
		}
	}

	@Test
	void aLimitOutOfRangeIsRefused() {
		User user = mock(User.class);
		AdminAgentRunReadService readService = mock(AdminAgentRunReadService.class);
		try (var reads = mockStatic(AdminAgentRunReadService.class)) {
			reads.when(() -> AdminAgentRunReadService.getInstance(user)).thenReturn(readService);

			assertThrows(IllegalArgumentException.class,
					reactor(new AdminGetAgentRunUsersReactor(), user, null, "500")::execute);
			verifyNoInteractions(readService);
		}
	}
}
