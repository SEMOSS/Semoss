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
package prerna.tcp.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import prerna.auth.User;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.workers.NativePyEngineWorker;

class NativePySocketClientUnitTests {

	private final List<String> storedInsights = new ArrayList<>();

	@AfterEach
	void tearDown() {
		storedInsights.forEach(id -> InsightStore.getInstance().remove(id));
		ThreadStore.remove();
	}

	private String store(User user) {
		Insight insight = new Insight();
		insight.setInsightId("py-client-" + UUID.randomUUID());
		insight.setUser(user);
		storedInsights.add(InsightStore.getInstance().put(insight));
		return insight.getInsightId();
	}

	/**
	 * Runs an engine request and returns the user handed to the worker, followed by
	 * the thread's user while the worker ran.
	 */
	private static List<Object> runEngineRequest(PayloadStruct ps) {
		NativePySocketClient client = spy(new NativePySocketClient());
		doReturn(null).when(client).executeCommand(any());
		List<Object> users = new ArrayList<>();
		try (MockedConstruction<NativePyEngineWorker> workers = mockConstruction(NativePyEngineWorker.class,
				(worker, context) -> {
					users.add(context.arguments().get(0));
					doAnswer(i -> users.add(ThreadStore.getUser())).when(worker).run();
				})) {
			client.processEngineRequest(ps);
		}
		return users;
	}

	@Test
	void runsAnEngineRequestAsTheExecutionUserAndClearsTheThread() {
		User owner = mock(User.class);
		User caller = mock(User.class);
		PayloadStruct ps = new PayloadStruct();
		ps.insightId = store(owner);
		ps.executionInsightId = store(caller);

		assertEquals(List.of(caller, caller), runEngineRequest(ps));
		assertNull(ThreadStore.getUser(), "the reader thread is shared by every user of the process");
	}

	@Test
	void runsAnEngineRequestAsTheInsightUserWithoutAnExecutionUser() {
		User owner = mock(User.class);
		PayloadStruct ps = new PayloadStruct();
		ps.insightId = store(owner);
		ps.executionInsightId = store(null);

		assertEquals(List.of(owner, owner), runEngineRequest(ps));
		assertNull(ThreadStore.getUser());
	}
}
