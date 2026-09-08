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
package prerna.playground.reactors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.engine.impl.model.Room;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.agent.run.AgentRoomNamer;
import prerna.sablecc2.comm.JobStreamEnvelopes;

class AskPlaygroundReactorTest {

	@Test
	void beforeRoomAskStartsAsynchronousRoomNaming() {
		String roomId = "room-id";
		String question = "How do I forecast quarterly revenue?";
		String engineId = "model-id";
		String userId = "user-id";
		String jobId = "job-id";

		Room room = mock(Room.class);
		when(room.getId()).thenReturn(roomId);
		when(room.getUserId()).thenReturn(userId);
		Insight insight = mock(Insight.class);

		TestAskPlaygroundReactor reactor = new TestAskPlaygroundReactor();
		reactor.setInsight(insight);

		CompletableFuture<String> expected = CompletableFuture.completedFuture("Quarterly Revenue Forecast");
		try (MockedStatic<ThreadStore> threadStore = mockStatic(ThreadStore.class);
				MockedStatic<AgentRoomNamer> namer = mockStatic(AgentRoomNamer.class);
				MockedStatic<JobStreamEnvelopes> stream = mockStatic(JobStreamEnvelopes.class)) {
			threadStore.when(ThreadStore::getJobId).thenReturn(jobId);
			namer.when(() -> AgentRoomNamer.nameRoomAsync(roomId, question, engineId, userId, insight))
					.thenReturn(expected);

			CompletableFuture<String> actual = reactor.invokeBeforeRoomAsk(room, question, engineId);

			namer.verify(() -> AgentRoomNamer.nameRoomAsync(roomId, question, engineId, userId, insight));
			stream.verify(() -> JobStreamEnvelopes.roomName(jobId, "Quarterly Revenue Forecast"));
			org.junit.jupiter.api.Assertions.assertSame(expected, actual);
		}
	}

	private static final class TestAskPlaygroundReactor extends AskPlaygroundReactor {

		private CompletableFuture<String> invokeBeforeRoomAsk(Room room, String question, String engineId) {
			return beforeRoomAsk(room, question, engineId);
		}
	}
}
