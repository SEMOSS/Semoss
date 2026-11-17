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
package prerna.reactor.collaboration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.collaboration.BrainMemoryUtils;
import prerna.collaboration.CollaborationUtils;
import prerna.collaboration.WorkInboxRoutine;
import prerna.engine.impl.model.Room;
import prerna.om.Insight;
import prerna.sablecc2.om.NounStore;

class BrainRememberReactorUnitTests {

	private final User user = mock(User.class);
	private final Insight insight = mock(Insight.class);
	private final Room room = mock(Room.class);

	private BrainRememberReactor reactor(Map<String, String> arguments) {
		when(insight.getRoomId()).thenReturn("room");
		when(insight.getUser()).thenReturn(user);
		when(user.getRoomHash()).thenReturn(Map.of("room", room));
		BrainRememberReactor reactor = new BrainRememberReactor() {
			@Override
			protected User getUser() {
				return user;
			}
		};
		reactor.setInsight(insight);
		NounStore store = new NounStore("remember");
		arguments.forEach((key, value) -> store.makeNoun(key).addLiteral(value));
		reactor.setNounStore(store);
		return reactor;
	}

	// as the model called it in a chat opened from a brief: the routine never read the memory it saved
	@Test
	void aNewPreferenceInAChatOpenedFromARoutineIsRefusedWithWhereItBelongs() {
		when(room.getOptionsMap())
				.thenReturn(Map.of(CollaborationUtils.ROOM_OPTION_ROUTINE_CHAT, WorkInboxRoutine.ID));
		try (MockedStatic<BrainMemoryUtils> memory = mockStatic(BrainMemoryUtils.class, CALLS_REAL_METHODS)) {
			memory.when(() -> BrainMemoryUtils.requireAssistantMemory(user)).thenAnswer(call -> null);
			memory.when(() -> BrainMemoryUtils.remember(any(), anyMap(), any())).thenReturn(Map.of("status", "saved"));

			IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
					() -> reactor(Map.of("text", "Leave other people's out-of-office events out of the daily brief.",
							"kind", "preference", "everywhere", "true")).execute());
			assertTrue(refusal.getMessage().startsWith("Not saved: this is not a memory."), refusal.getMessage());
			assertTrue(refusal.getMessage().contains("SetRoutinePreferences with routine \"inbox-daily-brief\""));
			memory.verify(() -> BrainMemoryUtils.remember(any(), anyMap(), any()), never());

			// a fact, and a correction of a memory, are still memories there
			assertEquals("saved", ((Map<?, ?>) reactor(Map.of("text", "Dana now manages Acme.", "kind", "fact",
					"everywhere", "true")).execute().getValue()).get("status"));
			assertEquals("saved", ((Map<?, ?>) reactor(Map.of("text", "The owner prefers formal replies.", "kind",
					"preference", "replaces", "m:abc")).execute().getValue()).get("status"));
			memory.verify(() -> BrainMemoryUtils.remember(any(), anyMap(), any()), times(2));
		}
	}

	@Test
	void aPreferenceInAnyOtherChatIsStillAMemory() {
		when(room.getOptionsMap()).thenReturn(Map.of("modelId", "text-model"));
		try (MockedStatic<BrainMemoryUtils> memory = mockStatic(BrainMemoryUtils.class, CALLS_REAL_METHODS)) {
			memory.when(() -> BrainMemoryUtils.requireAssistantMemory(user)).thenAnswer(call -> null);
			memory.when(() -> BrainMemoryUtils.remember(any(), anyMap(), any())).thenReturn(Map.of("status", "saved"));

			assertEquals("saved", ((Map<?, ?>) reactor(Map.of("text", "The owner prefers short email replies.", "kind",
					"preference", "everywhere", "true")).execute().getValue()).get("status"));
		}
	}
}
