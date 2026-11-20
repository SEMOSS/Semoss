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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.engine.impl.model.Room;
import prerna.playground.PlaygroundUtils;

class CollaborationUtilsUnitTests {

	private static Room room(String projectId, Map<String, Object> options) {
		Room room = new Room();
		room.setProjectId(projectId);
		room.setOptionsMap(new HashMap<>(options));
		return room;
	}

	private static Room collaboration(Map<String, Object> options) {
		return room(CollaborationUtils.COLLABORATION_PROJECT_ID, options);
	}

	@Test
	void aHomeChatIsTheOwnersAssistantWithNoThread() {
		Room home = collaboration(Map.of("modelId", "m-1"));
		assertTrue(CollaborationUtils.isAssistantRoom(home));
		assertNull(CollaborationUtils.threadIdOf(home));
	}

	@Test
	void aRoomOpenedFromAThreadNamesItsThread() {
		Room opened = collaboration(Map.of(CollaborationUtils.ROOM_OPTION_SOURCE,
				Map.of("version", 1, "threadId", "th-1", "channel", "email")));
		assertTrue(CollaborationUtils.isAssistantRoom(opened));
		assertEquals("th-1", CollaborationUtils.threadIdOf(opened));
	}

	@Test
	void anOlderWorkThreadRoomStillNamesItsThread() {
		Room older = collaboration(Map.of(CollaborationUtils.ROOM_OPTION_WORK_THREAD,
				Map.of("threadId", "session:9f0c", "contextRevision", "")));
		assertTrue(CollaborationUtils.isAssistantRoom(older));
		assertEquals("session:9f0c", CollaborationUtils.threadIdOf(older));
	}

	@Test
	void aBlankThreadIdIsNoThread() {
		assertNull(CollaborationUtils.threadIdOf(collaboration(Map.of(CollaborationUtils.ROOM_OPTION_SOURCE,
				Map.of("threadId", " ")))));
	}

	@Test
	void aDelegationRoomIsNotAnAssistantRoom() {
		Room delegation = collaboration(Map.of(CollaborationUtils.ROOM_OPTION_DELEGATION_ACTION_ID, "a-1",
				CollaborationUtils.ROOM_OPTION_SOURCE, Map.of("threadId", "th-1")));
		assertFalse(CollaborationUtils.isAssistantRoom(delegation));
		assertNull(CollaborationUtils.threadIdOf(delegation));
	}

	@Test
	void aPlaygroundRoomIsNotAnAssistantRoom() {
		Room playground = room(PlaygroundUtils.PLAYGROUND_PROJECT_ID,
				Map.of(CollaborationUtils.ROOM_OPTION_SOURCE, Map.of("threadId", "th-1")));
		assertFalse(CollaborationUtils.isAssistantRoom(playground));
		assertNull(CollaborationUtils.threadIdOf(playground));
		assertFalse(CollaborationUtils.isAssistantRoom(null));
	}
}
