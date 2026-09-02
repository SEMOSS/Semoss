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
/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 * Licensed under the Apache License, Version 2.0 (the "License");
 ******************************************************************************/
package prerna.engine.impl.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;

class RoomMessageStoreUnitTests {

	private static final String INVALID_PARENT_JSON = "[{\"io\":\"INPUT\",\"messageId\":\"child\",\"parentMessageId\":\"missing\",\"parts\":[]}]";

	@Test
	void loadAllowsMessagesWithInvalidParents() {
		Room room = roomWithId();

		List<AbstractMessage> messages = RoomMessageStore.loadFromPersistedJson(room, INVALID_PARENT_JSON);

		assertEquals(1, messages.size());
		assertEquals("child", messages.get(0).getMessageId());
		assertEquals("missing", messages.get(0).getParentMessageId());
	}

	@Test
	void providerPayloadRejectsMessagesWithInvalidParents() {
		Room room = roomWithId();
		List<AbstractMessage> messages = RoomMessageStore.loadFromPersistedJson(room, INVALID_PARENT_JSON);

		assertThrows(IllegalStateException.class, () -> RoomMessageStore.providerMessageHistory(room, messages));
	}

	@Test
	void persistRejectsMessagesWithInvalidParents() {
		Room room = roomWithId();
		room.setMessages(RoomMessageStore.loadFromPersistedJson(room, INVALID_PARENT_JSON));

		assertThrows(IllegalStateException.class, () -> RoomMessageStore.persist(room, "user-id"));
	}

	@Test
	void providerPayloadUsesTheGuardrailReplacementMessage() {
		Room room = roomWithId();
		InputMessage original = InputMessage.builder(room).withText("api_key=secret").build();
		room.getMessages().add(original);
		original.setFullInputPrompt("api_key=[masked]");

		String payload = RoomMessageStore.messageHistoryWithNewMessage(room, original);

		assertTrue(payload.contains("api_key=[masked]"));
		assertFalse(payload.contains("api_key=secret"));
		assertEquals("api_key=[masked]", original.getFullInputPrompt());
	}

	private static Room roomWithId() {
		Room room = new Room();
		room.setId("room-id");
		return room;
	}
}
