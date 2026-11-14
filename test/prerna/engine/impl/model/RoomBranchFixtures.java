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
package prerna.engine.impl.model;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.om.Insight;

/** Rooms, messages and a store without its database, for the room repair tests. */
public final class RoomBranchFixtures {

	private RoomBranchFixtures() {
	}

	/** The real store with its database and cache reads and writes left out. */
	public static MockedStatic<RoomMessageStore> storeWithoutDatabase() {
		MockedStatic<RoomMessageStore> store = mockStatic(RoomMessageStore.class, CALLS_REAL_METHODS);
		store.when(() -> RoomMessageStore.refreshFromLatestProjection(any(), any())).thenAnswer(call -> null);
		store.when(() -> RoomMessageStore.persist(any(Room.class), anyString())).thenReturn(true);
		store.when(() -> RoomMessageStore.persist(any(Room.class), anyString(), any(), any())).thenReturn(true);
		return store;
	}

	public static Room room() {
		Room room = new Room();
		room.setId("room-repair-test");
		room.setMessages(new ArrayList<>());
		room.setInsight(insight());
		return room;
	}

	public static Insight insight() {
		Insight insight = mock(Insight.class);
		User user = mock(User.class, RETURNS_DEEP_STUBS);
		when(user.getPrimaryLoginToken().getId()).thenReturn("repair-user");
		when(insight.getUser()).thenReturn(user);
		return insight;
	}

	public static InputMessage input(Room room, String text) {
		return InputMessage.builder(room).withText(text).build();
	}

	public static ResponseMessage text(String text) {
		return ResponseMessage.text(text);
	}

	public static ResponseMessage call(String... ids) {
		List<Map<String, Object>> calls = new ArrayList<>();
		for (String id : ids) {
			calls.add(Map.of("id", id, "name", "ReadFile", "arguments", Map.of("path", id + ".txt")));
		}
		return ResponseMessage.toolResponses(calls);
	}

	public static InputMessage results(Room room, String... ids) {
		InputMessage.Builder builder = InputMessage.builder(room);
		for (String id : ids) {
			builder.withToolResult(id, "ReadFile", "output-" + id, null, "success", false);
		}
		return builder.build();
	}

	public static <T extends AbstractMessage> T add(Room room, T message, AbstractMessage parent) {
		message.setRoom(room);
		message.setParentMessageId(parent == null ? null : parent.getMessageId());
		room.getMessages().add(message);
		return message;
	}

}
