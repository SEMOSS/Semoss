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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.om.Insight;

/**
 * Verifies {@link AgentRunner#persistWorkspaceOnEmptyRoom}: a RunAgent
 * workspaceId is saved on a room only while the room has no messages, and only
 * for an active workspace the user can view.
 */
class AgentRunnerWorkspacePersistenceTest {

	private static final String ROOM_ID = "room-ws-1";
	private static final String USER_ID = "user-1";
	private static final String WORKSPACE_ID = "ws-1";

	private Room room;
	private Insight insight;
	private User user;
	private Map<String, Object> options;
	private List<AbstractMessage> messages;

	@BeforeEach
	void setUp() {
		room = mock(Room.class);
		insight = mock(Insight.class);
		user = mock(User.class);
		options = new HashMap<>();
		messages = new ArrayList<>();
		when(room.getId()).thenReturn(ROOM_ID);
		when(room.getUserId()).thenReturn(USER_ID);
		when(room.getOptionsMap()).thenReturn(options);
		when(room.getMessages()).thenReturn(messages);
		when(insight.getUser()).thenReturn(user);
	}

	private static Map<String, Object> workspaceRow(boolean active) {
		Map<String, Object> row = new HashMap<>();
		row.put("name", "My Agent");
		row.put("is_active", active);
		return row;
	}

	@Test
	void savesWorkspaceOnEmptyRoom() {
		try (MockedStatic<ModelInferenceLogsUtils> logs = Mockito.mockStatic(ModelInferenceLogsUtils.class);
				MockedStatic<SecurityProjectUtils> security = Mockito.mockStatic(SecurityProjectUtils.class)) {
			logs.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(WORKSPACE_ID)).thenReturn(workspaceRow(true));
			security.when(() -> SecurityProjectUtils.userCanViewProject(user, WORKSPACE_ID)).thenReturn(true);

			AgentRunner.persistWorkspaceOnEmptyRoom(room, insight, WORKSPACE_ID);

			logs.verify(() -> ModelInferenceLogsUtils.setRoomWorkspaceId(ROOM_ID, USER_ID, WORKSPACE_ID));
			Map<String, Object> expectedWorkspace = new HashMap<>();
			expectedWorkspace.put("workspace_id", WORKSPACE_ID);
			expectedWorkspace.put("name", "My Agent");
			Map<String, Object> expectedOptions = Collections.singletonMap("workspace", expectedWorkspace);
			logs.verify(() -> ModelInferenceLogsUtils.setRoomOptions(ROOM_ID, USER_ID, expectedOptions));
			Mockito.verify(room).setOptionsMap(expectedOptions);
		}
	}

	@Test
	void leavesRoomWithMessagesUnchanged() {
		messages.add(mock(AbstractMessage.class));
		try (MockedStatic<ModelInferenceLogsUtils> logs = Mockito.mockStatic(ModelInferenceLogsUtils.class);
				MockedStatic<SecurityProjectUtils> security = Mockito.mockStatic(SecurityProjectUtils.class)) {
			logs.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(WORKSPACE_ID)).thenReturn(workspaceRow(true));
			security.when(() -> SecurityProjectUtils.userCanViewProject(user, WORKSPACE_ID)).thenReturn(true);

			AgentRunner.persistWorkspaceOnEmptyRoom(room, insight, WORKSPACE_ID);

			assertNothingSaved(logs);
		}
	}

	@Test
	void skipsWhenNoWorkspaceIdGiven() {
		try (MockedStatic<ModelInferenceLogsUtils> logs = Mockito.mockStatic(ModelInferenceLogsUtils.class)) {
			AgentRunner.persistWorkspaceOnEmptyRoom(room, insight, null);
			AgentRunner.persistWorkspaceOnEmptyRoom(room, insight, "  ");

			assertNothingSaved(logs);
		}
	}

	@Test
	void skipsMissingOrDisabledWorkspace() {
		try (MockedStatic<ModelInferenceLogsUtils> logs = Mockito.mockStatic(ModelInferenceLogsUtils.class);
				MockedStatic<SecurityProjectUtils> security = Mockito.mockStatic(SecurityProjectUtils.class)) {
			security.when(() -> SecurityProjectUtils.userCanViewProject(any(User.class), anyString()))
					.thenReturn(true);

			logs.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(WORKSPACE_ID)).thenReturn(null);
			AgentRunner.persistWorkspaceOnEmptyRoom(room, insight, WORKSPACE_ID);

			logs.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(WORKSPACE_ID)).thenReturn(workspaceRow(false));
			AgentRunner.persistWorkspaceOnEmptyRoom(room, insight, WORKSPACE_ID);

			assertNothingSaved(logs);
		}
	}

	@Test
	void skipsWorkspaceUserCannotView() {
		try (MockedStatic<ModelInferenceLogsUtils> logs = Mockito.mockStatic(ModelInferenceLogsUtils.class);
				MockedStatic<SecurityProjectUtils> security = Mockito.mockStatic(SecurityProjectUtils.class)) {
			logs.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(WORKSPACE_ID)).thenReturn(workspaceRow(true));
			security.when(() -> SecurityProjectUtils.userCanViewProject(user, WORKSPACE_ID)).thenReturn(false);

			AgentRunner.persistWorkspaceOnEmptyRoom(room, insight, WORKSPACE_ID);

			assertNothingSaved(logs);
		}
	}

	private void assertNothingSaved(MockedStatic<ModelInferenceLogsUtils> logs) {
		logs.verify(() -> ModelInferenceLogsUtils.setRoomWorkspaceId(anyString(), anyString(), anyString()),
				Mockito.never());
		logs.verify(() -> ModelInferenceLogsUtils.setRoomOptions(anyString(), anyString(), anyMap()),
				Mockito.never());
		Mockito.verify(room, Mockito.never()).setOptionsMap(any());
		assertFalse(options.containsKey("workspace"));
		assertEquals(0, options.size());
	}
}
