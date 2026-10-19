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
package prerna.reactor.agent.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomSystemPrompt;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;

class AgentPromptCompositionUnitTests {

	private static final String AGENT_ID = "database-explorer";
	private static final String AGENT_PROMPT = "Analyze data and explain its scope.";
	private static final String CONTEXT = "Active database: sample-db. Dialect: POSTGRES.";

	private Room room(String instructions, Boolean override) {
		Room room = new Room();
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(mock(User.class));
		room.setInsight(insight);
		Map<String, Object> options = new HashMap<>();
		options.put("workspace", Map.of("workspace_id", AGENT_ID));
		options.put("instructions", instructions);
		if (override != null) {
			options.put("overrideSystemPrompt", override);
		}
		room.setOptionsMap(options);
		return room;
	}

	private AgentConfig load(Room room, String workspaceId) {
		return AgentConfigLoader.load(room, null, "model-1", Map.of(), Map.of(), 30, 0, workspaceId);
	}

	@Test
	void appendsRoomContextToConfiguredAgentAndPreservesSkills() {
		Room room = room(CONTEXT, false);
		try (var workspaces = mockStatic(ModelInferenceLogsUtils.class);
				var security = mockStatic(SecurityProjectUtils.class)) {
			workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(AGENT_ID))
					.thenReturn(Map.of("system_prompt", "Legacy prompt"));
			workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceConfigJson(AGENT_ID)).thenReturn(new JSONObject()
					.put("system_prompt", AGENT_PROMPT).put("skills", List.of(Map.of("skill_id", "database"))));
			security.when(() -> SecurityProjectUtils.userCanViewProject(room.getInsight().getUser(), AGENT_ID))
					.thenReturn(true);
			AgentConfig config = load(room, AGENT_ID);
			assertEquals(AGENT_PROMPT + "\n\n" + CONTEXT, config.getAuthoredPrompt());
			assertEquals(List.of("database"), config.getSkills().stream().map(skill -> skill.get("skill_id")).toList());
		}
	}

	@Test
	void explicitAgentSelectionWinsOverAnOlderRoomWorkspace() {
		Room room = room(CONTEXT, false);
		try (var workspaces = mockStatic(ModelInferenceLogsUtils.class);
				var security = mockStatic(SecurityProjectUtils.class)) {
			workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry("custom-agent"))
					.thenReturn(Map.of("system_prompt", "Custom analyst instructions"));
			security.when(() -> SecurityProjectUtils.userCanViewProject(room.getInsight().getUser(), "custom-agent"))
					.thenReturn(true);
			assertEquals("Custom analyst instructions\n\n" + CONTEXT, load(room, "custom-agent").getAuthoredPrompt());
		}
	}

	@Test
	void chatAndAgentPathsAgreeForAppendOverrideAndLegacyOptions() {
		for (Boolean override : new Boolean[] { false, true, null }) {
			Room room = room(CONTEXT, override);
			try (var workspaces = mockStatic(ModelInferenceLogsUtils.class);
					var security = mockStatic(SecurityProjectUtils.class)) {
				workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(AGENT_ID))
						.thenReturn(Map.of("system_prompt", AGENT_PROMPT));
				security.when(() -> SecurityProjectUtils.userCanViewProject(room.getInsight().getUser(), AGENT_ID))
						.thenReturn(true);
				String expected = Boolean.FALSE.equals(override) ? AGENT_PROMPT + "\n\n" + CONTEXT : CONTEXT;
				assertEquals(expected, load(room, AGENT_ID).getAuthoredPrompt());
				assertEquals(expected, room.getSystemPromptForModel());
			}
		}
	}

	@Test
	void blankInstructionsRetainAgentPromptInEitherMode() {
		for (boolean override : new boolean[] { false, true }) {
			Room room = room(" \n\t", override);
			try (var workspaces = mockStatic(ModelInferenceLogsUtils.class);
					var security = mockStatic(SecurityProjectUtils.class)) {
				workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(AGENT_ID))
						.thenReturn(Map.of("system_prompt", AGENT_PROMPT));
				security.when(() -> SecurityProjectUtils.userCanViewProject(room.getInsight().getUser(), AGENT_ID))
						.thenReturn(true);
				assertEquals(AGENT_PROMPT, load(room, AGENT_ID).getAuthoredPrompt());
				assertEquals(AGENT_PROMPT, room.getSystemPromptForModel());
			}
		}
	}

	@Test
	void roomsWithoutAnAgentCanStillUseInstructions() {
		Room room = new Room();
		room.setOptionsMap(Map.of("instructions", CONTEXT, "overrideSystemPrompt", false));
		assertEquals(CONTEXT, load(room, null).getAuthoredPrompt());
		assertEquals(CONTEXT, room.getSystemPromptForModel());
		room.setOptionsMap(Map.of());
		assertNull(load(room, null).getAuthoredPrompt());
		assertNull(room.getSystemPromptForModel());
	}

	@Test
	void appendingDoesNotBypassWorkspaceAccessChecks() {
		Room room = room(CONTEXT, false);
		try (var workspaces = mockStatic(ModelInferenceLogsUtils.class);
				var security = mockStatic(SecurityProjectUtils.class)) {
			workspaces.when(() -> ModelInferenceLogsUtils.getWorkspaceEntry(AGENT_ID))
					.thenReturn(Map.of("system_prompt", AGENT_PROMPT));
			assertThrows(IllegalArgumentException.class, () -> load(room, AGENT_ID));
			assertThrows(IllegalArgumentException.class, room::getSystemPromptForModel);
		}
	}

	@Test
	void malformedOptionsFallBackAndOnlyBooleanFalseEnablesAppending() {
		assertEquals(AGENT_PROMPT, RoomSystemPrompt.resolve("{", () -> AGENT_PROMPT));
		assertEquals(AGENT_PROMPT, RoomSystemPrompt.resolve("null", () -> AGENT_PROMPT));
		assertEquals(CONTEXT,
				RoomSystemPrompt.resolve(
						new JSONObject().put("instructions", CONTEXT).put("overrideSystemPrompt", "false").toString(),
						() -> AGENT_PROMPT));
	}
}
