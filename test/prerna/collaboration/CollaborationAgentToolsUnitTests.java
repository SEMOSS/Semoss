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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.impl.model.Room;
import prerna.io.connector.ConnectorScopeAccess;
import prerna.reactor.agent.mcp.MCPUtility;

class CollaborationAgentToolsUnitTests {

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> allDefinitions() throws Exception {
		Method method = CollaborationAgentTools.class.getDeclaredMethod("definitions", boolean.class, boolean.class,
				boolean.class);
		method.setAccessible(true);
		return (List<Map<String, Object>>) method.invoke(null, true, true, true);
	}

	@Test
	void everyDedicatedToolHasAViewAndItsOwnApprovalSchema() throws Exception {
		List<Map<String, Object>> tools = allDefinitions();
		assertEquals(60, tools.size());
		for (Map<String, Object> value : tools) {
			JSONObject tool = new JSONObject(value);
			JSONObject meta = tool.getJSONObject("_meta");
			String uri = meta.getJSONObject(MCPUtility.SMSS_MCP_UI).getString(MCPUtility.UI_RESOURCE_URI);
			assertTrue(uri.startsWith(MCPUtility.UI_COMPONENT_SCHEME), tool.getString("name"));
			assertTrue(tool.getJSONObject("inputSchema").similar(meta.getJSONObject("SMSS_TOOL_INPUT_SCHEMA")),
					tool.getString("name"));
		}
	}

	@Test
	void existingConnectorAndEmailEditorViewsArePreserved() throws Exception {
		for (Map<String, Object> value : allDefinitions()) {
			JSONObject tool = new JSONObject(value);
			JSONObject ui = tool.getJSONObject("_meta").getJSONObject(MCPUtility.SMSS_MCP_UI);
			switch (tool.getString("name")) {
			case "ListEvents" -> assertEquals("component://calendar/agenda?provider=microsoft",
					ui.getString(MCPUtility.UI_RESOURCE_URI));
			case "ListM365Mail" ->
				assertEquals("component://mail/list?provider=microsoft", ui.getString(MCPUtility.UI_RESOURCE_URI));
			case "SendEmail" -> assertEquals("email-send", ui.getString("component"));
			case "EditTopic" ->
				assertEquals("component://collaboration/tool?tool=EditTopic", ui.getString(MCPUtility.UI_RESOURCE_URI));
			default -> {
			}
			}
		}
	}

	@Test
	void orgChartIsAnAutomaticNativeToolOnlyInTheOwnersRootChat() {
		Room room = new Room();
		room.setProjectId(CollaborationUtils.COLLABORATION_PROJECT_ID);
		User user = mock(User.class);
		try (MockedStatic<BrainMemoryUtils> memory = mockStatic(BrainMemoryUtils.class)) {
			JSONObject tool = new JSONObject(CollaborationAgentTools.definitions(room, user, true).stream()
					.filter(t -> "ShowOrgChart".equals(t.get("name"))).findFirst().orElseThrow());
			JSONObject meta = tool.getJSONObject("_meta");
			assertEquals("WorkShowOrgChart", meta.getString(MCPUtility.SMSS_FUNCTION_NAME));
			assertEquals(MCPUtility.MCPExecution.AUTO.getValue(), meta.getString(MCPUtility.SMSS_MCP_EXECUTION));
			assertEquals("org-chart", meta.getJSONObject(MCPUtility.SMSS_MCP_UI).getString(MCPUtility.UI_COMPONENT));
			assertEquals("inline", meta.getJSONObject(MCPUtility.SMSS_MCP_UI).getString(MCPUtility.UI_DISPLAY_LOCATION));
			assertTrue(CollaborationAgentTools.definitions(room, user, false).stream()
					.noneMatch(t -> "ShowOrgChart".equals(t.get("name"))));
			room.setOptionsMap(Map.of(CollaborationUtils.ROOM_OPTION_ROUTINE, true));
			assertTrue(CollaborationAgentTools.definitions(room, user, true).stream()
					.noneMatch(t -> "ShowOrgChart".equals(t.get("name"))));
		}
	}

	@Test
	void changingRoutinePreferencesWaitsForTheOwnerSoAnUnattendedRoutineNeverGetsIt() throws Exception {
		for (Map<String, Object> value : allDefinitions()) {
			JSONObject tool = new JSONObject(value);
			if ("SetRoutinePreferences".equals(tool.getString("name"))) {
				assertEquals(MCPUtility.MCPExecution.ASK.getValue(),
						tool.getJSONObject("_meta").getString(MCPUtility.SMSS_MCP_EXECUTION));
				assertEquals(WorkRoutinePreferences.ROUTINE_IDS, tool.getJSONObject("inputSchema")
						.getJSONObject("properties").getJSONObject("routine").getJSONArray("enum").toList());
				return;
			}
		}
		throw new AssertionError("SetRoutinePreferences is not a collaboration tool");
	}

	@Test
	void readingAMeetingTranscriptIsOfferedOnlyWhereTheSignInAsksForItsScope() throws Exception {
		// nothing in this test asks for the scope, as on a deployment whose
		// organization has not consented to it
		assertTrue(allDefinitions().stream().noneMatch(tool -> "GetMeetingTranscript".equals(tool.get("name"))));

		try (MockedStatic<ConnectorScopeAccess> scopes = mockStatic(ConnectorScopeAccess.class)) {
			scopes.when(() -> ConnectorScopeAccess.requestsAnyScope(AuthProvider.MICROSOFT,
					List.of("OnlineMeetingTranscript.Read.All"))).thenReturn(true);
			List<Map<String, Object>> tools = allDefinitions();
			assertEquals(61, tools.size());
			JSONObject tool = new JSONObject(tools.stream()
					.filter(value -> "GetMeetingTranscript".equals(value.get("name"))).findFirst().orElseThrow());
			// a read, so it runs without waiting for the owner and a routine keeps it
			assertEquals(MCPUtility.MCPExecution.AUTO.getValue(),
					tool.getJSONObject("_meta").getString(MCPUtility.SMSS_MCP_EXECUTION));
			assertEquals("Id of the event, as returned by ListEvents.", tool.getJSONObject("inputSchema")
					.getJSONObject("properties").getJSONObject("id").getString("description"));
		}
	}

	@Test
	void definitionsDoNotShareMutableApprovalSchemasBetweenRuns() throws Exception {
		List<Map<String, Object>> first = allDefinitions();
		Map<?, ?> meta = (Map<?, ?>) first.getFirst().get("_meta");
		Map<?, ?> schema = (Map<?, ?>) meta.get("SMSS_TOOL_INPUT_SCHEMA");
		schema.clear();
		JSONObject second = new JSONObject(allDefinitions().getFirst());
		assertTrue(second.getJSONObject("inputSchema")
				.similar(second.getJSONObject("_meta").getJSONObject("SMSS_TOOL_INPUT_SCHEMA")));
	}
}
