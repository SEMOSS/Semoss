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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import com.google.gson.Gson;

import prerna.reactor.agent.mcp.MCPUtility;
import prerna.util.Utility;

class RoomMCPTest {

	@TempDir
	Path tempDir;

	/**
	 * A room playback tool executes against the room's own virtual toolbox while
	 * its sidebar UI is served from the deployed web app. The UI identity now
	 * travels in {@code SMSS_MCP_UI.resourceURI} as a {@code system://} URI, so no
	 * project id has to be stamped onto the tool to build an iframe URL.
	 */
	@Test
	@SuppressWarnings("unchecked")
	void enrichesRoomToolWithInsightExecutionAndSystemAppUi() throws Exception {
		String roomId = "room-mcp-test";
		Path roomFolder = tempDir.resolve("room").resolve(roomId);
		Path mcpDir = roomFolder.resolve("mcp");
		Files.createDirectories(mcpDir);
		Files.writeString(mcpDir.resolve("pixel_mcp.json"), """
				{
				  "tools": [{
				    "name": "play_checkout",
				    "title": "Play checkout",
				    "description": "Replay checkout",
				    "inputSchema": {"type": "object", "properties": {}},
				    "_meta": {
				      "SMSS_ENGINE_ID": "a-stale-engine-id",
				      "SMSS_FUNCTION_NAME": "PlayPlaywrightSocketsRoomRecording",
				      "SMSS_MCP_EXECUTION": "ask",
				      "SMSS_MCP_UI": {
				        "displayLocation": "sidebar",
				        "resourceURI": "system://browser-automation/"
				      }
				    }
				  }]
				}
				""");

		Map<String, Object> options = Map.of("mcp", List.of(Map.of("type", MCPUtility.ROOM_MCP_TYPE, "id",
				MCPUtility.ROOM_MCP_ID, "name", MCPUtility.ROOM_MCP_NAME)));
		Timestamp now = Timestamp.from(Instant.now());
		Room room;
		try (MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			utility.when(Utility::getBaseFolder).thenReturn(tempDir.toString());
			room = new Room(roomId, "user", "Room", null, null, null, true, now, now, "[]", false,
					new Gson().toJson(options), null, null);
		}

		List<Map<String, Object>> tools = room.getAllToolsJsonForRoom(Integer.MAX_VALUE);
		assertEquals(1, tools.size());
		String llmName = String.valueOf(tools.get(0).get("name"));
		Map<String, Object> lookup = room.getToolLookupByLLMName().get(llmName);
		assertTrue(lookup.containsKey("_meta"));
		Map<String, Object> meta = (Map<String, Object>) lookup.get("_meta");

		// The fixture declares a wrong SMSS_ENGINE_ID on purpose: the room replaces it,
		// so a generator never has to write it.
		assertEquals(MCPUtility.ROOM_MCP_ID, meta.get(MCPUtility.SMSS_ENGINE_ID));
		assertEquals("play_checkout", meta.get(MCPUtility.SMSS_ORIGINAL_TOOL_NAME));
		assertEquals("PlayPlaywrightSocketsRoomRecording", meta.get(MCPUtility.SMSS_FUNCTION_NAME));

		// no project id is threaded through for the UI any more; the engine level
		// value is all that remains
		assertEquals(MCPUtility.ROOM_MCP_ID, meta.get(MCPUtility.SMSS_PROJECT_ID));

		// the system app URI survives the engine/tool meta merge intact
		Map<String, Object> ui = (Map<String, Object>) meta.get(MCPUtility.SMSS_MCP_UI);
		assertEquals("system://browser-automation/", ui.get(MCPUtility.UI_RESOURCE_URI));
	}

	/**
	 * The aliasing applied when tools are shown to the model is only half
	 * reversible by string work: the engine id prefix can be stripped, but a name
	 * truncated to a provider limit cannot be rebuilt. The room keeps the real name
	 * in its lookup map, so resolving through the room returns it exactly.
	 */
	@Test
	void resolvesTruncatedAliasBackToTheOriginalToolName() throws Exception {
		String roomId = "room-alias-test";
		Path mcpDir = tempDir.resolve("room").resolve(roomId).resolve("mcp");
		Files.createDirectories(mcpDir);
		Files.writeString(mcpDir.resolve("pixel_mcp.json"), """
				{
				  "tools": [{
				    "name": "play_a_recording_title_long_enough_to_be_cut_by_the_limit",
				    "inputSchema": {"type": "object", "properties": {}},
				    "_meta": {"SMSS_FUNCTION_NAME": "PlayPlaywrightSocketsRoomRecording"}
				  }]
				}
				""");

		Map<String, Object> options = Map.of("mcp", List.of(Map.of("type", MCPUtility.ROOM_MCP_TYPE, "id",
				MCPUtility.ROOM_MCP_ID, "name", MCPUtility.ROOM_MCP_NAME)));
		Timestamp now = Timestamp.from(Instant.now());
		Room room;
		try (MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			utility.when(Utility::getBaseFolder).thenReturn(tempDir.toString());
			room = new Room(roomId, "user", "Room", null, null, null, true, now, now, "[]", false,
					new Gson().toJson(options), null, null);
		}

		// 64 is the OpenAI/Azure cap, the only setting that truncates today
		List<Map<String, Object>> tools = room.getAllToolsJsonForRoom(64);
		String aliasedName = String.valueOf(tools.get(0).get("name"));

		// the alias really is a lossy shortening, not just a prefixed name
		assertTrue(aliasedName.length() <= 64);
		assertNotEquals("play_a_recording_title_long_enough_to_be_cut_by_the_limit", aliasedName);

		assertEquals("play_a_recording_title_long_enough_to_be_cut_by_the_limit",
				room.resolveOriginalToolName(aliasedName));
	}

	@Test
	void resolveOriginalToolNamePassesThroughWhatItDoesNotKnow() throws Exception {
		String roomId = "room-alias-passthrough";
		Path mcpDir = tempDir.resolve("room").resolve(roomId).resolve("mcp");
		Files.createDirectories(mcpDir);
		Files.writeString(mcpDir.resolve("pixel_mcp.json"), """
				{"tools": []}
				""");

		Timestamp now = Timestamp.from(Instant.now());
		Room room;
		try (MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			utility.when(Utility::getBaseFolder).thenReturn(tempDir.toString());
			room = new Room(roomId, "user", "Room", null, null, null, true, now, now, "[]", false,
					new Gson().toJson(Map.of()), null, null);
		}

		// nothing in the map, so callers keep whatever they were given and fall back
		assertEquals("something_unmapped", room.resolveOriginalToolName("something_unmapped"));
		assertEquals("", room.resolveOriginalToolName(""));
		assertEquals(null, room.resolveOriginalToolName(null));
	}
}
