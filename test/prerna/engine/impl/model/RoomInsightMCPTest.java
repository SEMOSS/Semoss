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
 *******************************************************************************/
package prerna.engine.impl.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

import prerna.engine.impl.InsightMCP;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.util.Utility;

class RoomInsightMCPTest {

	@TempDir
	Path tempDir;

	@Test
	@SuppressWarnings("unchecked")
	void enrichesRoomToolWithSeparateExecutionAndPortalIdentities() throws Exception {
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
				      "SMSS_ENGINE_ID": "__insight__",
				      "SMSS_PROJECT_ID": "playwright-project",
				      "SMSS_FUNCTION_NAME": "PlayPlaywrightSocketsRoomRecording",
				      "SMSS_MCP_EXECUTION": "ask",
				      "SMSS_MCP_UI": {"displayLocation": "sidebar", "resourceURI": "/"}
				    }
				  }]
				}
				""");

		Map<String, Object> options = Map.of("mcp", List.of(Map.of(
				"type", "INSIGHT",
				"id", InsightMCP.INSIGHT_MCP_ID,
				"name", InsightMCP.INSIGHT_MCP_NAME)));
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
		assertEquals(InsightMCP.INSIGHT_MCP_ID, meta.get(MCPUtility.SMSS_ENGINE_ID));
		assertEquals("playwright-project", meta.get(MCPUtility.SMSS_PROJECT_ID));
		assertEquals("play_checkout", meta.get(MCPUtility.SMSS_ORIGINAL_TOOL_NAME));
	}
}
