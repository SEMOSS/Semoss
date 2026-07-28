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
package prerna.engine.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.reactor.agent.mcp.MCPUtility;

class InternalMCPTest {

	@TempDir
	Path tempDir;

	@Test
	void readsToolsFromExplicitRoomFolder() throws Exception {
		Path mcpDir = tempDir.resolve("mcp");
		Files.createDirectories(mcpDir);
		Files.writeString(mcpDir.resolve("pixel_mcp.json"), """
				{
				  "tools": [{
				    "name": "play_checkout",
				    "description": "Replay checkout",
				    "inputSchema": {"type": "object", "properties": {}}
				  }]
				}
				""");

		JSONObject result = InternalMCP.genFromFolder(tempDir.toString(), "room-123", "Room Recordings",
				MCPUtility.INTERNAL_MCP_TYPE).getMCPTools();

		assertEquals("play_checkout", result.getJSONArray("tools").getJSONObject(0).getString("name"));
		assertEquals("room-123",
				result.getJSONObject("_meta").getString(MCPUtility.SMSS_ENGINE_ID));
	}

	@Test
	void mergesPythonAndPixelToolsFromTheSameFolder() throws Exception {
		Path mcpDir = tempDir.resolve("mcp");
		Files.createDirectories(mcpDir);
		Files.writeString(mcpDir.resolve("py_mcp.json"), """
				{
				  "tools": [{
				    "name": "summarize",
				    "inputSchema": {"type": "object", "properties": {}}
				  }]
				}
				""");
		Files.writeString(mcpDir.resolve("pixel_mcp.json"), """
				{
				  "tools": [{
				    "name": "play_checkout",
				    "inputSchema": {"type": "object", "properties": {}}
				  }]
				}
				""");

		JSONObject result = InternalMCP.genFromFolder(tempDir.toString(), "room-123", "Room Recordings",
				MCPUtility.INTERNAL_MCP_TYPE).getMCPTools();

		assertEquals(2, result.getJSONArray("tools").length());
	}

	@Test
	void rejectsABlankFolder() {
		assertThrows(IllegalArgumentException.class,
				() -> InternalMCP.genFromFolder("  ", "room-123", "Room Recordings", MCPUtility.INTERNAL_MCP_TYPE));
	}
}
