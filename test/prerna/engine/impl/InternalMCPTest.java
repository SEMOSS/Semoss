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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.execptions.SemossMCPException;

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

		JSONObject result = InternalMCP.genFromRoomFolder(tempDir.toString()).getMCPTools();

		assertEquals("play_checkout", result.getJSONArray("tools").getJSONObject(0).getString("name"));
		assertEquals(MCPUtility.ROOM_MCP_ID, result.getJSONObject("_meta").getString(MCPUtility.SMSS_ENGINE_ID));
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

		JSONObject result = InternalMCP.genFromRoomFolder(tempDir.toString()).getMCPTools();

		assertEquals(2, result.getJSONArray("tools").length());
	}

	@Test
	void rejectsABlankFolder() {
		assertThrows(IllegalArgumentException.class, () -> InternalMCP.genFromRoomFolder("  "));
	}

	/**
	 * Writes a room folder holding two tools whose names share a leading slice, so
	 * exact / truncated / shadowing lookups can all be exercised.
	 */
	private InternalMCP twoToolFolder() throws Exception {
		Path mcpDir = tempDir.resolve("mcp");
		Files.createDirectories(mcpDir);
		Files.writeString(mcpDir.resolve("pixel_mcp.json"), """
				{
				  "tools": [
				    { "name": "play_checkout_flow_with_a_very_long_generated_name",
				      "inputSchema": {"type": "object", "properties": {}},
				      "_meta": {"SMSS_FUNCTION_NAME": "LongOne"} },
				    { "name": "play_checkout",
				      "inputSchema": {"type": "object", "properties": {}},
				      "_meta": {"SMSS_FUNCTION_NAME": "ShortOne"} }
				  ]
				}
				""");
		return InternalMCP.genFromRoomFolder(tempDir.toString());
	}

	private static String resolvedFunction(InternalMCP mcp, String requested) throws Exception {
		java.lang.reflect.Method m = InternalMCP.class.getDeclaredMethod("getFunction", String.class, String.class);
		m.setAccessible(true);
		java.lang.reflect.Method pixelPath = InternalMCP.class.getDeclaredMethod("pixelMcpPath");
		pixelPath.setAccessible(true);
		JSONObject tool = (JSONObject) m.invoke(mcp, requested, pixelPath.invoke(mcp));
		return tool == null ? null : tool.getJSONObject("_meta").getString("SMSS_FUNCTION_NAME");
	}

	@Test
	void exactNameWinsOverALongerToolListedFirst() throws Exception {
		// "play_checkout" is a prefix of the first tool, but an exact match exists
		assertEquals("ShortOne", resolvedFunction(twoToolFolder(), "play_checkout"));
	}

	@Test
	void aTruncatedNameIsNotGuessedAt() throws Exception {
		// what a 64-char-limited provider would send back if the alias were never
		// reversed. Matching is exact, so this fails rather than quietly running the
		// tool it happens to be a prefix of
		assertNull(resolvedFunction(twoToolFolder(), "play_checkout_flow_with_a_very"));
	}

	@Test
	void aNameThatOnlyAppearsMidStringDoesNotMatch() throws Exception {
		assertNull(resolvedFunction(twoToolFolder(), "checkout_flow"));
	}

	@Test
	void unknownToolErrorNamesTheLikelyTruncationVictim() throws Exception {
		Path mcpDir = tempDir.resolve("mcp");
		Files.createDirectories(mcpDir);
		Files.writeString(mcpDir.resolve("pixel_mcp.json"), """
				{
				  "tools": [{
				    "name": "play_checkout_flow_with_a_very_long_generated_name",
				    "inputSchema": {"type": "object", "properties": {}},
				    "_meta": {"SMSS_FUNCTION_NAME": "LongOne"}
				  }]
				}
				""");
		InternalMCP mcp = InternalMCP.genFromRoomFolder(tempDir.toString());

		// a prefix of a real tool: the error should point at the cause
		SemossMCPException truncated = assertThrows(SemossMCPException.class,
				() -> mcp.callTool("play_checkout_flow_with_a_very", Map.of(), null));
		assertTrue(truncated.getMessage().contains("play_checkout_flow_with_a_very_long_generated_name"),
				truncated.getMessage());
		assertTrue(truncated.getMessage().contains("truncated"), truncated.getMessage());

		// an unrelated name gets the plain message, with no misleading hint
		SemossMCPException unrelated = assertThrows(SemossMCPException.class,
				() -> mcp.callTool("something_else_entirely", Map.of(), null));
		assertTrue(unrelated.getMessage().contains("Unknown tool"), unrelated.getMessage());
		assertFalse(unrelated.getMessage().contains("truncated"), unrelated.getMessage());
	}

	@Test
	void unknownNameResolvesToNothing() throws Exception {
		assertNull(resolvedFunction(twoToolFolder(), "not_a_tool"));
	}

	@Test
	void genFromRoomFolderProducesAUsableMcp() throws Exception {
		assertNotNull(twoToolFolder().getMCPTools().getJSONArray("tools"));
	}
}
