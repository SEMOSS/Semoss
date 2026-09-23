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
package prerna.reactor.agent.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

/**
 * Covers the merge that lets every MCP generator regenerate its own tools
 * without discarding tools it did not write.
 */
class MCPUtilityMergeTest {

	private static final String MINE = "MakeSomethingMCP";
	private static final String SOMEONE_ELSE = "MakeOtherMCP";

	/** A tool as it looks after {@link MCPUtility#stampGenerator}. */
	private static JSONObject stampedTool(String name, String generatorId) {
		return new JSONObject().put("name", name).put("_meta",
				new JSONObject().put(MCPUtility.SMSS_MCP_GENERATOR, generatorId));
	}

	/** A hand written tool, with no generator marker. */
	private static JSONObject unstampedTool(String name) {
		return new JSONObject().put("name", name);
	}

	private static JSONObject fileWith(JSONObject... tools) {
		JSONArray array = new JSONArray();
		for (JSONObject tool : tools) {
			array.put(tool);
		}
		return new JSONObject().put("tools", array);
	}

	private static JSONArray names(JSONArray tools) {
		JSONArray result = new JSONArray();
		for (int i = 0; i < tools.length(); i++) {
			result.put(tools.getJSONObject(i).getString("name"));
		}
		return result;
	}

	@Test
	void stampGeneratorMarksEveryToolAndCreatesMetaWhenAbsent() {
		JSONArray tools = new JSONArray().put(unstampedTool("a"))
				.put(new JSONObject().put("name", "b").put("_meta", new JSONObject().put("keepMe", true)));

		MCPUtility.stampGenerator(tools, MINE);

		assertEquals(MINE, tools.getJSONObject(0).getJSONObject("_meta").getString(MCPUtility.SMSS_MCP_GENERATOR));
		JSONObject second = tools.getJSONObject(1).getJSONObject("_meta");
		assertEquals(MINE, second.getString(MCPUtility.SMSS_MCP_GENERATOR));
		// stamping must not discard metadata that was already there
		assertTrue(second.getBoolean("keepMe"));
	}

	@Test
	void aHandWrittenToolSurvivesRegeneration() {
		JSONObject existing = fileWith(unstampedTool("BrowseRoomFiles"), stampedTool("play_old", MINE));
		JSONArray generated = new JSONArray().put(stampedTool("play_new", MINE));

		JSONArray merged = MCPUtility.mergeGeneratedTools(existing, generated, MINE, true);

		// generated first, then the hand written tool; play_old is pruned
		assertEquals("[\"play_new\",\"BrowseRoomFiles\"]", names(merged).toString());
	}

	@Test
	void anotherGeneratorsToolsAreLeftAlone() {
		JSONObject existing = fileWith(stampedTool("theirs", SOMEONE_ELSE), stampedTool("mine_stale", MINE));
		JSONArray generated = new JSONArray().put(stampedTool("mine_fresh", MINE));

		JSONArray merged = MCPUtility.mergeGeneratedTools(existing, generated, MINE, true);

		assertEquals("[\"mine_fresh\",\"theirs\"]", names(merged).toString());
	}

	@Test
	void aGeneratedToolWinsOverAnExistingToolOfTheSameName() {
		JSONObject existing = fileWith(unstampedTool("play_google").put("stale", true));
		JSONArray generated = new JSONArray().put(stampedTool("play_google", MINE));

		JSONArray merged = MCPUtility.mergeGeneratedTools(existing, generated, MINE, true);

		// a name collision always resolves to the fresh definition, so the file can
		// never hold two tools of one name for lookup to choose between
		assertEquals(1, merged.length());
		assertFalse(merged.getJSONObject(0).has("stale"));
	}

	@Test
	void aPartialRunDoesNotPruneTheGeneratorsOtherTools() {
		JSONObject existing = fileWith(stampedTool("tool_a", MINE), stampedTool("tool_b", MINE));
		// a run scoped to one reactor regenerates only tool_a
		JSONArray generated = new JSONArray().put(stampedTool("tool_a", MINE));

		JSONArray pruning = MCPUtility.mergeGeneratedTools(existing, generated, MINE, true);
		assertEquals("[\"tool_a\"]", names(pruning).toString());

		// the same inputs, declared as a partial run, must keep tool_b
		JSONArray notPruning = MCPUtility.mergeGeneratedTools(existing, generated, MINE, false);
		assertEquals("[\"tool_a\",\"tool_b\"]", names(notPruning).toString());
	}

	@Test
	void mergeToleratesNoExistingFileAndAnAbsentToolsArray() {
		JSONArray generated = new JSONArray().put(stampedTool("only", MINE));

		assertEquals(1, MCPUtility.mergeGeneratedTools(null, generated, MINE, true).length());
		assertEquals(1, MCPUtility.mergeGeneratedTools(new JSONObject(), generated, MINE, true).length());
	}

	@Test
	void readMcpJsonReturnsNullForAMissingFile() {
		assertEquals(null, MCPUtility.readMcpJson("/no/such/path/pixel_mcp.json"));
	}
}
