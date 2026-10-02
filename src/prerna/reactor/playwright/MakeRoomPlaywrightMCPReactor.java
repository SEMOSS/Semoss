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
package prerna.reactor.playwright;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.FileSystemUtil;

/**
 * Generates room scoped Playwright playback tools.
 *
 * <p>
 * Reads every {@code playwright/*.json} recording from the room folder, turns
 * each into one playback tool, and merges the result into the room's
 * {@code mcp/pixel_mcp.json}, leaving tools written by other generators in
 * place. {@link MakePlaywrightMCPReactor} does the same for project assets.
 *
 * <p>
 * Pixel usage:
 * 
 * <pre>
 * MakeRoomPlaywrightMCP(roomId = "..."); // regenerate this room's playback tools
 * MakeRoomPlaywrightMCP(); // use the calling insight's own folder
 * </pre>
 */
public class MakeRoomPlaywrightMCPReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(MakeRoomPlaywrightMCPReactor.class);

	/** Relative path of recording files within the room folder. */
	private static final String RECORDINGS_REL = "/playwright";

	/**
	 * Stamped into every generated tool as {@link MCPUtility#SMSS_MCP_GENERATOR}
	 * and used on the next run to tell this reactor's own tools apart from tools
	 * that merely share the file.
	 */
	private static final String GENERATOR_ID = "MakeRoomPlaywrightMCP";

	public MakeRoomPlaywrightMCPReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey() };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());

		// If roomId is provided, bind the current insight to that room so that
		// getInsightFolder() returns the room's recording folder, not the empty
		// folder of the calling session.
		if (roomId != null && !roomId.isBlank()) {
			try {
				Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
				this.insight.setRoomForInsight(room);
			} catch (Exception e) {
				classLogger.warn("MakeRoomPlaywrightMCP: could not bind to room {}: {}", roomId, e.getMessage());
			}
		}

		String assetFolder = this.insight.getInsightFolder();
		if (assetFolder == null || assetFolder.isBlank()) {
			throw new IllegalStateException("No insight asset folder is available for this session.");
		}

		// Every .json recording in the room folder becomes one playback tool.
		File recordingsDir = new File(assetFolder + RECORDINGS_REL);
		File[] files = recordingsDir.exists()
				? recordingsDir.listFiles((d, name) -> name.toLowerCase().endsWith(".json"))
				: null;

		if (files == null || files.length == 0) {
			throw new IllegalArgumentException(
					"No Playwright recording files found in room insight assets under: " + RECORDINGS_REL);
		}

		// Sort by name for deterministic output
		Arrays.sort(files, (a, b) -> a.getName().compareToIgnoreCase(b.getName()));

		// One unparseable recording is skipped so it cannot take out the whole batch.
		JSONArray toolsArray = new JSONArray();
		for (File file : files) {
			try {
				StepsEnvelope envelope = PlaywrightUtility.readStepsEnvelope(file);
				toolsArray.put(PlaywrightMCPToolBuilder.buildRoomPlaybackTool(envelope, file.getName()));
			} catch (Exception e) {
				classLogger.warn("Skipping recording file {} - could not parse: {}", file.getName(), e.getMessage());
			}
		}

		if (toolsArray.isEmpty()) {
			throw new IllegalArgumentException("No valid Playwright recording files could be parsed.");
		}

		// Stamp first: ownership decides which existing tools the merge may replace.
		MCPUtility.stampGenerator(toolsArray, GENERATOR_ID);

		// Every recording was rebuilt, so a stamped tool that is absent had its
		// recording deleted.
		JSONArray mergedTools = MCPUtility.mergeGeneratedTools(
				MCPUtility.readMcpJson(assetFolder + MCPUtility.PIXEL_MCP_RELATIVE_PATH), toolsArray, GENERATOR_ID,
				true);
		JSONObject mcpJson = PlaywrightMCPToolBuilder.wrapMcpJson(mergedTools);

		String prettyJson = mcpJson.toString(4);
		FileSystemUtil.saveAssetFiles(assetFolder, List.of(MCPUtility.PIXEL_MCP_RELATIVE_PATH), List.of(prettyJson));

		classLogger.info("Saved room MCP to {}{} ({} playback tool(s) generated, {} other tool(s) preserved)",
				assetFolder, MCPUtility.PIXEL_MCP_RELATIVE_PATH, toolsArray.length(),
				mergedTools.length() - toolsArray.length());
		return new NounMetadata(mcpJson, PixelDataType.JSON_OBJECT);
	}

	@Override
	public String getReactorDescription() {
		return "Generates mcp/pixel_mcp.json for the current room insight from all playwright/*.json recordings "
				+ "saved in the same insight assets. Mirrors MakePlaywrightMCPReactor for room-level recordings.";
	}
}
