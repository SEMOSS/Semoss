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

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.FileSystemUtil;

/**
 * Generates room scoped MCP tools from named reactors.
 *
 * <p>
 * Builds the same tools {@link MakePixelMCPReactor} does and merges them into
 * the room's {@code mcp/pixel_mcp.json} instead of a project's, leaving tools
 * written by other generators in place. Use it to give one room a set of tools
 * without publishing them to everyone a project reaches.
 *
 * <p>
 * The reactors to expose are always named. A room is not scoped to a project,
 * and the project context of the insight holding it can be set more than once
 * over that room's life, so there is no project whose reactors could be scanned
 * on the room's behalf.
 *
 * <p>
 * The room's file is not part of any project's git repository, so nothing is
 * committed and no project tag is applied. Access is the room's own: a room the
 * caller does not own cannot be loaded.
 *
 * <p>
 * Pixel usage:
 *
 * <pre>
 * MakeRoomPixelMCP(reactor = ["MyReactor"]); // into the calling insight's room
 * MakeRoomPixelMCP(roomId = ["..."], reactor = ["MyReactor"]); // another room
 * </pre>
 */
public class MakeRoomPixelMCPReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(MakeRoomPixelMCPReactor.class);

	/**
	 * Stamped into every generated tool as {@link MCPUtility#SMSS_MCP_GENERATOR}
	 * and used on the next run to tell this reactor's own tools apart from tools
	 * that merely share the file. It differs from the project generator id so a
	 * room run never prunes tools the project run wrote.
	 */
	private static final String GENERATOR_ID = "MakeRoomPixelMCP";

	public MakeRoomPixelMCPReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey(), ReactorKeysEnum.REACTOR.getKey(),
				ReactorKeysEnum.MCP_METADATA.getKey() };
		this.keyRequired = new int[] { 0, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		// Binding to the room makes getInsightFolder() return that room's folder
		// rather than the empty folder of the calling session.
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		if (roomId != null && !roomId.isBlank()) {
			// A room that cannot be loaded is not the caller's to write to. Failing
			// here matters: carrying on would write the tools into the calling
			// session's own folder while reporting success for the named room.
			Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
			this.insight.setRoomForInsight(room);
		}

		String assetFolder = this.insight.getInsightFolder();
		if (assetFolder == null || assetFolder.isBlank()) {
			throw new IllegalStateException("No insight asset folder is available for this session.");
		}

		List<String> reactorNames = getNounAsStringList(ReactorKeysEnum.REACTOR.getKey());
		if (reactorNames == null || reactorNames.isEmpty()) {
			throw new IllegalArgumentException("Name the reactors to expose with '" + ReactorKeysEnum.REACTOR.getKey()
					+ "'. A room has no project whose reactors could be scanned for it.");
		}
		List<Map<String, Object>> mcpMetadataList = getList(ReactorKeysEnum.MCP_METADATA.getKey());

		// No project: the reactors are resolved by name against this insight.
		JSONArray toolsArray = PixelMCPToolBuilder.buildTools(this.insight, null, reactorNames, null, mcpMetadataList);

		// Stamp first: ownership decides which existing tools the merge may replace.
		MCPUtility.stampGenerator(toolsArray, GENERATOR_ID);

		// The named reactors are the whole set this generator owns for the room, so
		// one it wrote before and was not asked for again is dropped. Tools carrying
		// another generator's stamp are left alone.
		JSONArray mergedTools = MCPUtility.mergeGeneratedTools(
				MCPUtility.readMcpJson(assetFolder + MCPUtility.PIXEL_MCP_RELATIVE_PATH), toolsArray, GENERATOR_ID,
				true);

		JSONObject mcpJson = PixelMCPToolBuilder.wrapMcpJson(mergedTools);
		FileSystemUtil.saveAssetFiles(assetFolder, List.of(MCPUtility.PIXEL_MCP_RELATIVE_PATH),
				List.of(mcpJson.toString(4)));

		classLogger.info("Saved room MCP to {}{} ({} tool(s) generated, {} other tool(s) preserved)", assetFolder,
				MCPUtility.PIXEL_MCP_RELATIVE_PATH, toolsArray.length(), mergedTools.length() - toolsArray.length());
		return new NounMetadata(mcpJson, PixelDataType.JSON_OBJECT);
	}

	@Override
	public String getReactorDescription() {
		return """
				Generates mcp/pixel_mcp.json in the room's folder from the reactors named, \
				so the tools are available to that room alone rather than to a whole project. \
				Reactors are named explicitly, since a room is not scoped to a project whose \
				reactors could be scanned for it.\
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ROOM_ID.getKey())) {
			return "The room whose folder the tools are written to. If not passed, the calling insight's own room folder is used.";
		} else if (key.equals(ReactorKeysEnum.REACTOR.getKey())) {
			return "The list of reactors to turn into mcp tools in the room's pixel_mcp.json";
		}
		return super.getDescriptionForKey(key);
	}

}
