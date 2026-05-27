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
package prerna.engine.impl.model.inferencetracking.reactors;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.Room;
import prerna.theme.PlaygroundThemeUtils;
import prerna.engine.impl.model.RoomUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class CreateRoomReactor extends AbstractReactor {

	public CreateRoomReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NAME.getKey(), ReactorKeysEnum.CONTEXT.getKey(),
				ReactorKeysEnum.VECTORDB.getKey(), ReactorKeysEnum.FUNCTION.getKey(),
				ReactorKeysEnum.WORKSPACE_ID.getKey(), ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = insight.getUser();

		String roomName = this.keyValue.get(ReactorKeysEnum.NAME.getKey());
		String context = this.keyValue.get(ReactorKeysEnum.CONTEXT.getKey());
		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());
		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());

		Map<String, Object> options = null;
		if (workspaceId != null) {
			if (!SecurityProjectUtils.userCanViewProject(user, workspaceId)) {
				throw new IllegalArgumentException("Workspace " + workspaceId
						+ " does not exist or user does not have access to view the workspace");
			}
		} else {
			List<String> vectorDbs = getListString(ReactorKeysEnum.VECTORDB.getKey(), Collections.emptyList());
			List<String> tools = getListString(ReactorKeysEnum.FUNCTION.getKey(), Collections.emptyList());

			options = new HashMap<>();
			if (!tools.isEmpty()) {
				options.put("tools", tools);
			}
			if (!vectorDbs.isEmpty()) {
				options.put("vectorDbs", vectorDbs);
			}

			// Default MCP tools from admin theme config
			List<Map<String, Object>> defaultMcpTools = PlaygroundThemeUtils.getPlaygroundDefaultTools();
			if (!defaultMcpTools.isEmpty()) {
				options.put("mcp", defaultMcpTools);
			}
		}

		Room room = RoomUtils.createRoomIfNotExists(UUID.randomUUID().toString(), insight, null, roomName, workspaceId,
				options, context, projectId, null);

		if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))) {
			Path folderPath = Paths.get(room.getRoomFolderPath());
			try {
				Files.createDirectories(folderPath);
				this.insight.getUser().getUserSymlinkHelper().symlinkFolder(folderPath.toString());
			} catch (IOException e) {
				throw new UncheckedIOException("Failed to create and symlink room folder: " + folderPath, e);
			}
		}

		Map<String, Object> output = new HashMap<String, Object>();
		output.put("roomId", room.getId());
		return new NounMetadata(output, PixelDataType.MAP);
	}

}
