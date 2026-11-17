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
package prerna.reactor.room.fs;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.reactor.ReactorDefinition;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;

public class DeleteRoomAssetsReactor extends AbstractRoomAssetsReactor {

	public DeleteRoomAssetsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey(), ReactorKeysEnum.FILE_PATH.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String roomFolder = getRoomFolder(requireUser());

		List<String> filePaths = getNounAsStringList(ReactorKeysEnum.FILE_PATH.getKey());
		if (filePaths == null || filePaths.isEmpty()) {
			throw new IllegalArgumentException("Must pass at least one filePath to delete");
		}
		filePaths = Utility.normalizeFilePaths(filePaths);

		List<String> deletedPaths = new ArrayList<>();
		List<File> deletedFiles = new ArrayList<>();
		FileSystemUtil.deleteAssetFiles(roomFolder, filePaths, deletedPaths, deletedFiles);
		if (deletedFiles.isEmpty()) {
			throw new IllegalArgumentException("Could not find any of the files passed in to delete");
		}

		pushRoom();
		return NounMetadata.getSuccessNounMessage("Success!");
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = new HashMap<>();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		meta.put(MCPUtility.UI_DISPLAY_LOCATION, MCPUtility.MCPDisplayOption.SIDEBAR.getValue());
		return meta;
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("Delete one or more files or directories in a room's folder")
				.key(ReactorKeysEnum.FILE_PATH, key -> key.list(String.class).description(
						"Paths of the file(s) or directories to delete, relative to the room's folder. Paths that are missing are skipped."))
				.output(String.class, "Success! once at least one path was deleted");
	}

}
