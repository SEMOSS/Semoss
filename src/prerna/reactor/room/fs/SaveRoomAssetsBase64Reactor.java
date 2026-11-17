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

import java.util.List;

import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.FileSystemUtil;
import prerna.util.FileSystemUtil.SavedAsset;

public class SaveRoomAssetsBase64Reactor extends AbstractSaveRoomAssetsReactor {

	private static final String DECODE = "decode";

	public SaveRoomAssetsBase64Reactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.CONTENT.getKey(), DECODE, ReactorKeysEnum.OVERRIDE.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 0, 0 };
	}

	@Override
	protected List<SavedAsset> saveAssetFiles(String roomFolder, List<String> filePaths, List<String> contents,
			boolean override) {
		boolean decode = getBoolean(DECODE, true);
		return FileSystemUtil.saveAssetFilesBase64(roomFolder, filePaths, contents, decode, override);
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Saves one or more files to a room's folder. Content for each file is provided as base64 utf-8 encoded input.")
				.key(ReactorKeysEnum.FILE_PATH, "Names of the file(s) to save, relative to the room's folder.")
				.key(ReactorKeysEnum.CONTENT, "Contents of the file(s) to save. Content is base64 utf-8 string.")
				.key(DECODE, key -> key.type(Boolean.class).description(
						"Whether to decode the base64 utf-8 content string before writing to the file. Defaults to true."));
	}

}
