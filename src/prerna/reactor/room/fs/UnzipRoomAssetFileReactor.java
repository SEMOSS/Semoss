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
import java.io.IOException;
import java.util.Map;

import org.json.JSONObject;

import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.FileSystemUtil;
import prerna.util.FileSystemUtil.SavedAsset;
import prerna.util.Utility;
import prerna.util.ZipUtils;

public class UnzipRoomAssetFileReactor extends AbstractRoomAssetsReactor {

	public UnzipRoomAssetFileReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.OVERRIDE.getKey() };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String roomFolder = getRoomFolder(requireUser());

		String fileRelativePath = Utility.normalizePath(this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey()));
		if (fileRelativePath == null || fileRelativePath.isEmpty()) {
			throw new IllegalArgumentException("Must provide a valid filePath to the zip file");
		}
		while (fileRelativePath.startsWith("/")) {
			fileRelativePath = fileRelativePath.substring(1);
		}

		String zipFileLocation = roomFolder.replace("\\", "/") + "/" + fileRelativePath;
		File zipFile = new File(zipFileLocation);
		if (!zipFile.isFile()) {
			throw new IllegalArgumentException("Cannot find zip file '" + fileRelativePath + "'");
		}

		Map<String, String> extractedNames;
		try {
			extractedNames = ZipUtils.unzip(zipFileLocation, zipFile.getParent(),
					getBoolean(ReactorKeysEnum.OVERRIDE.getKey(), false));
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to unzip file. Detailed error = " + e.getMessage());
		}

		pushRoom();
		return FileSystemUtil.savedAssetsAsNounMetadata(FileSystemUtil.unzippedAssets(fileRelativePath, extractedNames));
	}

	@Override
	public JSONObject getMcpProperties() {
		JSONObject properties = super.getMcpProperties();
		properties.getJSONObject(ReactorKeysEnum.OVERRIDE.getKey()).put("default", false);
		return properties;
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("Unzips a zip file in a room's folder next to the zip file")
				.key(ReactorKeysEnum.FILE_PATH, "Path of the zip file to extract, relative to the room's folder.")
				.key(ReactorKeysEnum.OVERRIDE,
						key -> key.type(Boolean.class).description(FileSystemUtil.OVERRIDE_UNZIP_DESCRIPTION))
				.outputList(SavedAsset.class, FileSystemUtil.SAVED_ASSETS_OUTPUT_DESCRIPTION);
	}

}
