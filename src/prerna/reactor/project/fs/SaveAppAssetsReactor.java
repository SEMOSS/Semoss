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
package prerna.reactor.project.fs;

import java.util.List;

import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;
import prerna.util.gson.GsonUtility;

public class SaveAppAssetsReactor extends AbstractSaveAppAssetsReactor {

	public SaveAppAssetsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.CONTENT.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 0 };
	}

	@Override
	protected void saveAssetFiles(String assetFolder, List<String> filePaths, List<String> contents) {
		// Validate JSON files before saving
		validateJsonFiles(filePaths, contents);
		FileSystemUtil.saveAssetFiles(assetFolder, filePaths, contents);
	}

	/**
	 * Validates that all .json files contain valid JSON format
	 * @param filePaths list of file paths to check
	 * @param contents list of file contents corresponding to the file paths
	 * @throws IllegalArgumentException if any .json file has invalid JSON format
	 */
	private void validateJsonFiles(List<String> filePaths, List<String> contents) {
		for (int i = 0; i < filePaths.size(); i++) {
			String filePath = filePaths.get(i);
			if (filePath != null && filePath.toLowerCase().endsWith(".json")) {
				String content = contents.get(i);
				// Decode the content first (same as FileSystemUtil.saveAssetFiles does)
				content = Utility.decodeURIComponent(content);
				try {
					GsonUtility.validateJsonString(content);
				} catch (IllegalArgumentException e) {
					throw new IllegalArgumentException("Invalid JSON format in file '" + filePath + "': " + e.getMessage());
				}
			}
		}
	}

	@Override
	public String getReactorDescription() {
		return "Save a single or multiple files in the projects assets folder. Content is provided within <encode></encode> blocks.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the project/app";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Names of the file(s) to save. This relative path should assume the prefix of '/version/assets/' and not include the prefix in the string value.";
		} else if (key.equals(ReactorKeysEnum.CONTENT.getKey())) {
			return "Contents of the file(s) to save. Content is provided within <encode></encode> blocks.";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while saving the files within the git repository for the project";
		}
		return super.getDescriptionForKey(key);
	}

}
