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
package prerna.reactor.insights.fs;

import java.util.List;

import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.FileSystemUtil;

public class SaveInsightAssetsBase64Reactor extends AbstractSaveInsightAssetsReactor {

	private static final String DECODE = "decode";

	public SaveInsightAssetsBase64Reactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.CONTENT.getKey(), DECODE };
		this.keyRequired = new int[] { 1, 1, 0 };

//		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.CONTENT.getKey(),
//		ReactorKeysEnum.COMMENT_KEY.getKey(), DECODE };
//		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	protected void saveAssetFiles(String assetFolder, List<String> filePaths, List<String> contents) {
		boolean decode = getBoolean(DECODE, true);
		FileSystemUtil.saveAssetFilesBase64(assetFolder, filePaths, contents, decode);
	}

	@Override
	public String getReactorDescription() {
		return "Save a single or multiple files in the insight assets folder. Content for each file is provided as base64 utf-8 encoded input.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Names of the file(s) to save";
		} else if (key.equals(ReactorKeysEnum.CONTENT.getKey())) {
			return "Contents of the file(s) to save. Content is base64 utf-8 string.";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while saving the files within the git repository for the insight";
		} else if (key.equals(DECODE)) {
			return "Boolean to decode the base64 utf-8 content string before writing to the file. Default is true";
		}
		return super.getDescriptionForKey(key);
	}

}
