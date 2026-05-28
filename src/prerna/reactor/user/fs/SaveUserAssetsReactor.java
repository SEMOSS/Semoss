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
package prerna.reactor.user.fs;

import java.util.List;

import org.json.JSONObject;

import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.FileSystemUtil;

public class SaveUserAssetsReactor extends AbstractSaveUserAssetsReactor {

	public SaveUserAssetsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.CONTENT.getKey(),
				ReactorKeysEnum.COMMENT_KEY.getKey() };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	protected void saveAssetFiles(String assetFolder, List<String> filePaths, List<String> contents) {
		FileSystemUtil.saveAssetFiles(assetFolder, filePaths, contents);
	}

	@Override
	public String getReactorDescription() {
		return "Saves one or more files to the user's assets folder and commits them to the user's asset git repository.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Names of the file(s) to save. This relative path should assume the prefix of '/version/assets/' and not include the prefix in the string value.";
		} else if (key.equals(ReactorKeysEnum.CONTENT.getKey())) {
			return """
					Contents of the file(s) to save. \
					For convenience, instead of escaping quotes or backslashes you can wrap \
					the input within "<encode>your_text</encode>" and the system will encode it for you.
					""";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while saving the files within the git repository for the user's asset project";
		}
		return super.getDescriptionForKey(key);
	}

	@Override
	public JSONObject getMcpProperties() {
		JSONObject properties = super.getMcpProperties();
		properties.getJSONObject(ReactorKeysEnum.CONTENT.getKey()).put("description",
				"Contents of the file(s) to save.");
		return properties;
	}

}
