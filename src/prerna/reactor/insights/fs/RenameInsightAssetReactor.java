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

import org.json.JSONObject;

import prerna.cluster.util.ClusterUtil;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.FileSystemUtil;
import prerna.util.FileSystemUtil.SavedAsset;
import prerna.util.Utility;

public class RenameInsightAssetReactor extends AbstractReactor {

	public RenameInsightAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.NEW_VALUE.getKey(),
				ReactorKeysEnum.OVERRIDE.getKey() };
		this.keyRequired = new int[] { 1, 1, 0 };

//		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(),
//				ReactorKeysEnum.NEW_VALUE.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey() };
//		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String currentFileName = Utility.normalizePath(this.keyValue.get(this.keysToGet[0]));
		String newFileName = Utility.normalizePath(this.keyValue.get(this.keysToGet[1]));

		if (currentFileName == null || currentFileName.trim().isEmpty() || newFileName == null
				|| newFileName.trim().isEmpty()) {
			throw new IllegalArgumentException("Must pass both existing name and new name");
		}

		String assetFolder = this.insight.getInsightFolder();
//		String comment = this.keyValue.get(this.keysToGet[2]);
//		if (comment == null) {
//			comment = "rename: Renaming " + currentFileName + " to " + newFileName;
//		}

		SavedAsset savedAsset = FileSystemUtil.renameAsset(assetFolder, currentFileName, newFileName,
				getBoolean(ReactorKeysEnum.OVERRIDE.getKey(), false));

		// handle pushing to git and the cloud

//		List<String> toAdd = new ArrayList<>();
//		toAdd.add(Constants.ASSETS_FOLDER + DIR_SEPARATOR + newFileName);
//
//		List<String> toRemove = new ArrayList<>();
//		toRemove.add(Constants.ASSETS_FOLDER + DIR_SEPARATOR + currentFileName);
//
//		// Get the user's email
//		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
//		String email = accessToken.getEmail();
//		String author = accessToken.getUsername();
//
//		GitRepoUtils.addSpecificFiles(gitFolder, toAdd);
//		GitDestroyer.removeSpecificFiles(gitFolder, true, toRemove);
//		// commit it
//		GitRepoUtils.commitAddedFiles(gitFolder, comment, author, email);
//		// handle synchronization to the cloud
//		ClusterUtil.pushEngineFolder(engine, assetFolder);

		// push room to cloud storage
		if (this.insight.getRoomId() != null) {
			ClusterUtil.pushRoomAsync(this.insight.getRoomId());
		}

		return FileSystemUtil.savedAssetsAsNounMetadata(List.of(savedAsset));
	}

	@Override
	public JSONObject getMcpProperties() {
		JSONObject properties = super.getMcpProperties();
		properties.getJSONObject(ReactorKeysEnum.OVERRIDE.getKey()).put("default", false);
		return properties;
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("Rename a file or directory in the insight assets folder").key(
				ReactorKeysEnum.FILE_PATH,
				"Names of the file(s) to save. This relative path should assume the prefix of the insight folder.")
				.key(ReactorKeysEnum.NEW_VALUE,
						"The new path for the file or directory, with the same character restrictions you would expect on a typical file system. When something already exists there, override decides whether it is replaced or the entry gets the next copy number.")
				.key(ReactorKeysEnum.OVERRIDE,
						key -> key.type(Boolean.class).description(FileSystemUtil.OVERRIDE_KEEP_DESCRIPTION))
				.outputList(SavedAsset.class, FileSystemUtil.SAVED_ASSETS_OUTPUT_DESCRIPTION);
	}

}
