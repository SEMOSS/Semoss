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

import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.FileSystemUtil;
import prerna.util.FileSystemUtil.SavedAsset;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class NewUserAssetsFileReactor extends AbstractReactor {

	public NewUserAssetsFileReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey(),
				ReactorKeysEnum.OVERRIDE.getKey() };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();

		IProject project = user.getAssetProject();
		if (project == null) {
			throw new IllegalArgumentException("Unable to find user asset app");
		}

		String versionGitFolder = AssetUtility.getUserAssetVersionFolder(project.getProjectName(),
				project.getProjectId());
		String assetFolder = AssetUtility.getUserAssetFolder(project.getProjectName(), project.getProjectId());

		String filePath = Utility.normalizePath(this.keyValue.get(this.keysToGet[0]));
		if (filePath == null || filePath.isEmpty()) {
			throw new IllegalArgumentException("Must provide a valid filePath");
		}
		while (filePath.startsWith("/")) {
			filePath = filePath.substring(1);
		}
		SavedAsset savedAsset = FileSystemUtil.createNewAssetFile(assetFolder, filePath,
				getBoolean(ReactorKeysEnum.OVERRIDE.getKey(), false));
		String comment = this.keyValue.get(this.keysToGet[1]);
		if (comment == null) {
			comment = "add: created file " + savedAsset.savedPath();
		}

		List<String> gitRelativeFilePaths = new ArrayList<>();
		gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + "/" + savedAsset.savedPath());

		// Get the user's email
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getResolvedUsername();

		GitRepoUtils.addSpecificFiles(versionGitFolder, gitRelativeFilePaths);
		// commit it
		GitRepoUtils.commitAddedFiles(versionGitFolder, comment, author, email);
		// handle synchronization to the cloud
		ClusterUtil.pushUserAsset(project.getProjectId());

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
		return super.getReactorDefinition().description("Create a new empty file in the user's assets folder").key(
				ReactorKeysEnum.FILE_PATH,
				"Names of the file to create. This relative path should assume the prefix of '/version/assets/' and not include the prefix in the string value.")
				.key(ReactorKeysEnum.COMMENT_KEY,
						"Comment to add while creating and saving the new file within the git repository for the user's asset project")
				.key(ReactorKeysEnum.OVERRIDE,
						key -> key.type(Boolean.class).description(FileSystemUtil.OVERRIDE_KEEP_DESCRIPTION))
				.outputList(SavedAsset.class, FileSystemUtil.SAVED_ASSETS_OUTPUT_DESCRIPTION);
	}

}
