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

import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.FileSystemUtil;
import prerna.util.FileSystemUtil.SavedAsset;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class NewAppAssetsDirectoryReactor extends AbstractReactor {

	public NewAppAssetsDirectoryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.COMMENT_KEY.getKey(), ReactorKeysEnum.OVERRIDE.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();

		String projectId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException(
					"Project " + projectId + " does not exist or user does not have access to edit assets.");
		}
		IProject project = Utility.getProject(projectId);

		String versionGitFolder = EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.PROJECT,
				project.getEngineId(), project.getEngineName());
		String assetFolder = EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.PROJECT,
				project.getEngineId(), project.getEngineName());

		String filePath = Utility.normalizePath(this.keyValue.get(this.keysToGet[1]));
		if (filePath == null || filePath.isEmpty()) {
			throw new IllegalArgumentException("Must provide a valid filePath");
		}
		while (filePath.startsWith("/")) {
			filePath = filePath.substring(1);
		}
		SavedAsset savedAsset = FileSystemUtil.createNewAssetDirectory(assetFolder, filePath,
				getBoolean(ReactorKeysEnum.OVERRIDE.getKey(), false));
		String comment = this.keyValue.get(this.keysToGet[2]);
		if (comment == null) {
			comment = "add: created directory " + savedAsset.savedPath();
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
		ClusterUtil.pushProjectFolder(project, assetFolder);

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
		return super.getReactorDefinition().description("Create a new empty directory in the projects assets folder")
				.key(ReactorKeysEnum.PROJECT, "The unique id for the project/app")
				.key(ReactorKeysEnum.FILE_PATH,
						"Names of the file to create. This relative path should assume the prefix of '/version/assets/' and not include the prefix in the string value.")
				.key(ReactorKeysEnum.COMMENT_KEY,
						"Comment to add while creating and saving the new file within the git repository for the project")
				.key(ReactorKeysEnum.OVERRIDE,
						key -> key.type(Boolean.class).description(FileSystemUtil.OVERRIDE_DIRECTORY_DESCRIPTION))
				.outputList(SavedAsset.class, FileSystemUtil.SAVED_ASSETS_OUTPUT_DESCRIPTION);
	}

}
