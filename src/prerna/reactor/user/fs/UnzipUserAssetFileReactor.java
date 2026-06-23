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

import java.io.File;
import java.io.IOException;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.ZipUtils;
import prerna.util.git.GitRepoUtils;

public class UnzipUserAssetFileReactor extends AbstractReactor {

	public UnzipUserAssetFileReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		IProject project = user.getAssetProject();
		if (project == null) {
			throw new IllegalArgumentException("Unable to find user asset app");
		}

		String fileRelativePath = Utility.normalizePath(this.keyValue.get(this.keysToGet[0]));
		if (fileRelativePath == null || fileRelativePath.isEmpty()) {
			throw new IllegalArgumentException("Must provide a valid filePath to the zip file");
		}
		while (fileRelativePath.startsWith("/") || fileRelativePath.startsWith("\\")) {
			fileRelativePath = fileRelativePath.substring(1);
		}

		String assetFolder = AssetUtility.getUserAssetFolder(project.getProjectName(), project.getProjectId());
		String zipFileLocation = assetFolder.replace("\\", "/") + "/" + fileRelativePath;

		File zipFile = new File(zipFileLocation);
		if (zipFile.exists() && !zipFile.isFile()) {
			throw new IllegalArgumentException("Cannot find zip file '" + fileRelativePath + "'");
		}

		try {
			ZipUtils.unzip(zipFileLocation, zipFile.getParent());
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to unzip file. Detailed error = " + e.getMessage());
		}

		// track unzipped files in git
		String gitFolder = AssetUtility.getUserAssetVersionFolder(project.getProjectName(), project.getProjectId());
		GitRepoUtils.addAllFiles(gitFolder, false);
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String author = accessToken.getResolvedUsername();
		String email = accessToken.getEmail();
		GitRepoUtils.commitAddedFiles(gitFolder, "add: unzipped " + fileRelativePath, author, email);

		// handle synchronization to the cloud
		ClusterUtil.pushUserAsset(project.getProjectId());

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	@Override
	public String getReactorDescription() {
		return "Unzips a zip file located in the user's assets folder and commits the extracted files to git";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "The relative file path of the zip file within the user's assets folder to extract";
		}
		return super.getDescriptionForKey(key);
	}

}
