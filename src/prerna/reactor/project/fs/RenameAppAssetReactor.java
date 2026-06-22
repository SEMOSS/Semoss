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

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;
import prerna.util.git.GitDestroyer;
import prerna.util.git.GitRepoUtils;

public class RenameAppAssetReactor extends AbstractReactor {

	public RenameAppAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.NEW_VALUE.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String projectId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException(
					"Project " + projectId + " does not exist or user does not have access to edit assets.");
		}
		IProject project = Utility.getProject(projectId);

		String currentFileName = Utility.normalizePath(this.keyValue.get(this.keysToGet[1]));
		String newFileName = Utility.normalizePath(this.keyValue.get(this.keysToGet[2]));
		while (currentFileName != null && currentFileName.startsWith("/")) {
			currentFileName = currentFileName.substring(1);
		}
		while (newFileName != null && newFileName.startsWith("/")) {
			newFileName = newFileName.substring(1);
		}

		if (currentFileName == null || currentFileName.trim().isEmpty() || newFileName == null
				|| newFileName.trim().isEmpty()) {
			throw new IllegalArgumentException("Must pass both existing name and new name");
		}

		String versionGitFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(),
				project.getProjectId());
		String assetFolder = AssetUtility.getProjectAssetsFolder(project.getProjectName(), project.getProjectId());
		String comment = this.keyValue.get(this.keysToGet[3]);
		if (comment == null) {
			comment = "rename: Renaming " + currentFileName + " to " + newFileName;
		}

		FileSystemUtil.renameAsset(assetFolder, currentFileName, newFileName);

		// handle pushing to git and the cloud

		List<String> toAdd = new ArrayList<>();
		toAdd.add(Constants.ASSETS_FOLDER + DIR_SEPARATOR + newFileName);

		List<String> toRemove = new ArrayList<>();
		toRemove.add(Constants.ASSETS_FOLDER + DIR_SEPARATOR + currentFileName);

		// Get the user's email
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getResolvedUsername();

		GitRepoUtils.addSpecificFiles(versionGitFolder, toAdd);
		GitDestroyer.removeSpecificFiles(versionGitFolder, true, toRemove);
		// commit it
		GitRepoUtils.commitAddedFiles(versionGitFolder, comment, author, email);
		// handle synchronization to the cloud
		ClusterUtil.pushProjectFolder(project, assetFolder);

		NounMetadata retNoun = NounMetadata.getSuccessNounMessage("Success!");
		return retNoun;
	}

	@Override
	public String getReactorDescription() {
		return "Rename a file or directory in the projects assets folder";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the project/app";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Names of the file(s) to save. This relative path should assume the prefix of '/version/assets/' and not include the prefix in the string value.";
		} else if (key.equals(ReactorKeysEnum.NEW_VALUE.getKey())) {
			return "The new name for the file or directory. This cannot be an name for an existing file or directory and has the same character restrictions you would expect on typical file system.";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while saving the files within the git repository for the project";
		}
		return super.getDescriptionForKey(key);
	}

}
