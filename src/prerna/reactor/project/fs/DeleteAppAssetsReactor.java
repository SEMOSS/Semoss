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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;

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
import prerna.util.FileSystemUtil;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class DeleteAppAssetsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DeleteAppAssetsReactor.class);

	public DeleteAppAssetsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.COMMENT_KEY.getKey() };
		this.keyRequired = new int[] { 1, 0, 0 };
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

		String versionGitFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(),
				project.getProjectId());
		String assetFolder = AssetUtility.getProjectAssetsFolder(project.getProjectName(), project.getProjectId());

		// Retrieve all file names and contents
		// get the list of file paths to delete
		List<String> filePaths = getNounAsStringList(this.keysToGet[1]);
		if (filePaths == null) {
			filePaths = new ArrayList<>();
		}
		if (filePaths.isEmpty()) {
			File[] allFilesInAssets = new File(assetFolder).listFiles();
			for (File f : allFilesInAssets) {
				filePaths.add(f.getName());
			}
		}
		filePaths = Utility.normalizeFilePaths(filePaths);

		String comment = this.keyValue.get(this.keysToGet[2]);
		if (comment == null) {
			comment = "remove: deleted " + String.join(", ", filePaths);
		}

		// Prepare to collect Git relative paths and actual File objects
		List<String> gitRelativeFilePaths = new ArrayList<>();
		List<File> deletedFiles = new ArrayList<>();

		// iterate each provided path and delete it
		FileSystemUtil.deleteAssetFiles(assetFolder, filePaths, gitRelativeFilePaths, deletedFiles);

		if (deletedFiles.isEmpty()) {
			throw new IllegalArgumentException("Could not find any of the files passed in to delete");
		}

		// Get the user's email
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getUsername();

		// Stage the file deletions in git
		// Using git add --update (setUpdate=true) to detect all deletions on disk
		// This is more reliable than GitDestroyer.removeSpecificFiles() which uses
		// addFilepattern and can silently fail if the pattern doesn't match the index
		try (Git git = Git.open(new File(versionGitFolder))) {
			git.add().addFilepattern(".").setUpdate(true).call();
		} catch (Exception e) {
			classLogger.error("Error staging deleted files in git for project {}", projectId, e);
		}
		// commit it
		GitRepoUtils.commitAddedFiles(versionGitFolder, comment, author, email);
		// handle synchronization to the cloud
		ClusterUtil.pushProjectFolder(project, assetFolder);

		NounMetadata retNoun = NounMetadata.getSuccessNounMessage("Success!");
		return retNoun;
	}

	@Override
	public String getReactorDescription() {
		return "Delete a single or multiple files in the projects assets folder";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the project/app";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return """
					Names of the file(s) to delete. This relative path should assume the prefix of '/version/assets/' and not include the prefix in the string value.
					If no value passed in, all files in '/version/assets/' will be deleted.";
					""";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while removing the files within the git repository for the project";
		}
		return super.getDescriptionForKey(key);
	}

}
