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
package prerna.util.git.reactors;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.git.GitDestroyer;
import prerna.util.git.GitRepoUtils;

public class DeleteAssetReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DeleteAssetReactor.class);

	public DeleteAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(),
				ReactorKeysEnum.COMMENT_KEY.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// check if user is logged in
		User user = this.insight.getUser();
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}
		// Get the user's email
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getUsername();

		// get asset base folder
		String space = this.keyValue.get(this.keysToGet[1]);
		String baseFolderPath = AssetUtility.getRootFolderPath(this.insight, space, true);
		// relative path is used for git if insight is saved
		// or if we are dealing with project space
		String relativePath = "";
		if (space != null || insight.isSavedInsight()) {
			relativePath = AssetUtility.getAssetRelativePath(this.insight, space);
		}

		// get the list of file paths to delete
		List<String> filePaths = getNounAsStringList(this.keysToGet[0]);
		if (filePaths == null || filePaths.isEmpty()) {
			throw new IllegalArgumentException("Must pass in at least one file name to delete");
		}
		String comment = this.keyValue.get(this.keysToGet[2]);
		if (comment == null) {
			comment = "remove: DeleteAsset executed";
		}
		// Prepare to collect Git relative paths and actual File objects
		List<String> gitRelativeFilePaths = new ArrayList<>();
		List<File> deletedFiles = new ArrayList<>();

		// iterate each provided path and delete it
		for (String rawPath : filePaths) {
			String inputFilePath = Utility.normalizePath(rawPath.trim());
			if (inputFilePath == null || inputFilePath.isEmpty()) {
				continue;
			}

			String realFilePath = baseFolderPath + "/" + inputFilePath;
			realFilePath = realFilePath.replace("\\", "/");
			File realFile = new File(realFilePath);
			if (!realFile.exists()) {
				classLogger.warn("Cannot find the folder/file at path {}. Skipping.", inputFilePath);
				continue;
			}

			if (realFile.isDirectory()) {
				try {
					FileUtils.deleteDirectory(realFile);
				} catch (IOException e) {
					classLogger.error("Failed to delete folder at path {}", inputFilePath, e);
					throw new IllegalArgumentException(
							"Error occurred trying to delete folder at path " + inputFilePath);
				}
			} else {
				try {
					FileUtils.forceDelete(realFile);
				} catch (IOException e) {
					classLogger.error("Failed to delete file at path {}", inputFilePath, e);
					throw new IllegalArgumentException("Error occurred trying to delete file at path " + inputFilePath);
				}
			}

			// Collect for Git and cluster sync
			gitRelativeFilePaths.add(relativePath + "/" + inputFilePath);
			deletedFiles.add(realFile);
		}

		if (deletedFiles.isEmpty()) {
			throw new IllegalArgumentException("Could not find any of the files passed in to delete");
		}

		// commit deletions to Git (for each path, only if it has gitVersionFolder)
		String gitVersionFolder = null;
		if (insight.isSavedInsight() && (space == null || AssetUtility.INSIGHT_SPACE_KEY.equalsIgnoreCase(space))) {
			IProject project = Utility.getProject(this.insight.getProjectId());
			gitVersionFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), insight.getProjectId())
					.replace("\\", "/");
		} else if (AssetUtility.USER_SPACE_KEY.equalsIgnoreCase(space)) {
			AuthProvider provider = user.getPrimaryLogin();
			String userProjectId = user.getAssetProjectId(provider);
			if (userProjectId != null && !userProjectId.isEmpty()) {
				IProject userProject = Utility.getUserAssetProject(userProjectId);
				gitVersionFolder = AssetUtility
						.getUserAssetFolder(userProject.getProjectName(), userProject.getProjectId())
						.replace("\\", "/");
			}
		} else if (space != null && !space.isEmpty()) {
			IProject project = Utility.getProject(space);
			gitVersionFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), space).replace("\\", "/");
		}

		if (gitVersionFolder != null) {
			// remove/commit gitRelativeFilePaths whose realFilePath started with
			// gitVersionFolder.
			List<String> toDelete = new ArrayList<>();
			for (File f : deletedFiles) {
				String deletedRealPath = f.getAbsolutePath().replace("\\", "/");
				if (deletedRealPath.startsWith(gitVersionFolder)) {
					// build the exact Git-relative path for that single deleted file:
					String relativeGit = relativePath + "/"
							+ Utility.normalizePath(f.getAbsolutePath().substring(baseFolderPath.length() + 1));
					toDelete.add(relativeGit);
				}
			}
			if (!toDelete.isEmpty()) {
				GitDestroyer.removeSpecificFiles(gitVersionFolder, true, toDelete);
				GitRepoUtils.commitAddedFiles(gitVersionFolder, comment, author, email);
			}
		}

		// TODO: consolidate below with above
		// TODO: create new methods to directly deleteFromStorage instead of an entire
		// sync

		// push to the cloud
		if (ClusterUtil.IS_CLUSTER) {
			for (File f : deletedFiles) {
				String parentPath = f.getParent();
				if (parentPath == null) {
					continue;
				}

				// is it a user asset change
				if (AssetUtility.USER_SPACE_KEY.equalsIgnoreCase(space)) {
					AuthProvider provider = user.getPrimaryLogin();
					String projectId = user.getAssetProjectId(provider);
					if (projectId != null && !projectId.isEmpty()) {
						ClusterUtil.pushUserAsset(projectId);
					}
				}
				// is it an insight asset change
				else if (space == null || space.trim().isEmpty() || space.equals(AssetUtility.INSIGHT_SPACE_KEY)) {
					if (this.insight.isSavedInsight()) {
						IProject project = Utility.getProject(this.insight.getProjectId());
						// we can limit the push the parent directory of the deleted content
						ClusterUtil.pushProjectFolder(project, parentPath);
					}
				}
				// this is a project asset. space is the projectId
				else {
					IProject project = Utility.getProject(space);
					// we can limit the push the parent directory of the deleted content
					ClusterUtil.pushProjectFolder(project, parentPath);
				}
			}
		}

		return NounMetadata.getSuccessNounMessage("Success!");
	}

	@Override
	public String getReactorDescription() {
		return "Delete a single or multiple files in the space's file repository";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_NAME.getKey())) {
			return "Names of the file(s) to delete";
		} else if (key.equals(ReactorKeysEnum.SPACE.getKey())) {
			return "This is an optional field to determine the space in which the relative file path exists (user project space, current insight space, project id space).";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while removing the files within the git repository associated with the space";
		}
		return super.getDescriptionForKey(key);
	}

}
