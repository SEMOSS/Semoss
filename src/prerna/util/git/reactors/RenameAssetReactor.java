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
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.git.GitDestroyer;
import prerna.util.git.GitRepoUtils;

public class RenameAssetReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(RenameAssetReactor.class);

	public RenameAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.NEW_VALUE.getKey(),
				ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		// check login user
		User user = this.insight.getUser();
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		// input paths check
		String currentFileName = Utility.normalizePath(keyValue.get(this.keysToGet[0]));
		String newFileName = Utility.normalizePath(keyValue.get(this.keysToGet[1]));
		String space = keyValue.get(this.keysToGet[2]);

		if (currentFileName == null || (currentFileName = currentFileName.trim()).isEmpty() || newFileName == null
				|| (newFileName = newFileName.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must pass both existing name and new name");
		}

		String baseFolderPath = AssetUtility.getRootFolderPath(this.insight, space, true);
		String relativePath = AssetUtility.getAssetRelativePath(this.insight, space);
		String comment = this.keyValue.get(this.keysToGet[3]);
		if (comment == null) {
			comment = "rename: Renaming " + currentFileName + " to " + newFileName;
		}

		String oldAbs = (baseFolderPath + "/" + currentFileName).replace("\\", "/");
		String newAbs = (baseFolderPath + "/" + newFileName).replace("\\", "/");
		File oldFile = new File(oldAbs);
		File newFile = new File(newAbs);

		// validation checks
		if (!oldFile.exists()) {
			throw new IllegalArgumentException("Cannot find file/folder to rename: " + currentFileName);
		}
		if (newFile.exists()) {
			throw new IllegalArgumentException("A file or directory exists with the new name: " + newFileName);
		}

		try {
			FileUtils.forceMkdirParent(newFile);
		} catch (IOException e) {
			classLogger.error("Failed to create parent directory for {}", newFileName, e);
			throw new SemossPixelException(
					NounMetadata.getErrorNounMessage("Unable to create parent directory for " + newFileName));
		}

		// rename the file/folder
		try {
			if (oldFile.isDirectory()) {
				FileUtils.moveDirectory(oldFile, newFile);
			} else {
				FileUtils.moveFile(oldFile, newFile);
			}
		} catch (IOException e) {
			classLogger.error("Failed to rename {} to {}", currentFileName, newFileName, e);
			SemossPixelException ex = new SemossPixelException(
					NounMetadata.getErrorNounMessage("Failed to rename " + currentFileName));
			ex.setContinueThreadOfExecution(false);
			throw ex;
		}

		// handle pushing to git and the cloud

		List<String> toAdd = new ArrayList<>();
		List<String> toRemove = new ArrayList<>();

		// check the file to see if it is version/
		// if not add it here
		// make the asset folder to be the first piece of the file path
		// need to get the first piece of fileName
		// add it to the asset
		// and pass that as asset folder
		String[] fileTokens = currentFileName.split("/");
		String baseDir = fileTokens[0];
		baseFolderPath = baseFolderPath + "/" + baseDir;
		currentFileName = currentFileName.replace(baseDir, "");
		// we dont want to start with a "/"
		if (relativePath.isEmpty()) {
			if (currentFileName.startsWith(DIR_SEPARATOR)) {
				toRemove.add(currentFileName.substring(1));
			} else {
				toRemove.add(currentFileName);
			}
		} else {
			toRemove.add(relativePath + DIR_SEPARATOR + currentFileName);
		}
		newFileName = newFileName.replace(baseDir, "");
		// we dont want to start with a "/"
		if (relativePath.isEmpty()) {
			if (newFileName.startsWith(DIR_SEPARATOR)) {
				toAdd.add(newFileName.substring(1));
			} else {
				toAdd.add(newFileName);
			}
		} else {
			toAdd.add(relativePath + DIR_SEPARATOR + newFileName);
		}
		GitRepoUtils.addSpecificFiles(baseFolderPath, toAdd);
		GitDestroyer.removeSpecificFiles(baseFolderPath, true, toRemove);
		// Get the user's email
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getUsername();

		// commit it
		GitRepoUtils.commitAddedFiles(baseFolderPath, comment, author, email);
		// handle synchronization to the cloud
		if (AssetUtility.USER_SPACE_KEY.equalsIgnoreCase(space)) {
			AuthProvider provider = user.getPrimaryLogin();
			String projectId = user.getAssetProjectId(provider);
			if (projectId != null && !(projectId.isEmpty())) {
				ClusterUtil.pushUserAsset(projectId);
			}
		} else {
			// if space is null or it is in the insight, push using insight id to get engine
			if (space == null || space.trim().isEmpty() || space.equals(AssetUtility.INSIGHT_SPACE_KEY)) {
				IProject project = Utility.getProject(this.insight.getProjectId());
				ClusterUtil.pushProjectFolder(project, baseFolderPath);
			} else {
				// this is a project asset. space is the projectId
				IProject project = Utility.getProject(space);
				ClusterUtil.pushProjectFolder(project, baseFolderPath);
			}
		}

		return NounMetadata.getSuccessNounMessage("Renamed successfully");
	}

	@Override
	public String getReactorDescription() {
		return "Rename a file or directory";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Name of an existing file or directory";
		} else if (key.equals(ReactorKeysEnum.NEW_VALUE.getKey())) {
			return "The new name for the file or directory. This cannot be an name for an existing file or directory and has the same character restrictions you would expect on typical file system.";
		} else if (key.equals(ReactorKeysEnum.SPACE.getKey())) {
			return "This is an optional field to determine the space in which the relative file path exists (user project space, current insight space, project id space).";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add with the rename into the git repository associated with the space";
		}
		return super.getDescriptionForKey(key);
	}

}
