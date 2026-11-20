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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
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
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class SaveAssetReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SaveAssetReactor.class);

	public SaveAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_NAME.getKey(), ReactorKeysEnum.CONTENT.getKey(),
				ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		// Retrieve all file names and contents
		List<String> fileNames = getNounAsStringList(this.keysToGet[0]);
		List<String> contents = getNounAsStringList(this.keysToGet[1]);

		if (fileNames == null || fileNames.isEmpty() || contents == null || contents.isEmpty()) {
			throw new IllegalArgumentException("Must pass in at least one file name and content to save");
		}
		if (fileNames.size() != contents.size()) {
			throw new IllegalArgumentException("Number of file names and contents must match");
		}

		String space = this.keyValue.get(this.keysToGet[2]);
		String assetFolder = AssetUtility.getRootFolderPath(this.insight, space, true);
		String relativePath = AssetUtility.getAssetRelativePath(this.insight, space);
		String comment = this.keyValue.get(this.keysToGet[3]);
		if (comment == null) {
			comment = "add: SaveAsset executed";
		}

		boolean strictScriptSource = Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.STRICT_SCRIPT_SOURCE));
		// we will iterate here so that we dont have partial asset changes
		for (int i = 0; i < fileNames.size(); i++) {
			String rawFileName = fileNames.get(i).trim();
			String fileName = Utility.normalizePath(rawFileName);

			// limit saving R/Py Files in prod - no new files can be created but they can be
			// sourced
			if (strictScriptSource) {
				String extension = FilenameUtils.getExtension(fileName);
				if ("py".equalsIgnoreCase(extension) || "R".equalsIgnoreCase(extension)) {
					throw new IllegalArgumentException("User is not allowed to create or save R or Py scripts");
				}
			}

			// you cannot save at root level if you are in user/project space
			if (space != null && !space.isEmpty() && !space.equals(AssetUtility.INSIGHT_SPACE_KEY)
					&& !fileName.contains("/")) {
				return NounMetadata
						.getErrorNounMessage("You cannot create directory / files at this level for space = " + space);
			}
		}

		// iterate each fileName/content pair
		for (int i = 0; i < fileNames.size(); i++) {
			String rawFileName = fileNames.get(i).trim();
			String fileName = Utility.normalizePath(rawFileName);
			if (fileName == null || fileName.isEmpty()) {
				continue;
			}

			String filePath = assetFolder + "/" + fileName;
			String content = contents.get(i);
			File file = new File(filePath);
			try {
				FileUtils.writeStringToFile(file, content, Charset.forName("UTF-8"));
			} catch (IOException e) {
				classLogger.error("Failed to save file {}", fileName, e);
				NounMetadata error = NounMetadata.getErrorNounMessage("Unable to save file: " + fileName);
				SemossPixelException exception = new SemossPixelException(error);
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
		}

		// if we do not want this reactor to push to git/cloud
		// all the below logic can be removed

		NounMetadata warning = null;
		// add to git / push to cloud
		// we only do this if this is a saved insight or project/user space
		if (space == null || space.trim().isEmpty() || space.equals(AssetUtility.INSIGHT_SPACE_KEY)) {
			// if we are in the insight space
			// it must be a saved insight
			if (!this.insight.isSavedInsight()) {
				warning = NounMetadata.getWarningNounMessage(
						"Unable to commit file. All files will be commited once the insight is saved.");
			}
		}

		if (warning == null) {
			// add file to git
			List<String> gitRelativeFilePaths = new ArrayList<>();
			for (int i = 0; i < fileNames.size(); i++) {
				String rawFileName = fileNames.get(i).trim();
				String fileName = Utility.normalizePath(rawFileName);
				if (fileName == null || fileName.isEmpty()) {
					continue;
				}

				// check the file to see if it is version/
				// if not add it here
				// make the asset folder to be the first piece of the file path
				// need to get the first piece of fileName
				// add it to the asset
				// and pass that as asset folder
				String[] fileTokens = fileName.split("/");
				String baseDir = fileTokens[0];
				assetFolder = assetFolder + "/" + baseDir;
				fileName = fileName.replace(baseDir, "");
				// we dont want to start with a "/"
				if (relativePath.isEmpty()) {
					if (fileName.startsWith(DIR_SEPARATOR)) {
						gitRelativeFilePaths.add(fileName.substring(1));
					} else {
						gitRelativeFilePaths.add(fileName);
					}
				} else {
					gitRelativeFilePaths.add(relativePath + DIR_SEPARATOR + fileName);
				}
			}

			GitRepoUtils.addSpecificFiles(assetFolder, gitRelativeFilePaths);
			// Get the user's email
			AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
			String email = accessToken.getEmail();
			String author = accessToken.getUsername();

			// commit it
			GitRepoUtils.commitAddedFiles(assetFolder, comment, author, email);
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
					ClusterUtil.pushProjectFolder(project, assetFolder);
				} else {
					// this is a project asset. space is the projectId
					IProject project = Utility.getProject(space);
					ClusterUtil.pushProjectFolder(project, assetFolder);
				}
			}
		}

		NounMetadata retNoun = NounMetadata.getSuccessNounMessage("Success!");
		if (warning != null) {
			retNoun.addAdditionalReturn(warning);
		}
		return retNoun;
	}

	@Override
	public String getReactorDescription() {
		return "Save a single or multiple files in the space's file repository";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_NAME.getKey())) {
			return "Names of the file(s) to save";
		} else if (key.equals(ReactorKeysEnum.CONTENT.getKey())) {
			return "Contents of the file(s) to save";
		} else if (key.equals(ReactorKeysEnum.SPACE.getKey())) {
			return "This is an optional field to determine the space in which the relative file path exists (user project space, current insight space, project id space).";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while saving the files within the git repository associated with the space";
		}
		return super.getDescriptionForKey(key);
	}

}
