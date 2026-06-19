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
package prerna.reactor.engine.fs;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class DeleteEngineAssetsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DeleteEngineAssetsReactor.class);

	public DeleteEngineAssetsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
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

		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Engine " + engineId + " does not exist or user does not have access to edit assets.");
		}
		IEngine engine = Utility.getEngine(engineId);

		String versionGitFolder = EngineUtility.getSpecificEngineVersionFolder(engine.getCatalogType(),
				engine.getEngineId(), engine.getEngineName());
		String assetFolder = EngineUtility.getSpecificEngineAssetsFolder(engine.getCatalogType(), engine.getEngineId(),
				engine.getEngineName());

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
			comment = "remove: delete engine assets executed";
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
		String author = accessToken.getResolvedUsername();

		// Stage the file deletions in git
		try (Git git = Git.open(new File(versionGitFolder))) {
			git.add().addFilepattern(".").setUpdate(true).call();
		} catch (Exception e) {
			classLogger.error("Error staging deleted files in git for engine {}", engineId, e);
		}
		// commit it
		GitRepoUtils.commitAddedFiles(versionGitFolder, comment, author, email);
		// handle synchronization to the cloud
		ClusterUtil.pushEngineFolder(engine, assetFolder);

		NounMetadata retNoun = NounMetadata.getSuccessNounMessage("Success!");
		return retNoun;
	}

	@Override
	public String getReactorDescription() {
		return "Delete a single or multiple files in the engine folder";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The unique id for the engine";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return """
					Names of the file(s) to delete. This relative path should assume the prefix of '/version/assets/' and not include the prefix in the string value.
					If no value passed in, all files in '/version/assets/' will be deleted.";
					""";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while removing the files within the git repository for the engine";
		}
		return super.getDescriptionForKey(key);
	}

}
