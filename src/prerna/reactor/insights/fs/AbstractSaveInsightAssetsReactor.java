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

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;

public abstract class AbstractSaveInsightAssetsReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		// Retrieve all file paths and contents
		List<String> filePaths = getNounAsStringList(this.keysToGet[0]);
		List<String> contents = getNounAsStringList(this.keysToGet[1]);

		if (filePaths == null || filePaths.isEmpty() || contents == null || contents.isEmpty()) {
			throw new IllegalArgumentException("Must pass in at least one file name and content to save");
		}
		if (filePaths.size() != contents.size()) {
			throw new IllegalArgumentException("Number of file names and contents must match");
		}
		filePaths = Utility.normalizeFilePaths(filePaths);

		String assetFolder = this.insight.getInsightFolder();
//		String comment = this.keyValue.get(this.keysToGet[2]);
//		if (comment == null) {
//			comment = "add: SaveAppAssets executed";
//		}

		// Check strict script source settings once
		boolean strictScriptSource = Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.STRICT_SCRIPT_SOURCE));
		FileSystemUtil.validateAssetFiles(filePaths, strictScriptSource);
		saveAssetFiles(assetFolder, filePaths, contents);

		// add file to git
//		List<String> gitRelativeFilePaths = new ArrayList<>();
//		for (int i = 0; i < filePaths.size(); i++) {
//			String rawFileName = filePaths.get(i).trim();
//			String fileName = Utility.normalizePath(rawFileName);
//			if (fileName == null || fileName.isEmpty()) {
//				continue;
//			}
//
//			// for git, we need to add the assets folder which is assumed in the path
//			gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + DIR_SEPARATOR + fileName);
//		}
//
//		// Get the user's email
//		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
//		String email = accessToken.getEmail();
//		String author = accessToken.getUsername();
//
//		GitRepoUtils.addSpecificFiles(gitFolder, gitRelativeFilePaths);
//		// commit it
//		GitRepoUtils.commitAddedFiles(gitFolder, comment, author, email);
//		// handle synchronization to the cloud
//		ClusterUtil.pushEngineFolder(engine, assetFolder);

		// push room to cloud storage
		if (this.insight.getRoomId() != null) {
			ClusterUtil.pushRoomAsync(this.insight.getRoomId());
		}

		NounMetadata retNoun = NounMetadata.getSuccessNounMessage("Success!");
		return retNoun;
	}

	/**
	 * 
	 * @param assetFolder
	 * @param filePaths
	 * @param contents
	 */
	protected abstract void saveAssetFiles(String assetFolder, List<String> filePaths, List<String> contents);

}
