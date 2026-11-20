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
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class CopyUserAssetsToInsightReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CopyUserAssetsToInsightReactor.class);

	public CopyUserAssetsToInsightReactor() {
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

		// Retrieve all file paths and contents
		List<String> filePaths = getNounAsStringList(this.keysToGet[0]);

		if (filePaths == null || filePaths.isEmpty()) {
			throw new IllegalArgumentException("Must pass in at least one file name and content to save");
		}

		String insightFolder = this.insight.getInsightFolder();
		String assetFolder = AssetUtility.getUserAssetFolder(project.getProjectName(), project.getProjectId());
		// Check strict script source settings once
		boolean strictScriptSource = Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.STRICT_SCRIPT_SOURCE));

		// we will iterate here so that we dont have partial asset changes
		for (int i = 0; i < filePaths.size(); i++) {
			String rawFileName = filePaths.get(i).trim();
			String fileName = Utility.normalizePath(rawFileName);

			// limit saving R/Py Files in prod - no new files can be created but they can be
			// sourced
			if (strictScriptSource) {
				String extension = FilenameUtils.getExtension(fileName);
				if ("py".equalsIgnoreCase(extension) || "R".equalsIgnoreCase(extension)) {
					throw new IllegalArgumentException("User is not allowed to create or save R or Py scripts");
				}
			}
		}

		// iterate each fileName/content pair
		for (int i = 0; i < filePaths.size(); i++) {
			String rawFileName = filePaths.get(i).trim();
			String fileName = Utility.normalizePath(rawFileName);
			if (fileName == null || fileName.isEmpty()) {
				continue;
			}
			while (fileName.startsWith("/")) {
				fileName = fileName.substring(1);
			}

			String filePath = assetFolder + "/" + fileName;
			File file = new File(filePath);

			String insightFilePath = insightFolder + "/" + file.getName();
			File insightFile = new File(insightFilePath);

			try {
				FileUtils.copyFile(file, insightFile);
			} catch (IOException e) {
				classLogger.error("Failed to copy user asset '{}' into insight folder: {}", fileName, e.getMessage(),
						e);
				NounMetadata error = NounMetadata.getErrorNounMessage("Unable to save file: " + fileName);
				SemossPixelException exception = new SemossPixelException(error);
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
		}

		NounMetadata retNoun = NounMetadata.getSuccessNounMessage("Success!");
		return retNoun;
	}

	@Override
	public String getReactorDescription() {
		return "Copy a single or multiple files from the user's assets folder to the current insight";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Names of the file(s) to copy. This relative path should assume the prefix of '/version/assets/' and not include the prefix in the string value.";
		}
		return super.getDescriptionForKey(key);
	}

}
