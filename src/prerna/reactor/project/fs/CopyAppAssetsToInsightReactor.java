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
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;

public class CopyAppAssetsToInsightReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CopyAppAssetsToInsightReactor.class);

	public CopyAppAssetsToInsightReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.FILE_PATH.getKey() };
		this.keyRequired = new int[] { 1, 1 };
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
		// editors/owners can copy any asset; view-only users are confined to the public
		// folder
		boolean canEdit = SecurityProjectUtils.userCanEditProject(user, projectId);
		if (!canEdit && !SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException(
					"Project " + projectId + " does not exist or user does not have access to view assets.");
		}
		IProject project = Utility.getProject(projectId);

		// Retrieve all file paths and contents
		List<String> filePaths = getNounAsStringList(this.keysToGet[1]);

		if (filePaths == null || filePaths.isEmpty()) {
			throw new IllegalArgumentException("Must pass in at least one file name and content to save");
		}

		String insightFolder = this.insight.getInsightFolder();
		String assetFolder = EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.PROJECT,
				project.getEngineId(), project.getEngineName());
		// Check strict script source settings once
		boolean strictScriptSource = Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.STRICT_SCRIPT_SOURCE));

		// we will iterate here so that we dont have partial asset changes
		for (int i = 0; i < filePaths.size(); i++) {
			String rawFileName = filePaths.get(i).trim();
			String fileName = Utility.normalizePath(rawFileName);

			// view-only users may only copy files from within the public folder
			if (!canEdit && !FileSystemUtil.isWithinPublicAssetFolder(fileName)) {
				throw new IllegalArgumentException(
						"User only has read access to the 'public' folder within the assets folder.");
			}

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
				classLogger.error("Error copying asset file {} to the insight folder", fileName, e);
				NounMetadata error = NounMetadata.getErrorNounMessage("Unable to copy file: " + fileName);
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
		return "Copy a single or multiple files from the projects assets folder to the current insight";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the project/app";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Names of the file(s) to copy. This relative path should assume the prefix of '/version/assets/' and not include the prefix in the string value.";
		}
		return super.getDescriptionForKey(key);
	}

}
