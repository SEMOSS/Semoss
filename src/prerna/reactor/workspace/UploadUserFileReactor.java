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
package prerna.reactor.workspace;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.UserAssetUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class UploadUserFileReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(UploadUserFileReactor.class);

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	/*
	 * TODO: DONT BELIEVE THIS WORKS WITH CLOUD ?
	 * 
	 * 
	 */

	public UploadUserFileReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.RELATIVE_PATH.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String uploadedFilePath = Utility.normalizePath(this.keyValue.get(this.keysToGet[0]));
		if (uploadedFilePath == null || uploadedFilePath.isEmpty()) {
			throw new IllegalArgumentException("Must input file path for the user file");
		}
		uploadedFilePath = Utility.normalizeParam(uploadedFilePath);

		String relativeFilePath = this.keyValue.get(this.keysToGet[1]);
		if (relativeFilePath == null || relativeFilePath.isEmpty()) {
			relativeFilePath = "";
		} else {
			relativeFilePath = Utility.normalizeParam(relativeFilePath);
		}

		File uploadedFile = new File(uploadedFilePath);

		String assetProjectId = null;
		User user = this.insight.getUser();
		if (user != null) {
			AuthProvider token = user.getPrimaryLogin();
			if (token != null) {
				assetProjectId = user.getAssetProjectId(token);
				Utility.getProject(assetProjectId);
			}
		}

		if (assetProjectId == null) {
			throw new IllegalArgumentException("Unable to find Asset App ID for user");
		}

		String baseUserFolderPath = AssetUtility.getRootFolderPath(this.insight, AssetUtility.USER_SPACE_KEY, true);
		File baseUserFolder = new File(baseUserFolderPath);
		if (!baseUserFolder.exists()) {
			throw new IllegalArgumentException("Unable to find user asset app directory");
		}

		// Where we are storing their information under version. Make the version folder
		// if it doesn't exist.
		String userFolderPath = baseUserFolderPath + DIR_SEPARATOR + "version";
		File userFolder = new File(userFolderPath);
		Boolean newFolder = userFolder.mkdir();
		if (ClusterUtil.IS_CLUSTER) {
			if (newFolder) {
				File hidden = new File(userFolderPath + DIR_SEPARATOR + UserAssetUtils.HIDDEN_FILE);
				try {
					hidden.createNewFile();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		String fileName = uploadedFile.getName().toLowerCase();
		// copy file into the directory from tmp upload space if it is valid. For now
		// its just .R files
		if (!fileName.toLowerCase().endsWith(".r") || !fileName.toLowerCase().endsWith(".py")) {
			throw new IllegalArgumentException("File must be of type .r or .py");
		}

		try {
			FileUtils.copyFile(uploadedFile, new File(userFolder.getAbsolutePath() + DIR_SEPARATOR + relativeFilePath
					+ DIR_SEPARATOR + uploadedFile.getName()));
			ClusterUtil.pushEngine(assetProjectId);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to copy file");
		}

		Map<String, Object> uploadUserData = new HashMap<String, Object>();
		uploadUserData.put("uploadedFile", uploadedFilePath);
		uploadUserData.put("app", assetProjectId);
		return new NounMetadata(uploadUserData, PixelDataType.MAP, PixelOperationType.USER_UPLOAD);
	}

}
