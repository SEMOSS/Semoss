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
package prerna.reactor.util;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.unoserver.Unoserver;

public class ConvertFileUnoserverReactor extends AbstractReactor {

	public ConvertFileUnoserverReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.FORMAT.getKey(),
				ReactorKeysEnum.OUTPUT_FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
		this.keyRequired = new int[] { 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String fileRelativePath = Utility.normalizePath(this.keyValue.get(this.keysToGet[0]));
		String targetFormat = this.keyValue.get(this.keysToGet[1]);
		String outputRelativePath = this.keyValue.get(this.keysToGet[2]);
		String space = this.keyValue.get(this.keysToGet[3]);

		if (targetFormat == null || (targetFormat = targetFormat.trim()).isEmpty()) {
			targetFormat = Unoserver.DEFAULT_TARGET_FORMAT;
		}
		while (targetFormat.startsWith(".")) {
			targetFormat = targetFormat.substring(1);
		}

		String baseFolder = AssetUtility.getRootFolderPath(this.insight, space, true);
		File sourceFile = new File(baseFolder + "/" + fileRelativePath);
		if (!sourceFile.exists() || !sourceFile.isFile()) {
			throw new IllegalArgumentException("Cannot find file '" + fileRelativePath + "'");
		}

		File targetFile = null;
		String convertedRelativePath = null;
		if (outputRelativePath == null || (outputRelativePath = Utility.normalizePath(outputRelativePath.trim())).isEmpty()) {
			String outputFileName = FilenameUtils.getBaseName(sourceFile.getName()) + "." + targetFormat;
			targetFile = new File(sourceFile.getParentFile(), outputFileName);
			convertedRelativePath = FilenameUtils.getPath(fileRelativePath) + outputFileName;
		} else {
			targetFile = new File(baseFolder + "/" + outputRelativePath);
			convertedRelativePath = outputRelativePath;
		}

		Unoserver unoserver = new Unoserver();
		File convertedFile = unoserver.convertToFile(sourceFile, targetFormat, targetFile.getAbsolutePath());

		if (ClusterUtil.IS_CLUSTER) {
			if (AssetUtility.USER_SPACE_KEY.equalsIgnoreCase(space)) {
				AuthProvider provider = user.getPrimaryLogin();
				String projectId = user.getAssetProjectId(provider);
				if (projectId != null && !(projectId.isEmpty())) {
					ClusterUtil.pushUserAsset(projectId);
				}
			} else if (space == null || space.trim().isEmpty() || space.equals(AssetUtility.INSIGHT_SPACE_KEY)) {
				if (this.insight.getRoomId() != null) {
					ClusterUtil.pushRoomAsync(this.insight.getRoomId());
				} else if (this.insight.isSavedInsight()) {
					IProject project = Utility.getProject(this.insight.getProjectId());
					ClusterUtil.pushProjectFolder(project, convertedFile.getParent());
				}
			} else {
				IProject project = Utility.getProject(space);
				ClusterUtil.pushProjectFolder(project, convertedFile.getParent());
			}
		}

		Map<String, Object> fileDetails = new HashMap<>();
		fileDetails.put("fileName", convertedFile.getName());
		fileDetails.put("filePath", convertedRelativePath);
		fileDetails.put("sourceSize", Utility.getReadableFileSize(sourceFile.length()));
		fileDetails.put("size", Utility.getReadableFileSize(convertedFile.length()));
		return new NounMetadata(fileDetails, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Converts a document to another format (e.g. docx to pdf) using the configured unoserver microservice. "
				+ "The converted file is written next to the source file";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "This is a required value containing the relative file path of the document to convert";
		} else if (key.equals(ReactorKeysEnum.FORMAT.getKey())) {
			return "This is an optional value for the target format to convert to (e.g. pdf, docx, html, txt). Defaults to "
					+ Unoserver.DEFAULT_TARGET_FORMAT;
		} else if (key.equals(ReactorKeysEnum.OUTPUT_FILE_PATH.getKey())) {
			return "This is an optional value containing the relative file path to write the converted file to. If not provided, the converted file is written next to the source file with the target format as its extension";
		} else if (key.equals(ReactorKeysEnum.SPACE.getKey())) {
			return "This is an optional field to determine the space in which the relative file path exists (user project space, current insight space, project id space).";
		}
		return super.getDescriptionForKey(key);
	}

}
