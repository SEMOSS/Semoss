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
import java.io.IOException;
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
import prerna.util.ZipUtils;

public class GzipFileReactor extends AbstractReactor {

	public GzipFileReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(),
				ReactorKeysEnum.FILE_NAME.getKey() };
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

		// specify the folder from the base
		String fileRelativePath = Utility.normalizePath(keyValue.get(keysToGet[0]));
		String space = this.keyValue.get(this.keysToGet[1]);
		String outputFileName = this.keyValue.get(this.keysToGet[1]);
		// if security enables, you need proper permissions
		// this takes in the insight and does a user check that the user has access to
		// perform the operations
		String baseFolder = AssetUtility.getRootFolderPath(this.insight, space, true);
		String fileToGzip = (baseFolder + "/" + fileRelativePath).replace('\\', '/');
		File fileToGzipF = new File(fileToGzip);
		if (fileToGzipF.exists() && !fileToGzipF.isFile()) {
			throw new IllegalArgumentException("Cannot find zip file '" + fileRelativePath + "')");
		}

		if (outputFileName == null || (outputFileName = outputFileName.trim()).isEmpty()) {
			outputFileName = FilenameUtils.getName(fileToGzip) + ".gz";
		}

		File gzipFile = new File(fileToGzipF.getParent(), outputFileName);
		try {
			ZipUtils.compressGzipFile(fileToGzip, gzipFile.getAbsolutePath());
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to gzip file. Detailed error = " + e.getMessage());
		}

		if (ClusterUtil.IS_CLUSTER) {
			// is it in the user space?
			if (AssetUtility.USER_SPACE_KEY.equalsIgnoreCase(space)) {
				AuthProvider provider = user.getPrimaryLogin();
				String projectId = user.getAssetProjectId(provider);
				if (projectId != null && !(projectId.isEmpty())) {
					ClusterUtil.pushUserAsset(projectId);
				}
				// is it in the insight space of a saved insight?
			} else if (space == null || space.trim().isEmpty() || space.equals(AssetUtility.INSIGHT_SPACE_KEY)) {
				if (this.insight.isSavedInsight()) {
					IProject project = Utility.getProject(this.insight.getProjectId());
					ClusterUtil.pushProjectFolder(project, gzipFile.getParent());
				}
				// this is in the project space where space = project id
			} else {
				IProject project = Utility.getProject(space);
				ClusterUtil.pushProjectFolder(project, gzipFile.getParent());
			}
		}

		Map<String, Object> fileDetails = new HashMap<>();
		fileDetails.put("fileName", gzipFile.getName());
		fileDetails.put("sourceSize", Utility.getReadableFileSize(fileToGzipF.length()));
		fileDetails.put("size", Utility.getReadableFileSize(gzipFile.length()));
		return new NounMetadata(fileDetails, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used to gzip a file";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "This is a required value containing the relative file path of a single file to be compressed";
		} else if (key.equals(ReactorKeysEnum.SPACE.getKey())) {
			return "This is an optional field to determine the space in which the relative file path exists (user project space, current insight space, project id space).";
		} else if (key.equals(ReactorKeysEnum.FILE_NAME.getKey())) {
			return "This is a optional value to determine the filename of the gzip file. If not provided, the current file name will be appended with '.gz'";
		}
		return super.getDescriptionForKey(key);
	}

}
