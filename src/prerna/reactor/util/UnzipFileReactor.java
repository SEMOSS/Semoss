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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.ZipUtils;
import prerna.util.git.GitRepoUtils;

public class UnzipFileReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(UnzipFileReactor.class);

	public UnzipFileReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
		this.keyRequired = new int[] { 1, 0 };
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
		String zipFileLocation = null;

		// if security enables, you need proper permissions
		// this takes in the insight and does a user check that the user has access to
		// perform the operations
		IEngine.CATALOG_TYPE engineType = null;
		IEngine engine = null;

		if (space == null || space.isEmpty()) {
			zipFileLocation = insight.getInsightFolder();
		} else if (AssetUtility.USER_SPACE_KEY.equals(space)) {
			zipFileLocation = AssetUtility.getRootFolderPath(this.insight, space, true);
		} else {
			try {
				engine = Utility.getEngine(space);
			} catch (Exception ex) {
				// ignore
			}
			if (engine == null) {
				engine = Utility.getProject(space);
			}

			if (engine == null) {
				throw new NullPointerException("Unknown engine or project with id " + space);
			}

			checkEngineEditSecurity(engine, user);

			engineType = engine.getCatalogType();
			zipFileLocation = EngineUtility.getSpecificEngineAssetsFolder(engineType, space, engine.getEngineName());
		}
		zipFileLocation = zipFileLocation.replace("\\", "/") + "/" + fileRelativePath;

		File zipFile = new File(zipFileLocation);
		if (zipFile.exists() && !zipFile.isFile()) {
			throw new IllegalArgumentException("Cannot find zip file '" + fileRelativePath + "'");
		}

		try {
			ZipUtils.unzip(zipFileLocation, zipFile.getParent());
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to unzip file. Detailed error = " + e.getMessage());
		}

		// track unzipped files in git when space is a project
		if (engine != null && engineType == CATALOG_TYPE.PROJECT) {
			try {
				String gitFolder = EngineUtility.getSpecificEngineVersionFolder(CATALOG_TYPE.PROJECT,
						engine.getEngineId(), engine.getEngineName());
				GitRepoUtils.addAllFiles(gitFolder, false);
				AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
				String author = accessToken.getUsername();
				String email = accessToken.getEmail();
				GitRepoUtils.commitAddedFiles(gitFolder, "add: unzipped " + fileRelativePath, author, email);
			} catch (Exception e) {
				classLogger.error("Error committing unzipped files to git for project {}", space, e);
			}
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
					ClusterUtil.pushProjectFolder(project, zipFile.getParent());
				}
			} else {
				if (engineType == CATALOG_TYPE.PROJECT) {
					ClusterUtil.pushProjectFolder((IProject) engine, zipFile.getParent());
				} else {
					ClusterUtil.pushEngineFolder(engine, zipFile.getParent());
				}

			}
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	@Override
	public String getReactorDescription() {
		return "Unzips the updated project and routes the extracted files";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "This is a required value containing the relative file path of the single zip file to be imported";
		} else if (key.equals(ReactorKeysEnum.SPACE.getKey())) {
			return "This is an optional field to determine the space in which the relative file path exists (user project space, current insight space, engine/project id space).";
		}
		return super.getDescriptionForKey(key);
	}

}
