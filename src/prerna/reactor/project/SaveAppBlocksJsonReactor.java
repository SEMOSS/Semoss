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
package prerna.reactor.project;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.project.impl.notebook.INotebookHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;
import prerna.util.gson.GsonUtility;
import prerna.util.gson.LocalDateTimeAdapter;
import prerna.util.gson.ZonedDateTimeAdapter;

public class SaveAppBlocksJsonReactor extends AbstractReactor {

	protected static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.registerTypeAdapter(LocalDateTime.class, new LocalDateTimeAdapter())
			.registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeAdapter())
			// we want pretty printing so git diff is readable
			.setPrettyPrinting().create();

	private static final Logger classLogger = LogManager.getLogger(SaveAppBlocksJsonReactor.class);

	private static final String CLASS_NAME = SaveAppBlocksJsonReactor.class.getName();

	public SaveAppBlocksJsonReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.JSON.getKey(),
				ReactorKeysEnum.COMMENT_KEY.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);

		User user = this.insight.getUser();
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);

		if (projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Must input an project id");
		}

		// make sure valid id for user
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
		if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			// you dont have access
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
		}

		Map<String, Object> json = getBlocksJSON();
		if (json == null || json.isEmpty()) {
			throw new IllegalArgumentException("Must provide the blocks JSON");
		}

		String comment = this.keyValue.get(this.keysToGet[2]);

		IProject project = Utility.getProject(projectId);
		String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
		File blocksJsonFile = new File(portalsFolder + "/" + IProject.BLOCK_FILE_NAME);
		if (blocksJsonFile.exists() && blocksJsonFile.isFile()) {
			blocksJsonFile.delete();
		}

		try {
			GsonUtility.writeObjectToJsonFile(blocksJsonFile, GSON, json);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(
					"Was unable to save the blocks json to the project folder. Errror = " + e.getMessage());
		}

		// add file to git
		List<String> files = new Vector<>();
		files.add(blocksJsonFile.getAbsolutePath());
		String projectVersionFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId);
		GitRepoUtils.addSpecificFiles(projectVersionFolder, files);
		// commit it
		GitRepoUtils.commitAddedFiles(projectVersionFolder, comment, user);

		if (ClusterUtil.IS_CLUSTER) {
			logger.info("Syncing project for cloud backup");
			ClusterUtil.pushProjectFolder(project, projectVersionFolder);
			SecurityProjectUtils.setPortalPublish(user, projectId);
		}

		// auto save the engine dependencies as well
		Map<String, String> engineDependenciesMap = project.getEngineDependencies();
		Set<String> engineDependencyIds = new HashSet<>(engineDependenciesMap.values());
		engineDependencyIds.remove(INotebookHelper.UNDEFINED_VALUE);
		SecurityProjectUtils.updateProjectDependenciesWithoutType(user, projectId, engineDependencyIds);
		SecurityProjectUtils.updateProjectLastEditedDate(projectId);

		return new NounMetadata(true, PixelDataType.MAP);
	}

	private Map<String, Object> getBlocksJSON() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.JSON.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}

			List<NounMetadata> strGrs = mapGrs.getNounsOfType(PixelDataType.CONST_STRING);
			if (strGrs != null && !strGrs.isEmpty()) {
				String jsonStr = (String) strGrs.get(0).getValue();

				// validate this is actually JSON
				GsonUtility.validateJsonString(jsonStr);

				return GSON.fromJson(jsonStr, Map.class);
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}

		return null;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.JSON.getKey())) {
			return "The JSON that represents the blocks for the app";
		}
		return super.getDescriptionForKey(key);
	}

}
