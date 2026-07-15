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
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.reflect.TypeToken;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.gson.GsonUtility;

public class GetAppBlocksJsonReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GetAppBlocksJsonReactor.class);

	public GetAppBlocksJsonReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
	
		if(projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Must input an project id");
		}
		
		// make sure valid id for user
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if(!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			// you dont have access
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
		}
		
		IProject project = Utility.getProject(projectId);
		if(project.requirePublish(true)) {
			classLogger.info(project.getProjectId() + " had to pull from cloud");
		}
		
		String portalsFolder = AssetUtility.getProjectPortalsFolder(project.getProjectId());
		
		// Try blocks.ipynb first (notebook apps), then fall back to blocks.json
		File blocksJsonFile = new File(portalsFolder + "/" + IProject.NOTEBOOK_IPYNB_FILE_NAME);
		if (!blocksJsonFile.exists() || !blocksJsonFile.isFile()) {
			blocksJsonFile = new File(portalsFolder + "/" + IProject.BLOCK_FILE_NAME);
		}
		if(!blocksJsonFile.exists() || !blocksJsonFile.isFile()) {
			throw new IllegalArgumentException("No blocks json file exists for this app");
		}
		Map<String, Object> blocksJson;
		try {
			blocksJson = (Map<String, Object>) GsonUtility.readJsonFileToObject(blocksJsonFile, new TypeToken<Map<String, Object>>() {}.getType());
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to read the blocks json file. Error = " + e.getMessage());
		}
		
		return new NounMetadata(blocksJson, PixelDataType.MAP);
	}

}
