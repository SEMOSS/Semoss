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
package prerna.reactor.project.template;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class AddTemplateReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AddTemplateReactor.class);
	private static final String CLASS_NAME = AddTemplateReactor.class.getName();

	public AddTemplateReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.TEMPLATE_NAME.getKey(),
				ReactorKeysEnum.TEMPLATE_FILE.getKey() };
	}

	public NounMetadata execute() {
		organizeKeys();
		Logger logger = getLogger(CLASS_NAME);
		
		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		String templateFile = this.keyValue.get(ReactorKeysEnum.TEMPLATE_FILE.getKey());
		String templateName = this.keyValue.get(ReactorKeysEnum.TEMPLATE_NAME.getKey());
		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must input a project id");
		}
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to edit the project");
		}

		IProject project = Utility.getProject(projectId);
		String versionFolder = AssetUtility.getProjectAppRootFolder(project.getProjectName(), projectId);
		String fileToMove = versionFolder;
		if(templateFile.startsWith("/") || templateFile.startsWith("\\")) {
			fileToMove += templateFile;
		} else {
			fileToMove += "/" + templateFile;
		}
		fileToMove = fileToMove.replace('\\', '/');
		File f = new File(fileToMove);
		// we will move this file over
		String baseF = this.insight.getInsightFolder();
		String tempMove = baseF + "/" + UUID.randomUUID().toString() + "." + FilenameUtils.getExtension(fileToMove);
		File newF = new File(tempMove);
		if(newF.getParentFile().exists()) {
			newF.getParentFile().mkdirs();
		}
		try {
			FileUtils.moveFile(f, newF);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred moving the template into the template folder");
		}
		
		logger.info("Starting to synchronize templates with template directory");
		// pull from cloud
		ClusterUtil.pullProjectFolder(project, AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId));
		// move back the file
		try {
			FileUtils.moveFile(newF, f);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred moving the template into the template folder");
		}
		// write/update to properties file
		Map<String, String> templateDataMap = TemplateUtility.addTemplate(projectId, templateFile, templateName);
		// push to cloud
		ClusterUtil.pushProjectFolder(project, AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId));
		logger.info("Finished synchronizing templates with template directory");

		// returning back the updated template information which will contain all the 
		// template information with template name as key and file name as the value
		return new NounMetadata(templateDataMap, PixelDataType.MAP);
	}

}
