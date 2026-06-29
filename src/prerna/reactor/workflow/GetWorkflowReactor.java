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
package prerna.reactor.workflow;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

/**
 * GetWorkflow(project=["<appId>"])
 *
 * Reads the workflow graph JSON from
 * project/<appId>/app_root/version/assets/portals/workflow.json
 * Returns the raw JSON string.
 */
public class GetWorkflowReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetWorkflowReactor.class);
	private static final String WORKFLOW_FILE = "workflow.json";

	public GetWorkflowReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String projectId = this.keyValue.get(this.keysToGet[0]);

		if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			NounMetadata noun = new NounMetadata(
				"User does not have permission to view project " + projectId,
				PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException ex = new SemossPixelException(noun);
			ex.setContinueThreadOfExecution(false);
			throw ex;
		}

		String portalsFolder = AssetUtility.getProjectPortalsFolder(projectId);
		File workflowFile = new File(portalsFolder + "/" + WORKFLOW_FILE);

		if (!workflowFile.exists()) {
			// Return empty workflow skeleton if none saved yet
			return new NounMetadata(
				"{\"version\":\"1.0\",\"nodes\":[],\"edges\":[]}",
				PixelDataType.CONST_STRING);
		}

		try {
			String json = FileUtils.readFileToString(workflowFile, StandardCharsets.UTF_8);
			return new NounMetadata(json, PixelDataType.CONST_STRING);
		} catch (IOException e) {
			classLogger.error("Failed to read workflow.json for project " + projectId, e);
			NounMetadata noun = new NounMetadata(
				"Failed to read workflow: " + e.getMessage(),
				PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException ex = new SemossPixelException(noun);
			ex.setContinueThreadOfExecution(false);
			throw ex;
		}
	}
}
