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
package prerna.reactor.automation.project;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns the git commit history for an automation project's workflow definition.
 *
 * <p>
 * Each entry contains the commit author, timestamp, full revision SHA, and
 * message recorded when the Automation graph or an owned developer-node source
 * was saved.
 *
 * <p>Pixel: {@code ListAutomationVersions(project=["<projectId>"])}
 */
public class ListAutomationVersionsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ListAutomationVersionsReactor.class);

	private static final int DEFAULT_LIMIT = 25;
	private static final int MAXIMUM_LIMIT = 100;

	public ListAutomationVersionsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.LIMIT.getKey() };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account in order to list automation versions",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException error = new SemossPixelException(noun);
			error.setContinueThreadOfExecution(false);
			throw error;
		}

		IProject project = AutomationProjectService.getViewableAutomationProject(user,
				this.keyValue.get(ReactorKeysEnum.PROJECT.getKey()));
		String projectId = project.getProjectId();

		int limit = getInt(ReactorKeysEnum.LIMIT.getKey(), DEFAULT_LIMIT);
		if (limit <= 0) {
			throw new IllegalArgumentException("limit must be a positive integer.");
		}
		limit = Math.min(limit, MAXIMUM_LIMIT);

		List<Map<String, Object>> commits = AutomationVersionService.listVersions(project, limit);

		classLogger.info("Returning {} version entries for automation project {}", commits.size(), projectId);
		return new NounMetadata(commits, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Returns the git commit history (author, time, revision hash, message) for each time "
				+ "SaveAutomation was called on the given automation project.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.PROJECT.getKey().equals(key)) {
			return "ID or alias of the automation project whose version history should be listed.";
		}
		if (ReactorKeysEnum.LIMIT.getKey().equals(key)) {
			return "Maximum number of versions to return. Defaults to " + DEFAULT_LIMIT + ", capped at "
					+ MAXIMUM_LIMIT + ".";
		}
		return super.getDescriptionForKey(key);
	}
}
