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
package prerna.io.connector.jira;

import java.util.Map;

import org.javatuples.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class JiraDeleteIssueReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraDeleteIssueReactor.class);

	private static final String PROJECT = "project";
	private static final String JIRAID = "jiraid";
	private static final String DELETE_SUBTASKS   = "deleteSubtasks";

	public JiraDeleteIssueReactor() {
		this.keysToGet = new String[] { PROJECT, JIRAID, DELETE_SUBTASKS };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String projectName = JiraUtils.validateProjectKey(this.keyValue.get(PROJECT));
			String jiraId = JiraUtils.validateIssueKey(this.keyValue.get(JIRAID));
			String deleteSubtasksRaw = this.keyValue.get(DELETE_SUBTASKS);
			boolean deleteSubtasks = "true".equalsIgnoreCase(deleteSubtasksRaw);
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();
			Map<String, Object> result = JiraHelper.deleteIssue(accessToken, baseUrl, projectName, jiraId, deleteSubtasks);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while deleting a Jira issue", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to delete a Jira issue", e);
			throw new SemossPixelException(
					"An error occurred while deleting the Jira issue. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Permanently deletes a Jira issue by project key and issue key.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(PROJECT)) {
			return "Required Jira project key in uppercase (for example, RTJ).";
		} else if (key.equals(JIRAID)) {
			return "Required Jira issue key in KEY-NUMBER format (for example, RTJ-123).";
		} else if (key.equals(DELETE_SUBTASKS)) {
			return "Optional boolean string ('true' or 'false') to delete subtasks with the parent issue. Defaults to false.";
		}
		return super.getDescriptionForKey(key);
	}
}