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

import java.util.List;
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

public class JiraWorklogReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraWorklogReactor.class);

	private static final String ACTION = "action";
	private static final String JIRAID = "jiraid";
	private static final String WORKLOG_ID = "worklogId";
	private static final String TIME_SPENT = "timeSpent";
	private static final String COMMENT = "comment";
	private static final String STARTED = "started";

	public JiraWorklogReactor() {
		this.keysToGet = new String[] { ACTION, JIRAID, WORKLOG_ID, TIME_SPENT, COMMENT, STARTED };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String action = this.keyValue.get(ACTION);
			if (action == null || action.trim().isEmpty()) {
				throw new SemossPixelException("The action parameter is required. Valid values are: get, add, update, delete.");
			}
			action = action.trim().toLowerCase();

			String issueKey = JiraUtils.validateIssueKey(this.keyValue.get(JIRAID));
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();

			switch (action) {
			case "get": {
				List<Map<String, Object>> result = JiraHelper.getWorklogs(accessToken, baseUrl, issueKey);
				return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			case "add": {
				String timeSpent = this.keyValue.get(TIME_SPENT);
				if (timeSpent == null || timeSpent.trim().isEmpty()) {
					throw new SemossPixelException("Time spent (timeSpent) is required for the add action.");
				}
				String comment = this.keyValue.get(COMMENT);
				String started = this.keyValue.get(STARTED);
				Map<String, Object> result = JiraHelper.logWork(accessToken, baseUrl, issueKey, timeSpent, comment, started);
				return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			case "update": {
				String worklogId = JiraUtils.validateNumericId(this.keyValue.get(WORKLOG_ID), "worklogId");
				String timeSpent = this.keyValue.get(TIME_SPENT);
				if (timeSpent == null || timeSpent.trim().isEmpty()) {
					throw new SemossPixelException("Time spent (timeSpent) is required for the update action.");
				}
				String comment = this.keyValue.get(COMMENT);
				String started = this.keyValue.get(STARTED);
				Map<String, Object> result = JiraHelper.updateWorklog(accessToken, baseUrl, issueKey, worklogId,
						timeSpent, comment, started);
				return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			case "delete": {
				String worklogId = JiraUtils.validateNumericId(this.keyValue.get(WORKLOG_ID), "worklogId");
				Map<String, Object> result = JiraHelper.deleteWorklog(accessToken, baseUrl, issueKey, worklogId);
				return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			default:
				throw new SemossPixelException("Invalid action '" + action + "'. Valid values are: get, add, update, delete.");
			}
		} catch (SemossPixelException e) {
			classLogger.error("Error in JiraWorklogReactor", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to execute JiraWorklogReactor", e);
			throw new SemossPixelException(
					"An error occurred in JiraWorklogReactor. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Manages worklog entries on a Jira issue. Supports get, add, update, and delete operations.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ACTION)) {
			return "Required operation to perform. Valid values: get - lists all worklogs (jiraid required). add - logs new work (jiraid, timeSpent required; comment, started optional). update - updates a worklog entry (jiraid, worklogId, timeSpent required; comment, started optional). delete - removes a worklog entry (jiraid, worklogId required).";
		} else if (key.equals(JIRAID)) {
			return "Required Jira issue key in KEY-NUMBER format (for example, RTJ-123).";
		} else if (key.equals(WORKLOG_ID)) {
			return "Numeric worklog id from a previous get response. Required for update and delete actions.";
		} else if (key.equals(TIME_SPENT)) {
			return "Time in Jira notation (for example, 2h 30m, 1d, 45m). Supports weeks (w), days (d), hours (h), and minutes (m). Required for add and update actions.";
		} else if (key.equals(COMMENT)) {
			return "Optional plain-text worklog note describing the work performed. This is not an issue comment.";
		} else if (key.equals(STARTED)) {
			return "Optional start datetime in ISO 8601 format (for example, 2026-04-07T09:00:00.000+0000). Defaults to now for add action.";
		}
		return super.getDescriptionForKey(key);
	}
}
