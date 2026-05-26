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

public class JiraCommentReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraCommentReactor.class);

	private static final String ACTION = "action";
	private static final String JIRAID = "jiraid";
	private static final String COMMENT_ID = "commentId";
	private static final String COMMENT = "comment";

	public JiraCommentReactor() {
		this.keysToGet = new String[] { ACTION, JIRAID, COMMENT_ID, COMMENT };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String action = this.keyValue.get(ACTION);
			if (action == null || action.trim().isEmpty()) {
				throw new SemossPixelException("The action parameter is required. Valid values are: get, add, edit, delete.");
			}
			action = action.trim().toLowerCase();

			String issueKey = JiraUtils.validateIssueKey(this.keyValue.get(JIRAID));
			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();

			switch (action) {
			case "get": {
				List<Map<String, Object>> result = JiraHelper.getComments(accessToken, baseUrl, issueKey);
				return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			case "add": {
				String comment = this.keyValue.get(COMMENT);
				if (comment == null || comment.trim().isEmpty()) {
					throw new SemossPixelException("Comment text is required for the add action.");
				}
				Map<String, Object> result = JiraHelper.addComment(accessToken, baseUrl, issueKey, comment);
				return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			case "edit": {
				String commentId = JiraUtils.validateNumericId(this.keyValue.get(COMMENT_ID), "commentId");
				String comment = this.keyValue.get(COMMENT);
				if (comment == null || comment.trim().isEmpty()) {
					throw new SemossPixelException("Comment text is required for the edit action.");
				}
				Map<String, Object> result = JiraHelper.editComment(accessToken, baseUrl, issueKey, commentId, comment);
				return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			case "delete": {
				String commentId = JiraUtils.validateNumericId(this.keyValue.get(COMMENT_ID), "commentId");
				Map<String, Object> result = JiraHelper.deleteComment(accessToken, baseUrl, issueKey, commentId);
				return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			default:
				throw new SemossPixelException("Invalid action '" + action + "'. Valid values are: get, add, edit, delete.");
			}
		} catch (SemossPixelException e) {
			classLogger.error("Error in JiraCommentReactor", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to execute JiraCommentReactor", e);
			throw new SemossPixelException(
					"An error occurred in JiraCommentReactor. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Manages comments on a Jira issue. Supports get, add, edit, and delete operations.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ACTION)) {
			return "Required operation to perform. Valid values: get - lists all comments (jiraid required). add - adds a new comment (jiraid, comment required). edit - updates an existing comment (jiraid, commentId, comment required). delete - removes a comment (jiraid, commentId required).";
		} else if (key.equals(JIRAID)) {
			return "Required Jira issue key in KEY-NUMBER format (for example, RTJ-123).";
		} else if (key.equals(COMMENT_ID)) {
			return "Numeric comment id from a previous get response. Required for edit and delete actions.";
		} else if (key.equals(COMMENT)) {
			return "Plain-text issue comment body. Required for add and edit actions.";
		}
		return super.getDescriptionForKey(key);
	}
}
