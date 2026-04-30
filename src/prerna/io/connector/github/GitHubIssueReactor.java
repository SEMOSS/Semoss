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
package prerna.io.connector.github;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Manages GitHub issue listing, retrieval, mutation, comments, and search.
 */
public class GitHubIssueReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GitHubIssueReactor.class);

	private static final String ACTION = "action";
	private static final String OWNER = "owner";
	private static final String REPO = "repo";
	private static final String ISSUE_NUMBER = "issueNumber";
	private static final String TITLE = "title";
	private static final String BODY = "body";
	private static final String STATE = "state";
	private static final String ASSIGNEES = "assignees";
	private static final String LABELS = "labels";
	private static final String COMMENT_ID = "commentId";
	private static final String QUERY = "query";
	private static final String PAGE = "page";
	private static final String PER_PAGE = "perPage";

	/**
	 * Configures supported keys for issue actions.
	 */
	public GitHubIssueReactor() {
		this.keysToGet = new String[] { ACTION, OWNER, REPO, ISSUE_NUMBER, TITLE, BODY, STATE, ASSIGNEES, LABELS,
				COMMENT_ID, QUERY, PAGE, PER_PAGE };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	/**
	 * Executes the issue action specified by {@code action}.
	 *
	 * @return issue operation result metadata
	 */
	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String action = null;
		try {
			String accessToken = GitHubUtils.getGitHubToken(this.insight.getUser());
			action = this.keyValue.get(ACTION);
			if (action == null || action.trim().isEmpty()) {
				throw new SemossPixelException(ACTION + " is required.");
			}
			action = action.trim().toLowerCase();

			int page = 1;
			String pageValue = this.keyValue.get(PAGE);
			if (pageValue != null && !pageValue.trim().isEmpty()) {
				try {
					page = Integer.parseInt(pageValue.trim());
				} catch (NumberFormatException e) {
					throw new SemossPixelException("Invalid number for pagination: '" + pageValue.trim() + "'.", e);
				}
			}

			int perPage = 30;
			String perPageValue = this.keyValue.get(PER_PAGE);
			if (perPageValue != null && !perPageValue.trim().isEmpty()) {
				try {
					perPage = Integer.parseInt(perPageValue.trim());
				} catch (NumberFormatException e) {
					throw new SemossPixelException("Invalid number for pagination: '" + perPageValue.trim() + "'.", e);
				}
			}

			String owner = this.keyValue.get(OWNER);
			if (owner != null) {
				owner = owner.trim();
				if (owner.isEmpty()) {
					owner = null;
				}
			}

			String repo = this.keyValue.get(REPO);
			if (repo != null) {
				repo = repo.trim();
				if (repo.isEmpty()) {
					repo = null;
				}
			}

			List<String> assignees = null;
			String assigneesValue = this.keyValue.get(ASSIGNEES);
			if (assigneesValue != null && !assigneesValue.trim().isEmpty()) {
				assignees = new ArrayList<String>();
				for (String value : assigneesValue.split(",")) {
					if (value != null && !value.trim().isEmpty()) {
						assignees.add(value.trim());
					}
				}
				if (assignees.isEmpty()) {
					assignees = null;
				}
			}

			List<String> labels = null;
			String labelsValue = this.keyValue.get(LABELS);
			if (labelsValue != null && !labelsValue.trim().isEmpty()) {
				labels = new ArrayList<String>();
				for (String value : labelsValue.split(",")) {
					if (value != null && !value.trim().isEmpty()) {
						labels.add(value.trim());
					}
				}
				if (labels.isEmpty()) {
					labels = null;
				}
			}

			Object result;
			switch (action) {
			case "list": {
				if (owner == null) {
					throw new SemossPixelException(OWNER + " is required.");
				}
				if (repo == null) {
					throw new SemossPixelException(REPO + " is required.");
				}
				String state = this.keyValue.get(STATE);
				if (state != null) {
					state = state.trim();
					if (state.isEmpty()) {
						state = null;
					}
				}
				result = GitHubHelper.listIssues(accessToken, owner, repo, state, page, perPage);
				break;
			}
			case "get": {
				if (owner == null) {
					throw new SemossPixelException(OWNER + " is required.");
				}
				if (repo == null) {
					throw new SemossPixelException(REPO + " is required.");
				}
				String issueNumberValue = this.keyValue.get(ISSUE_NUMBER);
				if (issueNumberValue == null || issueNumberValue.trim().isEmpty()) {
					throw new SemossPixelException(ISSUE_NUMBER + " is required.");
				}
				int issueNumber;
				try {
					issueNumber = Integer.parseInt(issueNumberValue.trim());
				} catch (NumberFormatException e) {
					throw new SemossPixelException(
							ISSUE_NUMBER + " must be a number, got: '" + issueNumberValue.trim() + "'.", e);
				}
				if (issueNumber <= 0) {
					throw new SemossPixelException(ISSUE_NUMBER + " must be a positive number.");
				}
				result = GitHubHelper.getIssue(accessToken, owner, repo, issueNumber);
				break;
			}
			case "get_comments": {
				if (owner == null) {
					throw new SemossPixelException(OWNER + " is required.");
				}
				if (repo == null) {
					throw new SemossPixelException(REPO + " is required.");
				}
				String issueNumberValue = this.keyValue.get(ISSUE_NUMBER);
				if (issueNumberValue == null || issueNumberValue.trim().isEmpty()) {
					throw new SemossPixelException(ISSUE_NUMBER + " is required.");
				}
				int issueNumber;
				try {
					issueNumber = Integer.parseInt(issueNumberValue.trim());
				} catch (NumberFormatException e) {
					throw new SemossPixelException(
							ISSUE_NUMBER + " must be a number, got: '" + issueNumberValue.trim() + "'.", e);
				}
				if (issueNumber <= 0) {
					throw new SemossPixelException(ISSUE_NUMBER + " must be a positive number.");
				}
				result = GitHubHelper.getIssueComments(accessToken, owner, repo, issueNumber, page, perPage);
				break;
			}
			case "create": {
				if (owner == null) {
					throw new SemossPixelException(OWNER + " is required.");
				}
				if (repo == null) {
					throw new SemossPixelException(REPO + " is required.");
				}
				String title = this.keyValue.get(TITLE);
				if (title == null || title.trim().isEmpty()) {
					throw new SemossPixelException(TITLE + " is required.");
				}
				String body = this.keyValue.get(BODY);
				if (body != null) {
					body = body.trim();
					if (body.isEmpty()) {
						body = null;
					}
				}
				result = GitHubHelper.createIssue(accessToken, owner, repo, title.trim(), body, assignees, labels);
				break;
			}
			case "update": {
				if (owner == null) {
					throw new SemossPixelException(OWNER + " is required.");
				}
				if (repo == null) {
					throw new SemossPixelException(REPO + " is required.");
				}
				String issueNumberValue = this.keyValue.get(ISSUE_NUMBER);
				if (issueNumberValue == null || issueNumberValue.trim().isEmpty()) {
					throw new SemossPixelException(ISSUE_NUMBER + " is required.");
				}
				int issueNumber;
				try {
					issueNumber = Integer.parseInt(issueNumberValue.trim());
				} catch (NumberFormatException e) {
					throw new SemossPixelException(
							ISSUE_NUMBER + " must be a number, got: '" + issueNumberValue.trim() + "'.", e);
				}
				if (issueNumber <= 0) {
					throw new SemossPixelException(ISSUE_NUMBER + " must be a positive number.");
				}
				String title = this.keyValue.get(TITLE);
				if (title != null) {
					title = title.trim();
					if (title.isEmpty()) {
						title = null;
					}
				}
				String body = this.keyValue.get(BODY);
				if (body != null) {
					body = body.trim();
					if (body.isEmpty()) {
						body = null;
					}
				}
				String state = this.keyValue.get(STATE);
				if (state != null) {
					state = state.trim();
					if (state.isEmpty()) {
						state = null;
					}
				}
				result = GitHubHelper.updateIssue(accessToken, owner, repo, issueNumber, title, body, state, assignees,
						labels);
				break;
			}
			case "add_comment": {
				if (owner == null) {
					throw new SemossPixelException(OWNER + " is required.");
				}
				if (repo == null) {
					throw new SemossPixelException(REPO + " is required.");
				}
				String issueNumberValue = this.keyValue.get(ISSUE_NUMBER);
				if (issueNumberValue == null || issueNumberValue.trim().isEmpty()) {
					throw new SemossPixelException(ISSUE_NUMBER + " is required.");
				}
				int issueNumber;
				try {
					issueNumber = Integer.parseInt(issueNumberValue.trim());
				} catch (NumberFormatException e) {
					throw new SemossPixelException(
							ISSUE_NUMBER + " must be a number, got: '" + issueNumberValue.trim() + "'.", e);
				}
				if (issueNumber <= 0) {
					throw new SemossPixelException(ISSUE_NUMBER + " must be a positive number.");
				}
				String body = this.keyValue.get(BODY);
				if (body == null || body.trim().isEmpty()) {
					throw new SemossPixelException(BODY + " is required.");
				}
				result = GitHubHelper.addIssueComment(accessToken, owner, repo, issueNumber, body.trim());
				break;
			}
			case "edit_comment": {
				if (owner == null) {
					throw new SemossPixelException(OWNER + " is required.");
				}
				if (repo == null) {
					throw new SemossPixelException(REPO + " is required.");
				}
				String commentIdValue = this.keyValue.get(COMMENT_ID);
				if (commentIdValue == null || commentIdValue.trim().isEmpty()) {
					throw new SemossPixelException(COMMENT_ID + " is required.");
				}
				long commentId;
				try {
					commentId = Long.parseLong(commentIdValue.trim());
				} catch (NumberFormatException e) {
					throw new SemossPixelException(
							COMMENT_ID + " must be a number, got: '" + commentIdValue.trim() + "'.", e);
				}
				if (commentId <= 0) {
					throw new SemossPixelException(COMMENT_ID + " must be a positive number.");
				}
				String body = this.keyValue.get(BODY);
				if (body == null || body.trim().isEmpty()) {
					throw new SemossPixelException(BODY + " is required.");
				}
				result = GitHubHelper.editIssueComment(accessToken, owner, repo, commentId, body.trim());
				break;
			}
			case "delete_comment": {
				if (owner == null) {
					throw new SemossPixelException(OWNER + " is required.");
				}
				if (repo == null) {
					throw new SemossPixelException(REPO + " is required.");
				}
				String commentIdValue = this.keyValue.get(COMMENT_ID);
				if (commentIdValue == null || commentIdValue.trim().isEmpty()) {
					throw new SemossPixelException(COMMENT_ID + " is required.");
				}
				long commentId;
				try {
					commentId = Long.parseLong(commentIdValue.trim());
				} catch (NumberFormatException e) {
					throw new SemossPixelException(
							COMMENT_ID + " must be a number, got: '" + commentIdValue.trim() + "'.", e);
				}
				if (commentId <= 0) {
					throw new SemossPixelException(COMMENT_ID + " must be a positive number.");
				}
				result = GitHubHelper.deleteIssueComment(accessToken, owner, repo, commentId);
				break;
			}
			case "search": {
				String query = this.keyValue.get(QUERY);
				if (query == null || query.trim().isEmpty()) {
					throw new SemossPixelException(QUERY + " is required.");
				}
				result = GitHubHelper.searchIssues(accessToken, query.trim(), page, perPage);
				break;
			}
			case "list_labels": {
				if (owner == null) {
					throw new SemossPixelException(OWNER + " is required.");
				}
				if (repo == null) {
					throw new SemossPixelException(REPO + " is required.");
				}
				result = GitHubHelper.listLabels(accessToken, owner, repo, page, perPage);
				break;
			}
			case "list_collaborators": {
				if (owner == null) {
					throw new SemossPixelException(OWNER + " is required.");
				}
				if (repo == null) {
					throw new SemossPixelException(REPO + " is required.");
				}
				result = GitHubHelper.listCollaborators(accessToken, owner, repo, page, perPage);
				break;
			}
			default:
				throw new SemossPixelException("Invalid action '" + action
						+ "'. Valid values are: list, get, get_comments, create, update, add_comment, edit_comment, delete_comment, search, list_labels, list_collaborators.");
			}

			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Failed to execute GitHubIssueReactor for action '{}'.", action, e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Unexpected error in GitHubIssueReactor for action '{}'.", action, e);
			throw new SemossPixelException("An error occurred in GitHubIssueReactor. Error message: " + e.getMessage(),
					e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getReactorDescription() {
		return "Lists, retrieves, creates, updates, comments on, and searches GitHub issues, and can list labels and collaborators.";
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected String getDescriptionForKey(String key) {
		if (ACTION.equals(key)) {
			return "Required action: list, get, get_comments, create, update, add_comment, edit_comment, delete_comment, search, list_labels, or list_collaborators.";
		} else if (OWNER.equals(key)) {
			return "Repository owner. Required for all actions except search.";
		} else if (REPO.equals(key)) {
			return "Repository name. Required for all actions except search.";
		} else if (ISSUE_NUMBER.equals(key)) {
			return "Issue number. Required for get, get_comments, update, and add_comment.";
		} else if (TITLE.equals(key)) {
			return "Issue title. Required for create. Optional for update.";
		} else if (BODY.equals(key)) {
			return "Issue body or comment text. Required for add_comment and edit_comment. Optional for create and update.";
		} else if (STATE.equals(key)) {
			return "Issue state filter (list) or update value (update). Valid values: open, closed.";
		} else if (ASSIGNEES.equals(key)) {
			return "Comma-separated GitHub usernames. Used by create and update.";
		} else if (LABELS.equals(key)) {
			return "Comma-separated labels. Used by create and update.";
		} else if (COMMENT_ID.equals(key)) {
			return "Comment ID. Required for edit_comment and delete_comment.";
		} else if (QUERY.equals(key)) {
			return "GitHub issue search query. Required for search.";
		} else if (PAGE.equals(key)) {
			return "Pagination page number. Defaults to 1.";
		} else if (PER_PAGE.equals(key)) {
			return "Pagination size per page. Defaults to 30 and is capped at 100.";
		}
		return super.getDescriptionForKey(key);
	}
}
