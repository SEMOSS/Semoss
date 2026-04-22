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

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import org.javatuples.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.sablecc2.om.execptions.SemossPixelException;

public class JiraUtils {

	private static final Logger classLogger = LogManager.getLogger(JiraUtils.class);

	private static final String JIRA_API_BASE = "https://api.atlassian.com/ex/jira/";

	private static final Pattern ISSUE_KEY_PATTERN = Pattern.compile("^[A-Z][A-Z0-9_]+-\\d+$");
	private static final Pattern PROJECT_KEY_PATTERN = Pattern.compile("^[A-Z][A-Z0-9_]+$");
	private static final Pattern NUMERIC_ID_PATTERN = Pattern.compile("^\\d+$");

	/**
	 * Validates and trims a Jira issue key (e.g. {@code RTJ-123}).
	 *
	 * @param issueKey raw issue key input
	 * @return trimmed issue key
	 * @throws SemossPixelException if null, empty, or malformed
	 */
	public static String validateIssueKey(String issueKey) {
		if (issueKey == null || issueKey.trim().isEmpty()) {
			throw new SemossPixelException("Issue key (jiraid) is required and cannot be empty.");
		}
		String trimmed = issueKey.trim().toUpperCase();
		if (!ISSUE_KEY_PATTERN.matcher(trimmed).matches()) {
			throw new SemossPixelException(
					"Invalid issue key format: '" + issueKey.trim() + "'. Expected format like RTJ-123.");
		}
		return trimmed;
	}

	/**
	 * Validates and trims a Jira project key (e.g. {@code RTJ}).
	 *
	 * @param projectKey raw project key input
	 * @return trimmed project key
	 * @throws SemossPixelException if null, empty, or malformed
	 */
	public static String validateProjectKey(String projectKey) {
		if (projectKey == null || projectKey.trim().isEmpty()) {
			throw new SemossPixelException("Project key (project) is required and cannot be empty.");
		}
		String trimmed = projectKey.trim().toUpperCase();
		if (!PROJECT_KEY_PATTERN.matcher(trimmed).matches()) {
			throw new SemossPixelException(
					"Invalid project key format: '" + projectKey.trim() + "'. Expected uppercase format like RTJ.");
		}
		return trimmed;
	}

	/**
	 * Validates and trims a numeric Jira ID (comment, worklog, attachment, link).
	 *
	 * @param id        raw ID input
	 * @param fieldName display name for error messages (e.g. {@code "commentId"})
	 * @return trimmed numeric ID
	 * @throws SemossPixelException if null, empty, or non-numeric
	 */
	public static String validateNumericId(String id, String fieldName) {
		if (id == null || id.trim().isEmpty()) {
			throw new SemossPixelException(fieldName + " is required and cannot be empty.");
		}
		String trimmed = id.trim();
		if (!NUMERIC_ID_PATTERN.matcher(trimmed).matches()) {
			throw new SemossPixelException(
					"Invalid " + fieldName + ": '" + trimmed + "'. Must be a numeric ID.");
		}
		return trimmed;
	}

	/**
	 * Retrieves both the Jira access token and base URL for the current user in a
	 * single token lookup.
	 *
	 * @param user current session user
	 * @return a {@link Pair} containing the access token and base URL
	 */
	public static Pair<String, String> getJiraCredentials(User user) {
		AccessToken jiraToken = getJiraToken(user);
		String accessToken = jiraToken.getAccess_token();
		String cloudId = jiraToken.getId();
		if (cloudId == null || cloudId.trim().isEmpty()) {
			classLogger.error("Jira Cloud ID not found on token.");
			throw new SemossPixelException("Jira Cloud ID not found on token. Please reconnect your Jira account.");
		}
		return new Pair<>(accessToken, JIRA_API_BASE + URLEncoder.encode(cloudId.trim(), StandardCharsets.UTF_8));
	}

	/**
	 * Gets the Jira token object for a user with consistent validation.
	 *
	 * @param user current session user
	 * @return Jira {@link AccessToken}
	 */
	private static AccessToken getJiraToken(User user) {
		if (user == null) {
			classLogger.error("User session is null.");
			throw new SemossPixelException("User session is null. Please log in with Jira.");
		}
		AccessToken jiraToken = user.getAccessToken(AuthProvider.JIRA);
        if (jiraToken == null) {
            classLogger.error("No Jira token found for user.");
            throw new SemossPixelException("No Jira token found for user. Please connect your Jira account.");
        }
        return jiraToken;
	}
}
