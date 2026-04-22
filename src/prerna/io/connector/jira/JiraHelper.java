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

import java.io.File;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.mime.FileBody;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.ToNumberPolicy;

import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.security.HttpHelperUtility;

public final class JiraHelper {

	private static final Logger classLogger = LogManager.getLogger(JiraHelper.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	// Jira REST paths
	private static final String API_PATH_ISSUE = "/rest/api/3/issue";
	private static final String API_PATH_PROJECT = "/rest/api/3/project";
	private static final String API_PATH_CREATEMETA = "/rest/api/3/issue/createmeta";
	private static final String API_PATH_SEARCH = "/rest/api/3/search/jql";
	private static final String API_PATH_ISSUETYPE = "/rest/api/3/issuetype";
	private static final String API_PATH_ISSUETYPE_PROJECT = "/rest/api/3/issuetype/project";
	private static final String API_PATH_ISSUELINK = "/rest/api/3/issueLink";
	private static final String API_PATH_ISSUELINKTYPE = "/rest/api/3/issueLinkType";
	private static final String API_PATH_ATTACHMENT = "/rest/api/3/attachment";
	private static final String API_PATH_PRIORITY = "/rest/api/3/priority";
	private static final String API_PATH_ASSIGNABLE_USERS = "/rest/api/3/user/assignable/search";
	private static final String API_SUFFIX_COMMENT = "/comment";
	private static final String API_SUFFIX_TRANSITIONS = "/transitions";
	private static final String API_SUFFIX_WORKLOG = "/worklog";
	private static final String API_SUFFIX_ASSIGNEE = "/assignee";
	private static final String API_SUFFIX_ATTACHMENTS = "/attachments";
	private static final String API_SUFFIX_EDITMETA = "/editmeta";

	// Jira entity field keys
	private static final String FIELD_ACCOUNT_ID = "accountId";
	private static final String FIELD_ASSIGNEE = "assignee";
	private static final String FIELD_ATTACHMENT_ID = "attachmentId";
	private static final String FIELD_AUTHOR = "author";
	private static final String FIELD_BODY = "body";
	private static final String FIELD_COMMENT = "comment";
	private static final String FIELD_COMMENT_ID = "commentId";
	private static final String FIELD_COMMENTS = "comments";
	private static final String FIELD_CONTENT_URL = "contentUrl";
	private static final String FIELD_CREATED = "created";
	private static final String FIELD_DESCRIPTION = "description";
	private static final String FIELD_DISPLAY_NAME = "displayName";
	private static final String FIELD_ID = "id";
	private static final String FIELD_ISSUE_TYPE_ID = "issuetypeid";
	private static final String FIELD_JIRA_ID = "jiraid";
	private static final String FIELD_KEY = "key";
	private static final String FIELD_LINK_TYPE = "linkType";
	private static final String FIELD_NAME = "name";
	private static final String FIELD_PROJECT = "project";
	private static final String FIELD_TO = "to";
	private static final String FIELD_WORKLOG_ID = "worklogId";

	private static final String FIELD_ENVIRONMENT = "environment";

	// Search and workflow field keys
	private static final String FIELD_FIELDS = "fields";
	private static final String FIELD_ISSUES = "issues";
	private static final String FIELD_JQL = "jql";
	private static final String FIELD_MAX_RESULTS = "maxResults";
	private static final String FIELD_NEXT_PAGE_TOKEN = "nextPageToken";
	private static final String FIELD_SUCCESS = "success";
	private static final String FIELD_TRANSITIONS = "transitions";

	// Atlassian Document Format (ADF) type keys
	private static final String ADF_CONTENT = "content";
	private static final String ADF_TEXT = "text";
	private static final String ADF_TYPE = "type";

	// Default display values
	private static final String DEFAULT_UNASSIGNED = "Unassigned";

	private JiraHelper() {

	}

	/**
	 * Retrieves all Jira projects accessible to the authenticated user.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @return list of maps, each containing {@code id}, {@code project},
	 *         {@code name}, {@code projectTypeKey}, and {@code lead}
	 */
	public static List<Map<String, Object>> getAllProjects(String accessToken, String baseUrl) {
		final String projectTypeKey = "projectTypeKey";
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);
			String response = HttpHelperUtility.getRequest(baseUrl + API_PATH_PROJECT, headers, null, null, null);

			JsonArray projectArray = JsonParser.parseString(response).getAsJsonArray();
			List<Map<String, Object>> projects = new ArrayList<>();
			for (JsonElement el : projectArray) {
				JsonObject p = el.getAsJsonObject();
				Map<String, Object> proj = new HashMap<>();
				proj.put(FIELD_ID, getText(p, FIELD_ID));
				proj.put(FIELD_PROJECT, getText(p, FIELD_KEY));
				proj.put(FIELD_NAME, getText(p, FIELD_NAME));
				proj.put(projectTypeKey, getText(p, projectTypeKey));
				JsonElement leadEl = p.get("lead");
				proj.put("lead",
						leadEl != null && leadEl.isJsonObject() ? getText(leadEl.getAsJsonObject(), FIELD_DISPLAY_NAME)
								: "");
				projects.add(proj);
			}
			return projects;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in getAllProjects: {}", e.getMessage(), e);
			throw new SemossPixelException("Failed to retrieve Jira projects. Error: " + e.getMessage());
		}
	}

	/**
	 * Retrieves issue types, optionally scoped to a specific project.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param projectKey  optional project key to scope issue types; pass
	 *                    {@code null} for all instance issue types
	 * @return list of maps, each containing {@code issuetypeid}, {@code name}, and
	 *         {@code subtask}
	 */
	public static List<Map<String, Object>> getIssueTypes(String accessToken, String baseUrl, String projectKey) {
		final String subtask = "subtask";
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);
			String url;

			if (projectKey != null && !projectKey.trim().isEmpty()) {
				String projectId = resolveProjectId(baseUrl, accessToken, projectKey);
				url = baseUrl + API_PATH_ISSUETYPE_PROJECT + "?projectId="
						+ URLEncoder.encode(projectId, StandardCharsets.UTF_8);
			} else {
				url = baseUrl + API_PATH_ISSUETYPE;
			}

			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
			JsonArray typeArray = JsonParser.parseString(response).getAsJsonArray();

			List<Map<String, Object>> types = new ArrayList<>();
			for (JsonElement el : typeArray) {
				JsonObject t = el.getAsJsonObject();
				Map<String, Object> type = new HashMap<>();
				type.put(FIELD_ISSUE_TYPE_ID, getText(t, FIELD_ID));
				type.put(FIELD_NAME, getText(t, FIELD_NAME));
				type.put(subtask, getBoolean(t, subtask));
				types.add(type);
			}
			return types;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in getIssueTypes for '{}': {}", projectKey, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to retrieve issue types for project '" + projectKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Retrieves users assignable to a Jira project.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param projectKey  project key to search assignable users for
	 * @param query       optional search string to filter users by name or email
	 * @return list of maps, each containing {@code assignee}, {@code displayName},
	 *         and {@code emailAddress}
	 */
	public static List<Map<String, Object>> getAssignableUsers(String accessToken, String baseUrl, String projectKey,
			String query) {
		final String emailAddress = "emailAddress";
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);
			StringBuilder url = new StringBuilder(baseUrl).append(API_PATH_ASSIGNABLE_USERS).append("?project=")
					.append(URLEncoder.encode(projectKey, StandardCharsets.UTF_8));
			if (query != null && !query.trim().isEmpty()) {
				url.append("&query=").append(URLEncoder.encode(query, StandardCharsets.UTF_8));
			}
			String response = HttpHelperUtility.getRequest(url.toString(), headers, null, null, null);

			JsonArray userArray = JsonParser.parseString(response).getAsJsonArray();
			List<Map<String, Object>> users = new ArrayList<>();
			for (JsonElement el : userArray) {
				JsonObject u = el.getAsJsonObject();
				Map<String, Object> userMap = new HashMap<>();
				userMap.put(FIELD_ASSIGNEE, getText(u, FIELD_ACCOUNT_ID));
				userMap.put(FIELD_DISPLAY_NAME, getText(u, FIELD_DISPLAY_NAME));
				userMap.put(emailAddress, getText(u, emailAddress));
				users.add(userMap);
			}
			return users;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in getAssignableUsers for '{}': {}", projectKey, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to retrieve assignable users for project '" + projectKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Retrieves all available priority levels from Jira.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @return list of maps, each containing {@code id} and {@code name}
	 */
	public static List<Map<String, Object>> getPriorities(String accessToken, String baseUrl) {
		final String values = "values";
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);
			String response = HttpHelperUtility.getRequest(baseUrl + API_PATH_PRIORITY, headers, null, null, null);

			JsonElement rootEl = JsonParser.parseString(response);
			JsonArray priorityArray;
			if (rootEl.isJsonArray()) {
				priorityArray = rootEl.getAsJsonArray();
			} else if (rootEl.isJsonObject() && rootEl.getAsJsonObject().has(values)) {
				JsonElement valEl = rootEl.getAsJsonObject().get(values);
				priorityArray = valEl.isJsonArray() ? valEl.getAsJsonArray() : new JsonArray();
			} else {
				priorityArray = new JsonArray();
			}

			List<Map<String, Object>> priorities = new ArrayList<>();
			for (JsonElement el : priorityArray) {
				JsonObject p = el.getAsJsonObject();
				Map<String, Object> priority = new HashMap<>();
				priority.put(FIELD_ID, getText(p, FIELD_ID));
				priority.put(FIELD_NAME, getText(p, FIELD_NAME));
				priorities.add(priority);
			}
			return priorities;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in getPriorities: {}", e.getMessage(), e);
			throw new SemossPixelException("Failed to retrieve Jira priorities. Error: " + e.getMessage());
		}
	}

	/**
	 * Searches Jira issues using a raw JQL query string.
	 *
	 * @param accessToken   Jira OAuth access token
	 * @param baseUrl       Jira API base URL including cloud ID
	 * @param jql           raw JQL query string
	 * @param nextPageToken optional pagination token from a previous response
	 * @param maxResults    maximum number of results per page
	 * @return map containing paginated issue list
	 */
	public static Map<String, Object> searchIssues(String accessToken, String baseUrl, String jql, String nextPageToken,
			int maxResults) {
		try {
			return executeJqlSearch(accessToken, baseUrl, jql, nextPageToken, maxResults);
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in searchIssues: {}", e.getMessage(), e);
			throw new SemossPixelException("Failed to search Jira issues. Error: " + e.getMessage());
		}
	}

	/**
	 * Fetches full details of a single Jira issue by its key.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param issueKey    issue key (for example, {@code PROJECT-123})
	 * @return map containing {@code id}, {@code jiraid}, and {@code fields}
	 */
	public static Map<String, Object> readIssue(String accessToken, String baseUrl, String issueKey) {
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);
			String response = HttpHelperUtility.getRequest(
					baseUrl + API_PATH_ISSUE + "/" + URLEncoder.encode(issueKey, StandardCharsets.UTF_8), headers, null,
					null, null);

			JsonObject root = JsonParser.parseString(response).getAsJsonObject();
			Map<String, Object> issue = new HashMap<>();
			issue.put(FIELD_ID, getText(root, FIELD_ID));
			issue.put(FIELD_JIRA_ID, getText(root, FIELD_KEY));
			JsonElement fieldsEl = root.get(FIELD_FIELDS);
			if (fieldsEl != null && fieldsEl.isJsonObject()) {
				issue.put(FIELD_FIELDS, GSON.fromJson(fieldsEl, Map.class));
			}
			return issue;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in readIssue '{}': {}", issueKey, e.getMessage(), e);
			throw new SemossPixelException("Failed to retrieve issue '" + issueKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Deletes a Jira issue.
	 *
	 * @param accessToken    Jira OAuth access token
	 * @param baseUrl        Jira API base URL including cloud ID
	 * @param projectKey     project key the issue must belong to
	 * @param jiraId         issue key to delete
	 * @param deleteSubtasks whether to delete subtasks when deleting the issue
	 * @return map containing {@code success}
	 */
	public static Map<String, Object> deleteIssue(String accessToken, String baseUrl, String projectKey, String jiraId,
			boolean deleteSubtasks) {
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);
			String issueUrl = baseUrl + API_PATH_ISSUE + "/" + URLEncoder.encode(jiraId, StandardCharsets.UTF_8);

			String issueResponse = HttpHelperUtility.getRequest(issueUrl, headers, null, null, null);
			JsonObject root = JsonParser.parseString(issueResponse).getAsJsonObject();
			String actualProjectKey = "";
			JsonElement fieldsEl = root.get(FIELD_FIELDS);
			if (fieldsEl != null && fieldsEl.isJsonObject()) {
				JsonElement projectEl = fieldsEl.getAsJsonObject().get(FIELD_PROJECT);
				if (projectEl != null && projectEl.isJsonObject()) {
					actualProjectKey = getText(projectEl.getAsJsonObject(), FIELD_KEY);
				}
			}

			if (!projectKey.equalsIgnoreCase(actualProjectKey)) {
				throw new SemossPixelException("Issue " + jiraId + " does not belong to project " + projectKey);
			}

			String deleteUrl = deleteSubtasks ? issueUrl + "?deleteSubtasks=true" : issueUrl;
			HttpHelperUtility.deleteRequestStringBody(deleteUrl, headers, null, null, null);

			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_SUCCESS, true);
			return result;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in deleteIssue '{}': {}", jiraId, e.getMessage(), e);
			throw new SemossPixelException("Failed to delete issue '" + jiraId + "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Returns the available workflow transitions for a Jira issue.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param issueKey    issue key (for example, {@code PROJECT-123})
	 * @return list of maps, each containing {@code id}, {@code name}, and
	 *         {@code toStatus}
	 */
	public static List<Map<String, Object>> getTransitions(String accessToken, String baseUrl, String issueKey) {
		final String toStatus = "toStatus";
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);
			String url = baseUrl + API_PATH_ISSUE + "/" + URLEncoder.encode(issueKey, StandardCharsets.UTF_8)
					+ API_SUFFIX_TRANSITIONS;
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);

			JsonObject root = JsonParser.parseString(response).getAsJsonObject();
			List<Map<String, Object>> transitionList = new ArrayList<>();
			JsonElement transEl = root.get(FIELD_TRANSITIONS);
			if (transEl != null && transEl.isJsonArray()) {
				for (JsonElement el : transEl.getAsJsonArray()) {
					JsonObject t = el.getAsJsonObject();
					Map<String, Object> transition = new HashMap<>();
					transition.put(FIELD_ID, getText(t, FIELD_ID));
					transition.put(FIELD_NAME, getText(t, FIELD_NAME));
					JsonElement toEl = t.get(FIELD_TO);
					transition.put(toStatus,
							toEl != null && toEl.isJsonObject() ? getText(toEl.getAsJsonObject(), FIELD_NAME) : "");
					transitionList.add(transition);
				}
			}
			return transitionList;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in getTransitions for '{}': {}", issueKey, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to get transitions for issue '" + issueKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Retrieves all comments on a Jira issue.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param issueKey    issue key (for example, {@code PROJECT-123})
	 * @return list of maps, each containing {@code commentId}, {@code author},
	 *         {@code created}, {@code updated}, and {@code body}
	 */
	public static List<Map<String, Object>> getComments(String accessToken, String baseUrl, String issueKey) {
		final String updated = "updated";
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);
			String url = baseUrl + API_PATH_ISSUE + "/" + URLEncoder.encode(issueKey, StandardCharsets.UTF_8)
					+ API_SUFFIX_COMMENT;
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);

			JsonObject root = JsonParser.parseString(response).getAsJsonObject();
			List<Map<String, Object>> commentList = new ArrayList<>();
			JsonElement commentsEl = root.get(FIELD_COMMENTS);
			if (commentsEl != null && commentsEl.isJsonArray()) {
				for (JsonElement el : commentsEl.getAsJsonArray()) {
					JsonObject c = el.getAsJsonObject();
					Map<String, Object> comment = new HashMap<>();
					comment.put(FIELD_COMMENT_ID, getText(c, FIELD_ID));
					JsonElement authorEl = c.get(FIELD_AUTHOR);
					comment.put(FIELD_AUTHOR,
							authorEl != null && authorEl.isJsonObject()
									? getText(authorEl.getAsJsonObject(), FIELD_DISPLAY_NAME)
									: "");
					comment.put(FIELD_CREATED, getText(c, FIELD_CREATED));
					comment.put(updated, getText(c, updated));
					comment.put(FIELD_BODY, parseAdfToPlainText(c.get(FIELD_BODY)));
					commentList.add(comment);
				}
			}
			return commentList;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in getComments for '{}': {}", issueKey, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to get comments for issue '" + issueKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Adds a comment to a Jira issue.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param issueKey    issue key (for example, {@code PROJECT-123})
	 * @param commentText plain text comment body
	 * @return map containing {@code commentId}, {@code author}, {@code created},
	 *         and {@code success}
	 */
	public static Map<String, Object> addComment(String accessToken, String baseUrl, String issueKey,
			String commentText) {
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);

			String url = baseUrl + API_PATH_ISSUE + "/" + URLEncoder.encode(issueKey, StandardCharsets.UTF_8)
					+ API_SUFFIX_COMMENT;
			String response = HttpHelperUtility.postRequestStringBody(url, headers,
					GSON.toJson(Map.of(FIELD_BODY, buildAdfDocument(commentText))), ContentType.APPLICATION_JSON, null,
					null, null);

			JsonObject root = JsonParser.parseString(response).getAsJsonObject();
			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_COMMENT_ID, getText(root, FIELD_ID));
			JsonElement authorEl = root.get(FIELD_AUTHOR);
			result.put(FIELD_AUTHOR,
					authorEl != null && authorEl.isJsonObject()
							? getText(authorEl.getAsJsonObject(), FIELD_DISPLAY_NAME)
							: "");
			result.put(FIELD_CREATED, getText(root, FIELD_CREATED));
			result.put(FIELD_SUCCESS, true);
			return result;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in addComment for '{}': {}", issueKey, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to add comment to issue '" + issueKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Edits an existing comment on a Jira issue.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param issueKey    issue key (for example, {@code PROJECT-123})
	 * @param commentId   the numeric comment ID
	 * @param commentText updated plain text comment body
	 * @return map containing {@code commentId}, {@code author}, {@code created},
	 *         {@code updated}, and {@code success}
	 */
	public static Map<String, Object> editComment(String accessToken, String baseUrl, String issueKey, String commentId,
			String commentText) {
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);

			String url = baseUrl + API_PATH_ISSUE + "/" + URLEncoder.encode(issueKey, StandardCharsets.UTF_8)
					+ API_SUFFIX_COMMENT + "/" + URLEncoder.encode(commentId, StandardCharsets.UTF_8);
			String response = HttpHelperUtility.putRequestStringBody(url, headers,
					GSON.toJson(Map.of(FIELD_BODY, buildAdfDocument(commentText))), ContentType.APPLICATION_JSON, null,
					null, null);

			JsonObject root = JsonParser.parseString(response).getAsJsonObject();
			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_COMMENT_ID, getText(root, FIELD_ID));
			JsonElement authorEl = root.get(FIELD_AUTHOR);
			result.put(FIELD_AUTHOR,
					authorEl != null && authorEl.isJsonObject()
							? getText(authorEl.getAsJsonObject(), FIELD_DISPLAY_NAME)
							: "");
			result.put(FIELD_CREATED, getText(root, FIELD_CREATED));
			result.put("updated", getText(root, "updated"));
			result.put(FIELD_SUCCESS, true);
			return result;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in editComment for '{}' comment '{}': {}", issueKey, commentId, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to edit comment '" + commentId + "' on issue '" + issueKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Deletes a comment from a Jira issue.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param issueKey    issue key (for example, {@code PROJECT-123})
	 * @param commentId   the numeric comment ID
	 * @return map containing {@code success}
	 */
	public static Map<String, Object> deleteComment(String accessToken, String baseUrl, String issueKey,
			String commentId) {
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);

			String url = baseUrl + API_PATH_ISSUE + "/" + URLEncoder.encode(issueKey, StandardCharsets.UTF_8)
					+ API_SUFFIX_COMMENT + "/" + URLEncoder.encode(commentId, StandardCharsets.UTF_8);
			HttpHelperUtility.deleteRequestStringBody(url, headers, null, null, null);

			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_SUCCESS, true);
			return result;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in deleteComment for '{}' comment '{}': {}", issueKey, commentId, e.getMessage(),
					e);
			throw new SemossPixelException("Failed to delete comment '" + commentId + "' on issue '" + issueKey
					+ "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Assigns a Jira issue to a user, or removes the assignment.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param issueKey    issue key (for example, {@code PROJECT-123})
	 * @param accountId   the assignee's account ID, or {@code null}/empty to
	 *                    unassign
	 * @return map containing {@code jiraid}, {@code assignee}, and {@code success}
	 */
	public static Map<String, Object> assignIssue(String accessToken, String baseUrl, String issueKey,
			String accountId) {
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);

			String url = baseUrl + API_PATH_ISSUE + "/" + URLEncoder.encode(issueKey, StandardCharsets.UTF_8)
					+ API_SUFFIX_ASSIGNEE;
			Map<String, Object> body = new HashMap<>();
			body.put(FIELD_ACCOUNT_ID, (accountId != null && !accountId.trim().isEmpty()) ? accountId : null);
			HttpHelperUtility.putRequestStringBody(url, headers, GSON.toJson(body), ContentType.APPLICATION_JSON, null,
					null, null);

			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_JIRA_ID, issueKey);
			result.put(FIELD_ASSIGNEE,
					(accountId != null && !accountId.trim().isEmpty()) ? accountId : DEFAULT_UNASSIGNED);
			result.put(FIELD_SUCCESS, true);
			return result;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in assignIssue '{}': {}", issueKey, e.getMessage(), e);
			throw new SemossPixelException("Failed to assign issue '" + issueKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Retrieves attachments for a Jira issue.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param issueKey    issue key (for example, {@code PROJECT-123})
	 * @return list of maps, each containing {@code attachmentId}, {@code filename},
	 *         {@code mimeType}, {@code size}, {@code created}, {@code contentUrl},
	 *         and {@code author}
	 */
	public static List<Map<String, Object>> getAttachments(String accessToken, String baseUrl, String issueKey) {
		final String attachment = "attachment";
		final String filename = "filename";
		final String mimeType = "mimeType";
		final String size = "size";
		final String contentApiField = "content";
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);
			String response = HttpHelperUtility.getRequest(baseUrl + API_PATH_ISSUE + "/"
					+ URLEncoder.encode(issueKey, StandardCharsets.UTF_8) + "?fields=attachment", headers, null, null,
					null);

			JsonObject root = JsonParser.parseString(response).getAsJsonObject();
			JsonArray attachments = new JsonArray();
			JsonElement fieldsEl = root.get(FIELD_FIELDS);
			if (fieldsEl != null && fieldsEl.isJsonObject()) {
				JsonElement attEl = fieldsEl.getAsJsonObject().get(attachment);
				if (attEl != null && attEl.isJsonArray()) {
					attachments = attEl.getAsJsonArray();
				}
			}

			List<Map<String, Object>> attachmentList = new ArrayList<>();
			for (JsonElement el : attachments) {
				JsonObject a = el.getAsJsonObject();
				Map<String, Object> att = new HashMap<>();
				att.put(FIELD_ATTACHMENT_ID, getText(a, FIELD_ID));
				att.put(filename, getText(a, filename));
				att.put(mimeType, getText(a, mimeType));
				att.put(size, getLong(a, size));
				att.put(FIELD_CREATED, getText(a, FIELD_CREATED));
				att.put(FIELD_CONTENT_URL, getText(a, contentApiField));
				JsonElement authorEl = a.get(FIELD_AUTHOR);
				att.put(FIELD_AUTHOR,
						authorEl != null && authorEl.isJsonObject()
								? getText(authorEl.getAsJsonObject(), FIELD_DISPLAY_NAME)
								: "");
				attachmentList.add(att);
			}
			return attachmentList;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in getAttachments for '{}': {}", issueKey, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to get attachments for issue '" + issueKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Adds a file attachment to a Jira issue.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param issueKey    issue key (for example, {@code PROJECT-123})
	 * @param filePath    absolute path to the file to attach
	 * @return map containing {@code attachmentId}, {@code filename}, {@code size},
	 *         and {@code success}
	 */
	public static Map<String, Object> addAttachment(String accessToken, String baseUrl, String issueKey,
			String filePath) {
		try {
			validateJiraContext(accessToken, baseUrl);

			String url = baseUrl + API_PATH_ISSUE + "/" + URLEncoder.encode(issueKey, StandardCharsets.UTF_8)
					+ API_SUFFIX_ATTACHMENTS;

			File file = new File(filePath);
			if (!file.exists() || !file.isFile()) {
				throw new SemossPixelException("File not found: " + filePath);
			}
			String response;
			try (CloseableHttpClient httpClient = HttpClientBuilder.create().useSystemProperties().build()) {
				HttpPost post = new HttpPost(url);
				post.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken);
				post.addHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());
				post.addHeader("X-Atlassian-Token", "no-check");

				HttpEntity entity = MultipartEntityBuilder.create()
						.addPart("file", new FileBody(file, ContentType.APPLICATION_OCTET_STREAM)).build();
				post.setEntity(entity);

				response = httpClient.execute(post, httpResponse -> {
					int statusCode = httpResponse.getCode();
					String body = EntityUtils.toString(httpResponse.getEntity(), StandardCharsets.UTF_8);
					if (statusCode < 200 || statusCode >= 300) {
						throw new SemossPixelException(
								"Jira attachment upload failed (HTTP " + statusCode + "): " + body);
					}
					return body;
				});
			}

			JsonElement rootEl = JsonParser.parseString(response);
			JsonObject firstAttachment = rootEl.isJsonArray() && rootEl.getAsJsonArray().size() > 0
					? rootEl.getAsJsonArray().get(0).getAsJsonObject()
					: rootEl.getAsJsonObject();

			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_ATTACHMENT_ID, getText(firstAttachment, FIELD_ID));
			result.put("filename", getText(firstAttachment, "filename"));
			result.put("size", getLong(firstAttachment, "size"));
			result.put(FIELD_SUCCESS, true);
			return result;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in addAttachment for '{}': {}", issueKey, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to add attachment to issue '" + issueKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Creates a link between two Jira issues.
	 *
	 * @param accessToken  Jira OAuth access token
	 * @param baseUrl      Jira API base URL including cloud ID
	 * @param linkType     the link type name (for example, {@code "Blocks"},
	 *                     {@code "Relates"})
	 * @param inwardIssue  the issue key for the inward side of the link
	 * @param outwardIssue the issue key for the outward side of the link
	 * @return map containing {@code success}
	 */
	public static Map<String, Object> linkIssues(String accessToken, String baseUrl, String linkType,
			String inwardIssue, String outwardIssue) {
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);

			Map<String, Object> body = new HashMap<>();
			body.put("type", Map.of(FIELD_NAME, linkType));
			body.put("inwardIssue", Map.of(FIELD_KEY, inwardIssue));
			body.put("outwardIssue", Map.of(FIELD_KEY, outwardIssue));

			HttpHelperUtility.postRequestStringBody(baseUrl + API_PATH_ISSUELINK, headers, GSON.toJson(body),
					ContentType.APPLICATION_JSON, null, null, null);

			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_SUCCESS, true);
			result.put("linkType", linkType);
			result.put("inwardIssue", inwardIssue);
			result.put("outwardIssue", outwardIssue);
			return result;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in linkIssues: {}", e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to link issues '" + inwardIssue + "' and '" + outwardIssue + "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Retrieves all issue link types available in the Jira instance.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @return list of maps, each containing {@code id}, {@code linkType},
	 *         {@code inward}, and {@code outward}
	 */
	public static List<Map<String, Object>> getIssueLinkTypes(String accessToken, String baseUrl) {
		final String issueLinkTypes = "issueLinkTypes";
		final String inward = "inward";
		final String outward = "outward";
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);
			String response = HttpHelperUtility.getRequest(baseUrl + API_PATH_ISSUELINKTYPE, headers, null, null, null);

			JsonObject root = JsonParser.parseString(response).getAsJsonObject();
			List<Map<String, Object>> linkTypeList = new ArrayList<>();
			JsonElement linkTypesEl = root.get(issueLinkTypes);
			if (linkTypesEl != null && linkTypesEl.isJsonArray()) {
				for (JsonElement el : linkTypesEl.getAsJsonArray()) {
					JsonObject lt = el.getAsJsonObject();
					Map<String, Object> linkType = new HashMap<>();
					linkType.put(FIELD_ID, getText(lt, FIELD_ID));
					linkType.put(FIELD_LINK_TYPE, getText(lt, FIELD_NAME));
					linkType.put(inward, getText(lt, inward));
					linkType.put(outward, getText(lt, outward));
					linkTypeList.add(linkType);
				}
			}
			return linkTypeList;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in getIssueLinkTypes: {}", e.getMessage(), e);
			throw new SemossPixelException("Failed to retrieve issue link types. Error: " + e.getMessage());
		}
	}

	/**
	 * Logs work against a Jira issue.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param issueKey    issue key (for example, {@code PROJECT-123})
	 * @param timeSpent   time in Jira notation (for example, {@code "2h 30m"},
	 *                    {@code "1d"})
	 * @param comment     optional comment for the worklog entry
	 * @param started     optional start datetime in ISO format; defaults to now
	 * @return map containing {@code worklogId}, {@code timeSpent},
	 *         {@code timeSpentSeconds}, {@code author}, {@code created}, and
	 *         {@code success}
	 */
	public static Map<String, Object> logWork(String accessToken, String baseUrl, String issueKey, String timeSpent,
			String comment, String started) {
		final String timeSpentField = "timeSpent";
		final String startedField = "started";
		final String timeSpentSecondsField = "timeSpentSeconds";
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);

			Map<String, Object> body = new HashMap<>();
			body.put(timeSpentField, timeSpent);
			if (comment != null && !comment.trim().isEmpty()) {
				body.put(FIELD_COMMENT, buildAdfDocument(comment));
			}
			if (started != null && !started.trim().isEmpty()) {
				body.put(startedField, started);
			}

			String url = baseUrl + API_PATH_ISSUE + "/" + URLEncoder.encode(issueKey, StandardCharsets.UTF_8)
					+ API_SUFFIX_WORKLOG;
			String response = HttpHelperUtility.postRequestStringBody(url, headers, GSON.toJson(body),
					ContentType.APPLICATION_JSON, null, null, null);

			JsonObject root = JsonParser.parseString(response).getAsJsonObject();
			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_WORKLOG_ID, getText(root, FIELD_ID));
			result.put(timeSpentField, getText(root, timeSpentField));
			result.put(timeSpentSecondsField, getLong(root, timeSpentSecondsField));
			JsonElement authorEl = root.get(FIELD_AUTHOR);
			result.put(FIELD_AUTHOR,
					authorEl != null && authorEl.isJsonObject()
							? getText(authorEl.getAsJsonObject(), FIELD_DISPLAY_NAME)
							: "");
			result.put(FIELD_CREATED, getText(root, FIELD_CREATED));
			result.put(FIELD_SUCCESS, true);
			return result;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in logWork for '{}': {}", issueKey, e.getMessage(), e);
			throw new SemossPixelException("Failed to log work on issue '" + issueKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Retrieves worklog entries for a Jira issue.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param issueKey    issue key (for example, {@code PROJECT-123})
	 * @return list of maps, each containing {@code worklogId}, {@code author},
	 *         {@code timeSpent}, {@code timeSpentSeconds}, {@code started},
	 *         {@code created}, {@code updated}, and {@code comment}
	 */
	public static List<Map<String, Object>> getWorklogs(String accessToken, String baseUrl, String issueKey) {
		final String worklogs = "worklogs";
		final String timeSpentField = "timeSpent";
		final String startedField = "started";
		final String timeSpentSecondsField = "timeSpentSeconds";
		final String updatedField = "updated";
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);
			String url = baseUrl + API_PATH_ISSUE + "/" + URLEncoder.encode(issueKey, StandardCharsets.UTF_8)
					+ API_SUFFIX_WORKLOG;
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);

			JsonObject root = JsonParser.parseString(response).getAsJsonObject();
			List<Map<String, Object>> worklogList = new ArrayList<>();
			JsonElement worklogsEl = root.get(worklogs);
			if (worklogsEl != null && worklogsEl.isJsonArray()) {
				for (JsonElement el : worklogsEl.getAsJsonArray()) {
					JsonObject w = el.getAsJsonObject();
					Map<String, Object> entry = new HashMap<>();
					entry.put(FIELD_WORKLOG_ID, getText(w, FIELD_ID));
					JsonElement authorEl = w.get(FIELD_AUTHOR);
					entry.put(FIELD_AUTHOR,
							authorEl != null && authorEl.isJsonObject()
									? getText(authorEl.getAsJsonObject(), FIELD_DISPLAY_NAME)
									: "");
					entry.put(timeSpentField, getText(w, timeSpentField));
					entry.put(timeSpentSecondsField, getLong(w, timeSpentSecondsField));
					entry.put(startedField, getText(w, startedField));
					entry.put(FIELD_CREATED, getText(w, FIELD_CREATED));
					entry.put(updatedField, getText(w, updatedField));
					entry.put(FIELD_COMMENT, parseAdfToPlainText(w.get(FIELD_COMMENT)));
					worklogList.add(entry);
				}
			}
			return worklogList;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in getWorklogs for '{}': {}", issueKey, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to get worklogs for issue '" + issueKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Removes a link between two Jira issues.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param linkId      the issue link ID to remove
	 * @return map containing {@code success}
	 */
	public static Map<String, Object> unlinkIssues(String accessToken, String baseUrl, String linkId) {
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);

			HttpHelperUtility.deleteRequestStringBody(
					baseUrl + API_PATH_ISSUELINK + "/" + URLEncoder.encode(linkId, StandardCharsets.UTF_8), headers,
					null, null, null);

			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_SUCCESS, true);
			return result;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in unlinkIssues for link '{}': {}", linkId, e.getMessage(), e);
			throw new SemossPixelException("Failed to remove issue link '" + linkId + "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Deletes an attachment from a Jira issue.
	 *
	 * @param accessToken  Jira OAuth access token
	 * @param baseUrl      Jira API base URL including cloud ID
	 * @param attachmentId the attachment ID to delete
	 * @return map containing {@code success}
	 */
	public static Map<String, Object> deleteAttachment(String accessToken, String baseUrl, String attachmentId) {
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);

			HttpHelperUtility.deleteRequestStringBody(
					baseUrl + API_PATH_ATTACHMENT + "/" + URLEncoder.encode(attachmentId, StandardCharsets.UTF_8),
					headers, null, null, null);

			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_SUCCESS, true);
			return result;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in deleteAttachment '{}': {}", attachmentId, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to delete attachment '" + attachmentId + "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Updates an existing worklog entry on a Jira issue.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param issueKey    issue key (for example, {@code PROJECT-123})
	 * @param worklogId   the worklog entry ID to update
	 * @param timeSpent   updated time in Jira notation (for example, {@code "3h"},
	 *                    {@code "1d 2h"})
	 * @param comment     optional updated comment for the worklog entry
	 * @param started     optional updated start datetime in ISO format
	 * @return map containing {@code worklogId}, {@code timeSpent},
	 *         {@code timeSpentSeconds}, {@code author}, {@code updated}, and
	 *         {@code success}
	 */
	public static Map<String, Object> updateWorklog(String accessToken, String baseUrl, String issueKey,
			String worklogId, String timeSpent, String comment, String started) {
		final String timeSpentField = "timeSpent";
		final String startedField = "started";
		final String timeSpentSecondsField = "timeSpentSeconds";
		final String updatedField = "updated";
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);

			Map<String, Object> body = new HashMap<>();
			body.put(timeSpentField, timeSpent);
			if (comment != null && !comment.trim().isEmpty()) {
				body.put(FIELD_COMMENT, buildAdfDocument(comment));
			}
			if (started != null && !started.trim().isEmpty()) {
				body.put(startedField, started);
			}

			String url = baseUrl + API_PATH_ISSUE + "/" + URLEncoder.encode(issueKey, StandardCharsets.UTF_8)
					+ API_SUFFIX_WORKLOG + "/" + URLEncoder.encode(worklogId, StandardCharsets.UTF_8);
			String response = HttpHelperUtility.putRequestStringBody(url, headers, GSON.toJson(body),
					ContentType.APPLICATION_JSON, null, null, null);

			JsonObject root = JsonParser.parseString(response).getAsJsonObject();
			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_WORKLOG_ID, getText(root, FIELD_ID));
			result.put(timeSpentField, getText(root, timeSpentField));
			result.put(timeSpentSecondsField, getLong(root, timeSpentSecondsField));
			JsonElement authorEl = root.get(FIELD_AUTHOR);
			result.put(FIELD_AUTHOR,
					authorEl != null && authorEl.isJsonObject()
							? getText(authorEl.getAsJsonObject(), FIELD_DISPLAY_NAME)
							: "");
			result.put(updatedField, getText(root, updatedField));
			result.put(FIELD_SUCCESS, true);
			return result;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in updateWorklog for '{}' worklog '{}': {}", issueKey, worklogId, e.getMessage(),
					e);
			throw new SemossPixelException("Failed to update worklog '" + worklogId + "' on issue '" + issueKey
					+ "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Deletes a worklog entry from a Jira issue.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param issueKey    issue key (for example, {@code PROJECT-123})
	 * @param worklogId   the worklog entry ID to delete
	 * @return map containing {@code success}
	 */
	public static Map<String, Object> deleteWorklog(String accessToken, String baseUrl, String issueKey,
			String worklogId) {
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);

			String url = baseUrl + API_PATH_ISSUE + "/" + URLEncoder.encode(issueKey, StandardCharsets.UTF_8)
					+ API_SUFFIX_WORKLOG + "/" + URLEncoder.encode(worklogId, StandardCharsets.UTF_8);
			HttpHelperUtility.deleteRequestStringBody(url, headers, null, null, null);

			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_SUCCESS, true);
			return result;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in deleteWorklog for '{}' worklog '{}': {}", issueKey, worklogId, e.getMessage(),
					e);
			throw new SemossPixelException("Failed to delete worklog '" + worklogId + "' on issue '" + issueKey
					+ "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Executes a JQL search against the Jira search API and returns a paginated
	 * result map. Clamps {@code maxResults} to the range [1, 100], defaulting to 50
	 * when the provided value is less than 1.
	 *
	 * @param accessToken   Jira OAuth access token
	 * @param baseUrl       Jira API base URL including cloud ID
	 * @param jql           raw JQL query string
	 * @param nextPageToken optional pagination token from a previous response
	 * @param maxResults    requested page size (clamped to 1–100, default 50)
	 * @return map containing {@code issues}, {@code isLast}, {@code maxResults},
	 *         and optionally {@code nextPageToken}
	 */
	private static Map<String, Object> executeJqlSearch(String accessToken, String baseUrl, String jql,
			String nextPageToken, int maxResults) throws Exception {
		final String isLast = "isLast";

		Map<String, String> headers = buildHeaders(accessToken);
		int safeMax = (maxResults < 1) ? 50 : Math.min(maxResults, 100);

		Map<String, Object> body = new HashMap<>();
		body.put(FIELD_JQL, jql);
		body.put(FIELD_MAX_RESULTS, safeMax);
		body.put(FIELD_FIELDS, List.of("*all"));
		if (nextPageToken != null && !nextPageToken.trim().isEmpty()) {
			body.put(FIELD_NEXT_PAGE_TOKEN, nextPageToken);
		}

		String response = HttpHelperUtility.postRequestStringBody(baseUrl + API_PATH_SEARCH, headers, GSON.toJson(body),
				ContentType.APPLICATION_JSON, null, null, null);
		JsonObject root = JsonParser.parseString(response).getAsJsonObject();

		List<Map<String, Object>> issueList = new ArrayList<>();
		JsonElement issuesEl = root.get(FIELD_ISSUES);
		if (issuesEl != null && issuesEl.isJsonArray()) {
			for (JsonElement issue : issuesEl.getAsJsonArray()) {
				issueList.add(parseIssueSummary(issue.getAsJsonObject()));
			}
		}

		Map<String, Object> result = new HashMap<>();
		result.put(FIELD_ISSUES, issueList);
		result.put(isLast, root.has(isLast) && !root.get(isLast).isJsonNull() ? root.get(isLast).getAsBoolean() : true);
		result.put(FIELD_MAX_RESULTS, safeMax);
		if (root.has(FIELD_NEXT_PAGE_TOKEN)) {
			result.put(FIELD_NEXT_PAGE_TOKEN, getText(root, FIELD_NEXT_PAGE_TOKEN));
		}
		return result;
	}

	/**
	 * Returns the fields available when creating an issue for a given project +
	 * issue type. Calls GET
	 * /rest/api/3/issue/createmeta/{projectKey}/issuetypes/{issueTypeId}.
	 * <p>
	 * Each returned field includes its schema type so callers know the expected
	 * value structure (plain string, object with id/key, array, etc.) and any
	 * allowed values for enum-style fields.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param projectKey  Jira project key (e.g. "PROJ")
	 * @param issueTypeId Jira issue type ID (numeric string)
	 * @return list of maps with {@code fieldId}, {@code name}, {@code required},
	 *         {@code hasDefaultValue}, {@code key}, {@code schema}, and optionally
	 *         {@code allowedValues}
	 */
	public static List<Map<String, Object>> getCreateMetaFields(String accessToken, String baseUrl, String projectKey,
			String issueTypeId) {
		final String FIELD_FIELDID = "fieldId";
		final String FIELD_REQUIRED = "required";
		final String FIELD_HAS_DEFAULT = "hasDefaultValue";
		final String FIELD_REPORTER = "reporter";
		final String FIELD_SCHEMA = "schema";
		final String FIELD_ALLOWED_VALUES = "allowedValues";
		final String FIELD_VALUE = "value";
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);
			String url = baseUrl + API_PATH_CREATEMETA + "/" + URLEncoder.encode(projectKey, StandardCharsets.UTF_8)
					+ "/issuetypes/" + URLEncoder.encode(issueTypeId, StandardCharsets.UTF_8);
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
			JsonObject root = JsonParser.parseString(response).getAsJsonObject();
			JsonElement fieldsEl = root.get(FIELD_FIELDS);
			List<Map<String, Object>> results = new ArrayList<>();
			if (fieldsEl != null && fieldsEl.isJsonArray()) {
				for (JsonElement el : fieldsEl.getAsJsonArray()) {
					JsonObject f = el.getAsJsonObject();
					Map<String, Object> entry = new HashMap<>();
					String fieldId = f.has(FIELD_FIELDID) && !f.get(FIELD_FIELDID).isJsonNull()
							? f.get(FIELD_FIELDID).getAsString()
							: null;
					entry.put(FIELD_FIELDID, fieldId);
					entry.put(FIELD_NAME,
							f.has(FIELD_NAME) && !f.get(FIELD_NAME).isJsonNull() ? f.get(FIELD_NAME).getAsString()
									: null);
					entry.put(FIELD_REQUIRED, FIELD_REPORTER.equals(fieldId) ? false : getBoolean(f, FIELD_REQUIRED));
					entry.put(FIELD_HAS_DEFAULT, getBoolean(f, FIELD_HAS_DEFAULT));
					entry.put(FIELD_KEY,
							f.has(FIELD_KEY) && !f.get(FIELD_KEY).isJsonNull() ? f.get(FIELD_KEY).getAsString() : null);

					JsonElement schemaEl = f.get(FIELD_SCHEMA);
					if (schemaEl != null && schemaEl.isJsonObject()) {
						entry.put(FIELD_SCHEMA, GSON.fromJson(schemaEl, Map.class));
					}

					JsonElement allowedEl = f.get(FIELD_ALLOWED_VALUES);
					if (allowedEl != null && allowedEl.isJsonArray() && allowedEl.getAsJsonArray().size() > 0) {
						List<Map<String, Object>> allowedValues = new ArrayList<>();
						for (JsonElement avEl : allowedEl.getAsJsonArray()) {
							JsonObject av = avEl.getAsJsonObject();
							Map<String, Object> valEntry = new HashMap<>();
							if (av.has(FIELD_ID)) {
								valEntry.put(FIELD_ID, av.get(FIELD_ID).getAsString());
							}
							if (av.has(FIELD_NAME)) {
								valEntry.put(FIELD_NAME, av.get(FIELD_NAME).getAsString());
							}
							if (av.has(FIELD_KEY)) {
								valEntry.put(FIELD_KEY, av.get(FIELD_KEY).getAsString());
							}
							if (av.has(FIELD_VALUE)) {
								valEntry.put(FIELD_VALUE, av.get(FIELD_VALUE).getAsString());
							}
							allowedValues.add(valEntry);
						}
						entry.put(FIELD_ALLOWED_VALUES, allowedValues);
					}
					results.add(entry);
				}
			}
			return results;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error fetching create-meta fields for project '{}' issueType '{}': {}", projectKey,
					issueTypeId, e.getMessage(), e);
			throw new SemossPixelException("Failed to get fields for project '" + projectKey + "' issue type '"
					+ issueTypeId + "': " + e.getMessage());
		}
	}

	/**
	 * Creates a Jira issue using a dynamic field map. The field map is sent inside
	 * a {@code "fields"} wrapper as required by the Jira REST API.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param fieldValues map of field names to values
	 * @return result map with {@code id}, {@code jiraid}, and {@code success}
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> createIssueFromMap(String accessToken, String baseUrl,
			Map<String, Object> fieldValues) {
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);

			Map<String, Object> requestBody = new HashMap<>();
			requestBody.put(FIELD_FIELDS, convertAdfFields(fieldValues));
			String jsonBody = GSON.toJson(requestBody);

			String response = HttpHelperUtility.postRequestStringBody(baseUrl + API_PATH_ISSUE, headers, jsonBody,
					ContentType.APPLICATION_JSON, null, null, null);
			Map<String, Object> resp = GSON.fromJson(response, Map.class);

			if (resp == null || (resp.get(FIELD_ID) == null && resp.get(FIELD_KEY) == null)) {
				StringBuilder errorDetail = new StringBuilder();
				if (resp != null) {
					Object errors = resp.get("errors");
					Object errorMessages = resp.get("errorMessages");
					if (errors != null) {
						errorDetail.append(" errors=").append(errors);
					}
					if (errorMessages != null) {
						errorDetail.append(" errorMessages=").append(errorMessages);
					}
				}
				throw new SemossPixelException(
						"Jira did not return an issue id or key. The issue was not created." + errorDetail.toString());
			}

			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_ID, resp.get(FIELD_ID).toString());
			result.put(FIELD_JIRA_ID, resp.get(FIELD_KEY).toString());
			result.put(FIELD_SUCCESS, true);
			return result;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in createIssueFromMap: {}", e.getMessage(), e);
			throw new SemossPixelException("Failed to create Jira issue from map. Error: " + e.getMessage());
		}
	}

	/**
	 * Updates a Jira issue's fields using the editmeta API to validate which keys
	 * are accepted. Only keys present in the editmeta response are sent via
	 * {@code PUT /rest/api/3/issue/{key}}. Any keys not recognized as editable
	 * fields are rejected with an error.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param issueKey    Jira issue key (e.g. "PROJ-123")
	 * @param fieldValues map of field names to values (must only contain editable
	 *                    fields)
	 * @return result map with {@code jiraid} and {@code success}
	 */
	public static Map<String, Object> updateIssueFromMap(String accessToken, String baseUrl, String issueKey,
			Map<String, Object> fieldValues) {
		try {
			validateJiraContext(accessToken, baseUrl);

			if (fieldValues == null || fieldValues.isEmpty()) {
				throw new SemossPixelException(
						"No fields provided for update. Please specify at least one field to update.");
			}

			Map<String, String> headers = buildHeaders(accessToken);

			Set<String> editableFieldKeys = getEditableFieldKeys(accessToken, baseUrl, issueKey);

			Set<String> invalidKeys = new HashSet<>();
			for (String key : fieldValues.keySet()) {
				if (!editableFieldKeys.contains(key)) {
					invalidKeys.add(key);
				}
			}
			if (!invalidKeys.isEmpty()) {
				throw new SemossPixelException("The following keys are not editable fields for issue '" + issueKey
						+ "': " + invalidKeys + ". Editable fields are: " + editableFieldKeys);
			}

			Map<String, Object> requestBody = new HashMap<>();
			requestBody.put(FIELD_FIELDS, convertAdfFields(fieldValues));
			String jsonBody = GSON.toJson(requestBody);
			HttpHelperUtility.putRequestStringBody(
					baseUrl + API_PATH_ISSUE + "/" + URLEncoder.encode(issueKey, StandardCharsets.UTF_8), headers,
					jsonBody, ContentType.APPLICATION_JSON, null, null, null);

			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_JIRA_ID, issueKey);
			result.put(FIELD_SUCCESS, true);
			return result;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in updateIssueFromMap '{}': {}", issueKey, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to update issue '" + issueKey + "' from map. Error: " + e.getMessage());
		}
	}

	/**
	 * Fetches the set of editable field keys for a specific Jira issue using the
	 * editmeta endpoint ({@code GET /rest/api/3/issue/{key}/editmeta}).
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param issueKey    Jira issue key
	 * @return set of field key strings that Jira accepts for this issue
	 */
	private static Set<String> getEditableFieldKeys(String accessToken, String baseUrl, String issueKey) {
		try {
			Map<String, String> headers = buildHeaders(accessToken);
			String url = baseUrl + API_PATH_ISSUE + "/" + URLEncoder.encode(issueKey, StandardCharsets.UTF_8)
					+ API_SUFFIX_EDITMETA;
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
			JsonObject root = JsonParser.parseString(response).getAsJsonObject();
			JsonElement fieldsEl = root.get(FIELD_FIELDS);
			Set<String> fieldKeys = new HashSet<>();
			if (fieldsEl != null && fieldsEl.isJsonObject()) {
				fieldKeys.addAll(fieldsEl.getAsJsonObject().keySet());
			}
			return fieldKeys;
		} catch (Exception e) {
			classLogger.error("Failed to fetch editmeta for '{}': {}", issueKey, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to retrieve editable fields for issue '" + issueKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Returns the fields editable on an existing Jira issue. Calls {@code GET
	 * /rest/api/3/issue/{key}/editmeta}.
	 * <p>
	 * The editmeta response uses an object keyed by field ID (unlike createmeta
	 * which returns an array). This method normalises the response into the same
	 * {@code List<Map<String, Object>>} format as
	 * {@link #getCreateMetaFields(String, String, String, String)}.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param issueKey    Jira issue key (e.g. "RTJ-42")
	 * @return list of maps with {@code fieldId}, {@code name}, {@code required},
	 *         {@code schema}, and optionally {@code allowedValues}
	 */
	public static List<Map<String, Object>> getEditMetaFields(String accessToken, String baseUrl, String issueKey) {
		final String FIELD_REQUIRED = "required";
		final String FIELD_SCHEMA = "schema";
		final String FIELD_ALLOWED_VALUES = "allowedValues";
		final String FIELD_VALUE = "value";
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);
			String url = baseUrl + API_PATH_ISSUE + "/" + URLEncoder.encode(issueKey, StandardCharsets.UTF_8)
					+ API_SUFFIX_EDITMETA;
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
			JsonObject root = JsonParser.parseString(response).getAsJsonObject();
			JsonElement fieldsEl = root.get(FIELD_FIELDS);
			List<Map<String, Object>> results = new ArrayList<>();
			if (fieldsEl != null && fieldsEl.isJsonObject()) {
				for (Map.Entry<String, JsonElement> entry : fieldsEl.getAsJsonObject().entrySet()) {
					String fieldId = entry.getKey();
					JsonObject f = entry.getValue().getAsJsonObject();
					Map<String, Object> fieldEntry = new HashMap<>();
					fieldEntry.put("fieldId", fieldId);
					fieldEntry.put(FIELD_NAME,
							f.has(FIELD_NAME) && !f.get(FIELD_NAME).isJsonNull() ? f.get(FIELD_NAME).getAsString()
									: null);
					fieldEntry.put(FIELD_REQUIRED, getBoolean(f, FIELD_REQUIRED));

					JsonElement schemaEl = f.get(FIELD_SCHEMA);
					if (schemaEl != null && schemaEl.isJsonObject()) {
						fieldEntry.put(FIELD_SCHEMA, GSON.fromJson(schemaEl, Map.class));
					}

					JsonElement allowedEl = f.get(FIELD_ALLOWED_VALUES);
					if (allowedEl != null && allowedEl.isJsonArray() && allowedEl.getAsJsonArray().size() > 0) {
						List<Map<String, Object>> allowedValues = new ArrayList<>();
						for (JsonElement avEl : allowedEl.getAsJsonArray()) {
							JsonObject av = avEl.getAsJsonObject();
							Map<String, Object> valEntry = new HashMap<>();
							if (av.has(FIELD_ID)) {
								valEntry.put(FIELD_ID, av.get(FIELD_ID).getAsString());
							}
							if (av.has(FIELD_NAME)) {
								valEntry.put(FIELD_NAME, av.get(FIELD_NAME).getAsString());
							}
							if (av.has(FIELD_KEY)) {
								valEntry.put(FIELD_KEY, av.get(FIELD_KEY).getAsString());
							}
							if (av.has(FIELD_VALUE)) {
								valEntry.put(FIELD_VALUE, av.get(FIELD_VALUE).getAsString());
							}
							allowedValues.add(valEntry);
						}
						fieldEntry.put(FIELD_ALLOWED_VALUES, allowedValues);
					}
					results.add(fieldEntry);
				}
			}
			return results;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error fetching editmeta fields for issue '{}': {}", issueKey, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to get editable fields for issue '" + issueKey + "': " + e.getMessage());
		}
	}

	/**
	 * Resolves a project key or numeric ID to a Jira project ID string. If
	 * {@code projectIdOrKey} is already all digits it is returned as-is; otherwise
	 * a GET to {@code /rest/api/3/project/{key}} is made to fetch the numeric ID.
	 *
	 * @param urlBase        Jira API base URL including cloud ID
	 * @param accessToken    Jira OAuth access token
	 * @param projectIdOrKey project key (e.g. {@code "PROJ"}) or numeric ID
	 * @return numeric project ID string
	 * @throws Exception if the HTTP call fails or the project cannot be found
	 */
	private static String resolveProjectId(String urlBase, String accessToken, String projectIdOrKey) throws Exception {
		if (projectIdOrKey == null || projectIdOrKey.trim().isEmpty()) {
			throw new SemossPixelException("Project key is required.");
		}
		if (projectIdOrKey.chars().allMatch(Character::isDigit)) {
			return projectIdOrKey;
		}
		Map<String, String> headers = buildHeaders(accessToken);
		String projectUrl = urlBase + API_PATH_PROJECT + "/"
				+ URLEncoder.encode(projectIdOrKey, StandardCharsets.UTF_8);
		String response = HttpHelperUtility.getRequest(projectUrl, headers, null, null, null);
		JsonObject projectNode = JsonParser.parseString(response).getAsJsonObject();
		String resolvedId = getText(projectNode, FIELD_ID);
		if (resolvedId.isEmpty()) {
			throw new SemossPixelException("Unable to resolve project ID for project '" + projectIdOrKey
					+ "'. Check the project key is correct.");
		}
		return resolvedId;
	}

	/**
	 * Builds the standard HTTP headers required for all Jira REST API calls.
	 *
	 * @param accessToken Jira OAuth access token
	 * @return map of header name to value
	 */
	private static Map<String, String> buildHeaders(String accessToken) {
		Map<String, String> headers = new HashMap<>();
		headers.put(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken);
		headers.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
		headers.put(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());
		headers.put(HttpHeaders.USER_AGENT, "Semoss-Jira-Connector/1.0");
		return headers;
	}

	/**
	 * Transitions a Jira issue to the target status by finding the matching
	 * workflow transition and executing it. Matches against the transition's target
	 * status name (case-insensitive).
	 *
	 * @param accessToken  Jira OAuth access token
	 * @param baseUrl      Jira API base URL including cloud ID
	 * @param issueKey     issue key (for example, {@code PROJECT-123})
	 * @param targetStatus desired status name (for example, {@code "Done"},
	 *                     {@code "In Progress"})
	 */
	public static void transitionIssue(String accessToken, String baseUrl, String issueKey, String targetStatus) {
		Map<String, String> headers = buildHeaders(accessToken);
		try {
			String url = baseUrl + API_PATH_ISSUE + "/" + URLEncoder.encode(issueKey, StandardCharsets.UTF_8)
					+ API_SUFFIX_TRANSITIONS;
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
			JsonObject root = JsonParser.parseString(response).getAsJsonObject();

			String transitionId = null;
			JsonElement transEl = root.get(FIELD_TRANSITIONS);
			if (transEl != null && transEl.isJsonArray()) {
				for (JsonElement el : transEl.getAsJsonArray()) {
					JsonObject t = el.getAsJsonObject();
					JsonElement toEl = t.get(FIELD_TO);
					String toStatusName = toEl != null && toEl.isJsonObject()
							? getText(toEl.getAsJsonObject(), FIELD_NAME)
							: "";
					if (targetStatus.equalsIgnoreCase(toStatusName)) {
						transitionId = getText(t, FIELD_ID);
						break;
					}
				}
			}

			if (transitionId == null) {
				throw new SemossPixelException("No available transition to status '" + targetStatus + "' for issue '"
						+ issueKey + "'. Use JiraGetTransitions to see valid target statuses.");
			}

			Map<String, Object> body = Map.of("transition", Map.of(FIELD_ID, transitionId));
			HttpHelperUtility.postRequestStringBody(url, headers, GSON.toJson(body), ContentType.APPLICATION_JSON, null,
					null, null);
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error transitioning issue '{}' to status '{}': {}", issueKey, targetStatus,
					e.getMessage(), e);
			throw new SemossPixelException("Failed to transition issue '" + issueKey + "' to status '" + targetStatus
					+ "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Transitions a Jira issue using an explicit transition ID.
	 *
	 * @param accessToken  Jira OAuth access token
	 * @param baseUrl      Jira API base URL including cloud ID
	 * @param issueKey     issue key (for example, {@code PROJECT-123})
	 * @param transitionId the numeric transition ID
	 */
	public static void transitionIssueById(String accessToken, String baseUrl, String issueKey, String transitionId) {
		try {
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);
			String url = baseUrl + API_PATH_ISSUE + "/" + URLEncoder.encode(issueKey, StandardCharsets.UTF_8)
					+ API_SUFFIX_TRANSITIONS;
			Map<String, Object> body = Map.of("transition", Map.of(FIELD_ID, transitionId));
			HttpHelperUtility.postRequestStringBody(url, headers, GSON.toJson(body), ContentType.APPLICATION_JSON, null,
					null, null);
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error transitioning issue '{}' with transition id '{}': {}", issueKey, transitionId,
					e.getMessage(), e);
			throw new SemossPixelException("Transition with id '" + transitionId + "' failed for issue '" + issueKey
					+ "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Validates that both the access token and base URL are non-null and non-blank.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @throws SemossPixelException if either value is null or blank
	 */
	private static void validateJiraContext(String accessToken, String baseUrl) {
		if (accessToken == null || accessToken.trim().isEmpty()) {
			throw new SemossPixelException("Jira access token must not be empty.");
		}
		if (baseUrl == null || baseUrl.trim().isEmpty()) {
			throw new SemossPixelException("Jira base URL must not be empty.");
		}
	}

	/**
	 * Converts plain-text string values for fields that Jira requires in Atlassian
	 * Document Format (ADF) into proper ADF structures. Non-string values and
	 * fields not requiring ADF are passed through unchanged.
	 *
	 * @param fieldValues map of Jira field keys to their values
	 * @return new map with ADF-required fields converted to ADF document structures
	 */
	private static Map<String, Object> convertAdfFields(Map<String, Object> fieldValues) {
		Map<String, Object> converted = new HashMap<>(fieldValues);
		for (Map.Entry<String, Object> entry : converted.entrySet()) {
			if (entry.getValue() instanceof String && isAdfField(entry.getKey())) {
				entry.setValue(buildAdfDocument((String) entry.getValue()));
			}
		}
		return converted;
	}

	/**
	 * Returns {@code true} if the given Jira field key requires Atlassian Document
	 * Format (ADF) instead of plain text in the v3 REST API.
	 *
	 * @param fieldKey Jira field key to check
	 * @return {@code true} if the field must be sent as an ADF document
	 */
	private static boolean isAdfField(String fieldKey) {
		return FIELD_DESCRIPTION.equals(fieldKey) || FIELD_ENVIRONMENT.equals(fieldKey);
	}

	/**
	 * Extracts a minimal issue summary map from a raw Jira issue JSON object,
	 * containing {@code id}, {@code jiraid}, and a fully-deserialized
	 * {@code fields} map.
	 *
	 * @param issue Jira issue JSON object from a search or read response
	 * @return map with {@code id}, {@code jiraid}, and {@code fields}
	 */
	private static Map<String, Object> parseIssueSummary(JsonObject issue) {
		Map<String, Object> result = new HashMap<>();
		result.put(FIELD_ID, getText(issue, FIELD_ID));
		result.put(FIELD_JIRA_ID, getText(issue, FIELD_KEY));
		JsonElement fieldsEl = issue.get(FIELD_FIELDS);
		if (fieldsEl != null && fieldsEl.isJsonObject()) {
			result.put(FIELD_FIELDS, GSON.fromJson(fieldsEl, Map.class));
		}
		return result;
	}

	/**
	 * Extracts plain text from an Atlassian Document Format (ADF) JSON element.
	 * Uses an iterative depth-first traversal to concatenate all text nodes.
	 *
	 * @param adfEl the ADF root element (may be null, missing, or non-object)
	 * @return extracted plain text, or empty string if no text found
	 */
	private static String parseAdfToPlainText(JsonElement adfEl) {
		if (adfEl == null || adfEl.isJsonNull() || !adfEl.isJsonObject()) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		List<JsonElement> stack = new ArrayList<>();
		stack.add(adfEl);
		while (!stack.isEmpty()) {
			JsonElement current = stack.remove(stack.size() - 1);
			if (!current.isJsonObject()) {
				continue;
			}
			JsonObject obj = current.getAsJsonObject();
			if (obj.has(ADF_TEXT)) {
				sb.append(obj.get(ADF_TEXT).getAsString()).append(" ");
			}
			if (obj.has(ADF_CONTENT)) {
				JsonElement children = obj.get(ADF_CONTENT);
				if (children.isJsonArray()) {
					JsonArray arr = children.getAsJsonArray();
					for (int i = arr.size() - 1; i >= 0; i--) {
						stack.add(arr.get(i));
					}
				}
			}
		}
		return sb.toString().trim();
	}

	/**
	 * Builds an Atlassian Document Format (ADF) document from plain text. Splits on
	 * double newlines for paragraphs and single newlines for hard breaks.
	 *
	 * @param text plain text input
	 * @return ADF document structure as a Map
	 */
	static Map<String, Object> buildAdfDocument(String text) {
		if (text == null || text.isEmpty()) {
			text = " ";
		}
		final String doc = "doc";
		final String version = "version";
		final String paragraph = "paragraph";
		final String hardBreak = "hardBreak";

		List<Map<String, Object>> paragraphs = new ArrayList<>();
		String[] blocks = text.split("\\n\\n");
		for (String block : blocks) {
			List<Map<String, Object>> inlineContent = new ArrayList<>();
			String[] lines = block.split("\\n");
			for (int i = 0; i < lines.length; i++) {
				if (i > 0) {
					inlineContent.add(Map.of(ADF_TYPE, hardBreak));
				}
				if (!lines[i].isEmpty()) {
					inlineContent.add(Map.of(ADF_TYPE, ADF_TEXT, ADF_TEXT, lines[i]));
				}
			}
			if (!inlineContent.isEmpty()) {
				paragraphs.add(Map.of(ADF_TYPE, paragraph, ADF_CONTENT, inlineContent));
			}
		}
		if (paragraphs.isEmpty()) {
			paragraphs
					.add(Map.of(ADF_TYPE, paragraph, ADF_CONTENT, List.of(Map.of(ADF_TYPE, ADF_TEXT, ADF_TEXT, text))));
		}
		return Map.of(ADF_TYPE, doc, version, 1, ADF_CONTENT, paragraphs);
	}

	/**
	 * Returns the string value of {@code key} in {@code obj}, or {@code ""} if the
	 * key is absent or JSON null.
	 *
	 * @param obj JSON object to read from
	 * @param key field name to look up
	 * @return string value, or {@code ""} if absent or null
	 */
	private static String getText(JsonObject obj, String key) {
		JsonElement el = obj.has(key) ? obj.get(key) : null;
		return (el != null && !el.isJsonNull()) ? el.getAsString() : "";
	}

	/**
	 * Returns the boolean value of {@code key} in {@code obj}, or {@code false} if
	 * the key is absent or JSON null.
	 *
	 * @param obj JSON object to read from
	 * @param key field name to look up
	 * @return boolean value, or {@code false} if absent or null
	 */
	private static boolean getBoolean(JsonObject obj, String key) {
		JsonElement el = obj.has(key) ? obj.get(key) : null;
		return (el != null && !el.isJsonNull()) ? el.getAsBoolean() : false;
	}

	/**
	 * Returns the long value of {@code key} in {@code obj}, or {@code 0} if the key
	 * is absent or JSON null.
	 *
	 * @param obj JSON object to read from
	 * @param key field name to look up
	 * @return long value, or {@code 0} if absent or null
	 */
	private static long getLong(JsonObject obj, String key) {
		JsonElement el = obj.has(key) ? obj.get(key) : null;
		return (el != null && !el.isJsonNull()) ? el.getAsLong() : 0L;
	}
}
