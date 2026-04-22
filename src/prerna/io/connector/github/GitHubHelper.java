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

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.security.HttpHelperUtility;

/**
 * Utility methods for GitHub REST operations used by GitHub reactors.
 */
public final class GitHubHelper {

	private static final Logger classLogger = LogManager.getLogger(GitHubHelper.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final String API_BASE = "https://api.github.com";
	private static final String PATH_USER = "/user";
	private static final String PATH_USERS = "/users/";
	private static final String PATH_USER_REPOS = "/user/repos";
	private static final String PATH_REPOS = "/repos/";
	private static final String PATH_ISSUES = "/issues";
	private static final String PATH_COMMENTS = "/comments";
	private static final String PATH_PULLS = "/pulls";
	private static final String PATH_FILES = "/files";
	private static final String PATH_BRANCHES = "/branches";
	private static final String PATH_GIT_REFS = "/git/refs";
	private static final String PATH_GIT_REF_HEADS = "/git/ref/heads/";
	private static final String PATH_SEARCH_REPOS = "/search/repositories";
	private static final String PATH_SEARCH_ISSUES = "/search/issues";
	private static final String PATH_LABELS = "/labels";
	private static final String PATH_COLLABORATORS = "/collaborators";
	private static final String PATH_USER_ORGS = "/user/orgs";
	private static final String PATH_ORGS = "/orgs/";

	private static final String ACCEPT_GITHUB_JSON = "application/vnd.github+json";
	private static final String API_VERSION_VALUE = "2022-11-28";
	private static final String HEADER_API_VERSION = "X-GitHub-Api-Version";
	private static final String HEADER_USER_AGENT = "User-Agent";
	private static final String USER_AGENT_VALUE = "Semoss-GitHub-Connector/1.0";
	private static final Pattern OWNER_PATTERN = Pattern.compile("^[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})$");
	private static final Pattern REPO_PATTERN = Pattern.compile("^[A-Za-z0-9._-]+$");

	private static final int MAX_TITLE_LENGTH = 1024;
	private static final int MAX_BODY_LENGTH = 65536;
	private static final int DEFAULT_PER_PAGE = 100;

	/**
	 * Utility class; not instantiable.
	 */
	private GitHubHelper() {
	}

	/**
	 * Gets the authenticated GitHub user's profile.
	 *
	 * @param accessToken GitHub OAuth token
	 * @return user profile
	 */
	public static Map<String, Object> getAuthenticatedUser(String accessToken) {
		try {
			validateToken(accessToken);
			Map<String, String> headers = buildHeaders(accessToken);
			String response = HttpHelperUtility.getRequest(API_BASE + PATH_USER, headers, null, null, null);
			return toUserMap(readJson(response));
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to get authenticated GitHub user from endpoint '{}'.", PATH_USER, e);
			throw new SemossPixelException("Failed to get authenticated GitHub user: " + e.getMessage(), e);
		}
	}

	/**
	 * Gets a public GitHub user's profile by username.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param username    GitHub username
	 * @return user profile
	 */
	public static Map<String, Object> getUserByUsername(String accessToken, String username) {
		try {
			validateToken(accessToken);
			String safeUsername = username == null ? null : username.trim();
			if (safeUsername == null || safeUsername.isEmpty()) {
				throw new SemossPixelException("username is required.");
			}
			if (!OWNER_PATTERN.matcher(safeUsername).matches()) {
				throw new SemossPixelException("username '" + safeUsername
						+ "' contains invalid GitHub owner characters. Only alphanumeric characters and hyphens are allowed.");
			}
			Map<String, String> headers = buildHeaders(accessToken);
			String response = HttpHelperUtility.getRequest(API_BASE + PATH_USERS + encode(safeUsername), headers, null,
					null, null);
			return toUserMap(readJson(response));
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to get GitHub user by username: {}", username, e);
			throw new SemossPixelException("Failed to get GitHub user by username: " + e.getMessage(), e);
		}
	}

	/**
	 * Lists repositories for the authenticated user or a provided owner.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       optional owner or organization
	 * @param page        page number
	 * @param perPage     page size
	 * @return repository summaries
	 */
	public static List<Map<String, Object>> listRepositories(String accessToken, String owner, int page, int perPage) {
		try {
			validateToken(accessToken);
			Map<String, String> headers = buildHeaders(accessToken);
			StringBuilder url;
			if (owner == null || owner.trim().isEmpty()) {
				url = new StringBuilder(API_BASE + PATH_USER_REPOS);
			} else {
				owner = owner.trim();
				if (!OWNER_PATTERN.matcher(owner).matches()) {
					throw new SemossPixelException("owner '" + owner
							+ "' contains invalid GitHub owner characters. Only alphanumeric characters and hyphens are allowed.");
				}
				url = new StringBuilder(API_BASE + PATH_USERS + encode(owner) + "/repos");
			}
			appendPagination(url, page, perPage);

			JsonNode root = readJson(HttpHelperUtility.getRequest(url.toString(), headers, null, null, null));
			List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
			for (JsonNode item : root) {
				results.add(toRepositoryMap(item));
			}
			return results;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to list GitHub repositories for owner '{}' (page={}, perPage={}).", owner, page,
					perPage, e);
			throw new SemossPixelException("Failed to list GitHub repositories: " + e.getMessage(), e);
		}
	}

	/**
	 * Searches GitHub repositories.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param query       GitHub search query
	 * @param page        page number
	 * @param perPage     page size
	 * @return search result map with total_count and items
	 */
	public static Map<String, Object> searchRepositories(String accessToken, String query, int page, int perPage) {
		try {
			validateToken(accessToken);
			String safeQuery = query == null ? null : query.trim();
			if (safeQuery == null || safeQuery.isEmpty()) {
				throw new SemossPixelException("query is required.");
			}
			Map<String, String> headers = buildHeaders(accessToken);
			StringBuilder url = new StringBuilder(API_BASE + PATH_SEARCH_REPOS);
			url.append("?q=").append(encode(safeQuery));
			appendPagination(url, page, perPage);

			JsonNode root = readJson(HttpHelperUtility.getRequest(url.toString(), headers, null, null, null));
			List<Map<String, Object>> items = new ArrayList<Map<String, Object>>();
			for (JsonNode item : root.path("items")) {
				items.add(toRepositoryMap(item));
			}

			Map<String, Object> result = new HashMap<String, Object>();
			result.put("total_count", root.path("total_count").asInt());
			result.put("incomplete_results", root.path("incomplete_results").asBoolean(false));
			result.put("items", items);
			return result;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to search GitHub repositories for query '{}' (page={}, perPage={}).", query, page,
					perPage, e);
			throw new SemossPixelException("Failed to search GitHub repositories: " + e.getMessage(), e);
		}
	}

	/**
	 * Lists organizations the authenticated user belongs to.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param page        page number
	 * @param perPage     page size
	 * @return organization summaries
	 */
	public static List<Map<String, Object>> listUserOrganizations(String accessToken, int page, int perPage) {
		try {
			validateToken(accessToken);
			Map<String, String> headers = buildHeaders(accessToken);
			StringBuilder url = new StringBuilder(API_BASE + PATH_USER_ORGS);
			appendPagination(url, page, perPage);

			JsonNode root = readJson(HttpHelperUtility.getRequest(url.toString(), headers, null, null, null));
			List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
			for (JsonNode item : root) {
				results.add(toOrganizationMap(item));
			}
			return results;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to list GitHub organizations (page={}, perPage={}).", page, perPage, e);
			throw new SemossPixelException("Failed to list GitHub organizations: " + e.getMessage(), e);
		}
	}

	/**
	 * Lists repositories for an organization. Returns all repos (including private)
	 * that the authenticated user can access.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param org         organization login name
	 * @param page        page number
	 * @param perPage     page size
	 * @return repository summaries
	 */
	public static List<Map<String, Object>> listOrgRepositories(String accessToken, String org, int page, int perPage) {
		try {
			validateToken(accessToken);
			String safeOrg = org == null ? null : org.trim();
			if (safeOrg == null || safeOrg.isEmpty()) {
				throw new SemossPixelException("owner (organization) is required.");
			}
			if (!OWNER_PATTERN.matcher(safeOrg).matches()) {
				throw new SemossPixelException("owner '" + safeOrg
						+ "' contains invalid characters. Only alphanumeric characters and hyphens are allowed.");
			}
			Map<String, String> headers = buildHeaders(accessToken);
			StringBuilder url = new StringBuilder(API_BASE + PATH_ORGS + encode(safeOrg) + "/repos");
			appendPagination(url, page, perPage);

			JsonNode root = readJson(HttpHelperUtility.getRequest(url.toString(), headers, null, null, null));
			List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
			for (JsonNode item : root) {
				results.add(toRepositoryMap(item));
			}
			return results;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to list GitHub organization repositories for {}", org, e);
			throw new SemossPixelException("Failed to list GitHub organization repositories: " + e.getMessage(), e);
		}
	}

	/**
	 * Lists labels in a repository.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param page        page number
	 * @param perPage     page size
	 * @return label summaries
	 */
	public static List<Map<String, Object>> listLabels(String accessToken, String owner, String repo, int page,
			int perPage) {
		try {
			validateToken(accessToken);
			Map<String, String> headers = buildHeaders(accessToken);
			StringBuilder url = new StringBuilder(buildRepoUrl(owner, repo) + PATH_LABELS);
			appendPagination(url, page, perPage);

			JsonNode root = readJson(HttpHelperUtility.getRequest(url.toString(), headers, null, null, null));
			List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
			for (JsonNode item : root) {
				results.add(toLabelMap(item));
			}
			return results;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to list GitHub labels for {}/{}", owner, repo, e);
			throw new SemossPixelException("Failed to list GitHub labels: " + e.getMessage(), e);
		}
	}

	/**
	 * Lists collaborators in a repository.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param page        page number
	 * @param perPage     page size
	 * @return collaborator summaries
	 */
	public static List<Map<String, Object>> listCollaborators(String accessToken, String owner, String repo, int page,
			int perPage) {
		try {
			validateToken(accessToken);
			Map<String, String> headers = buildHeaders(accessToken);
			StringBuilder url = new StringBuilder(buildRepoUrl(owner, repo) + PATH_COLLABORATORS);
			appendPagination(url, page, perPage);

			JsonNode root = readJson(HttpHelperUtility.getRequest(url.toString(), headers, null, null, null));
			List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
			for (JsonNode item : root) {
				results.add(toCollaboratorMap(item));
			}
			return results;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to list GitHub collaborators for {}/{}", owner, repo, e);
			throw new SemossPixelException("Failed to list GitHub collaborators: " + e.getMessage(), e);
		}
	}

	/**
	 * Lists branches in a repository.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param page        page number
	 * @param perPage     page size
	 * @return branch summaries
	 */
	public static List<Map<String, Object>> listBranches(String accessToken, String owner, String repo, int page,
			int perPage) {
		try {
			validateToken(accessToken);
			Map<String, String> headers = buildHeaders(accessToken);
			StringBuilder url = new StringBuilder(buildRepoUrl(owner, repo) + PATH_BRANCHES);
			appendPagination(url, page, perPage);

			JsonNode root = readJson(HttpHelperUtility.getRequest(url.toString(), headers, null, null, null));
			List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
			for (JsonNode item : root) {
				results.add(toBranchMap(item));
			}
			return results;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to list GitHub branches for {}/{}", owner, repo, e);
			throw new SemossPixelException("Failed to list GitHub branches: " + e.getMessage(), e);
		}
	}

	/**
	 * Creates a new branch from an existing branch or the default branch.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param branch      new branch name
	 * @param fromBranch  source branch name, optional
	 * @return branch creation result
	 */
	public static Map<String, Object> createBranch(String accessToken, String owner, String repo, String branch,
			String fromBranch) {
		try {
			validateToken(accessToken);
			String safeBranch = branch == null ? null : branch.trim();
			if (safeBranch == null || safeBranch.isEmpty()) {
				throw new SemossPixelException("branch is required.");
			}
			Map<String, String> headers = buildHeaders(accessToken);

			String sourceBranch = fromBranch == null ? null : fromBranch.trim();
			if (sourceBranch != null && sourceBranch.isEmpty()) {
				sourceBranch = null;
			}
			if (sourceBranch == null) {
				JsonNode repository = readJson(
						HttpHelperUtility.getRequest(buildRepoUrl(owner, repo), headers, null, null, null));
				sourceBranch = repository.path("default_branch").asText();
				if (sourceBranch == null || sourceBranch.trim().isEmpty()) {
					throw new SemossPixelException("Unable to determine the repository's default branch.");
				}
			}

			String refUrl = buildRepoUrl(owner, repo) + PATH_GIT_REF_HEADS + encode(sourceBranch);
			JsonNode sourceRef = readJson(HttpHelperUtility.getRequest(refUrl, headers, null, null, null));
			String sourceSha = sourceRef.path("object").path("sha").asText();
			if (sourceSha == null || sourceSha.trim().isEmpty()) {
				throw new SemossPixelException("Unable to resolve the source branch SHA for '" + sourceBranch + "'.");
			}

			ObjectNode requestBody = OBJECT_MAPPER.createObjectNode();
			requestBody.put("ref", "refs/heads/" + safeBranch);
			requestBody.put("sha", sourceSha);
			JsonNode createdRef = readJson(
					HttpHelperUtility.postRequestStringBody(buildRepoUrl(owner, repo) + PATH_GIT_REFS, headers,
							toJsonBody(requestBody), ContentType.APPLICATION_JSON, null, null, null));

			Map<String, Object> result = new HashMap<String, Object>();
			result.put("ref", createdRef.path("ref").asText());
			result.put("sha", createdRef.path("object").path("sha").asText());
			result.put("message", "Branch '" + safeBranch + "' created from '" + sourceBranch + "'.");
			return result;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to create GitHub branch {} in {}/{}", branch, owner, repo, e);
			throw new SemossPixelException("Failed to create GitHub branch: " + e.getMessage(), e);
		}
	}

	/**
	 * Lists issues in a repository.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param state       state filter
	 * @param page        page number
	 * @param perPage     page size
	 * @return issue summaries
	 */
	public static List<Map<String, Object>> listIssues(String accessToken, String owner, String repo, String state,
			int page, int perPage) {
		try {
			validateToken(accessToken);
			Map<String, String> headers = buildHeaders(accessToken);
			StringBuilder url = new StringBuilder(buildRepoUrl(owner, repo) + PATH_ISSUES);
			appendQueryParam(url, "state", state);
			appendPagination(url, page, perPage);

			JsonNode root = readJson(HttpHelperUtility.getRequest(url.toString(), headers, null, null, null));
			List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
			for (JsonNode item : root) {
				if (!item.path("pull_request").isMissingNode()) {
					continue;
				}
				results.add(toIssueSummaryMap(item));
			}
			return results;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to list GitHub issues for {}/{}", owner, repo, e);
			throw new SemossPixelException("Failed to list GitHub issues: " + e.getMessage(), e);
		}
	}

	/**
	 * Gets the details for a single issue.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param issueNumber issue number
	 * @return issue detail
	 */
	public static Map<String, Object> getIssue(String accessToken, String owner, String repo, int issueNumber) {
		try {
			validateToken(accessToken);
			Map<String, String> headers = buildHeaders(accessToken);
			JsonNode issueNode = readIssueNode(headers, owner, repo, issueNumber);
			assertIssueNode(issueNode, issueNumber);
			return toIssueDetailMap(issueNode);
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to get GitHub issue {} for {}/{}", issueNumber, owner, repo, e);
			throw new SemossPixelException("Failed to get GitHub issue: " + e.getMessage(), e);
		}
	}

	/**
	 * Gets comments for a GitHub issue.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param issueNumber issue number
	 * @param page        page number
	 * @param perPage     page size
	 * @return issue comments
	 */
	public static List<Map<String, Object>> getIssueComments(String accessToken, String owner, String repo,
			int issueNumber, int page, int perPage) {
		try {
			validateToken(accessToken);
			Map<String, String> headers = buildHeaders(accessToken);
			assertIssueNode(readIssueNode(headers, owner, repo, issueNumber), issueNumber);
			StringBuilder url = new StringBuilder(
					buildRepoUrl(owner, repo) + PATH_ISSUES + "/" + issueNumber + PATH_COMMENTS);
			appendPagination(url, page, perPage);

			JsonNode root = readJson(HttpHelperUtility.getRequest(url.toString(), headers, null, null, null));
			List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
			for (JsonNode item : root) {
				results.add(toCommentMap(item));
			}
			return results;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to get GitHub issue comments for {}/{}#{}", owner, repo, issueNumber, e);
			throw new SemossPixelException("Failed to get GitHub issue comments: " + e.getMessage(), e);
		}
	}

	/**
	 * Creates a GitHub issue.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param title       issue title
	 * @param body        issue body
	 * @param assignees   assignee usernames
	 * @param labels      labels
	 * @return creation result
	 */
	public static Map<String, Object> createIssue(String accessToken, String owner, String repo, String title,
			String body, List<String> assignees, List<String> labels) {
		try {
			validateToken(accessToken);
			String safeTitle = title == null ? null : title.trim();
			if (safeTitle == null || safeTitle.isEmpty()) {
				throw new SemossPixelException("title is required.");
			}
			if (safeTitle.length() > MAX_TITLE_LENGTH) {
				throw new SemossPixelException(
						"title exceeds the maximum length of " + MAX_TITLE_LENGTH + " characters.");
			}
			ObjectNode requestBody = OBJECT_MAPPER.createObjectNode();
			requestBody.put("title", safeTitle);
			if (body != null) {
				if (body.length() > MAX_BODY_LENGTH) {
					throw new SemossPixelException(
							"body exceeds the maximum length of " + MAX_BODY_LENGTH + " characters.");
				}
				requestBody.put("body", body);
			}
			addStringArrayIfPresent(requestBody, "assignees", assignees);
			addStringArrayIfPresent(requestBody, "labels", labels);

			Map<String, String> headers = buildHeaders(accessToken);
			String response = HttpHelperUtility.postRequestStringBody(buildRepoUrl(owner, repo) + PATH_ISSUES, headers,
					toJsonBody(requestBody), ContentType.APPLICATION_JSON, null, null, null);
			return toIssueMutationMap(readJson(response));
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to create GitHub issue for {}/{}", owner, repo, e);
			throw new SemossPixelException("Failed to create GitHub issue: " + e.getMessage(), e);
		}
	}

	/**
	 * Updates an existing GitHub issue.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param issueNumber issue number
	 * @param title       updated title
	 * @param body        updated body
	 * @param state       updated state
	 * @param assignees   updated assignees
	 * @param labels      updated labels
	 * @return update result
	 */
	public static Map<String, Object> updateIssue(String accessToken, String owner, String repo, int issueNumber,
			String title, String body, String state, List<String> assignees, List<String> labels) {
		try {
			validateToken(accessToken);
			Map<String, String> headers = buildHeaders(accessToken);
			assertIssueNode(readIssueNode(headers, owner, repo, issueNumber), issueNumber);
			ObjectNode requestBody = OBJECT_MAPPER.createObjectNode();
			if (title != null) {
				if (title.length() > MAX_TITLE_LENGTH) {
					throw new SemossPixelException(
							"title exceeds the maximum length of " + MAX_TITLE_LENGTH + " characters.");
				}
			}
			if (body != null) {
				if (body.length() > MAX_BODY_LENGTH) {
					throw new SemossPixelException(
							"body exceeds the maximum length of " + MAX_BODY_LENGTH + " characters.");
				}
			}
			putIfNotNull(requestBody, "title", title);
			putIfNotNull(requestBody, "body", body);
			putIfNotNull(requestBody, "state", state);
			addStringArrayIfPresent(requestBody, "assignees", assignees);
			addStringArrayIfPresent(requestBody, "labels", labels);

			if (requestBody.size() == 0) {
				throw new SemossPixelException("No fields provided for update.");
			}

			String response = HttpHelperUtility.patchRequestStringBody(
					buildRepoUrl(owner, repo) + PATH_ISSUES + "/" + issueNumber, headers, toJsonBody(requestBody),
					ContentType.APPLICATION_JSON, null, null, null);
			return toIssueMutationMap(readJson(response));
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to update GitHub issue {} for {}/{}", issueNumber, owner, repo, e);
			throw new SemossPixelException("Failed to update GitHub issue: " + e.getMessage(), e);
		}
	}

	/**
	 * Adds a comment to a GitHub issue.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param issueNumber issue number
	 * @param body        comment text
	 * @return comment result
	 */
	public static Map<String, Object> addIssueComment(String accessToken, String owner, String repo, int issueNumber,
			String body) {
		try {
			validateToken(accessToken);
			Map<String, String> headers = buildHeaders(accessToken);
			assertIssueNode(readIssueNode(headers, owner, repo, issueNumber), issueNumber);
			ObjectNode requestBody = OBJECT_MAPPER.createObjectNode();
			String safeBody = body == null ? null : body.trim();
			if (safeBody == null || safeBody.isEmpty()) {
				throw new SemossPixelException("body is required.");
			}
			if (safeBody.length() > MAX_BODY_LENGTH) {
				throw new SemossPixelException(
						"body exceeds the maximum length of " + MAX_BODY_LENGTH + " characters.");
			}
			requestBody.put("body", safeBody);

			String response = HttpHelperUtility.postRequestStringBody(
					buildRepoUrl(owner, repo) + PATH_ISSUES + "/" + issueNumber + PATH_COMMENTS, headers,
					toJsonBody(requestBody), ContentType.APPLICATION_JSON, null, null, null);
			return toCommentMap(readJson(response));
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to add GitHub issue comment for {}/{}#{}", owner, repo, issueNumber, e);
			throw new SemossPixelException("Failed to add GitHub issue comment: " + e.getMessage(), e);
		}
	}

	/**
	 * Edits an existing comment on a GitHub issue.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param commentId   comment ID
	 * @param body        updated comment text
	 * @return updated comment
	 */
	public static Map<String, Object> editIssueComment(String accessToken, String owner, String repo, long commentId,
			String body) {
		try {
			validateToken(accessToken);
			String safeBody = body == null ? null : body.trim();
			if (safeBody == null || safeBody.isEmpty()) {
				throw new SemossPixelException("body is required.");
			}
			if (safeBody.length() > MAX_BODY_LENGTH) {
				throw new SemossPixelException(
						"body exceeds the maximum length of " + MAX_BODY_LENGTH + " characters.");
			}
			ObjectNode requestBody = OBJECT_MAPPER.createObjectNode();
			requestBody.put("body", safeBody);

			Map<String, String> headers = buildHeaders(accessToken);
			String response = HttpHelperUtility.patchRequestStringBody(
					buildRepoUrl(owner, repo) + PATH_ISSUES + PATH_COMMENTS + "/" + commentId, headers,
					toJsonBody(requestBody), ContentType.APPLICATION_JSON, null, null, null);
			return toCommentMap(readJson(response));
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to edit GitHub issue comment {} for {}/{}", commentId, owner, repo, e);
			throw new SemossPixelException("Failed to edit GitHub issue comment: " + e.getMessage(), e);
		}
	}

	/**
	 * Deletes a comment on a GitHub issue.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param commentId   comment ID
	 * @return deletion result
	 */
	public static Map<String, Object> deleteIssueComment(String accessToken, String owner, String repo,
			long commentId) {
		try {
			validateToken(accessToken);
			Map<String, String> headers = buildHeaders(accessToken);
			HttpHelperUtility.deleteRequestStringBody(
					buildRepoUrl(owner, repo) + PATH_ISSUES + PATH_COMMENTS + "/" + commentId, headers, null, null,
					null);

			Map<String, Object> result = new HashMap<String, Object>();
			result.put("success", true);
			result.put("message", "Comment " + commentId + " deleted successfully.");
			return result;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to delete GitHub issue comment {} for {}/{}", commentId, owner, repo, e);
			throw new SemossPixelException("Failed to delete GitHub issue comment: " + e.getMessage(), e);
		}
	}

	/**
	 * Searches GitHub issues across repositories.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param query       GitHub search query
	 * @param page        page number
	 * @param perPage     page size
	 * @return search result map with total_count and items
	 */
	public static Map<String, Object> searchIssues(String accessToken, String query, int page, int perPage) {
		try {
			validateToken(accessToken);
			String safeQuery = query == null ? null : query.trim();
			if (safeQuery == null || safeQuery.isEmpty()) {
				throw new SemossPixelException("query is required.");
			}
			safeQuery = safeQuery + " type:issue";
			Map<String, String> headers = buildHeaders(accessToken);
			StringBuilder url = new StringBuilder(API_BASE + PATH_SEARCH_ISSUES);
			url.append("?q=").append(encode(safeQuery));
			appendPagination(url, page, perPage);

			JsonNode root = readJson(HttpHelperUtility.getRequest(url.toString(), headers, null, null, null));
			List<Map<String, Object>> items = new ArrayList<Map<String, Object>>();
			for (JsonNode item : root.path("items")) {
				items.add(toSearchIssueMap(item));
			}

			Map<String, Object> result = new HashMap<String, Object>();
			result.put("total_count", root.path("total_count").asInt());
			result.put("incomplete_results", root.path("incomplete_results").asBoolean(false));
			result.put("items", items);
			return result;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to search GitHub issues for query '{}' (page={}, perPage={}).", query, page,
					perPage, e);
			throw new SemossPixelException("Failed to search GitHub issues: " + e.getMessage(), e);
		}
	}

	/**
	 * Lists pull requests in a repository.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param state       state filter
	 * @param page        page number
	 * @param perPage     page size
	 * @return pull request summaries
	 */
	public static List<Map<String, Object>> listPullRequests(String accessToken, String owner, String repo,
			String state, int page, int perPage) {
		try {
			validateToken(accessToken);
			Map<String, String> headers = buildHeaders(accessToken);
			StringBuilder url = new StringBuilder(buildRepoUrl(owner, repo) + PATH_PULLS);
			appendQueryParam(url, "state", state);
			appendPagination(url, page, perPage);

			JsonNode root = readJson(HttpHelperUtility.getRequest(url.toString(), headers, null, null, null));
			List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
			for (JsonNode item : root) {
				results.add(toPullRequestSummaryMap(item));
			}
			return results;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to list GitHub pull requests for {}/{}", owner, repo, e);
			throw new SemossPixelException("Failed to list GitHub pull requests: " + e.getMessage(), e);
		}
	}

	/**
	 * Gets the details for a single pull request.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param pullNumber  pull request number
	 * @return pull request detail
	 */
	public static Map<String, Object> getPullRequest(String accessToken, String owner, String repo, int pullNumber) {
		try {
			validateToken(accessToken);
			Map<String, String> headers = buildHeaders(accessToken);
			String url = buildRepoUrl(owner, repo) + PATH_PULLS + "/" + pullNumber;
			return toPullRequestDetailMap(readJson(HttpHelperUtility.getRequest(url, headers, null, null, null)));
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to get GitHub pull request {} for {}/{}", pullNumber, owner, repo, e);
			throw new SemossPixelException("Failed to get GitHub pull request: " + e.getMessage(), e);
		}
	}

	/**
	 * Gets the changed files in a pull request.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param pullNumber  pull request number
	 * @param page        page number
	 * @param perPage     page size
	 * @return changed file summaries
	 */
	public static List<Map<String, Object>> getPullRequestFiles(String accessToken, String owner, String repo,
			int pullNumber, int page, int perPage) {
		try {
			validateToken(accessToken);
			Map<String, String> headers = buildHeaders(accessToken);
			StringBuilder url = new StringBuilder(
					buildRepoUrl(owner, repo) + PATH_PULLS + "/" + pullNumber + PATH_FILES);
			appendPagination(url, page, perPage);

			JsonNode root = readJson(HttpHelperUtility.getRequest(url.toString(), headers, null, null, null));
			List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
			for (JsonNode item : root) {
				results.add(toPullRequestFileMap(item));
			}
			return results;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to get GitHub pull request files for {}/{}#{}", owner, repo, pullNumber, e);
			throw new SemossPixelException("Failed to get GitHub pull request files: " + e.getMessage(), e);
		}
	}

	/**
	 * Gets conversation comments for a pull request.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param pullNumber  pull request number
	 * @param page        page number
	 * @param perPage     page size
	 * @return conversation comments
	 */
	public static List<Map<String, Object>> getPullRequestComments(String accessToken, String owner, String repo,
			int pullNumber, int page, int perPage) {
		try {
			validateToken(accessToken);
			Map<String, String> headers = buildHeaders(accessToken);
			assertPullRequestExists(headers, owner, repo, pullNumber);
			StringBuilder url = new StringBuilder(
					buildRepoUrl(owner, repo) + PATH_ISSUES + "/" + pullNumber + PATH_COMMENTS);
			appendPagination(url, page, perPage);

			JsonNode root = readJson(HttpHelperUtility.getRequest(url.toString(), headers, null, null, null));
			List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
			for (JsonNode item : root) {
				results.add(toCommentMap(item));
			}
			return results;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to get GitHub pull request comments for {}/{}#{}", owner, repo, pullNumber, e);
			throw new SemossPixelException("Failed to get GitHub pull request comments: " + e.getMessage(), e);
		}
	}

	/**
	 * Creates a pull request.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param title       pull request title
	 * @param body        pull request body
	 * @param head        source branch
	 * @param base        target branch
	 * @param assignees   assignee usernames
	 * @param labels      labels
	 * @return creation result
	 */
	public static Map<String, Object> createPullRequest(String accessToken, String owner, String repo, String title,
			String body, String head, String base, List<String> assignees, List<String> labels) {
		try {
			validateToken(accessToken);
			ObjectNode requestBody = OBJECT_MAPPER.createObjectNode();
			String safeTitle = title == null ? null : title.trim();
			if (safeTitle == null || safeTitle.isEmpty()) {
				throw new SemossPixelException("title is required.");
			}
			if (safeTitle.length() > MAX_TITLE_LENGTH) {
				throw new SemossPixelException(
						"title exceeds the maximum length of " + MAX_TITLE_LENGTH + " characters.");
			}
			String safeHead = head == null ? null : head.trim();
			if (safeHead == null || safeHead.isEmpty()) {
				throw new SemossPixelException("head is required.");
			}
			String safeBase = base == null ? null : base.trim();
			if (safeBase == null || safeBase.isEmpty()) {
				throw new SemossPixelException("base is required.");
			}
			requestBody.put("title", safeTitle);
			requestBody.put("head", safeHead);
			requestBody.put("base", safeBase);
			if (body != null) {
				if (body.length() > MAX_BODY_LENGTH) {
					throw new SemossPixelException(
							"body exceeds the maximum length of " + MAX_BODY_LENGTH + " characters.");
				}
				requestBody.put("body", body);
			}

			Map<String, String> headers = buildHeaders(accessToken);
			String response = HttpHelperUtility.postRequestStringBody(buildRepoUrl(owner, repo) + PATH_PULLS, headers,
					toJsonBody(requestBody), ContentType.APPLICATION_JSON, null, null, null);
			JsonNode createdPr = readJson(response);

			boolean hasAssignees = assignees != null && !assignees.isEmpty();
			boolean hasLabels = labels != null && !labels.isEmpty();
			if (hasAssignees || hasLabels) {
				int prNumber = createdPr.path("number").asInt();
				ObjectNode issueBody = OBJECT_MAPPER.createObjectNode();
				addStringArrayIfPresent(issueBody, "assignees", assignees);
				addStringArrayIfPresent(issueBody, "labels", labels);
				HttpHelperUtility.patchRequestStringBody(buildRepoUrl(owner, repo) + PATH_ISSUES + "/" + prNumber,
						headers, toJsonBody(issueBody), ContentType.APPLICATION_JSON, null, null, null);
			}

			return toPullRequestMutationMap(createdPr);
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to create GitHub pull request for {}/{}", owner, repo, e);
			throw new SemossPixelException("Failed to create GitHub pull request: " + e.getMessage(), e);
		}
	}

	/**
	 * Updates a pull request.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param pullNumber  pull request number
	 * @param title       updated title
	 * @param body        updated body
	 * @param state       updated state
	 * @param assignees   updated assignees
	 * @param labels      updated labels
	 * @return update result
	 */
	public static Map<String, Object> updatePullRequest(String accessToken, String owner, String repo, int pullNumber,
			String title, String body, String state, List<String> assignees, List<String> labels) {
		try {
			validateToken(accessToken);
			ObjectNode requestBody = OBJECT_MAPPER.createObjectNode();
			if (title != null) {
				if (title.length() > MAX_TITLE_LENGTH) {
					throw new SemossPixelException(
							"title exceeds the maximum length of " + MAX_TITLE_LENGTH + " characters.");
				}
			}
			if (body != null) {
				if (body.length() > MAX_BODY_LENGTH) {
					throw new SemossPixelException(
							"body exceeds the maximum length of " + MAX_BODY_LENGTH + " characters.");
				}
			}
			putIfNotNull(requestBody, "title", title);
			putIfNotNull(requestBody, "body", body);
			putIfNotNull(requestBody, "state", state);

			boolean hasAssignees = assignees != null && !assignees.isEmpty();
			boolean hasLabels = labels != null && !labels.isEmpty();

			if (requestBody.size() == 0 && !hasAssignees && !hasLabels) {
				throw new SemossPixelException("No fields provided for update.");
			}

			Map<String, String> headers = buildHeaders(accessToken);
			JsonNode resultNode = null;

			if (requestBody.size() > 0) {
				String response = HttpHelperUtility.patchRequestStringBody(
						buildRepoUrl(owner, repo) + PATH_PULLS + "/" + pullNumber, headers, toJsonBody(requestBody),
						ContentType.APPLICATION_JSON, null, null, null);
				resultNode = readJson(response);
			}

			if (hasAssignees || hasLabels) {
				ObjectNode issueBody = OBJECT_MAPPER.createObjectNode();
				addStringArrayIfPresent(issueBody, "assignees", assignees);
				addStringArrayIfPresent(issueBody, "labels", labels);
				String issueResponse = HttpHelperUtility.patchRequestStringBody(
						buildRepoUrl(owner, repo) + PATH_ISSUES + "/" + pullNumber, headers, toJsonBody(issueBody),
						ContentType.APPLICATION_JSON, null, null, null);
				if (resultNode == null) {
					resultNode = readJson(issueResponse);
				}
			}

			return toPullRequestMutationMap(resultNode);
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to update GitHub pull request {} for {}/{}", pullNumber, owner, repo, e);
			throw new SemossPixelException("Failed to update GitHub pull request: " + e.getMessage(), e);
		}
	}

	/**
	 * Adds a conversation comment to a pull request.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param pullNumber  pull request number
	 * @param body        comment text
	 * @return comment result
	 */
	public static Map<String, Object> addPullRequestComment(String accessToken, String owner, String repo,
			int pullNumber, String body) {
		try {
			validateToken(accessToken);
			Map<String, String> headers = buildHeaders(accessToken);
			assertPullRequestExists(headers, owner, repo, pullNumber);
			ObjectNode requestBody = OBJECT_MAPPER.createObjectNode();
			String safeBody = body == null ? null : body.trim();
			if (safeBody == null || safeBody.isEmpty()) {
				throw new SemossPixelException("body is required.");
			}
			if (safeBody.length() > MAX_BODY_LENGTH) {
				throw new SemossPixelException(
						"body exceeds the maximum length of " + MAX_BODY_LENGTH + " characters.");
			}
			requestBody.put("body", safeBody);

			String response = HttpHelperUtility.postRequestStringBody(
					buildRepoUrl(owner, repo) + PATH_ISSUES + "/" + pullNumber + PATH_COMMENTS, headers,
					toJsonBody(requestBody), ContentType.APPLICATION_JSON, null, null, null);
			return toCommentMap(readJson(response));
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to add GitHub pull request comment for {}/{}#{}", owner, repo, pullNumber, e);
			throw new SemossPixelException("Failed to add GitHub pull request comment: " + e.getMessage(), e);
		}
	}

	/**
	 * Edits an existing comment on a GitHub pull request.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param commentId   comment ID
	 * @param body        updated comment text
	 * @return updated comment
	 */
	public static Map<String, Object> editPullRequestComment(String accessToken, String owner, String repo,
			long commentId, String body) {
		try {
			validateToken(accessToken);
			String safeBody = body == null ? null : body.trim();
			if (safeBody == null || safeBody.isEmpty()) {
				throw new SemossPixelException("body is required.");
			}
			if (safeBody.length() > MAX_BODY_LENGTH) {
				throw new SemossPixelException(
						"body exceeds the maximum length of " + MAX_BODY_LENGTH + " characters.");
			}
			ObjectNode requestBody = OBJECT_MAPPER.createObjectNode();
			requestBody.put("body", safeBody);

			Map<String, String> headers = buildHeaders(accessToken);
			String response = HttpHelperUtility.patchRequestStringBody(
					buildRepoUrl(owner, repo) + PATH_ISSUES + PATH_COMMENTS + "/" + commentId, headers,
					toJsonBody(requestBody), ContentType.APPLICATION_JSON, null, null, null);
			return toCommentMap(readJson(response));
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to edit GitHub pull request comment {} for {}/{}", commentId, owner, repo, e);
			throw new SemossPixelException("Failed to edit GitHub pull request comment: " + e.getMessage(), e);
		}
	}

	/**
	 * Deletes a comment on a GitHub pull request.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param commentId   comment ID
	 * @return deletion result
	 */
	public static Map<String, Object> deletePullRequestComment(String accessToken, String owner, String repo,
			long commentId) {
		try {
			validateToken(accessToken);
			Map<String, String> headers = buildHeaders(accessToken);
			HttpHelperUtility.deleteRequestStringBody(
					buildRepoUrl(owner, repo) + PATH_ISSUES + PATH_COMMENTS + "/" + commentId, headers, null, null,
					null);

			Map<String, Object> result = new HashMap<String, Object>();
			result.put("success", true);
			result.put("message", "Comment " + commentId + " deleted successfully.");
			return result;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to delete GitHub pull request comment {} for {}/{}", commentId, owner, repo, e);
			throw new SemossPixelException("Failed to delete GitHub pull request comment: " + e.getMessage(), e);
		}
	}

	/**
	 * Searches pull requests across repositories.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param query       GitHub search query
	 * @param page        page number
	 * @param perPage     page size
	 * @return search result map with total_count and items
	 */
	public static Map<String, Object> searchPullRequests(String accessToken, String query, int page, int perPage) {
		try {
			validateToken(accessToken);
			String safeQuery = query == null ? null : query.trim();
			if (safeQuery == null || safeQuery.isEmpty()) {
				throw new SemossPixelException("query is required.");
			}
			safeQuery = safeQuery + " type:pr";
			Map<String, String> headers = buildHeaders(accessToken);
			StringBuilder url = new StringBuilder(API_BASE + PATH_SEARCH_ISSUES);
			url.append("?q=").append(encode(safeQuery));
			appendPagination(url, page, perPage);

			JsonNode root = readJson(HttpHelperUtility.getRequest(url.toString(), headers, null, null, null));
			List<Map<String, Object>> items = new ArrayList<Map<String, Object>>();
			for (JsonNode item : root.path("items")) {
				items.add(toSearchPullRequestMap(item));
			}

			Map<String, Object> result = new HashMap<String, Object>();
			result.put("total_count", root.path("total_count").asInt());
			result.put("incomplete_results", root.path("incomplete_results").asBoolean(false));
			result.put("items", items);
			return result;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to search GitHub pull requests for query '{}' (page={}, perPage={}).", query,
					page, perPage, e);
			throw new SemossPixelException("Failed to search GitHub pull requests: " + e.getMessage(), e);
		}
	}

	/**
	 * Deletes a branch from a repository. Only allowed when the authenticated user
	 * is the repository owner and the branch is not the default or a protected
	 * branch.
	 *
	 * @param accessToken GitHub OAuth token
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param branch      branch name to delete
	 * @return deletion result
	 */
	public static Map<String, Object> deleteBranch(String accessToken, String owner, String repo, String branch) {
		try {
			validateToken(accessToken);
			String safeBranch = branch == null ? null : branch.trim();
			if (safeBranch == null || safeBranch.isEmpty()) {
				throw new SemossPixelException("branch is required.");
			}
			Map<String, String> headers = buildHeaders(accessToken);

			// Verify the authenticated user is the repo owner
			JsonNode userNode = readJson(HttpHelperUtility.getRequest(API_BASE + PATH_USER, headers, null, null, null));
			String authenticatedLogin = nullableText(userNode.path("login"));
			if (authenticatedLogin == null || !authenticatedLogin.equalsIgnoreCase(owner)) {
				throw new SemossPixelException(
						"Branch deletion is only allowed on repositories owned by the authenticated user.");
			}

			// Verify the branch exists and is not the default branch
			JsonNode repoNode = readJson(
					HttpHelperUtility.getRequest(buildRepoUrl(owner, repo), headers, null, null, null));
			String defaultBranch = nullableText(repoNode.path("default_branch"));
			if (safeBranch.equals(defaultBranch)) {
				throw new SemossPixelException("Cannot delete the default branch '" + safeBranch + "'.");
			}

			// Verify the branch is not protected
			String branchUrl = buildRepoUrl(owner, repo) + PATH_BRANCHES + "/" + encode(safeBranch);
			JsonNode branchNode = readJson(HttpHelperUtility.getRequest(branchUrl, headers, null, null, null));
			if (branchNode.path("protected").asBoolean(false)) {
				throw new SemossPixelException("Cannot delete protected branch '" + safeBranch + "'.");
			}

			// Delete the branch via the git refs API
			String refUrl = buildRepoUrl(owner, repo) + "/git/refs/heads/" + encode(safeBranch);
			HttpHelperUtility.deleteRequestStringBody(refUrl, headers, null, null, null);

			Map<String, Object> result = new HashMap<String, Object>();
			result.put("success", true);
			result.put("message", "Branch '" + safeBranch + "' deleted successfully.");
			return result;
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to delete GitHub branch {} in {}/{}", branch, owner, repo, e);
			throw new SemossPixelException("Failed to delete GitHub branch: " + e.getMessage(), e);
		}
	}

	/**
	 * Builds standard GitHub API request headers.
	 *
	 * @param accessToken GitHub OAuth token
	 * @return request headers map
	 */
	private static Map<String, String> buildHeaders(String accessToken) {
		Map<String, String> headers = new HashMap<String, String>();
		headers.put(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken);
		headers.put(HttpHeaders.ACCEPT, ACCEPT_GITHUB_JSON);
		headers.put(HEADER_API_VERSION, API_VERSION_VALUE);
		headers.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
		headers.put(HEADER_USER_AGENT, USER_AGENT_VALUE);
		return headers;
	}

	/**
	 * Validates and builds the repository API URL for an owner/repository pair.
	 *
	 * @param owner repository owner
	 * @param repo  repository name
	 * @return API URL for the repository
	 */
	private static String buildRepoUrl(String owner, String repo) {
		String safeOwner = owner == null ? null : owner.trim();
		if (safeOwner == null || safeOwner.isEmpty()) {
			throw new SemossPixelException("owner is required.");
		}
		if (!OWNER_PATTERN.matcher(safeOwner).matches()) {
			throw new SemossPixelException("owner '" + safeOwner
					+ "' contains invalid GitHub owner characters. Only alphanumeric characters and hyphens are allowed.");
		}

		String safeRepo = repo == null ? null : repo.trim();
		if (safeRepo == null || safeRepo.isEmpty()) {
			throw new SemossPixelException("repo is required.");
		}
		if (".".equals(safeRepo) || "..".equals(safeRepo) || !REPO_PATTERN.matcher(safeRepo).matches()) {
			throw new SemossPixelException("repo '" + safeRepo
					+ "' contains invalid GitHub repository characters. Only alphanumeric characters, dots, hyphens, and underscores are allowed.");
		}

		return API_BASE + PATH_REPOS + encode(safeOwner) + "/" + encode(safeRepo);
	}

	/**
	 * Validates that an access token is present.
	 *
	 * @param accessToken GitHub OAuth token
	 */
	private static void validateToken(String accessToken) {
		if (accessToken == null || accessToken.trim().isEmpty()) {
			throw new SemossPixelException("GitHub access token must not be empty.");
		}
	}

	/**
	 * Appends normalized page and per-page values to a URL.
	 *
	 * @param url     URL builder to update
	 * @param page    requested page number
	 * @param perPage requested page size
	 */
	private static void appendPagination(StringBuilder url, int page, int perPage) {
		url.append(url.indexOf("?") >= 0 ? "&" : "?");
		url.append("page=").append(normalizePage(page));
		url.append("&per_page=").append(normalizePerPage(perPage));
	}

	/**
	 * Appends a query parameter when the value is non-empty.
	 *
	 * @param url   URL builder to update
	 * @param key   query parameter key
	 * @param value query parameter value
	 */
	private static void appendQueryParam(StringBuilder url, String key, String value) {
		if (value == null || value.trim().isEmpty()) {
			return;
		}
		url.append(url.indexOf("?") >= 0 ? "&" : "?");
		url.append(key).append("=").append(encode(value.trim()));
	}

	/**
	 * Serializes a JSON object node into a request body string.
	 *
	 * @param node JSON request body
	 * @return serialized JSON string
	 */
	private static String toJsonBody(ObjectNode node) {
		try {
			return OBJECT_MAPPER.writeValueAsString(node);
		} catch (Exception e) {
			throw new SemossPixelException("Failed to serialize request body: " + e.getMessage(), e);
		}
	}

	/**
	 * Parses a JSON response string. Empty responses return an empty object node.
	 *
	 * @param response raw response body
	 * @return parsed JSON node
	 */
	private static JsonNode readJson(String response) {
		try {
			if (response == null || response.trim().isEmpty()) {
				return OBJECT_MAPPER.createObjectNode();
			}
			return OBJECT_MAPPER.readTree(response);
		} catch (Exception e) {
			throw new SemossPixelException("Failed to parse GitHub API response: " + e.getMessage(), e);
		}
	}

	/**
	 * URL-encodes a value for safe use in path and query segments.
	 *
	 * @param value raw value
	 * @return encoded value
	 */
	private static String encode(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8);
	}

	/**
	 * Normalizes page values so they are always at least 1.
	 *
	 * @param page requested page number
	 * @return normalized page number
	 */
	private static int normalizePage(int page) {
		return page <= 0 ? 1 : page;
	}

	/**
	 * Normalizes per-page values to the supported GitHub range.
	 *
	 * @param perPage requested page size
	 * @return normalized page size
	 */
	private static int normalizePerPage(int perPage) {
		if (perPage <= 0) {
			return DEFAULT_PER_PAGE;
		}
		return Math.min(perPage, 100);
	}

	/**
	 * Adds a string field to a JSON body only when the value is non-null.
	 *
	 * @param requestBody body being built
	 * @param key         JSON field name
	 * @param value       field value
	 */
	private static void putIfNotNull(ObjectNode requestBody, String key, String value) {
		if (value != null) {
			requestBody.put(key, value);
		}
	}

	/**
	 * Adds a string array field to a JSON body when values are provided.
	 *
	 * @param requestBody body being built
	 * @param key         JSON field name
	 * @param values      values to add
	 */
	private static void addStringArrayIfPresent(ObjectNode requestBody, String key, List<String> values) {
		if (values == null) {
			return;
		}
		ArrayNode array = requestBody.putArray(key);
		for (String value : values) {
			array.add(value);
		}
	}

	/**
	 * Maps a GitHub user response node into connector output fields.
	 *
	 * @param userNode GitHub user JSON node
	 * @return mapped user details
	 */
	private static Map<String, Object> toUserMap(JsonNode userNode) {
		Map<String, Object> userMap = new HashMap<String, Object>();
		userMap.put("login", nullableText(userNode.path("login")));
		userMap.put("name", nullableText(userNode.path("name")));
		userMap.put("email", nullableText(userNode.path("email")));
		userMap.put("bio", nullableText(userNode.path("bio")));
		userMap.put("avatar_url", nullableText(userNode.path("avatar_url")));
		userMap.put("public_repos", userNode.path("public_repos").asInt());
		userMap.put("followers", userNode.path("followers").asInt());
		userMap.put("following", userNode.path("following").asInt());
		userMap.put("html_url", nullableText(userNode.path("html_url")));
		userMap.put("created_at", nullableText(userNode.path("created_at")));
		userMap.put("company", nullableText(userNode.path("company")));
		userMap.put("location", nullableText(userNode.path("location")));
		return userMap;
	}

	/**
	 * Maps a GitHub repository node into connector output fields.
	 *
	 * @param repoNode repository JSON node
	 * @return mapped repository summary
	 */
	private static Map<String, Object> toRepositoryMap(JsonNode repoNode) {
		Map<String, Object> repoMap = new HashMap<String, Object>();
		repoMap.put("id", repoNode.path("id").asLong());
		repoMap.put("name", nullableText(repoNode.path("name")));
		repoMap.put("full_name", nullableText(repoNode.path("full_name")));
		repoMap.put("description", nullableText(repoNode.path("description")));
		repoMap.put("private", repoNode.path("private").asBoolean(false));
		repoMap.put("html_url", nullableText(repoNode.path("html_url")));
		repoMap.put("default_branch", nullableText(repoNode.path("default_branch")));
		repoMap.put("language", nullableText(repoNode.path("language")));
		repoMap.put("updated_at", nullableText(repoNode.path("updated_at")));
		repoMap.put("stargazers_count", repoNode.path("stargazers_count").asInt());
		repoMap.put("owner", nullableText(repoNode.path("owner").path("login")));
		return repoMap;
	}

	/**
	 * Maps a GitHub organization node into connector output fields.
	 *
	 * @param orgNode organization JSON node
	 * @return mapped organization summary
	 */
	private static Map<String, Object> toOrganizationMap(JsonNode orgNode) {
		Map<String, Object> orgMap = new HashMap<String, Object>();
		orgMap.put("login", nullableText(orgNode.path("login")));
		orgMap.put("description", nullableText(orgNode.path("description")));
		orgMap.put("avatar_url", nullableText(orgNode.path("avatar_url")));
		return orgMap;
	}

	/**
	 * Maps a GitHub branch node into connector output fields.
	 *
	 * @param branchNode branch JSON node
	 * @return mapped branch summary
	 */
	private static Map<String, Object> toBranchMap(JsonNode branchNode) {
		Map<String, Object> branchMap = new HashMap<String, Object>();
		branchMap.put("name", nullableText(branchNode.path("name")));
		branchMap.put("protected", branchNode.path("protected").asBoolean(false));
		branchMap.put("commit_sha", nullableText(branchNode.path("commit").path("sha")));
		return branchMap;
	}

	/**
	 * Maps a pull request node into summary fields.
	 *
	 * @param pullRequestNode pull request JSON node
	 * @return mapped pull request summary
	 */
	private static Map<String, Object> toPullRequestSummaryMap(JsonNode pullRequestNode) {
		Map<String, Object> pullRequestMap = new HashMap<String, Object>();
		pullRequestMap.put("number", pullRequestNode.path("number").asInt());
		pullRequestMap.put("title", nullableText(pullRequestNode.path("title")));
		pullRequestMap.put("state", nullableText(pullRequestNode.path("state")));
		pullRequestMap.put("user", nullableText(pullRequestNode.path("user").path("login")));
		pullRequestMap.put("head_ref", nullableText(pullRequestNode.path("head").path("ref")));
		pullRequestMap.put("head_sha", nullableText(pullRequestNode.path("head").path("sha")));
		pullRequestMap.put("base_ref", nullableText(pullRequestNode.path("base").path("ref")));
		pullRequestMap.put("draft", pullRequestNode.path("draft").asBoolean(false));
		pullRequestMap.put("created_at", nullableText(pullRequestNode.path("created_at")));
		pullRequestMap.put("updated_at", nullableText(pullRequestNode.path("updated_at")));
		pullRequestMap.put("html_url", nullableText(pullRequestNode.path("html_url")));
		return pullRequestMap;
	}

	/**
	 * Maps a pull request node into detailed fields.
	 *
	 * @param pullRequestNode pull request JSON node
	 * @return mapped pull request detail
	 */
	private static Map<String, Object> toPullRequestDetailMap(JsonNode pullRequestNode) {
		Map<String, Object> pullRequestMap = toPullRequestSummaryMap(pullRequestNode);
		pullRequestMap.put("body", nullableText(pullRequestNode.path("body")));
		pullRequestMap.put("mergeable", nullableBoolean(pullRequestNode.path("mergeable")));
		pullRequestMap.put("merged", pullRequestNode.path("merged").asBoolean(false));
		pullRequestMap.put("additions", pullRequestNode.path("additions").asInt());
		pullRequestMap.put("deletions", pullRequestNode.path("deletions").asInt());
		pullRequestMap.put("changed_files", pullRequestNode.path("changed_files").asInt());
		pullRequestMap.put("requested_reviewers", toLoginList(pullRequestNode.path("requested_reviewers")));
		return pullRequestMap;
	}

	/**
	 * Maps a pull request node into fields returned after create/update actions.
	 *
	 * @param pullRequestNode pull request JSON node
	 * @return mapped mutation result
	 */
	private static Map<String, Object> toPullRequestMutationMap(JsonNode pullRequestNode) {
		Map<String, Object> pullRequestMap = new HashMap<String, Object>();
		pullRequestMap.put("number", pullRequestNode.path("number").asInt());
		pullRequestMap.put("title", nullableText(pullRequestNode.path("title")));
		pullRequestMap.put("state", nullableText(pullRequestNode.path("state")));
		pullRequestMap.put("draft", pullRequestNode.path("draft").asBoolean(false));
		pullRequestMap.put("html_url", nullableText(pullRequestNode.path("html_url")));
		return pullRequestMap;
	}

	/**
	 * Maps a pull request file node into connector output fields.
	 *
	 * @param fileNode pull request file JSON node
	 * @return mapped changed-file summary
	 */
	private static Map<String, Object> toPullRequestFileMap(JsonNode fileNode) {
		Map<String, Object> fileMap = new HashMap<String, Object>();
		fileMap.put("filename", nullableText(fileNode.path("filename")));
		fileMap.put("status", nullableText(fileNode.path("status")));
		fileMap.put("additions", fileNode.path("additions").asInt());
		fileMap.put("deletions", fileNode.path("deletions").asInt());
		fileMap.put("changes", fileNode.path("changes").asInt());
		fileMap.put("patch", nullableText(fileNode.path("patch")));
		return fileMap;
	}

	/**
	 * Maps an issue node into summary fields.
	 *
	 * @param issueNode issue JSON node
	 * @return mapped issue summary
	 */
	private static Map<String, Object> toIssueSummaryMap(JsonNode issueNode) {
		Map<String, Object> issueMap = new HashMap<String, Object>();
		issueMap.put("number", issueNode.path("number").asInt());
		issueMap.put("title", nullableText(issueNode.path("title")));
		issueMap.put("state", nullableText(issueNode.path("state")));
		issueMap.put("user", nullableText(issueNode.path("user").path("login")));
		issueMap.put("assignees", toLoginList(issueNode.path("assignees")));
		issueMap.put("labels", toNamedList(issueNode.path("labels"), "name"));
		issueMap.put("created_at", nullableText(issueNode.path("created_at")));
		issueMap.put("updated_at", nullableText(issueNode.path("updated_at")));
		issueMap.put("html_url", nullableText(issueNode.path("html_url")));
		issueMap.put("comments", issueNode.path("comments").asInt());
		return issueMap;
	}

	/**
	 * Maps an issue node into detailed fields.
	 *
	 * @param issueNode issue JSON node
	 * @return mapped issue detail
	 */
	private static Map<String, Object> toIssueDetailMap(JsonNode issueNode) {
		Map<String, Object> issueMap = toIssueSummaryMap(issueNode);
		issueMap.put("body", nullableText(issueNode.path("body")));
		issueMap.put("state_reason", nullableText(issueNode.path("state_reason")));
		issueMap.put("closed_at", nullableText(issueNode.path("closed_at")));

		JsonNode milestoneNode = issueNode.path("milestone");
		if (!milestoneNode.isMissingNode() && !milestoneNode.isNull()) {
			Map<String, Object> milestoneMap = new HashMap<String, Object>();
			milestoneMap.put("number", milestoneNode.path("number").asInt());
			milestoneMap.put("title", nullableText(milestoneNode.path("title")));
			milestoneMap.put("state", nullableText(milestoneNode.path("state")));
			milestoneMap.put("due_on", nullableText(milestoneNode.path("due_on")));
			issueMap.put("milestone", milestoneMap);
		}

		return issueMap;
	}

	/**
	 * Maps an issue node into fields returned after create/update actions.
	 *
	 * @param issueNode issue JSON node
	 * @return mapped mutation result
	 */
	private static Map<String, Object> toIssueMutationMap(JsonNode issueNode) {
		Map<String, Object> issueMap = new HashMap<String, Object>();
		issueMap.put("number", issueNode.path("number").asInt());
		issueMap.put("title", nullableText(issueNode.path("title")));
		issueMap.put("state", nullableText(issueNode.path("state")));
		issueMap.put("html_url", nullableText(issueNode.path("html_url")));
		return issueMap;
	}

	/**
	 * Maps an issue or pull request comment node into connector output fields.
	 *
	 * @param commentNode comment JSON node
	 * @return mapped comment
	 */
	private static Map<String, Object> toCommentMap(JsonNode commentNode) {
		Map<String, Object> commentMap = new HashMap<String, Object>();
		commentMap.put("id", commentNode.path("id").asLong());
		commentMap.put("user", nullableText(commentNode.path("user").path("login")));
		commentMap.put("body", nullableText(commentNode.path("body")));
		commentMap.put("created_at", nullableText(commentNode.path("created_at")));
		commentMap.put("updated_at", nullableText(commentNode.path("updated_at")));
		commentMap.put("html_url", nullableText(commentNode.path("html_url")));
		return commentMap;
	}

	/**
	 * Maps a label node into connector output fields.
	 *
	 * @param labelNode label JSON node
	 * @return mapped label
	 */
	private static Map<String, Object> toLabelMap(JsonNode labelNode) {
		Map<String, Object> labelMap = new HashMap<String, Object>();
		labelMap.put("name", nullableText(labelNode.path("name")));
		labelMap.put("color", nullableText(labelNode.path("color")));
		labelMap.put("description", nullableText(labelNode.path("description")));
		return labelMap;
	}

	/**
	 * Maps a collaborator node into connector output fields.
	 *
	 * @param collaboratorNode collaborator JSON node
	 * @return mapped collaborator
	 */
	private static Map<String, Object> toCollaboratorMap(JsonNode collaboratorNode) {
		Map<String, Object> collaboratorMap = new HashMap<String, Object>();
		collaboratorMap.put("login", nullableText(collaboratorNode.path("login")));
		collaboratorMap.put("avatar_url", nullableText(collaboratorNode.path("avatar_url")));
		collaboratorMap.put("role_name", nullableText(collaboratorNode.path("role_name")));
		return collaboratorMap;
	}

	/**
	 * Maps a search issue result node into connector output fields.
	 *
	 * @param issueNode search result issue node
	 * @return mapped issue search result
	 */
	private static Map<String, Object> toSearchIssueMap(JsonNode issueNode) {
		Map<String, Object> issueMap = new HashMap<String, Object>();
		issueMap.put("number", issueNode.path("number").asInt());
		issueMap.put("title", nullableText(issueNode.path("title")));
		issueMap.put("state", nullableText(issueNode.path("state")));
		issueMap.put("user", nullableText(issueNode.path("user").path("login")));
		issueMap.put("html_url", nullableText(issueNode.path("html_url")));
		issueMap.put("created_at", nullableText(issueNode.path("created_at")));
		issueMap.put("repository_url", nullableText(issueNode.path("repository_url")));
		return issueMap;
	}

	/**
	 * Maps a search pull request result node into connector output fields.
	 *
	 * @param pullRequestNode search result pull request node
	 * @return mapped pull request search result
	 */
	private static Map<String, Object> toSearchPullRequestMap(JsonNode pullRequestNode) {
		Map<String, Object> pullRequestMap = new HashMap<String, Object>();
		pullRequestMap.put("number", pullRequestNode.path("number").asInt());
		pullRequestMap.put("title", nullableText(pullRequestNode.path("title")));
		pullRequestMap.put("state", nullableText(pullRequestNode.path("state")));
		pullRequestMap.put("user", nullableText(pullRequestNode.path("user").path("login")));
		pullRequestMap.put("html_url", nullableText(pullRequestNode.path("html_url")));
		pullRequestMap.put("created_at", nullableText(pullRequestNode.path("created_at")));
		pullRequestMap.put("updated_at", nullableText(pullRequestNode.path("updated_at")));
		pullRequestMap.put("repository_url", nullableText(pullRequestNode.path("repository_url")));
		pullRequestMap.put("pull_request_url", nullableText(pullRequestNode.path("pull_request").path("url")));
		return pullRequestMap;
	}

	/**
	 * Extracts login values from a JSON user array node.
	 *
	 * @param usersNode users JSON array
	 * @return list of login values
	 */
	private static List<String> toLoginList(JsonNode usersNode) {
		return toNamedList(usersNode, "login");
	}

	/**
	 * Reads an issue node for a repository and issue number.
	 *
	 * @param headers     request headers
	 * @param owner       repository owner
	 * @param repo        repository name
	 * @param issueNumber issue number
	 * @return issue JSON node
	 */
	private static JsonNode readIssueNode(Map<String, String> headers, String owner, String repo, int issueNumber) {
		String issueUrl = buildRepoUrl(owner, repo) + PATH_ISSUES + "/" + issueNumber;
		return readJson(HttpHelperUtility.getRequest(issueUrl, headers, null, null, null));
	}

	/**
	 * Validates that an issue node is not a pull request node.
	 *
	 * @param issueNode   issue JSON node
	 * @param issueNumber issue number used for errors
	 */
	private static void assertIssueNode(JsonNode issueNode, int issueNumber) {
		if (!issueNode.path("pull_request").isMissingNode()) {
			throw new SemossPixelException("issueNumber '" + issueNumber
					+ "' refers to a pull request. Use GitHubPullRequestReactor instead.");
		}
	}

	/**
	 * Validates that a pull request exists by issuing a read request.
	 *
	 * @param headers    request headers
	 * @param owner      repository owner
	 * @param repo       repository name
	 * @param pullNumber pull request number
	 */
	private static void assertPullRequestExists(Map<String, String> headers, String owner, String repo,
			int pullNumber) {
		String pullRequestUrl = buildRepoUrl(owner, repo) + PATH_PULLS + "/" + pullNumber;
		HttpHelperUtility.getRequest(pullRequestUrl, headers, null, null, null);
	}

	/**
	 * Extracts values for a field from each element in an array node.
	 *
	 * @param itemsNode JSON array node
	 * @param fieldName field name to extract from each item
	 * @return list of extracted values
	 */
	private static List<String> toNamedList(JsonNode itemsNode, String fieldName) {
		List<String> values = new ArrayList<String>();
		if (itemsNode != null && itemsNode.isArray()) {
			for (JsonNode item : itemsNode) {
				String value = nullableText(item.path(fieldName));
				if (value != null) {
					values.add(value);
				}
			}
		}
		return values;
	}

	/**
	 * Returns the node text or {@code null} when node is missing/null/empty.
	 *
	 * @param node JSON node
	 * @return text value or {@code null}
	 */
	private static String nullableText(JsonNode node) {
		if (node == null || node.isMissingNode() || node.isNull()) {
			return null;
		}
		String value = node.asText();
		return value == null || value.isEmpty() ? null : value;
	}

	/**
	 * Returns the node boolean or {@code null} when node is missing or null.
	 *
	 * @param node JSON node
	 * @return boolean value or {@code null}
	 */
	private static Boolean nullableBoolean(JsonNode node) {
		if (node == null || node.isMissingNode() || node.isNull()) {
			return null;
		}
		return Boolean.valueOf(node.asBoolean());
	}
}
