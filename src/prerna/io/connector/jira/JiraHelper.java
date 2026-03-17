package prerna.io.connector.jira;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.HttpHelperUtility;

public class JiraHelper {

	private static final Logger classLogger = LogManager.getLogger(JiraHelper.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final String JIRA_ISSUE_URL = "/rest/api/3/issue";
	private static final String JIRA_PROJECT_URL = "/rest/api/3/project";

	private static final String COMMENT_SUFFIX = "/comment";
	private static final String TRANSITIONS_SUFFIX = "/transitions";

	private static final String ACCOUNT_ID = "accountId";
	private static final String ASSIGNEE = "assignee";
	private static final String AUTHOR = "author";
	private static final String BODY = "body";
	private static final String COMMENTS = "comments";
	private static final String CONTENT = "content";
	private static final String CREATED = "created";
	private static final String DESCRIPTION = "description";
	private static final String DISPLAY_NAME = "displayName";
	private static final String DUE_DATE = "duedate";
	private static final String FIELDS = "fields";
	private static final String ID = "id";
	private static final String ISSUES = "issues";
	private static final String ISSUE_TYPE = "issuetype";
	private static final String JQL = "jql";
	private static final String KEY = "key";
	private static final String LABELS = "labels";
	private static final String LEAD = "lead";
	private static final String MAX_RESULTS_KEY = "maxResults";
	private static final String NAME = "name";
	private static final String PRIORITY = "priority";
	private static final String PROJECT = "project";
	private static final String SELF = "self";
	private static final String STATUS = "status";
	private static final String SUCCESS = "success";
	private static final String SUMMARY = "summary";
	private static final String TEXT = "text";
	private static final String TO = "to";
	private static final String TRANSITION = "transition";
	private static final String NEXT_PAGE_TOKEN = "nextPageToken";
	private static final String TRANSITIONS = "transitions";
	private static final String TYPE = "type";

	private static final String UNASSIGNED = "Unassigned";

	private JiraHelper() {
	}

	/**
	 *
	 * @param user
	 * @return
	 */
	private static String[] getCredentials(User user) {
		final String jiraApiBase = "https://api.atlassian.com/ex/jira/";
		if (user == null) {
			throw new SemossPixelException("User session is null. Please log in with Jira.");
		}
		AccessToken jiraToken = user.getResourceAccessToken(AuthProvider.JIRA);
		if (jiraToken == null) {
			throw new SemossPixelException("No Jira access token found. Please connect your Jira account.");
		}
		String accessToken = jiraToken.getAccess_token();
		if (accessToken == null || accessToken.trim().isEmpty()) {
			throw new SemossPixelException("Jira access token is empty. Please reconnect your Jira account.");
		}
		String cloudId = jiraToken.getId();
		if (cloudId == null || cloudId.trim().isEmpty()) {
			throw new SemossPixelException("Jira Cloud ID not found on token. Please reconnect your Jira account.");
		}
		return new String[] { accessToken, jiraApiBase + cloudId };
	}

	/**
	 *
	 * @param accessToken
	 * @return
	 */
	private static Map<String, String> buildHeaders(String accessToken) {
		Map<String, String> headers = new HashMap<>();
		headers.put(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken);
		headers.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
		headers.put(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());
		return headers;
	}

	/**
	 *
	 * @param user
	 * @return
	 */
	public static NounMetadata getAllProjects(User user) {
		try {
			final String projectTypeKey = "projectTypeKey";
			String[] creds = getCredentials(user);
			Map<String, String> headers = buildHeaders(creds[0]);
			String response = HttpHelperUtility.getRequest(creds[1] + JIRA_PROJECT_URL, headers, null, null, null);

			JsonNode projectArray = OBJECT_MAPPER.readTree(response);
			List<Map<String, Object>> projects = new ArrayList<>();
			for (JsonNode p : projectArray) {
				Map<String, Object> proj = new HashMap<>();
				proj.put(ID, p.path(ID).asText());
				proj.put(KEY, p.path(KEY).asText());
				proj.put(NAME, p.path(NAME).asText());
				proj.put(projectTypeKey, p.path(projectTypeKey).asText());
				if (p.has(LEAD)) {
					proj.put(LEAD, p.path(LEAD).path(DISPLAY_NAME).asText());
				}
				projects.add(proj);
			}
			return new NounMetadata(projects, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in getAllProjects: {}", e.getMessage(), e);
			throw new SemossPixelException("Failed to retrieve Jira projects. Error: " + e.getMessage());
		}
	}

	/**
	 *
	 * @param user
	 * @param projectKey
	 * @return
	 */
	public static NounMetadata getIssueTypes(User user, String projectKey) {
		try {
			final String jiraIssueTypeUrl = "/rest/api/3/issuetype/project";
			final String jiraIssueTypeGlobalUrl = "/rest/api/3/issuetype";
			final String subtask = "subtask";
			String[] creds = getCredentials(user);
			Map<String, String> headers = buildHeaders(creds[0]);
			String url;

			if (projectKey != null && !projectKey.trim().isEmpty()) {
				String projectId = resolveProjectId(creds[1], creds[0], projectKey);
				url = creds[1] + jiraIssueTypeUrl + "?projectId="
						+ URLEncoder.encode(projectId, StandardCharsets.UTF_8);
			} else {
				url = creds[1] + jiraIssueTypeGlobalUrl;
			}

			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
			JsonNode typeArray = OBJECT_MAPPER.readTree(response);

			List<Map<String, Object>> types = new ArrayList<>();
			for (JsonNode t : typeArray) {
				Map<String, Object> type = new HashMap<>();
				type.put(ID, t.path(ID).asText());
				type.put(NAME, t.path(NAME).asText());
				type.put(subtask, t.path(subtask).asBoolean(false));
				types.add(type);
			}
			return new NounMetadata(types, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in getIssueTypes for '{}': {}", projectKey, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to retrieve issue types for project '" + projectKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 *
	 * @param user
	 * @param projectKey
	 * @param query
	 * @return
	 */
	public static NounMetadata getAssignableUsers(User user, String projectKey, String query) {
		try {
			final String jiraAssignableUserUrl = "/rest/api/3/user/assignable/search";
			final String emailAddress = "emailAddress";
			String[] creds = getCredentials(user);
			Map<String, String> headers = buildHeaders(creds[0]);
			StringBuilder url = new StringBuilder(creds[1]).append(jiraAssignableUserUrl).append("?project=")
					.append(URLEncoder.encode(projectKey, StandardCharsets.UTF_8));
			if (query != null && !query.trim().isEmpty()) {
				url.append("&query=").append(URLEncoder.encode(query, StandardCharsets.UTF_8));
			}
			String response = HttpHelperUtility.getRequest(url.toString(), headers, null, null, null);

			JsonNode userArray = OBJECT_MAPPER.readTree(response);
			List<Map<String, Object>> users = new ArrayList<>();
			for (JsonNode u : userArray) {
				Map<String, Object> userMap = new HashMap<>();
				userMap.put(ACCOUNT_ID, u.path(ACCOUNT_ID).asText());
				userMap.put(DISPLAY_NAME, u.path(DISPLAY_NAME).asText());
				userMap.put(emailAddress, u.path(emailAddress).asText(""));
				users.add(userMap);
			}
			return new NounMetadata(users, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in getAssignableUsers for '{}': {}", projectKey, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to retrieve assignable users for project '" + projectKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 *
	 * @param user
	 * @return
	 */
	public static NounMetadata getPriorities(User user) {
		try {
			final String jiraPriorityUrl = "/rest/api/3/priority";
			final String values = "values";
			String[] creds = getCredentials(user);
			Map<String, String> headers = buildHeaders(creds[0]);
			String response = HttpHelperUtility.getRequest(creds[1] + jiraPriorityUrl, headers, null, null, null);

			JsonNode root = OBJECT_MAPPER.readTree(response);

			JsonNode priorityArray = root.isArray() ? root : root.path(values);

			List<Map<String, Object>> priorities = new ArrayList<>();
			for (JsonNode p : priorityArray) {
				Map<String, Object> priority = new HashMap<>();
				priority.put(ID, p.path(ID).asText());
				priority.put(NAME, p.path(NAME).asText());
				priorities.add(priority);
			}
			return new NounMetadata(priorities, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in getPriorities: {}", e.getMessage(), e);
			throw new SemossPixelException("Failed to retrieve Jira priorities. Error: " + e.getMessage());
		}
	}

	/**
	 *
	 * @param user
	 * @param projectKey
	 * @param statusFilter
	 * @param assigneeFilter
	 * @param priorityFilter
	 * @param nextPageToken
	 * @param maxResults
	 * @return
	 */
	public static NounMetadata listIssues(User user, String projectKey, String statusFilter, String assigneeFilter,
			String priorityFilter, String nextPageToken, int maxResults) {
		try {
			String escapedProject = projectKey == null ? "" : projectKey.replace("\\", "\\\\").replace("\"", "\\\"");
			StringBuilder jql = new StringBuilder("project = \"").append(escapedProject).append("\"");
			if (statusFilter != null && !statusFilter.trim().isEmpty()) {
				String escapedStatus = statusFilter.replace("\\", "\\\\").replace("\"", "\\\"");
				jql.append(" AND status = \"").append(escapedStatus).append("\"");
			}
			if (assigneeFilter != null && !assigneeFilter.trim().isEmpty()) {
				String escapedAssignee = assigneeFilter.replace("\\", "\\\\").replace("\"", "\\\"");
				jql.append(" AND assignee = \"").append(escapedAssignee).append("\"");
			}
			if (priorityFilter != null && !priorityFilter.trim().isEmpty()) {
				String escapedPriority = priorityFilter.replace("\\", "\\\\").replace("\"", "\\\"");
				jql.append(" AND priority = \"").append(escapedPriority).append("\"");
			}
			jql.append(" ORDER BY created DESC");

			return executeJqlSearch(user, jql.toString(), nextPageToken, maxResults);

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in listIssues for '{}': {}", projectKey, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to list issues for project '" + projectKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 *
	 * @param user
	 * @param jql
	 * @param nextPageToken
	 * @param maxResults
	 * @return
	 */
	public static NounMetadata searchIssues(User user, String jql, String nextPageToken, int maxResults) {
		try {
			if (jql == null || jql.trim().isEmpty()) {
				throw new SemossPixelException("JQL query string is required.");
			}
			return executeJqlSearch(user, jql, nextPageToken, maxResults);

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in searchIssues: {}", e.getMessage(), e);
			throw new SemossPixelException("Failed to search Jira issues. Error: " + e.getMessage());
		}
	}

	/**
	 *
	 * @param user
	 * @param issueKey
	 * @return
	 */
	public static NounMetadata getIssue(User user, String issueKey) {
		try {
			final String assigneeAccountId = "assigneeAccountId";
			String[] creds = getCredentials(user);
			Map<String, String> headers = buildHeaders(creds[0]);
			String response = HttpHelperUtility.getRequest(creds[1] + JIRA_ISSUE_URL + "/" + issueKey, headers, null,
					null, null);

			JsonNode root = OBJECT_MAPPER.readTree(response);
			JsonNode f = root.path(FIELDS);

			Map<String, Object> ticket = new HashMap<>();
			ticket.put(ID, root.path(ID).asText());
			ticket.put(KEY, root.path(KEY).asText());
			ticket.put(SELF, root.path(SELF).asText());
			ticket.put(SUMMARY, f.path(SUMMARY).asText());
			ticket.put(STATUS, f.path(STATUS).path(NAME).asText());
			ticket.put(PRIORITY, f.path(PRIORITY).path(NAME).asText());
			ticket.put(ISSUE_TYPE, f.path(ISSUE_TYPE).path(NAME).asText());
			ticket.put(ASSIGNEE, f.path(ASSIGNEE).path(DISPLAY_NAME).asText(UNASSIGNED));
			ticket.put(assigneeAccountId, f.path(ASSIGNEE).path(ACCOUNT_ID).asText());
			ticket.put(DUE_DATE, f.path(DUE_DATE).asText());
			ticket.put(LABELS, f.path(LABELS).toString());
			JsonNode descriptionNode = f.path(DESCRIPTION);
			if (descriptionNode == null || descriptionNode.isNull() || descriptionNode.isMissingNode()) {
				ticket.put(DESCRIPTION, "");
			} else {
				StringBuilder descriptionTextBuilder = new StringBuilder();
				List<JsonNode> nodes = new ArrayList<>();
				nodes.add(descriptionNode);
				while (!nodes.isEmpty()) {
					JsonNode currentNode = nodes.remove(nodes.size() - 1);
					if (currentNode.has(TEXT)) {
						descriptionTextBuilder.append(currentNode.get(TEXT).asText()).append(" ");
					}
					if (currentNode.has(CONTENT)) {
						List<JsonNode> children = new ArrayList<>();
						for (JsonNode child : currentNode.get(CONTENT)) {
							children.add(child);
						}
						for (int i = children.size() - 1; i >= 0; i--) {
							nodes.add(children.get(i));
						}
					}
				}
				ticket.put(DESCRIPTION, descriptionTextBuilder.toString().trim());
			}

			return new NounMetadata(ticket, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in getIssue '{}': {}", issueKey, e.getMessage(), e);
			throw new SemossPixelException("Failed to retrieve issue '" + issueKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 *
	 * @param user
	 * @param projectKey
	 * @param summary
	 * @param description
	 * @param issueType
	 * @param assigneeAccountId
	 * @param priority
	 * @param dueDate
	 * @param parentKey
	 * @param status
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static NounMetadata createIssue(User user, String projectKey, String summary, String description,
			String issueType, String assigneeAccountId, String priority, String dueDate, String parentKey, String status) {
		try {
			final String parent = "parent";
			String[] creds = getCredentials(user);
			Map<String, String> headers = buildHeaders(creds[0]);

			Map<String, Object> fields = new HashMap<>();
			fields.put(PROJECT, Map.of(KEY, projectKey));
			fields.put(SUMMARY, summary);
			fields.put(ISSUE_TYPE, Map.of(NAME, issueType));

			if (description != null && !description.trim().isEmpty()) {
				fields.put(DESCRIPTION, buildAdfDocument(description));
			}
			if (assigneeAccountId != null && !assigneeAccountId.trim().isEmpty()) {
				fields.put(ASSIGNEE, Map.of(ACCOUNT_ID, assigneeAccountId));
			}
			if (priority != null && !priority.trim().isEmpty()) {
				fields.put(PRIORITY, Map.of(NAME, priority));
			}
			if (dueDate != null && !dueDate.trim().isEmpty()) {
				fields.put(DUE_DATE, dueDate);
			}
			if (parentKey != null && !parentKey.trim().isEmpty()) {
				fields.put(parent, Map.of(KEY, parentKey));
			}

			String response = HttpHelperUtility.postRequestStringBody(creds[1] + JIRA_ISSUE_URL, headers,
					GSON.toJson(Map.of(FIELDS, fields)), ContentType.APPLICATION_JSON, null, null, null);
			Map<String, Object> resp = GSON.fromJson(response, Map.class);

			String createdKey = resp.get(KEY) != null ? resp.get(KEY).toString() : null;
			String appliedStatus = null;
			if (status != null && !status.trim().isEmpty() && createdKey != null) {
				appliedStatus = applyTransitionByName(creds[1], creds[0], createdKey, status);
				classLogger.info("Post-create transition applied for '{}': {}", createdKey, appliedStatus);
			}

			Map<String, Object> result = new HashMap<>();
			result.put(ID, resp.get(ID) != null ? resp.get(ID).toString() : null);
			result.put(KEY, resp.get(KEY) != null ? resp.get(KEY).toString() : null);
			result.put(SELF, resp.get(SELF) != null ? resp.get(SELF).toString() : null);
			result.put(SUMMARY, summary);
			result.put(SUCCESS, true);
			if (appliedStatus != null) {
				result.put(STATUS, appliedStatus);
			}
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in createIssue: {}", e.getMessage(), e);
			throw new SemossPixelException("Failed to create Jira issue. Error: " + e.getMessage());
		}
	}

	/**
	 *
	 * @param user
	 * @param jiraId
	 * @param summary
	 * @param description
	 * @param assigneeAccountId
	 * @param priority
	 * @param dueDate
	 * @param status
	 * @return
	 */
	public static NounMetadata updateIssue(User user, String jiraId, String summary, String description,
			String assigneeAccountId, String priority, String dueDate, String status) {
		try {
			String[] creds = getCredentials(user);
			Map<String, String> headers = buildHeaders(creds[0]);

			Map<String, Object> fields = new HashMap<>();

			if (summary != null && !summary.trim().isEmpty()) {
				fields.put(SUMMARY, summary);
			}
			if (description != null && !description.trim().isEmpty()) {
				fields.put(DESCRIPTION, buildAdfDocument(description));
			}
			if (assigneeAccountId != null && !assigneeAccountId.trim().isEmpty()) {
				fields.put(ASSIGNEE, Map.of(ACCOUNT_ID, assigneeAccountId));
			}
			if (priority != null && !priority.trim().isEmpty()) {
				fields.put(PRIORITY, Map.of(NAME, priority));
			}
			if (dueDate != null && !dueDate.trim().isEmpty()) {
				fields.put(DUE_DATE, dueDate);
			}
			boolean hasFieldUpdates = !fields.isEmpty();
			boolean hasStatusUpdate = status != null && !status.trim().isEmpty();

			if (!hasFieldUpdates && !hasStatusUpdate) {
				throw new SemossPixelException(
						"No fields provided for update. Please specify at least one field to update.");
			}
			if (hasFieldUpdates) {
				HttpHelperUtility.putRequestStringBody(creds[1] + JIRA_ISSUE_URL + "/" + jiraId, headers,
						GSON.toJson(Map.of(FIELDS, fields)), ContentType.APPLICATION_JSON, null, null, null);
			}
			String appliedStatus = null;
			if (hasStatusUpdate) {
				appliedStatus = applyTransitionByName(creds[1], creds[0], jiraId, status);
			}

			Map<String, Object> result = new HashMap<>();
			result.put(KEY, jiraId);
			result.put(SUCCESS, true);
			if (appliedStatus != null) {
				result.put(STATUS, appliedStatus);
			}
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in updateIssue '{}': {}", jiraId, e.getMessage(), e);
			throw new SemossPixelException("Failed to update issue '" + jiraId + "'. Error: " + e.getMessage());
		}
	}

	/**
	 *
	 * @param urlBase
	 * @param accessToken
	 * @param issueKey
	 * @param statusName
	 * @return
	 * @throws Exception
	 */
	private static String applyTransitionByName(String urlBase, String accessToken,
			String issueKey, String statusName) throws Exception {
		String url = urlBase + JIRA_ISSUE_URL + "/" + issueKey + TRANSITIONS_SUFFIX;
		Map<String, String> headers = buildHeaders(accessToken);
		String getResponse = HttpHelperUtility.getRequest(url, headers, null, null, null);
		JsonNode transitions = OBJECT_MAPPER.readTree(getResponse).path(TRANSITIONS);
		for (JsonNode t : transitions) {
			if (t.path(TO).path(NAME).asText("").equalsIgnoreCase(statusName)) {
				HttpHelperUtility.postRequestStringBody(url, headers,
						GSON.toJson(Map.of(TRANSITION, Map.of(ID, t.path(ID).asText()))),
						ContentType.APPLICATION_JSON, null, null, null);
				return statusName;
			}
		}
		throw new SemossPixelException("Could not transition issue " + issueKey + " to '" + statusName + "'.");
	}

	/**
	 *
	 * @param user
	 * @param projectKey
	 * @param jiraId
	 * @param deleteSubtasks
	 * @return
	 */
	public static NounMetadata deleteIssue(User user, String projectKey, String jiraId, boolean deleteSubtasks) {
		try {
			String[] creds = getCredentials(user);
			Map<String, String> headers = buildHeaders(creds[0]);
			String issueUrl = creds[1] + JIRA_ISSUE_URL + "/" + jiraId;

			String issueResponse = HttpHelperUtility.getRequest(issueUrl, headers, null, null, null);
			JsonNode root = OBJECT_MAPPER.readTree(issueResponse);
			String actualProjectKey = root.path(FIELDS).path(PROJECT).path(KEY).asText();

			if (!projectKey.equalsIgnoreCase(actualProjectKey)) {
				throw new SemossPixelException("Issue " + jiraId + " does not belong to project " + projectKey);
			}

			String deleteUrl = deleteSubtasks ? issueUrl + "?deleteSubtasks=true" : issueUrl;
			HttpHelperUtility.deleteRequestStringBody(deleteUrl, headers, null, null, null);

			Map<String, Object> result = new HashMap<>();
			result.put(SUCCESS, true);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in deleteIssue '{}': {}", jiraId, e.getMessage(), e);
			throw new SemossPixelException("Failed to delete issue '" + jiraId + "'. Error: " + e.getMessage());
		}
	}

	/**
	 *
	 * @param user
	 * @param issueKey
	 * @return
	 */
	public static NounMetadata getTransitions(User user, String issueKey) {
		try {
			final String toStatus = "toStatus";
			String[] creds = getCredentials(user);
			Map<String, String> headers = buildHeaders(creds[0]);
			String url = creds[1] + JIRA_ISSUE_URL + "/" + issueKey + TRANSITIONS_SUFFIX;
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);

			JsonNode root = OBJECT_MAPPER.readTree(response);

			List<Map<String, Object>> transitionList = new ArrayList<>();
			for (JsonNode t : root.path(TRANSITIONS)) {
				Map<String, Object> transition = new HashMap<>();
				transition.put(ID, t.path(ID).asText());
				transition.put(NAME, t.path(NAME).asText());
				transition.put(toStatus, t.path(TO).path(NAME).asText());
				transitionList.add(transition);
			}

			return new NounMetadata(transitionList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in getTransitions for '{}': {}", issueKey, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to get transitions for issue '" + issueKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 *
	 * @param user
	 * @param issueKey
	 * @return
	 */
	public static NounMetadata getComments(User user, String issueKey) {
		try {
			final String updated = "updated";
			String[] creds = getCredentials(user);
			Map<String, String> headers = buildHeaders(creds[0]);
			String url = creds[1] + JIRA_ISSUE_URL + "/" + issueKey + COMMENT_SUFFIX;
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);

			JsonNode root = OBJECT_MAPPER.readTree(response);

			List<Map<String, Object>> commentList = new ArrayList<>();
			for (JsonNode c : root.path(COMMENTS)) {
				Map<String, Object> comment = new HashMap<>();
				comment.put(ID, c.path(ID).asText());
				comment.put(AUTHOR, c.path(AUTHOR).path(DISPLAY_NAME).asText());
				comment.put(CREATED, c.path(CREATED).asText());
				comment.put(updated, c.path(updated).asText());
				JsonNode bodyNode = c.path(BODY);
				if (bodyNode == null || bodyNode.isNull() || bodyNode.isMissingNode()) {
					comment.put(BODY, "");
				} else {
					StringBuilder bodyTextBuilder = new StringBuilder();
					List<JsonNode> nodes = new ArrayList<>();
					nodes.add(bodyNode);
					while (!nodes.isEmpty()) {
						JsonNode currentNode = nodes.remove(nodes.size() - 1);
						if (currentNode.has(TEXT)) {
							bodyTextBuilder.append(currentNode.get(TEXT).asText()).append(" ");
						}
						if (currentNode.has(CONTENT)) {
							List<JsonNode> children = new ArrayList<>();
							for (JsonNode child : currentNode.get(CONTENT)) {
								children.add(child);
							}
							for (int i = children.size() - 1; i >= 0; i--) {
								nodes.add(children.get(i));
							}
						}
					}
					comment.put(BODY, bodyTextBuilder.toString().trim());
				}
				commentList.add(comment);
			}

			return new NounMetadata(commentList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in getComments for '{}': {}", issueKey, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to get comments for issue '" + issueKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 *
	 * @param user
	 * @param issueKey
	 * @param commentText
	 * @return
	 */
	public static NounMetadata addComment(User user, String issueKey, String commentText) {
		try {
			String[] creds = getCredentials(user);
			Map<String, String> headers = buildHeaders(creds[0]);

			String url = creds[1] + JIRA_ISSUE_URL + "/" + issueKey + COMMENT_SUFFIX;
			String response = HttpHelperUtility.postRequestStringBody(url, headers,
					GSON.toJson(Map.of(BODY, buildAdfDocument(commentText))), ContentType.APPLICATION_JSON, null, null,
					null);

			JsonNode root = OBJECT_MAPPER.readTree(response);

			Map<String, Object> result = new HashMap<>();
			result.put(ID, root.path(ID).asText());
			result.put(AUTHOR, root.path(AUTHOR).path(DISPLAY_NAME).asText());
			result.put(CREATED, root.path(CREATED).asText());
			result.put(SUCCESS, true);

			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in addComment for '{}': {}", issueKey, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to add comment to issue '" + issueKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 *
	 * @param user
	 * @param jql
	 * @param nextPageToken
	 * @param maxResults
	 * @return
	 * @throws Exception
	 */
	private static NounMetadata executeJqlSearch(User user, String jql, String nextPageToken, int maxResults)
			throws Exception {
		final String jiraSearchUrl = "/rest/api/3/search/jql";
		final String isLast = "isLast";
		String[] creds = getCredentials(user);
		Map<String, String> headers = buildHeaders(creds[0]);

		Map<String, Object> body = new HashMap<>();
		body.put(JQL, jql);
		body.put(MAX_RESULTS_KEY, maxResults > 0 ? maxResults : 50);
		body.put(FIELDS, Arrays.asList(SUMMARY, STATUS, ASSIGNEE, PRIORITY, ISSUE_TYPE, DUE_DATE, LABELS));
		if (nextPageToken != null && !nextPageToken.trim().isEmpty()) {
			body.put(NEXT_PAGE_TOKEN, nextPageToken);
		}

		String response = HttpHelperUtility.postRequestStringBody(creds[1] + jiraSearchUrl, headers,
				GSON.toJson(body), ContentType.APPLICATION_JSON, null, null, null);
		JsonNode root = OBJECT_MAPPER.readTree(response);

		List<Map<String, Object>> issueList = new ArrayList<>();
		for (JsonNode issue : root.path(ISSUES)) {
			issueList.add(parseIssueSummary(issue));
		}

		Map<String, Object> result = new HashMap<>();
		result.put(ISSUES, issueList);
		result.put(isLast, root.path(isLast).asBoolean(true));
		result.put(MAX_RESULTS_KEY, maxResults > 0 ? maxResults : 50);
		if (root.has(NEXT_PAGE_TOKEN)) {
			result.put(NEXT_PAGE_TOKEN, root.path(NEXT_PAGE_TOKEN).asText());
		}

		return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	/**
	 *
	 * @param urlBase
	 * @param accessToken
	 * @param projectIdOrKey
	 * @return
	 * @throws Exception
	 */
	private static String resolveProjectId(String urlBase, String accessToken, String projectIdOrKey) throws Exception {
		if (projectIdOrKey == null || projectIdOrKey.trim().isEmpty()) {
			throw new SemossPixelException("Project key is required.");
		}
		if (projectIdOrKey.chars().allMatch(Character::isDigit)) {
			return projectIdOrKey;
		}
		Map<String, String> headers = buildHeaders(accessToken);
		String projectUrl = urlBase + JIRA_PROJECT_URL + "/"
				+ URLEncoder.encode(projectIdOrKey, StandardCharsets.UTF_8);
		String response = HttpHelperUtility.getRequest(projectUrl, headers, null, null, null);
		JsonNode projectNode = OBJECT_MAPPER.readTree(response);
		String resolvedId = projectNode.path(ID).asText();
		if (resolvedId == null || resolvedId.trim().isEmpty()) {
			throw new SemossPixelException("Unable to resolve project ID for project '" + projectIdOrKey
					+ "'. Check the project key is correct.");
		}
		return resolvedId;
	}

	/**
	 *
	 * @param issue
	 * @return
	 */
	private static Map<String, Object> parseIssueSummary(JsonNode issue) {
		JsonNode f = issue.path(FIELDS);
		Map<String, Object> ticket = new HashMap<>();
		ticket.put(ID, issue.path(ID).asText());
		ticket.put(KEY, issue.path(KEY).asText());
		ticket.put(SELF, issue.path(SELF).asText());
		ticket.put(SUMMARY, f.path(SUMMARY).asText());
		ticket.put(STATUS, f.path(STATUS).path(NAME).asText());
		ticket.put(PRIORITY, f.path(PRIORITY).path(NAME).asText());
		ticket.put(ISSUE_TYPE, f.path(ISSUE_TYPE).path(NAME).asText());
		ticket.put(ASSIGNEE, f.path(ASSIGNEE).path(DISPLAY_NAME).asText(UNASSIGNED));
		ticket.put(DUE_DATE, f.path(DUE_DATE).asText());
		return ticket;
	}

	/**
	 *
	 * @param text
	 * @return
	 */
	static Map<String, Object> buildAdfDocument(String text) {
		final String doc = "doc";
		final String version = "version";
		final String paragraph = "paragraph";
		return Map.of(TYPE, doc, version, 1, CONTENT,
				List.of(Map.of(TYPE, paragraph, CONTENT, List.of(Map.of(TYPE, TEXT, TEXT, text)))));
	}
}
