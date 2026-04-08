package prerna.io.connector.jira;

import java.io.File;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.security.HttpHelperUtility;


public final class JiraHelper {
	// Logger
	private static final Logger classLogger = LogManager.getLogger(JiraHelper.class);
	// Gson instance for JSON serialization/deserialization with specific configurations
	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();
	// ObjectMapper instance for JSON parsing
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	// Jira REST paths
	private static final String API_PATH_ISSUE = "/rest/api/3/issue";
	private static final String API_PATH_PROJECT = "/rest/api/3/project";
	private static final String API_SUFFIX_COMMENT = "/comment";
	private static final String API_SUFFIX_TRANSITIONS = "/transitions";

	// Jira entity field keys
	private static final String FIELD_ACCOUNT_ID = "accountId";
	private static final String FIELD_ASSIGNEE = "assignee";
	private static final String FIELD_AUTHOR = "author";
	private static final String FIELD_BODY = "body";
	private static final String FIELD_COMMENT = "comment";
	private static final String FIELD_COMMENTS = "comments";
	private static final String FIELD_CREATED = "created";
	private static final String FIELD_DESCRIPTION = "description";
	private static final String FIELD_DISPLAY_NAME = "displayName";
	private static final String FIELD_DUE_DATE = "duedate";
	private static final String FIELD_ID = "id";
	private static final String FIELD_ISSUE_TYPE = "issuetype";
	private static final String FIELD_KEY = "key";
	private static final String FIELD_LABELS = "labels";
	private static final String FIELD_LEAD = "lead";
	private static final String FIELD_NAME = "name";
	private static final String FIELD_PRIORITY = "priority";
	private static final String FIELD_PROJECT = "project";
	private static final String FIELD_SELF = "self";
	private static final String FIELD_STATUS = "status";
	private static final String FIELD_SUMMARY = "summary";
	private static final String FIELD_TO = "to";

	// Search and workflow field keys
	private static final String FIELD_FIELDS = "fields";
	private static final String FIELD_ISSUES = "issues";
	private static final String FIELD_JQL = "jql";
	private static final String FIELD_MAX_RESULTS = "maxResults";
	private static final String FIELD_NEXT_PAGE_TOKEN = "nextPageToken";
	private static final String FIELD_SUCCESS = "success";
	private static final String FIELD_TRANSITION = "transition";
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
	 * @return list of maps, each containing {@code id}, {@code key}, {@code name},
	 *         {@code projectTypeKey}, and {@code lead}
	 */
	public static List<Map<String, Object>> getAllProjects(String accessToken, String baseUrl) {
		try {
			final String projectTypeKey = "projectTypeKey";
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);
			String response = HttpHelperUtility.getRequest(baseUrl + API_PATH_PROJECT, headers, null, null, null);

			JsonNode projectArray = OBJECT_MAPPER.readTree(response);
			List<Map<String, Object>> projects = new ArrayList<>();
			for (JsonNode p : projectArray) {
				Map<String, Object> proj = new HashMap<>();
				proj.put(FIELD_ID, p.path(FIELD_ID).asText());
				proj.put(FIELD_KEY, p.path(FIELD_KEY).asText());
				proj.put(FIELD_NAME, p.path(FIELD_NAME).asText());
				proj.put(projectTypeKey, p.path(projectTypeKey).asText());
				if (p.has(FIELD_LEAD)) {
					proj.put(FIELD_LEAD, p.path(FIELD_LEAD).path(FIELD_DISPLAY_NAME).asText());
				}
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
	 * @return list of maps, each containing {@code id}, {@code name}, and
	 *         {@code subtask}
	 */
	public static List<Map<String, Object>> getIssueTypes(String accessToken, String baseUrl, String projectKey) {
		try {
			final String jiraIssueTypeUrl = "/rest/api/3/issuetype/project";
			final String jiraIssueTypeGlobalUrl = "/rest/api/3/issuetype";
			final String subtask = "subtask";
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);
			String url;

			if (projectKey != null && !projectKey.trim().isEmpty()) {
				String projectId = resolveProjectId(baseUrl, accessToken, projectKey);
				url = baseUrl + jiraIssueTypeUrl + "?projectId="
						+ URLEncoder.encode(projectId, StandardCharsets.UTF_8);
			} else {
				url = baseUrl + jiraIssueTypeGlobalUrl;
			}

			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
			JsonNode typeArray = OBJECT_MAPPER.readTree(response);

			List<Map<String, Object>> types = new ArrayList<>();
			for (JsonNode t : typeArray) {
				Map<String, Object> type = new HashMap<>();
				type.put(FIELD_ID, t.path(FIELD_ID).asText());
				type.put(FIELD_NAME, t.path(FIELD_NAME).asText());
				type.put(subtask, t.path(subtask).asBoolean(false));
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
	 * @return list of maps, each containing {@code accountId}, {@code displayName},
	 *         and {@code emailAddress}
	 */
	public static List<Map<String, Object>> getAssignableUsers(String accessToken, String baseUrl, String projectKey,
			String query) {
		try {
			final String jiraAssignableUserUrl = "/rest/api/3/user/assignable/search";
			final String emailAddress = "emailAddress";
			validateJiraContext(accessToken, baseUrl);
			validateRequiredString(projectKey, "Project key");
			Map<String, String> headers = buildHeaders(accessToken);
			StringBuilder url = new StringBuilder(baseUrl).append(jiraAssignableUserUrl).append("?project=")
					.append(URLEncoder.encode(projectKey, StandardCharsets.UTF_8));
			if (query != null && !query.trim().isEmpty()) {
				url.append("&query=").append(URLEncoder.encode(query, StandardCharsets.UTF_8));
			}
			String response = HttpHelperUtility.getRequest(url.toString(), headers, null, null, null);

			JsonNode userArray = OBJECT_MAPPER.readTree(response);
			List<Map<String, Object>> users = new ArrayList<>();
			for (JsonNode u : userArray) {
				Map<String, Object> userMap = new HashMap<>();
				userMap.put(FIELD_ACCOUNT_ID, u.path(FIELD_ACCOUNT_ID).asText());
				userMap.put(FIELD_DISPLAY_NAME, u.path(FIELD_DISPLAY_NAME).asText());
				userMap.put(emailAddress, u.path(emailAddress).asText(""));
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
		try {
			final String jiraPriorityUrl = "/rest/api/3/priority";
			final String values = "values";
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);
			String response = HttpHelperUtility.getRequest(baseUrl + jiraPriorityUrl, headers, null, null, null);

			JsonNode root = OBJECT_MAPPER.readTree(response);
			JsonNode priorityArray = root.isArray() ? root : root.path(values);

			List<Map<String, Object>> priorities = new ArrayList<>();
			for (JsonNode p : priorityArray) {
				Map<String, Object> priority = new HashMap<>();
				priority.put(FIELD_ID, p.path(FIELD_ID).asText());
				priority.put(FIELD_NAME, p.path(FIELD_NAME).asText());
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
	 * Lists Jira issues for a project with optional filters and pagination.
	 *
	 * @param accessToken    Jira OAuth access token
	 * @param baseUrl        Jira API base URL including cloud ID
	 * @param projectKey     project key to list issues for
	 * @param statusFilter   optional status filter
	 * @param assigneeFilter optional assignee account ID filter
	 * @param priorityFilter optional priority name filter
	 * @param nextPageToken  optional pagination token from a previous response
	 * @param maxResults     maximum number of results per page
	 * @return map containing {@code issues}, {@code isLast}, {@code maxResults},
	 *         and optionally {@code nextPageToken}
	 */
	public static Map<String, Object> listIssues(String accessToken, String baseUrl, String projectKey,
			String statusFilter, String assigneeFilter, String priorityFilter, String nextPageToken, int maxResults) {
		try {
			validateJiraContext(accessToken, baseUrl);
			validateRequiredString(projectKey, "Project key");
			StringBuilder jql = new StringBuilder("project = \"").append(JiraUtils.escapeJqlString(projectKey)).append("\"");
			if (statusFilter != null && !statusFilter.trim().isEmpty()) {
				jql.append(" AND status = \"").append(JiraUtils.escapeJqlString(statusFilter)).append("\"");
			}
			if (assigneeFilter != null && !assigneeFilter.trim().isEmpty()) {
				jql.append(" AND assignee = \"").append(JiraUtils.escapeJqlString(assigneeFilter)).append("\"");
			}
			if (priorityFilter != null && !priorityFilter.trim().isEmpty()) {
				jql.append(" AND priority = \"").append(JiraUtils.escapeJqlString(priorityFilter)).append("\"");
			}
			jql.append(" ORDER BY created DESC");

			return executeJqlSearch(accessToken, baseUrl, jql.toString(), nextPageToken, maxResults);

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in listIssues for '{}': {}", projectKey, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to list issues for project '" + projectKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Runs an arbitrary JQL query against Jira with pagination.
	 *
	 * @param accessToken   Jira OAuth access token
	 * @param baseUrl       Jira API base URL including cloud ID
	 * @param jql           JQL query string
	 * @param nextPageToken optional pagination token from a previous response
	 * @param maxResults    maximum number of results per page
	 * @return map containing paginated issue list with total count
	 */
	public static Map<String, Object> searchIssues(String accessToken, String baseUrl, String jql,
			String nextPageToken, int maxResults) {
		try {
			if (jql == null || jql.trim().isEmpty()) {
				throw new SemossPixelException("JQL query string is required.");
			}
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
	 * @return map containing issue fields
	 */
	public static Map<String, Object> readTicket(String accessToken, String baseUrl, String issueKey) {
		try {
			final String assigneeAccountId = "assigneeAccountId";
			final String reporter = "reporter";
			final String created = "created";
			final String updated = "updated";
			final String resolution = "resolution";
			final String parentKey = "parentKey";
			validateJiraContext(accessToken, baseUrl);
			validateRequiredString(issueKey, "Issue key");
			Map<String, String> headers = buildHeaders(accessToken);
			String response = HttpHelperUtility.getRequest(baseUrl + API_PATH_ISSUE + "/" + issueKey, headers, null,
					null, null);

			JsonNode root = OBJECT_MAPPER.readTree(response);
			JsonNode f = root.path(FIELD_FIELDS);

			Map<String, Object> ticket = new HashMap<>();
			ticket.put(FIELD_ID, root.path(FIELD_ID).asText());
			ticket.put(FIELD_KEY, root.path(FIELD_KEY).asText());
			ticket.put(FIELD_SELF, root.path(FIELD_SELF).asText());
			ticket.put(FIELD_SUMMARY, f.path(FIELD_SUMMARY).asText());
			ticket.put(FIELD_STATUS, f.path(FIELD_STATUS).path(FIELD_NAME).asText());
			ticket.put(FIELD_PRIORITY, f.path(FIELD_PRIORITY).path(FIELD_NAME).asText());
			ticket.put(FIELD_ISSUE_TYPE, f.path(FIELD_ISSUE_TYPE).path(FIELD_NAME).asText());
			ticket.put(FIELD_ASSIGNEE, f.path(FIELD_ASSIGNEE).path(FIELD_DISPLAY_NAME).asText(DEFAULT_UNASSIGNED));
			ticket.put(assigneeAccountId, f.path(FIELD_ASSIGNEE).path(FIELD_ACCOUNT_ID).asText());
			ticket.put(reporter, f.path(reporter).path(FIELD_DISPLAY_NAME).asText(""));
			ticket.put(FIELD_DUE_DATE, f.path(FIELD_DUE_DATE).asText());
			ticket.put(created, f.path(created).asText());
			ticket.put(updated, f.path(updated).asText());
			ticket.put(resolution, f.path(resolution).path(FIELD_NAME).asText(""));

			// Parse labels into a proper list
			JsonNode labelsNode = f.path(FIELD_LABELS);
			List<String> labels = new ArrayList<>();
			if (labelsNode.isArray()) {
				for (JsonNode lbl : labelsNode) {
					labels.add(lbl.asText());
				}
			}
			ticket.put(FIELD_LABELS, labels);

			// Parent key for subtasks
			JsonNode parentNode = f.path("parent");
			if (!parentNode.isMissingNode() && !parentNode.isNull()) {
				ticket.put(parentKey, parentNode.path(FIELD_KEY).asText());
			}

			// Parse issue links for unlink support
			JsonNode issueLinksNode = f.path("issuelinks");
			List<Map<String, Object>> issueLinks = new ArrayList<>();
			if (issueLinksNode.isArray()) {
				for (JsonNode link : issueLinksNode) {
					Map<String, Object> linkMap = new HashMap<>();
					linkMap.put(FIELD_ID, link.path(FIELD_ID).asText());
					linkMap.put(ADF_TYPE, link.path(ADF_TYPE).path(FIELD_NAME).asText());
					if (link.has("inwardIssue")) {
						JsonNode inward = link.path("inwardIssue");
						linkMap.put("direction", "inward");
						linkMap.put("linkedIssueKey", inward.path(FIELD_KEY).asText());
						linkMap.put("linkedIssueSummary", inward.path(FIELD_FIELDS).path(FIELD_SUMMARY).asText());
						linkMap.put("linkedIssueStatus", inward.path(FIELD_FIELDS).path(FIELD_STATUS).path(FIELD_NAME).asText());
					} else if (link.has("outwardIssue")) {
						JsonNode outward = link.path("outwardIssue");
						linkMap.put("direction", "outward");
						linkMap.put("linkedIssueKey", outward.path(FIELD_KEY).asText());
						linkMap.put("linkedIssueSummary", outward.path(FIELD_FIELDS).path(FIELD_SUMMARY).asText());
						linkMap.put("linkedIssueStatus", outward.path(FIELD_FIELDS).path(FIELD_STATUS).path(FIELD_NAME).asText());
					}
					issueLinks.add(linkMap);
				}
			}
			ticket.put("issuelinks", issueLinks);

			ticket.put(FIELD_DESCRIPTION, parseAdfToPlainText(f.path(FIELD_DESCRIPTION)));
			return ticket;

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in readTicket '{}': {}", issueKey, e.getMessage(), e);
			throw new SemossPixelException("Failed to retrieve issue '" + issueKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Creates a Jira issue with full field support.
	 *
	 * @param accessToken       Jira OAuth access token
	 * @param baseUrl           Jira API base URL including cloud ID
	 * @param projectKey        project key to create the issue in
	 * @param summary           issue title
	 * @param description       optional issue description
	 * @param issueType         issue type name (for example, {@code Bug})
	 * @param assigneeAccountId optional assignee account ID
	 * @param priority          optional priority name
	 * @param dueDate           optional due date in {@code YYYY-MM-DD} format
	 * @param parentKey         optional parent issue key for subtasks
	 * @param status            optional status to transition to after creation
	 * @return map containing {@code id}, {@code key}, {@code self}, {@code summary},
	 *         {@code success}, and optionally {@code status}
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> createIssue(String accessToken, String baseUrl, String projectKey,
			String summary, String description, String issueType, String assigneeAccountId, String priority,
			String dueDate, String parentKey, String status) {
		try {
			final String parent = "parent";
			validateJiraContext(accessToken, baseUrl);
			validateRequiredString(projectKey, "Project key");
			validateRequiredString(summary, "Summary");
			validateRequiredString(issueType, "Issue type");
			Map<String, String> headers = buildHeaders(accessToken);

			Map<String, Object> fields = new HashMap<>();
			fields.put(FIELD_PROJECT, Map.of(FIELD_KEY, projectKey));
			fields.put(FIELD_SUMMARY, summary);
			fields.put(FIELD_ISSUE_TYPE, Map.of(FIELD_NAME, issueType));

			if (description != null && !description.trim().isEmpty()) {
				fields.put(FIELD_DESCRIPTION, buildAdfDocument(description));
			}
			if (assigneeAccountId != null && !assigneeAccountId.trim().isEmpty()) {
				fields.put(FIELD_ASSIGNEE, Map.of(FIELD_ACCOUNT_ID, assigneeAccountId));
			}
			if (priority != null && !priority.trim().isEmpty()) {
				fields.put(FIELD_PRIORITY, Map.of(FIELD_NAME, priority));
			}
			if (dueDate != null && !dueDate.trim().isEmpty()) {
				fields.put(FIELD_DUE_DATE, dueDate);
			}
			if (parentKey != null && !parentKey.trim().isEmpty()) {
				fields.put(parent, Map.of(FIELD_KEY, parentKey));
			}

			String response = HttpHelperUtility.postRequestStringBody(baseUrl + API_PATH_ISSUE, headers,
					GSON.toJson(Map.of(FIELD_FIELDS, fields)), ContentType.APPLICATION_JSON, null, null, null);
			Map<String, Object> resp = GSON.fromJson(response, Map.class);

			String createdKey = resp.get(FIELD_KEY) != null ? resp.get(FIELD_KEY).toString() : null;
			String appliedStatus = null;
			String statusWarning = null;
			if (status != null && !status.trim().isEmpty() && createdKey != null) {
				try {
					appliedStatus = applyTransitionByName(baseUrl, accessToken, createdKey, status);
					classLogger.info("Post-create transition applied for '{}': {}", createdKey, appliedStatus);
				} catch (Exception te) {
					statusWarning = "Issue created successfully but status transition to '"
							+ status + "' failed: " + te.getMessage();
					classLogger.warn(statusWarning, te);
				}
			}

			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_ID, resp.get(FIELD_ID) != null ? resp.get(FIELD_ID).toString() : null);
			result.put(FIELD_KEY, resp.get(FIELD_KEY) != null ? resp.get(FIELD_KEY).toString() : null);
			result.put(FIELD_SELF, resp.get(FIELD_SELF) != null ? resp.get(FIELD_SELF).toString() : null);
			result.put(FIELD_SUMMARY, summary);
			result.put(FIELD_SUCCESS, true);
			if (appliedStatus != null) {
				result.put(FIELD_STATUS, appliedStatus);
			}
			if (statusWarning != null) {
				result.put("statusWarning", statusWarning);
			}
			return result;

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in createIssue: {}", e.getMessage(), e);
			throw new SemossPixelException("Failed to create Jira issue. Error: " + e.getMessage());
		}
	}

	/**
	 * Updates fields for an existing Jira issue.
	 *
	 * @param accessToken       Jira OAuth access token
	 * @param baseUrl           Jira API base URL including cloud ID
	 * @param jiraId            issue key to update (for example, {@code PROJECT-123})
	 * @param summary           optional new summary
	 * @param description       optional new description
	 * @param assigneeAccountId optional new assignee account ID
	 * @param priority          optional new priority name
	 * @param dueDate           optional new due date in {@code YYYY-MM-DD} format
	 * @param status            optional status to transition to
	 * @return map containing {@code key}, {@code success}, and optionally
	 *         {@code status}
	 */
	public static Map<String, Object> updateIssue(String accessToken, String baseUrl, String jiraId, String summary,
			String description, String assigneeAccountId, String priority, String dueDate, String status) {
		try {
			validateJiraContext(accessToken, baseUrl);
			validateRequiredString(jiraId, "Issue key");
			Map<String, String> headers = buildHeaders(accessToken);

			Map<String, Object> fields = new HashMap<>();
			if (summary != null && !summary.trim().isEmpty()) {
				fields.put(FIELD_SUMMARY, summary);
			}
			if (description != null && !description.trim().isEmpty()) {
				fields.put(FIELD_DESCRIPTION, buildAdfDocument(description));
			}
			if (assigneeAccountId != null && !assigneeAccountId.trim().isEmpty()) {
				fields.put(FIELD_ASSIGNEE, Map.of(FIELD_ACCOUNT_ID, assigneeAccountId));
			}
			if (priority != null && !priority.trim().isEmpty()) {
				fields.put(FIELD_PRIORITY, Map.of(FIELD_NAME, priority));
			}
			if (dueDate != null && !dueDate.trim().isEmpty()) {
				fields.put(FIELD_DUE_DATE, dueDate);
			}
			boolean hasFieldUpdates = !fields.isEmpty();
			boolean hasStatusUpdate = status != null && !status.trim().isEmpty();

			if (!hasFieldUpdates && !hasStatusUpdate) {
				throw new SemossPixelException(
						"No fields provided for update. Please specify at least one field to update.");
			}
			if (hasFieldUpdates) {
				HttpHelperUtility.putRequestStringBody(baseUrl + API_PATH_ISSUE + "/" + jiraId, headers,
						GSON.toJson(Map.of(FIELD_FIELDS, fields)), ContentType.APPLICATION_JSON, null, null, null);
			}
			String appliedStatus = null;
			String statusWarning = null;
			if (hasStatusUpdate) {
				try {
					appliedStatus = applyTransitionByName(baseUrl, accessToken, jiraId, status);
				} catch (Exception te) {
					statusWarning = "Fields updated successfully but status transition to '"
							+ status + "' failed: " + te.getMessage();
					classLogger.warn(statusWarning, te);
				}
			}

			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_KEY, jiraId);
			result.put(FIELD_SUCCESS, true);
			if (appliedStatus != null) {
				result.put(FIELD_STATUS, appliedStatus);
			}
			if (statusWarning != null) {
				result.put("statusWarning", statusWarning);
			}
			return result;

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in updateIssue '{}': {}", jiraId, e.getMessage(), e);
			throw new SemossPixelException("Failed to update issue '" + jiraId + "'. Error: " + e.getMessage());
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
	public static Map<String, Object> deleteIssue(String accessToken, String baseUrl, String projectKey,
			String jiraId, boolean deleteSubtasks) {
		try {
			validateJiraContext(accessToken, baseUrl);
			validateRequiredString(projectKey, "Project key");
			validateRequiredString(jiraId, "Issue key");
			Map<String, String> headers = buildHeaders(accessToken);
			String issueUrl = baseUrl + API_PATH_ISSUE + "/" + jiraId;

			String issueResponse = HttpHelperUtility.getRequest(issueUrl, headers, null, null, null);
			JsonNode root = OBJECT_MAPPER.readTree(issueResponse);
			String actualProjectKey = root.path(FIELD_FIELDS).path(FIELD_PROJECT).path(FIELD_KEY).asText();

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
	public static List<Map<String, Object>> getStatus(String accessToken, String baseUrl, String issueKey) {
		try {
			final String toStatus = "toStatus";
			validateJiraContext(accessToken, baseUrl);
			validateRequiredString(issueKey, "Issue key");
			Map<String, String> headers = buildHeaders(accessToken);
			String url = baseUrl + API_PATH_ISSUE + "/" + issueKey + API_SUFFIX_TRANSITIONS;
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);

			JsonNode root = OBJECT_MAPPER.readTree(response);

			List<Map<String, Object>> transitionList = new ArrayList<>();
			for (JsonNode t : root.path(FIELD_TRANSITIONS)) {
				Map<String, Object> transition = new HashMap<>();
				transition.put(FIELD_ID, t.path(FIELD_ID).asText());
				transition.put(FIELD_NAME, t.path(FIELD_NAME).asText());
				transition.put(toStatus, t.path(FIELD_TO).path(FIELD_NAME).asText());
				transitionList.add(transition);
			}
			return transitionList;

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in getStatus for '{}': {}", issueKey, e.getMessage(), e);
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
	 * @return list of maps, each containing {@code id}, {@code author},
	 *         {@code created}, {@code updated}, and {@code body}
	 */
	public static List<Map<String, Object>> getComments(String accessToken, String baseUrl, String issueKey) {
		try {
			final String updated = "updated";
			validateJiraContext(accessToken, baseUrl);
			validateRequiredString(issueKey, "Issue key");
			Map<String, String> headers = buildHeaders(accessToken);
			String url = baseUrl + API_PATH_ISSUE + "/" + issueKey + API_SUFFIX_COMMENT;
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);

			JsonNode root = OBJECT_MAPPER.readTree(response);

			List<Map<String, Object>> commentList = new ArrayList<>();
			for (JsonNode c : root.path(FIELD_COMMENTS)) {
				Map<String, Object> comment = new HashMap<>();
				comment.put(FIELD_ID, c.path(FIELD_ID).asText());
				comment.put(FIELD_AUTHOR, c.path(FIELD_AUTHOR).path(FIELD_DISPLAY_NAME).asText());
				comment.put(FIELD_CREATED, c.path(FIELD_CREATED).asText());
				comment.put(updated, c.path(updated).asText());
				comment.put(FIELD_BODY, parseAdfToPlainText(c.path(FIELD_BODY)));
				commentList.add(comment);
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
	 * @return map containing {@code id}, {@code author}, {@code created}, and
	 *         {@code success}
	 */
	public static Map<String, Object> addComment(String accessToken, String baseUrl, String issueKey,
			String commentText) {
		try {
			validateJiraContext(accessToken, baseUrl);
			validateRequiredString(issueKey, "Issue key");
			validateRequiredString(commentText, "Comment text");
			Map<String, String> headers = buildHeaders(accessToken);

			String url = baseUrl + API_PATH_ISSUE + "/" + issueKey + API_SUFFIX_COMMENT;
			String response = HttpHelperUtility.postRequestStringBody(url, headers,
					GSON.toJson(Map.of(FIELD_BODY, buildAdfDocument(commentText))), ContentType.APPLICATION_JSON, null, null,
					null);

			JsonNode root = OBJECT_MAPPER.readTree(response);

			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_ID, root.path(FIELD_ID).asText());
			result.put(FIELD_AUTHOR, root.path(FIELD_AUTHOR).path(FIELD_DISPLAY_NAME).asText());
			result.put(FIELD_CREATED, root.path(FIELD_CREATED).asText());
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

	private static Map<String, Object> executeJqlSearch(String accessToken, String baseUrl, String jql,
			String nextPageToken, int maxResults) throws Exception {
		final String jiraSearchUrl = "/rest/api/3/search/jql";
		final String isLast = "isLast";
		Map<String, String> headers = buildHeaders(accessToken);

		int safeMax = JiraUtils.clampMaxResults(maxResults);

		Map<String, Object> body = new HashMap<>();
		body.put(FIELD_JQL, jql);
		body.put(FIELD_MAX_RESULTS, safeMax);
		body.put(FIELD_FIELDS, Arrays.asList(FIELD_SUMMARY, FIELD_STATUS, FIELD_ASSIGNEE, FIELD_PRIORITY,
				FIELD_ISSUE_TYPE, FIELD_DUE_DATE, FIELD_LABELS));
		if (nextPageToken != null && !nextPageToken.trim().isEmpty()) {
			body.put(FIELD_NEXT_PAGE_TOKEN, nextPageToken);
		}

		String response = HttpHelperUtility.postRequestStringBody(baseUrl + jiraSearchUrl, headers,
				GSON.toJson(body), ContentType.APPLICATION_JSON, null, null, null);
		JsonNode root = OBJECT_MAPPER.readTree(response);

		List<Map<String, Object>> issueList = new ArrayList<>();
		for (JsonNode issue : root.path(FIELD_ISSUES)) {
			issueList.add(parseIssueSummary(issue));
		}

		Map<String, Object> result = new HashMap<>();
		result.put(FIELD_ISSUES, issueList);
		result.put(isLast, root.path(isLast).asBoolean(true));
		result.put(FIELD_MAX_RESULTS, safeMax);
		if (root.has(FIELD_NEXT_PAGE_TOKEN)) {
			result.put(FIELD_NEXT_PAGE_TOKEN, root.path(FIELD_NEXT_PAGE_TOKEN).asText());
		}
		return result;
	}

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
		JsonNode projectNode = OBJECT_MAPPER.readTree(response);
		String resolvedId = projectNode.path(FIELD_ID).asText();
		if (resolvedId == null || resolvedId.trim().isEmpty()) {
			throw new SemossPixelException("Unable to resolve project ID for project '" + projectIdOrKey
					+ "'. Check the project key is correct.");
		}
		return resolvedId;
	}

	private static String applyTransitionByName(String urlBase, String accessToken,
			String issueKey, String statusName) throws Exception {
		String url = urlBase + API_PATH_ISSUE + "/" + issueKey + API_SUFFIX_TRANSITIONS;
		Map<String, String> headers = buildHeaders(accessToken);
		String getResponse = HttpHelperUtility.getRequest(url, headers, null, null, null);
		JsonNode transitions = OBJECT_MAPPER.readTree(getResponse).path(FIELD_TRANSITIONS);
		for (JsonNode t : transitions) {
			if (t.path(FIELD_TO).path(FIELD_NAME).asText("").equalsIgnoreCase(statusName)) {
				HttpHelperUtility.postRequestStringBody(url, headers,
						GSON.toJson(Map.of(FIELD_TRANSITION, Map.of(FIELD_ID, t.path(FIELD_ID).asText()))),
						ContentType.APPLICATION_JSON, null, null, null);
				return statusName;
			}
		}
		throw new SemossPixelException("Could not transition issue " + issueKey + " to '" + statusName + "'.");
	}

	private static Map<String, String> buildHeaders(String accessToken) {
		Map<String, String> headers = new HashMap<>();
		headers.put(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken);
		headers.put(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
		headers.put(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());
		headers.put(HttpHeaders.USER_AGENT, "Semoss-Jira-Connector/1.0");
		return headers;
	}

	private static void validateJiraContext(String accessToken, String baseUrl) {
		if (accessToken == null || accessToken.trim().isEmpty()) {
			throw new SemossPixelException("Jira access token must not be empty.");
		}
		if (baseUrl == null || baseUrl.trim().isEmpty()) {
			throw new SemossPixelException("Jira base URL must not be empty.");
		}
	}

	private static void validateRequiredString(String value, String fieldName) {
		if (value == null || value.trim().isEmpty()) {
			throw new SemossPixelException(fieldName + " must not be empty.");
		}
	}

	private static Map<String, Object> parseIssueSummary(JsonNode issue) {
		JsonNode f = issue.path(FIELD_FIELDS);
		Map<String, Object> ticket = new HashMap<>();
		ticket.put(FIELD_ID, issue.path(FIELD_ID).asText());
		ticket.put(FIELD_KEY, issue.path(FIELD_KEY).asText());
		ticket.put(FIELD_SELF, issue.path(FIELD_SELF).asText());
		ticket.put(FIELD_SUMMARY, f.path(FIELD_SUMMARY).asText());
		ticket.put(FIELD_STATUS, f.path(FIELD_STATUS).path(FIELD_NAME).asText());
		ticket.put(FIELD_PRIORITY, f.path(FIELD_PRIORITY).path(FIELD_NAME).asText());
		ticket.put(FIELD_ISSUE_TYPE, f.path(FIELD_ISSUE_TYPE).path(FIELD_NAME).asText());
		ticket.put(FIELD_ASSIGNEE, f.path(FIELD_ASSIGNEE).path(FIELD_DISPLAY_NAME).asText(DEFAULT_UNASSIGNED));
		ticket.put(FIELD_ACCOUNT_ID, f.path(FIELD_ASSIGNEE).path(FIELD_ACCOUNT_ID).asText(""));
		ticket.put(FIELD_DUE_DATE, f.path(FIELD_DUE_DATE).asText());
		JsonNode labelsNode = f.path(FIELD_LABELS);
		List<String> labels = new ArrayList<>();
		if (labelsNode.isArray()) {
			for (JsonNode lbl : labelsNode) {
				labels.add(lbl.asText());
			}
		}
		ticket.put(FIELD_LABELS, labels);
		return ticket;
	}

	/**
	 * Extracts plain text from an Atlassian Document Format (ADF) JSON node.
	 * Uses an iterative depth-first traversal to concatenate all text nodes.
	 *
	 * @param adfNode the ADF root node (may be null, missing, or empty)
	 * @return extracted plain text, or empty string if no text found
	 */
	private static String parseAdfToPlainText(JsonNode adfNode) {
		if (adfNode == null || adfNode.isNull() || adfNode.isMissingNode()) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		List<JsonNode> stack = new ArrayList<>();
		stack.add(adfNode);
		while (!stack.isEmpty()) {
			JsonNode current = stack.remove(stack.size() - 1);
			if (current.has(ADF_TEXT)) {
				sb.append(current.get(ADF_TEXT).asText()).append(" ");
			}
			if (current.has(ADF_CONTENT)) {
				JsonNode children = current.get(ADF_CONTENT);
				for (int i = children.size() - 1; i >= 0; i--) {
					stack.add(children.get(i));
				}
			}
		}
		return sb.toString().trim();
	}

	/**
	 * Builds an Atlassian Document Format (ADF) document from plain text.
	 * Splits on double newlines for paragraphs and single newlines for hard breaks.
	 *
	 * @param text plain text input
	 * @return ADF document structure as a Map
	 */
	static Map<String, Object> buildAdfDocument(String text) {
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
			paragraphs.add(Map.of(ADF_TYPE, paragraph, ADF_CONTENT,
					List.of(Map.of(ADF_TYPE, ADF_TEXT, ADF_TEXT, text))));
		}
		return Map.of(ADF_TYPE, doc, version, 1, ADF_CONTENT, paragraphs);
	}

	// =========================================================================
	// New methods for enhanced Jira connector capabilities
	// =========================================================================

	/**
	 * Edits an existing comment on a Jira issue.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param issueKey    issue key (for example, {@code PROJECT-123})
	 * @param commentId   the numeric comment ID
	 * @param commentText updated plain text comment body
	 * @return map containing {@code id}, {@code author}, {@code created}, {@code updated}, and {@code success}
	 */
	public static Map<String, Object> editComment(String accessToken, String baseUrl, String issueKey,
			String commentId, String commentText) {
		try {
			validateJiraContext(accessToken, baseUrl);
			validateRequiredString(issueKey, "Issue key");
			validateRequiredString(commentId, "Comment ID");
			validateRequiredString(commentText, "Comment text");
			Map<String, String> headers = buildHeaders(accessToken);

			String url = baseUrl + API_PATH_ISSUE + "/" + issueKey + API_SUFFIX_COMMENT + "/" + commentId;
			String response = HttpHelperUtility.putRequestStringBody(url, headers,
					GSON.toJson(Map.of(FIELD_BODY, buildAdfDocument(commentText))), ContentType.APPLICATION_JSON,
					null, null, null);

			JsonNode root = OBJECT_MAPPER.readTree(response);

			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_ID, root.path(FIELD_ID).asText());
			result.put(FIELD_AUTHOR, root.path(FIELD_AUTHOR).path(FIELD_DISPLAY_NAME).asText());
			result.put(FIELD_CREATED, root.path(FIELD_CREATED).asText());
			result.put("updated", root.path("updated").asText());
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
			validateRequiredString(issueKey, "Issue key");
			validateRequiredString(commentId, "Comment ID");
			Map<String, String> headers = buildHeaders(accessToken);

			String url = baseUrl + API_PATH_ISSUE + "/" + issueKey + API_SUFFIX_COMMENT + "/" + commentId;
			HttpHelperUtility.deleteRequestStringBody(url, headers, null, null, null);

			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_SUCCESS, true);
			return result;

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in deleteComment for '{}' comment '{}': {}", issueKey, commentId, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to delete comment '" + commentId + "' on issue '" + issueKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Assigns a Jira issue to a user, or removes the assignment.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param issueKey    issue key (for example, {@code PROJECT-123})
	 * @param accountId   the assignee's account ID, or {@code null}/empty to unassign
	 * @return map containing {@code key}, {@code assignee}, and {@code success}
	 */
	public static Map<String, Object> assignIssue(String accessToken, String baseUrl, String issueKey,
			String accountId) {
		try {
			validateJiraContext(accessToken, baseUrl);
			validateRequiredString(issueKey, "Issue key");
			Map<String, String> headers = buildHeaders(accessToken);

			String url = baseUrl + API_PATH_ISSUE + "/" + issueKey + "/assignee";
			// null accountId means unassign; Jira expects {"accountId": null} or
			// {"accountId": "-1"} for automatic, but null accountId sets unassigned
			Map<String, Object> body = new HashMap<>();
			body.put(FIELD_ACCOUNT_ID, (accountId != null && !accountId.trim().isEmpty()) ? accountId : null);
			HttpHelperUtility.putRequestStringBody(url, headers, GSON.toJson(body), ContentType.APPLICATION_JSON,
					null, null, null);

			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_KEY, issueKey);
			result.put(FIELD_ASSIGNEE, (accountId != null && !accountId.trim().isEmpty()) ? accountId : DEFAULT_UNASSIGNED);
			result.put(FIELD_SUCCESS, true);
			return result;

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in assignIssue '{}': {}", issueKey, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to assign issue '" + issueKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Retrieves attachments for a Jira issue.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param issueKey    issue key (for example, {@code PROJECT-123})
	 * @return list of maps, each containing {@code id}, {@code filename}, {@code mimeType},
	 *         {@code size}, {@code created}, and {@code contentUrl}
	 */
	public static List<Map<String, Object>> getAttachments(String accessToken, String baseUrl, String issueKey) {
		try {
			final String attachment = "attachment";
			final String filename = "filename";
			final String mimeType = "mimeType";
			final String size = "size";
			final String contentUrl = "content";
			validateJiraContext(accessToken, baseUrl);
			validateRequiredString(issueKey, "Issue key");
			Map<String, String> headers = buildHeaders(accessToken);
			String response = HttpHelperUtility.getRequest(
					baseUrl + API_PATH_ISSUE + "/" + issueKey + "?fields=attachment", headers, null, null, null);

			JsonNode root = OBJECT_MAPPER.readTree(response);
			JsonNode attachments = root.path(FIELD_FIELDS).path(attachment);

			List<Map<String, Object>> attachmentList = new ArrayList<>();
			for (JsonNode a : attachments) {
				Map<String, Object> att = new HashMap<>();
				att.put(FIELD_ID, a.path(FIELD_ID).asText());
				att.put(filename, a.path(filename).asText());
				att.put(mimeType, a.path(mimeType).asText());
				att.put(size, a.path(size).asLong());
				att.put(FIELD_CREATED, a.path(FIELD_CREATED).asText());
				att.put(contentUrl, a.path(contentUrl).asText());
				att.put(FIELD_AUTHOR, a.path(FIELD_AUTHOR).path(FIELD_DISPLAY_NAME).asText());
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
	 * @return map containing {@code id}, {@code filename}, {@code size}, and {@code success}
	 */
	public static Map<String, Object> addAttachment(String accessToken, String baseUrl, String issueKey,
			String filePath) {
		try {
			validateJiraContext(accessToken, baseUrl);
			validateRequiredString(issueKey, "Issue key");
			validateRequiredString(filePath, "File path");

			String url = baseUrl + API_PATH_ISSUE + "/" + issueKey + "/attachments";

			File file = new File(filePath);
			if (!file.exists() || !file.isFile()) {
				throw new SemossPixelException("File not found: " + filePath);
			}

			// Jira attachment API requires multipart/form-data with X-Atlassian-Token: no-check
			String response;
			try (CloseableHttpClient httpClient = HttpClientBuilder.create().useSystemProperties().build()) {
				HttpPost post = new HttpPost(url);
				post.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken);
				post.addHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());
				post.addHeader("X-Atlassian-Token", "no-check");

				HttpEntity entity = MultipartEntityBuilder.create()
						.addPart("file", new FileBody(file, ContentType.APPLICATION_OCTET_STREAM))
						.build();
				post.setEntity(entity);

				response = httpClient.execute(post, resp -> EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8));
			}

			JsonNode root = OBJECT_MAPPER.readTree(response);
			JsonNode firstAttachment = root.isArray() && root.size() > 0 ? root.get(0) : root;

			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_ID, firstAttachment.path(FIELD_ID).asText());
			result.put("filename", firstAttachment.path("filename").asText());
			result.put("size", firstAttachment.path("size").asLong());
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
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param linkType    the link type name (for example, {@code "Blocks"}, {@code "Relates"})
	 * @param inwardIssue the issue key for the inward side of the link
	 * @param outwardIssue the issue key for the outward side of the link
	 * @return map containing {@code success}
	 */
	public static Map<String, Object> linkIssues(String accessToken, String baseUrl, String linkType,
			String inwardIssue, String outwardIssue) {
		try {
			final String issueLinkUrl = "/rest/api/3/issueLink";
			validateJiraContext(accessToken, baseUrl);
			validateRequiredString(linkType, "Link type");
			validateRequiredString(inwardIssue, "Inward issue key");
			validateRequiredString(outwardIssue, "Outward issue key");
			Map<String, String> headers = buildHeaders(accessToken);

			Map<String, Object> body = new HashMap<>();
			body.put("type", Map.of(FIELD_NAME, linkType));
			body.put("inwardIssue", Map.of(FIELD_KEY, inwardIssue));
			body.put("outwardIssue", Map.of(FIELD_KEY, outwardIssue));

			HttpHelperUtility.postRequestStringBody(baseUrl + issueLinkUrl, headers,
					GSON.toJson(body), ContentType.APPLICATION_JSON, null, null, null);

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
	 * @return list of maps, each containing {@code id}, {@code name}, {@code inward}, and {@code outward}
	 */
	public static List<Map<String, Object>> getIssueLinkTypes(String accessToken, String baseUrl) {
		try {
			final String issueLinkTypeUrl = "/rest/api/3/issueLinkType";
			final String issueLinkTypes = "issueLinkTypes";
			final String inward = "inward";
			final String outward = "outward";
			validateJiraContext(accessToken, baseUrl);
			Map<String, String> headers = buildHeaders(accessToken);
			String response = HttpHelperUtility.getRequest(baseUrl + issueLinkTypeUrl, headers, null, null, null);

			JsonNode root = OBJECT_MAPPER.readTree(response);
			JsonNode linkTypes = root.path(issueLinkTypes);

			List<Map<String, Object>> linkTypeList = new ArrayList<>();
			for (JsonNode lt : linkTypes) {
				Map<String, Object> linkType = new HashMap<>();
				linkType.put(FIELD_ID, lt.path(FIELD_ID).asText());
				linkType.put(FIELD_NAME, lt.path(FIELD_NAME).asText());
				linkType.put(inward, lt.path(inward).asText());
				linkType.put(outward, lt.path(outward).asText());
				linkTypeList.add(linkType);
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
	 * @param timeSpent   time in Jira notation (for example, {@code "2h 30m"}, {@code "1d"})
	 * @param comment     optional comment for the worklog entry
	 * @param started     optional start datetime in ISO format; defaults to now
	 * @return map containing worklog entry details and {@code success}
	 */
	public static Map<String, Object> logWork(String accessToken, String baseUrl, String issueKey,
			String timeSpent, String comment, String started) {
		try {
			final String worklogUrl = "/worklog";
			final String timeSpentField = "timeSpent";
			final String startedField = "started";
			validateJiraContext(accessToken, baseUrl);
			validateRequiredString(issueKey, "Issue key");
			validateRequiredString(timeSpent, "Time spent");
			Map<String, String> headers = buildHeaders(accessToken);

			Map<String, Object> body = new HashMap<>();
			body.put(timeSpentField, timeSpent);
			if (comment != null && !comment.trim().isEmpty()) {
				body.put(FIELD_COMMENT, buildAdfDocument(comment));
			}
			if (started != null && !started.trim().isEmpty()) {
				body.put(startedField, started);
			}

			String url = baseUrl + API_PATH_ISSUE + "/" + issueKey + worklogUrl;
			String response = HttpHelperUtility.postRequestStringBody(url, headers,
					GSON.toJson(body), ContentType.APPLICATION_JSON, null, null, null);

			JsonNode root = OBJECT_MAPPER.readTree(response);

			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_ID, root.path(FIELD_ID).asText());
			result.put(timeSpentField, root.path(timeSpentField).asText());
			result.put("timeSpentSeconds", root.path("timeSpentSeconds").asLong());
			result.put(FIELD_AUTHOR, root.path(FIELD_AUTHOR).path(FIELD_DISPLAY_NAME).asText());
			result.put(FIELD_CREATED, root.path(FIELD_CREATED).asText());
			result.put(FIELD_SUCCESS, true);
			return result;

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in logWork for '{}': {}", issueKey, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to log work on issue '" + issueKey + "'. Error: " + e.getMessage());
		}
	}

	/**
	 * Retrieves worklog entries for a Jira issue.
	 *
	 * @param accessToken Jira OAuth access token
	 * @param baseUrl     Jira API base URL including cloud ID
	 * @param issueKey    issue key (for example, {@code PROJECT-123})
	 * @return list of maps, each containing worklog entry details
	 */
	public static List<Map<String, Object>> getWorklogs(String accessToken, String baseUrl, String issueKey) {
		try {
			final String worklogUrl = "/worklog";
			final String worklogs = "worklogs";
			final String timeSpentField = "timeSpent";
			final String startedField = "started";
			validateJiraContext(accessToken, baseUrl);
			validateRequiredString(issueKey, "Issue key");
			Map<String, String> headers = buildHeaders(accessToken);
			String url = baseUrl + API_PATH_ISSUE + "/" + issueKey + worklogUrl;
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);

			JsonNode root = OBJECT_MAPPER.readTree(response);

			List<Map<String, Object>> worklogList = new ArrayList<>();
			for (JsonNode w : root.path(worklogs)) {
				Map<String, Object> entry = new HashMap<>();
				entry.put(FIELD_ID, w.path(FIELD_ID).asText());
				entry.put(FIELD_AUTHOR, w.path(FIELD_AUTHOR).path(FIELD_DISPLAY_NAME).asText());
				entry.put(timeSpentField, w.path(timeSpentField).asText());
				entry.put("timeSpentSeconds", w.path("timeSpentSeconds").asLong());
				entry.put(startedField, w.path(startedField).asText());
				entry.put(FIELD_CREATED, w.path(FIELD_CREATED).asText());
				entry.put("updated", w.path("updated").asText());
				entry.put(FIELD_BODY, parseAdfToPlainText(w.path("comment")));
				worklogList.add(entry);
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
			final String issueLinkUrl = "/rest/api/3/issueLink/";
			validateJiraContext(accessToken, baseUrl);
			validateRequiredString(linkId, "Link ID");
			Map<String, String> headers = buildHeaders(accessToken);

			HttpHelperUtility.deleteRequestStringBody(baseUrl + issueLinkUrl + linkId, headers, null, null, null);

			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_SUCCESS, true);
			return result;

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in unlinkIssues for link '{}': {}", linkId, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to remove issue link '" + linkId + "'. Error: " + e.getMessage());
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
			final String attachmentUrl = "/rest/api/3/attachment/";
			validateJiraContext(accessToken, baseUrl);
			validateRequiredString(attachmentId, "Attachment ID");
			Map<String, String> headers = buildHeaders(accessToken);

			HttpHelperUtility.deleteRequestStringBody(baseUrl + attachmentUrl + attachmentId, headers, null, null, null);

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
	 * @param timeSpent   updated time in Jira notation (for example, {@code "3h"}, {@code "1d 2h"})
	 * @param comment     optional updated comment for the worklog entry
	 * @param started     optional updated start datetime in ISO format
	 * @return map containing updated worklog entry details and {@code success}
	 */
	public static Map<String, Object> updateWorklog(String accessToken, String baseUrl, String issueKey,
			String worklogId, String timeSpent, String comment, String started) {
		try {
			final String worklogUrl = "/worklog/";
			final String timeSpentField = "timeSpent";
			final String startedField = "started";
			validateJiraContext(accessToken, baseUrl);
			validateRequiredString(issueKey, "Issue key");
			validateRequiredString(worklogId, "Worklog ID");
			validateRequiredString(timeSpent, "Time spent");
			Map<String, String> headers = buildHeaders(accessToken);

			Map<String, Object> body = new HashMap<>();
			body.put(timeSpentField, timeSpent);
			if (comment != null && !comment.trim().isEmpty()) {
				body.put(FIELD_COMMENT, buildAdfDocument(comment));
			}
			if (started != null && !started.trim().isEmpty()) {
				body.put(startedField, started);
			}

			String url = baseUrl + API_PATH_ISSUE + "/" + issueKey + worklogUrl + worklogId;
			String response = HttpHelperUtility.putRequestStringBody(url, headers,
					GSON.toJson(body), ContentType.APPLICATION_JSON, null, null, null);

			JsonNode root = OBJECT_MAPPER.readTree(response);

			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_ID, root.path(FIELD_ID).asText());
			result.put(timeSpentField, root.path(timeSpentField).asText());
			result.put("timeSpentSeconds", root.path("timeSpentSeconds").asLong());
			result.put(FIELD_AUTHOR, root.path(FIELD_AUTHOR).path(FIELD_DISPLAY_NAME).asText());
			result.put("updated", root.path("updated").asText());
			result.put(FIELD_SUCCESS, true);
			return result;

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in updateWorklog for '{}' worklog '{}': {}", issueKey, worklogId, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to update worklog '" + worklogId + "' on issue '" + issueKey + "'. Error: " + e.getMessage());
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
			final String worklogUrl = "/worklog/";
			validateJiraContext(accessToken, baseUrl);
			validateRequiredString(issueKey, "Issue key");
			validateRequiredString(worklogId, "Worklog ID");
			Map<String, String> headers = buildHeaders(accessToken);

			String url = baseUrl + API_PATH_ISSUE + "/" + issueKey + worklogUrl + worklogId;
			HttpHelperUtility.deleteRequestStringBody(url, headers, null, null, null);

			Map<String, Object> result = new HashMap<>();
			result.put(FIELD_SUCCESS, true);
			return result;

		} catch (SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Error in deleteWorklog for '{}' worklog '{}': {}", issueKey, worklogId, e.getMessage(), e);
			throw new SemossPixelException(
					"Failed to delete worklog '" + worklogId + "' on issue '" + issueKey + "'. Error: " + e.getMessage());
		}
	}
}
