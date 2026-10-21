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
package prerna.usertracking;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IRDBMSEngine;
import prerna.logging.SemossLogUtils;
import prerna.om.Insight;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.ConnectionUtils;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

/**
 * Central writer for the {@code USER_AUDIT_EVENTS} security audit trail.
 * <p>
 * Every write is best-effort: when user tracking is disabled the methods are
 * no-ops, and an insert failure is logged but never propagated to the business
 * action being audited. All JSON payloads and error messages are sanitized by
 * {@link AuditSanitizer} before they are stored, raw session ids are never
 * stored (only {@code SESSION_ID_HASH}), and request context (request id, IP,
 * user agent, method, path) is taken from the request-scoped logging context
 * when the caller does not supply it.
 * <p>
 * The kinds of event recorded and the values that describe them are fixed
 * lists, stored by name: {@link AuditEventType}, each with its
 * {@link AuditCategory} and {@link AuditSeverity}, {@link AuditStatus},
 * {@link AuditTargetType}, {@link AuditErrorCode} and {@link AuditSourceApp}.
 */
public final class UserAuditTrailUtils {

	private static final Logger classLogger = LogManager.getLogger(UserAuditTrailUtils.class);

	public static final String TABLE = "USER_AUDIT_EVENTS";

	/**
	 * Actor types that are not a login. {@code ACTOR_USER_TYPE} otherwise holds the
	 * actor's login type, such as NATIVE or MICROSOFT, so it stays text.
	 */
	public static final String ACTOR_TYPE_AGENT = "AGENT";
	public static final String ACTOR_TYPE_SYSTEM = "SYSTEM";

	/**
	 * The action of the {@link AuditEventType#PERMISSION_DELETE} SEMOSS records
	 * when a grant's end date passes and it removes the grant
	 */
	public static final String PERMISSION_EXPIRED_ACTION = "PERMISSION_EXPIRED";

	/**
	 * Logging context keys an agent run sets so its actions are attributed to the
	 * agent.
	 */
	public static final String AGENT_ID_CONTEXT_KEY = "auditAgentId";
	public static final String AGENT_NAME_CONTEXT_KEY = "auditAgentName";
	public static final String AGENT_RUN_ID_CONTEXT_KEY = "auditAgentRunId";

	/** Insert order of every column in {@code USER_AUDIT_EVENTS}. */
	public static final List<String> COLUMNS = List.of("EVENT_ID", "EVENT_TIME", "EVENT_OCCURRED_TIME", "EVENT_TYPE",
			"ACTION", "STATUS", "CATEGORY", "SEVERITY", "ACTOR_USER_ID", "ACTOR_USER_TYPE", "ACTOR_USER_NAME",
			"ACTOR_IS_ADMIN", "SUBJECT_USER_ID", "SUBJECT_USER_TYPE", "SUBJECT_USER_NAME", "SESSION_ID_HASH",
			"REQUEST_ID", "IP_ADDR", "USER_AGENT", "HTTP_METHOD", "REQUEST_PATH", "HTTP_STATUS", "TARGET_TYPE",
			"TARGET_ID", "TARGET_NAME", "PROJECT_ID", "ENGINE_ID", "INSIGHT_ID", "ROOM_ID", "OLD_VALUE", "NEW_VALUE",
			"DETAILS", "ERROR_CODE", "ERROR_MESSAGE", "SOURCE_APP", "SOURCE_MODULE", "SOURCE_CLASS", "HASH_PREVIOUS",
			"HASH_CURRENT");

	private static final String INSERT_AUDIT_EVENT = "INSERT INTO " + TABLE + " (" + String.join(", ", COLUMNS)
			+ ", AUDIT_SEQUENCE) VALUES (" + String.join(", ", COLUMNS.stream().map(c -> "?").toList()) + ", ?)";

	private static final Set<String> PLACEHOLDER_CONTEXT_VALUES = Set.of("UNKNOWN", "STARTUP", "SHUTDOWN",
			"NO_SESSION_ID", "NULL");

	private static final Pattern DENIAL_PATTERN = Pattern.compile("(?i)(only exposed for admins|must be an admin"
			+ "|is not an admin|not an admin|insufficient privileges|insufficient permission"
			+ "|does not have (edit |proper |the required |owner )?(access|permission|permissions)"
			+ "|do not have (access|permission)|don't have (access|permission)|not authorized|unauthorized"
			+ "|permission denied|access denied)");
	private static final Pattern ADMIN_DENIAL_PATTERN = Pattern
			.compile("(?i)(only exposed for admins|must be an admin|is not an admin|not an admin)");
	private static final Pattern LOGIN_REQUIRED_PATTERN = Pattern
			.compile("(?i)(must be logged in|login required|user is not logged in|anonymous users)");
	private static final Pattern NOT_FOUND_PATTERN = Pattern.compile("(?i)(does not exist|could not find|not found)");
	private static final Pattern REACTOR_CALL = Pattern.compile("([A-Za-z_][A-Za-z0-9_]*)\\s*\\(");

	/**
	 * The event a reactor's action is recorded as, and what it is done to.
	 *
	 * @param eventType  the event the action is recorded as
	 * @param targetType what the action is done to
	 */
	private record AuditedAction(AuditEventType eventType, AuditTargetType targetType) {
	}

	/**
	 * Reactors whose failures are audited, mapped to the event type and target type
	 * of the action that failed. Authorization denials are audited for every
	 * reactor regardless of this list.
	 */
	private static final Map<String, AuditedAction> AUDITED_REACTORS = Map.ofEntries(
			Map.entry("CreateProject", new AuditedAction(AuditEventType.PROJECT_CREATE, AuditTargetType.PROJECT)),
			Map.entry("CreateAppFromBlocks", new AuditedAction(AuditEventType.PROJECT_CREATE, AuditTargetType.PROJECT)),
			Map.entry("CreateAppFromTemplate",
					new AuditedAction(AuditEventType.PROJECT_CREATE, AuditTargetType.PROJECT)),
			Map.entry("UploadProject", new AuditedAction(AuditEventType.PROJECT_UPLOAD, AuditTargetType.PROJECT)),
			Map.entry("UploadProjectApp", new AuditedAction(AuditEventType.PROJECT_UPLOAD, AuditTargetType.PROJECT)),
			Map.entry("DeleteProject", new AuditedAction(AuditEventType.PROJECT_DELETE, AuditTargetType.PROJECT)),
			Map.entry("UploadEngine", new AuditedAction(AuditEventType.ENGINE_CREATE, AuditTargetType.ENGINE)),
			Map.entry("CreateModelEngine", new AuditedAction(AuditEventType.MODEL_CREATE, AuditTargetType.MODEL)),
			Map.entry("CreateVectorDatabaseEngine",
					new AuditedAction(AuditEventType.VECTOR_CREATE, AuditTargetType.VECTOR)),
			Map.entry("CreateStorageEngine", new AuditedAction(AuditEventType.STORAGE_CREATE, AuditTargetType.STORAGE)),
			Map.entry("CreateFunctionEngine",
					new AuditedAction(AuditEventType.FUNCTION_CREATE, AuditTargetType.FUNCTION)),
			Map.entry("CreatePythonFunctionEngine",
					new AuditedAction(AuditEventType.FUNCTION_CREATE, AuditTargetType.FUNCTION)),
			Map.entry("CreateGuardrailEngine",
					new AuditedAction(AuditEventType.GUARDRAIL_CREATE, AuditTargetType.GUARDRAIL)),
			Map.entry("CreateVenvEngine", new AuditedAction(AuditEventType.ENGINE_CREATE, AuditTargetType.VENV)),
			Map.entry("DeleteEngine", new AuditedAction(AuditEventType.ENGINE_DELETE, AuditTargetType.ENGINE)),
			Map.entry("DeleteDatabase", new AuditedAction(AuditEventType.ENGINE_DELETE, AuditTargetType.DATABASE)),
			Map.entry("SetEngineMetadata",
					new AuditedAction(AuditEventType.ENGINE_METADATA_UPDATE, AuditTargetType.ENGINE)),
			Map.entry("SetEngineDisplayName",
					new AuditedAction(AuditEventType.ENGINE_DISPLAY_NAME_UPDATE, AuditTargetType.ENGINE)),
			Map.entry("SetProjectMetadata",
					new AuditedAction(AuditEventType.PROJECT_METADATA_UPDATE, AuditTargetType.PROJECT)),
			Map.entry("LoadProjectMetadata",
					new AuditedAction(AuditEventType.PROJECT_METADATA_UPDATE, AuditTargetType.PROJECT)),
			Map.entry("SetProjectDisplayName",
					new AuditedAction(AuditEventType.PROJECT_DISPLAY_NAME_UPDATE, AuditTargetType.PROJECT)),
			Map.entry("SaveAppBlocksJson",
					new AuditedAction(AuditEventType.PROJECT_BLOCKS_UPDATE, AuditTargetType.PROJECT)),
			Map.entry("SetDatabaseMetadata",
					new AuditedAction(AuditEventType.ENGINE_METADATA_UPDATE, AuditTargetType.DATABASE)),
			Map.entry("LoadEngineMetadata",
					new AuditedAction(AuditEventType.ENGINE_METADATA_UPDATE, AuditTargetType.ENGINE)),
			Map.entry("AddWorkspace", new AuditedAction(AuditEventType.WORKSPACE_CREATE, AuditTargetType.WORKSPACE)),
			Map.entry("EditWorkspace", new AuditedAction(AuditEventType.WORKSPACE_UPDATE, AuditTargetType.WORKSPACE)),
			Map.entry("DeleteWorkspace", new AuditedAction(AuditEventType.WORKSPACE_DELETE, AuditTargetType.WORKSPACE)),
			Map.entry("CreateAutomation",
					new AuditedAction(AuditEventType.AUTOMATION_CREATE, AuditTargetType.AUTOMATION)),
			Map.entry("SaveAutomation",
					new AuditedAction(AuditEventType.AUTOMATION_UPDATE, AuditTargetType.AUTOMATION)),
			Map.entry("TriggerAutomation",
					new AuditedAction(AuditEventType.AUTOMATION_RUN_START, AuditTargetType.AUTOMATION)),
			Map.entry("CancelAutomationRun",
					new AuditedAction(AuditEventType.AUTOMATION_RUN_CANCEL, AuditTargetType.AUTOMATION)),
			Map.entry("ResumeAutomationRun",
					new AuditedAction(AuditEventType.AUTOMATION_RUN_RESUME, AuditTargetType.AUTOMATION)),
			Map.entry("ResolveAutomationAgentRunAction",
					new AuditedAction(AuditEventType.AUTOMATION_ACTION_RESOLVE, AuditTargetType.AUTOMATION)),
			Map.entry("CreateSkill", new AuditedAction(AuditEventType.SKILL_CREATE, AuditTargetType.SKILL)),
			Map.entry("UpdateSkill", new AuditedAction(AuditEventType.SKILL_UPDATE, AuditTargetType.SKILL)),
			Map.entry("DeleteSkill", new AuditedAction(AuditEventType.SKILL_DELETE, AuditTargetType.SKILL)),
			Map.entry("CloneSkill", new AuditedAction(AuditEventType.SKILL_CREATE, AuditTargetType.SKILL)),
			Map.entry("ScheduleJob", new AuditedAction(AuditEventType.JOB_CREATE, AuditTargetType.JOB)),
			Map.entry("EditScheduledJob", new AuditedAction(AuditEventType.JOB_UPDATE, AuditTargetType.JOB)),
			Map.entry("RemoveJobFromDB", new AuditedAction(AuditEventType.JOB_DELETE, AuditTargetType.JOB)),
			Map.entry("PauseJobTrigger", new AuditedAction(AuditEventType.JOB_PAUSE, AuditTargetType.JOB)),
			Map.entry("ResumeJobTrigger", new AuditedAction(AuditEventType.JOB_RESUME, AuditTargetType.JOB)),
			Map.entry("AdminUploadUsers", new AuditedAction(AuditEventType.USER_CREATE, AuditTargetType.USER)),
			Map.entry("AdminLoadLdapUsers", new AuditedAction(AuditEventType.USER_CREATE, AuditTargetType.USER)),
			Map.entry("AdminLockAccounts", new AuditedAction(AuditEventType.USER_DEACTIVATE, AuditTargetType.USER)),
			Map.entry("AdminUserAuditEvents", new AuditedAction(AuditEventType.AUDIT_QUERY, AuditTargetType.AUDIT)));

	/**
	 * Bounds the cache used to coalesce duplicate HTTP hooks for the same request.
	 */
	private static final long FAILURE_DEDUPE_WINDOW_MS = 60_000L;
	private static final int FAILURE_DEDUPE_MAX_ENTRIES = 10_000;
	private static final Map<String, Long> RECENT_FAILURES = new ConcurrentHashMap<>();

	private static final long ADMIN_CACHE_TTL_MS = 60_000L;
	private static final Map<String, CachedAdminFlag> ADMIN_CACHE = new ConcurrentHashMap<>();

	private UserAuditTrailUtils() {

	}

	/**
	 * Records an audit event in the user-tracking database. No-op when user
	 * tracking is disabled, so the audit trail follows the same deployment switch
	 * as the existing user activity tables. Never throws.
	 *
	 * @param event event payload to persist
	 */
	public static void recordEvent(AuditEvent event) {
		if (!Utility.isUserTrackingEnabled()) {
			return;
		}
		if (event == null) {
			return;
		}
		try {
			event.resolveDefaults(findCaller());
		} catch (Exception e) {
			classLogger.error("Failed to prepare audit event type={} action={}", event.eventType, event.action, e);
			return;
		}

		IRDBMSEngine userTrackingDb = SystemEngineRegistry.getUserTrackingDb();
		if (userTrackingDb == null) {
			classLogger.warn("User tracking database is not loaded; audit event type={} was not recorded",
					event.eventType);
			return;
		}
		try {
			QueryExecutionUtility.write(userTrackingDb, connection -> {
				AuditChainStore.Head head = AuditChainStore.advance(connection);
				event.hashPrevious = head.hash();
				event.hashCurrent = null;
				try (PreparedStatement ps = connection.prepareStatement(INSERT_AUDIT_EVENT)) {
					bind(userTrackingDb, ps, event);
					ps.setLong(COLUMNS.size() + 1, head.sequence());
					ps.executeUpdate();
				}
				// Hash the database representation, including its timestamp precision.
				String hash = AuditChainStore.hashPersistedRow(connection, head);
				QueryExecutionUtility.executeUpdate(connection,
						"UPDATE " + TABLE + " SET HASH_CURRENT=? WHERE AUDIT_SEQUENCE=?", ps -> {
							ps.setString(1, hash);
							ps.setLong(2, head.sequence());
						});
				QueryExecutionUtility.executeUpdate(connection,
						"UPDATE " + AuditChainStore.TABLE + " SET LAST_HASH=? WHERE CHAIN_ID='GLOBAL'",
						ps -> ps.setString(1, hash));
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Failed to record audit event type={} action={} targetType={} targetId={}",
					event.eventType, event.action, event.targetType, event.targetId, e);
		}
	}

	// ------------------------------------------------------------------------
	// authentication
	// ------------------------------------------------------------------------

	/**
	 * Records a successful login event.
	 *
	 * @param user      authenticated SEMOSS user
	 * @param provider  provider used for this login
	 * @param sessionId HTTP session id (hashed before storage)
	 * @param ipAddr    client IP address
	 */
	public static void recordLogin(User user, AuthProvider provider, String sessionId, String ipAddr) {
		Actor actor = getActor(user, provider);
		recordEvent(new AuditEvent().eventType(AuditEventType.LOGIN).status(AuditStatus.SUCCESS).actorUser(user)
				.actor(actor.userId, actor.userType, actor.userName).session(sessionId, null, ipAddr)
				.subject(actor.userId, actor.userType, actor.userName)
				.target(AuditTargetType.USER, actor.userId, actor.userName)
				.details(provider == null ? null : Map.of("provider", provider.getLabel())));
	}

	/**
	 * Records a successful logout/session-end event.
	 *
	 * @param user      SEMOSS user ending the session
	 * @param sessionId HTTP session id (hashed before storage)
	 * @param reason    explicit logout, timeout, or other session cleanup reason
	 */
	public static void recordLogout(User user, String sessionId, String reason) {
		recordLogout(user, sessionId, reason, null);
	}

	/**
	 * Records a successful logout/session-end event.
	 *
	 * @param user      SEMOSS user ending the session
	 * @param sessionId HTTP session id (hashed before storage)
	 * @param reason    explicit logout, timeout, or other session cleanup reason
	 * @param ipAddr    client IP address when the logout was request-driven
	 */
	public static void recordLogout(User user, String sessionId, String reason, String ipAddr) {
		Actor actor = getActor(user, null);
		recordEvent(new AuditEvent().eventType(AuditEventType.LOGOUT).action(reason).status(AuditStatus.SUCCESS)
				.actorUser(user).actor(actor.userId, actor.userType, actor.userName).session(sessionId, null, ipAddr)
				.subject(actor.userId, actor.userType, actor.userName)
				.target(AuditTargetType.USER, actor.userId, actor.userName)
				.details(reason == null ? null : Map.of("reason", reason)));
	}

	/**
	 * Records a failed login attempt. The attempted username is the subject; the
	 * password is never passed to this method.
	 *
	 * @param attemptedUsername username that was attempted, if any
	 * @param provider          login provider label (e.g. NATIVE, LDAP)
	 * @param errorCode         why the login did not succeed
	 * @param reason            sanitized reason
	 * @param ipAddr            client IP address
	 * @param httpStatus        HTTP status returned to the client
	 */
	public static void recordLoginFailed(String attemptedUsername, String provider, AuditErrorCode errorCode,
			String reason, String ipAddr, Integer httpStatus) {
		String subject = attemptedUsername == null || attemptedUsername.isBlank() ? null
				: AuditSanitizer.truncate(attemptedUsername.trim(), 255);
		AuditStatus status = errorCode == AuditErrorCode.INTERNAL_ERROR ? AuditStatus.ERROR : AuditStatus.FAILURE;
		Map<String, Object> details = new LinkedHashMap<>();
		putIfPresent(details, "provider", provider);
		recordEvent(new AuditEvent().eventType(AuditEventType.LOGIN_FAILED).action(AuditEventType.LOGIN.name())
				.status(status).actor(null, null, null).subject(subject, provider, null).subjectNameLookup(false)
				.target(AuditTargetType.USER, subject, null).session(null, null, ipAddr).httpStatus(httpStatus)
				.error(errorCode, reason).details(details));
	}

	// ------------------------------------------------------------------------
	// permissions
	// ------------------------------------------------------------------------

	public static void recordPermissionAdd(User user, AuditTargetType targetType, String targetId, String targetName,
			String projectId, String engineId, String insightId, String granteeId, String granteeType,
			String permission, Object details) {
		recordPermissionChange(user, AuditEventType.PERMISSION_ADD, targetType, targetId, targetName, projectId,
				engineId, insightId, granteeId, granteeType, null, permission, details);
	}

	public static void recordPermissionUpdate(User user, AuditTargetType targetType, String targetId, String targetName,
			String projectId, String engineId, String insightId, String granteeId, String granteeType,
			String oldPermission, String newPermission, Object details) {
		recordPermissionChange(user, AuditEventType.PERMISSION_UPDATE, targetType, targetId, targetName, projectId,
				engineId, insightId, granteeId, granteeType, oldPermission, newPermission, details);
	}

	public static void recordPermissionDelete(User user, AuditTargetType targetType, String targetId, String targetName,
			String projectId, String engineId, String insightId, String granteeId, String granteeType,
			String oldPermission, Object details) {
		recordPermissionChange(user, AuditEventType.PERMISSION_DELETE, targetType, targetId, targetName, projectId,
				engineId, insightId, granteeId, granteeType, oldPermission, null, details);
	}

	/**
	 * Records a permission the platform removed because its end date passed. The
	 * actor is the system, not a user.
	 */
	public static void recordExpiredPermissionRemoval(AuditTargetType targetType, String targetId, String projectId,
			String engineId, String insightId, String userId) {
		recordEvent(new AuditEvent().eventType(AuditEventType.PERMISSION_DELETE).action(PERMISSION_EXPIRED_ACTION)
				.status(AuditStatus.SUCCESS).systemActor().subject(userId, null, null)
				.target(targetType, targetId, null).context(projectId, engineId, insightId, null)
				.details(Map.of("reason", "EXPIRED")));
	}

	/**
	 * Records a team (group) permission change. Groups are not users, so the group
	 * is stored in {@code DETAILS} ({@code groupId}/{@code groupType}) and the
	 * subject user columns stay empty.
	 */
	public static void recordGroupPermissionChange(User user, AuditEventType eventType, AuditTargetType targetType,
			String targetId, String targetName, String projectId, String engineId, String insightId, String groupId,
			String groupType, String oldPermission, String newPermission, Object details) {
		Map<String, Object> detailMap = newDetails(details);
		putIfPresent(detailMap, "groupId", groupId);
		putIfPresent(detailMap, "groupType", groupType);
		recordEvent(new AuditEvent().eventType(eventType).status(AuditStatus.SUCCESS).actorUser(user)
				.target(targetType, targetId, targetName).context(projectId, engineId, insightId, null)
				.oldValue(oldPermission == null ? null : Map.of("permission", oldPermission))
				.newValue(newPermission == null ? null : Map.of("permission", newPermission)).details(detailMap));
	}

	/**
	 * Records a single summary event for a bulk permission operation (grant all,
	 * update all users, grant new users) instead of one row per affected user.
	 */
	public static void recordBulkPermissionChange(User user, String action, AuditTargetType targetType, String targetId,
			String projectId, String engineId, String insightId, String subjectUserId, String subjectUserType,
			String permission, Object details) {
		recordEvent(new AuditEvent().eventType(AuditEventType.PERMISSION_BULK_GRANT).action(action)
				.status(AuditStatus.SUCCESS).actorUser(user).subject(subjectUserId, subjectUserType, null)
				.target(targetType, targetId, null).context(projectId, engineId, insightId, null)
				.newValue(permission == null ? null : Map.of("permission", permission)).details(details));
	}

	public static void recordAccessRequestDecision(User user, AuditEventType decision, AuditTargetType targetType,
			String targetId, String targetName, String projectId, String engineId, String insightId, String requestId,
			String requesterId, String requesterType, String permission, Object details) {
		Map<String, Object> detailMap = newDetails(details);
		putIfPresent(detailMap, "requestId", requestId);
		putIfPresent(detailMap, "permission", permission);
		recordEvent(new AuditEvent().eventType(decision).status(AuditStatus.SUCCESS).actorUser(user)
				.subject(requesterId, requesterType, null).target(targetType, targetId, targetName)
				.context(projectId, engineId, insightId, null)
				.newValue(permission == null ? null : Map.of("permission", permission)).details(detailMap));
	}

	// ------------------------------------------------------------------------
	// user, group, and credential administration
	// ------------------------------------------------------------------------

	/**
	 * Records an administrative change to a user account.
	 *
	 * @param actorUser    the user performing the change (may be null; the request
	 *                     logging context is used instead)
	 * @param actorIsAdmin whether the actor is an admin, if already known
	 * @param eventType    one of the {@link AuditCategory#USER_ADMIN} events, such
	 *                     as {@link AuditEventType#USER_UPDATE}
	 */
	public static void recordUserAdmin(User actorUser, Boolean actorIsAdmin, AuditEventType eventType, String subjectId,
			String subjectType, String subjectName, Object oldValue, Object newValue, Object details) {
		recordEvent(new AuditEvent().eventType(eventType).status(AuditStatus.SUCCESS).actorUser(actorUser)
				.actorIsAdmin(actorIsAdmin).subject(subjectId, subjectType, subjectName)
				.target(AuditTargetType.USER, subjectId, subjectName).oldValue(oldValue).newValue(newValue)
				.details(details));
	}

	/**
	 * Records an administrative change to a team (group) or its membership.
	 *
	 * @param eventType one of the {@link AuditCategory#GROUP_ADMIN} events, such as
	 *                  {@link AuditEventType#GROUP_CREATE}
	 */
	public static void recordGroupAdmin(User actorUser, Boolean actorIsAdmin, AuditEventType eventType, String groupId,
			String groupType, String memberId, String memberType, Object oldValue, Object newValue, Object details) {
		Map<String, Object> detailMap = newDetails(details);
		putIfPresent(detailMap, "groupType", groupType);
		recordEvent(new AuditEvent().eventType(eventType).status(AuditStatus.SUCCESS).actorUser(actorUser)
				.actorIsAdmin(actorIsAdmin).subject(memberId, memberType, null)
				.target(AuditTargetType.GROUP, groupId, groupId).oldValue(oldValue).newValue(newValue)
				.details(detailMap));
	}

	// ------------------------------------------------------------------------
	// resource lifecycle
	// ------------------------------------------------------------------------

	public static void recordProjectLifecycle(User user, AuditEventType eventType, String projectId, String projectName,
			Object details) {
		recordEvent(new AuditEvent().eventType(eventType).status(AuditStatus.SUCCESS).actorUser(user)
				.target(AuditTargetType.PROJECT, projectId, projectName).context(projectId, null, null, null)
				.details(details));
	}

	public static void recordWorkspaceLifecycle(User user, AuditEventType eventType, String workspaceId,
			String workspaceName, Object details) {
		recordEvent(new AuditEvent().eventType(eventType).status(AuditStatus.SUCCESS).actorUser(user)
				.target(AuditTargetType.WORKSPACE, workspaceId, workspaceName).context(workspaceId, null, null, null)
				.details(details));
	}

	public static void recordEngineLifecycle(User user, AuditEventType eventType, AuditTargetType targetType,
			String engineId, String engineName, Object details) {
		recordEvent(new AuditEvent().eventType(eventType).status(AuditStatus.SUCCESS).actorUser(user)
				.target(targetType == null ? AuditTargetType.ENGINE : targetType, engineId, engineName)
				.context(null, engineId, null, null).details(details));
	}

	/**
	 * Records a lifecycle event for any other resource (automation, skill, job, API
	 * key, platform configuration).
	 */
	public static void recordResourceEvent(User user, AuditEventType eventType, AuditTargetType targetType,
			String targetId, String targetName, String projectId, Object oldValue, Object newValue, Object details) {
		recordEvent(new AuditEvent().eventType(eventType).status(AuditStatus.SUCCESS).actorUser(user)
				.target(targetType, targetId, targetName).context(projectId, null, null, null).oldValue(oldValue)
				.newValue(newValue).details(details));
	}

	/**
	 * Records a scheduled job event. The job group is the project (app) the job
	 * belongs to. The job recipe is never recorded because it can contain pixel
	 * with query text or credentials.
	 */
	public static void recordJobEvent(User user, AuditEventType eventType, String jobId, String jobName,
			String jobGroup, Object details) {
		Map<String, Object> detailMap = newDetails(details);
		putIfPresent(detailMap, "jobGroup", jobGroup);
		recordEvent(new AuditEvent().eventType(eventType).status(AuditStatus.SUCCESS).actorUser(user)
				.target(AuditTargetType.JOB, jobId, jobName).context(jobGroup, null, null, null).details(detailMap));
	}

	/**
	 * Records an automation (workflow) event. The automation is a project, so the
	 * project id is both the target and the project context.
	 *
	 * @param status how the action ended, or null for {@link AuditStatus#SUCCESS}
	 */
	public static void recordAutomationEvent(User user, AuditEventType eventType, String projectId, String runId,
			AuditStatus status, Object details) {
		Map<String, Object> detailMap = newDetails(details);
		putIfPresent(detailMap, "runId", runId);
		recordEvent(new AuditEvent().eventType(eventType).status(status == null ? AuditStatus.SUCCESS : status)
				.actorUser(user).target(AuditTargetType.AUTOMATION, projectId, null)
				.context(projectId, null, null, null).details(detailMap));
	}

	/**
	 * Records that an admin read or exported the audit trail.
	 *
	 * @param eventType {@link AuditEventType#AUDIT_QUERY},
	 *                  {@link AuditEventType#AUDIT_EXPORT} or
	 *                  {@link AuditEventType#AUDIT_VERIFY}
	 */
	public static void recordAuditAccess(User user, AuditEventType eventType, Object details) {
		recordEvent(new AuditEvent().eventType(eventType).status(AuditStatus.SUCCESS).actorUser(user)
				.target(AuditTargetType.AUDIT, TABLE, TABLE).details(details));
	}

	// ------------------------------------------------------------------------
	// failures
	// ------------------------------------------------------------------------

	/**
	 * Records a failed, denied, or errored action. The status and stable error code
	 * are derived from the error; the message is sanitized.
	 *
	 * @param user       actor
	 * @param eventType  the event that would have been recorded on success
	 * @param targetType affected resource type
	 * @param targetId   affected resource id
	 * @param error      the failure
	 * @param details    extra non-sensitive context
	 */
	public static void recordFailure(User user, AuditEventType eventType, AuditTargetType targetType, String targetId,
			Throwable error, Object details) {
		FailureClassification classification = classify(error, error == null ? null : error.getMessage());
		recordFailure(user, eventType, targetType, targetId, classification.status(), classification.errorCode(),
				error == null ? null : error.getMessage(), null, details);
	}

	/**
	 * Records a failed, denied, or errored action with an explicit status and code.
	 * Denials are stored as {@code AUTHORIZATION_DENIED} with the attempted action
	 * in {@code ACTION}.
	 */
	public static void recordFailure(User user, AuditEventType eventType, AuditTargetType targetType, String targetId,
			AuditStatus status, AuditErrorCode errorCode, String errorMessage, Integer httpStatus, Object details) {
		AuditEventType recordedType = status == AuditStatus.DENIED ? AuditEventType.AUTHORIZATION_DENIED : eventType;
		recordEvent(new AuditEvent().eventType(recordedType).action(eventType == null ? "UNKNOWN" : eventType.name())
				.status(status).actorUser(user).target(targetType, targetId, null)
				.context(targetType == AuditTargetType.PROJECT ? targetId : null,
						targetType == AuditTargetType.ENGINE ? targetId : null, null, null)
				.httpStatus(httpStatus).error(errorCode, errorMessage).details(details));
	}

	/**
	 * Records a failed pixel operation. Authorization denials are recorded for
	 * every reactor; other failures only for reactors that perform audited actions.
	 * Raw pixel text is never stored because it can contain SQL, passwords, or keys
	 * - only the reactor name is kept.
	 *
	 * @param insight      insight that ran the pixel (provides the actor)
	 * @param pixel        the pixel step that failed (used only to find the reactor
	 *                     name)
	 * @param reactorName  the reactor that was executing, when known
	 * @param error        thrown error, or null when the reactor returned an error
	 * @param errorMessage message of a returned error noun, when no error was
	 *                     thrown
	 */
	public static void recordPixelFailure(Insight insight, String pixel, String reactorName, Throwable error,
			String errorMessage) {
		try {
			if (!Utility.isUserTrackingEnabled()) {
				return;
			}
			String message = error != null ? error.getMessage() : errorMessage;
			FailureClassification classification = classify(error, message);
			String auditedReactor = findAuditedReactor(reactorName, pixel);
			boolean denied = classification.status() == AuditStatus.DENIED;
			if (!denied && auditedReactor == null) {
				return;
			}
			String reactor = auditedReactor != null ? auditedReactor : firstReactor(reactorName, pixel);
			AuditedAction audited = auditedReactor == null ? null : AUDITED_REACTORS.get(auditedReactor);
			// a denial of a reactor that is not audited is recorded with the reactor as its
			// action
			String action = audited == null ? reactor : audited.eventType().name();
			AuditEventType eventType = denied || audited == null ? AuditEventType.AUTHORIZATION_DENIED
					: audited.eventType();

			User user = insight == null ? null : insight.getUser();
			// Pixel failures are separate operations, even inside the same HTTP request.

			Map<String, Object> details = new LinkedHashMap<>();
			putIfPresent(details, "reactor", reactor);
			if (insight != null) {
				putIfPresent(details, "insightId", insight.getInsightId());
			}
			recordEvent(new AuditEvent().eventType(eventType).action(action == null ? "UNKNOWN" : action)
					.status(classification.status()).actorUser(user)
					.target(audited == null ? null : audited.targetType(), null, null)
					.context(null, null, insight == null ? null : insight.getInsightId(), null)
					.error(classification.errorCode(), message)
					.source(null, null, reactor == null ? null : reactor + "Reactor").details(details));
		} catch (Exception e) {
			classLogger.warn("Unable to record the audit event for a failed pixel operation", e);
		}
	}

	/**
	 * Records an authorization denial returned by a REST endpoint. The actor and
	 * request (path, method, IP) come from the request logging context. Only
	 * duplicate hooks carrying the same request ID are coalesced.
	 *
	 * @param errorCode  why the request was denied
	 * @param message    error message returned to the client
	 * @param httpStatus HTTP status returned to the client
	 */
	public static void recordHttpDenial(AuditErrorCode errorCode, String message, Integer httpStatus) {
		try {
			if (!Utility.isUserTrackingEnabled()) {
				return;
			}
			String path = contextValue(SemossLogUtils.ENDPOINT);
			String action = path == null ? "HTTP_REQUEST" : path.substring(path.lastIndexOf('/') + 1);
			if (action.isBlank()) {
				action = "HTTP_REQUEST";
			}
			String sanitized = AuditSanitizer.sanitizeMessage(message);
			if (isDuplicateFailure(dedupeKey(contextValue(SemossLogUtils.REQUEST_ID),
					AuditEventType.AUTHORIZATION_DENIED.name(), action, errorCode.name(), sanitized))) {
				return;
			}
			recordEvent(new AuditEvent().eventType(AuditEventType.AUTHORIZATION_DENIED).action(action)
					.status(AuditStatus.DENIED).httpStatus(httpStatus).error(errorCode, message));
		} catch (Exception e) {
			classLogger.warn("Unable to record the audit event for a denied request", e);
		}
	}

	/**
	 * Records HTTP authorization denials and failures of explicitly recognized
	 * permission/access-request mutations. Request bodies are never inspected.
	 *
	 * @param httpStatus status returned to the client
	 * @param message    error message returned to the client
	 */
	public static void recordHttpErrorResponse(int httpStatus, String message) {
		if (httpStatus < 400) {
			return;
		}
		if (httpStatus == 401 || httpStatus == 403) {
			recordHttpDenial(httpStatus == 401 ? AuditErrorCode.LOGIN_REQUIRED : AuditErrorCode.PERMISSION_DENIED,
					message == null ? "Request authorization denied" : message, httpStatus);
			return;
		}
		FailureClassification classification = classify(null, message);
		if (classification.status() == AuditStatus.DENIED) {
			recordHttpDenial(classification.errorCode(), message, httpStatus);
			return;
		}
		String path = contextValue(SemossLogUtils.ENDPOINT);
		if (path == null) {
			return;
		}
		Matcher endpoint = Pattern
				.compile(".*/auth/(?:admin/|group/)?(engine|project|insight|permissionBatch)/([A-Za-z]+)$")
				.matcher(path);
		if (!endpoint.matches()) {
			return;
		}
		String action = endpoint.group(2);
		String resource = endpoint.group(1).toUpperCase(Locale.ROOT);
		String title = resource.substring(0, 1) + resource.substring(1).toLowerCase(Locale.ROOT);
		AuditEventType eventType = null;
		AuditTargetType targetType;
		if (resource.equals("PERMISSIONBATCH")) {
			if (!action.equals("savePermissionBatches")) {
				return;
			}
			eventType = AuditEventType.PERMISSION_BATCH_APPLY;
			targetType = AuditTargetType.PERMISSION_BATCH;
		} else {
			targetType = AuditTargetType.valueOf(resource);
		}
		for (String prefix : List.of("add", "edit", "update", "remove")) {
			if (Set.of(prefix + title + "UserPermission", prefix + title + "UserPermissions",
					prefix + "Group" + title + "Permission").contains(action)) {
				eventType = prefix.equals("add") ? AuditEventType.PERMISSION_ADD
						: prefix.equals("remove") ? AuditEventType.PERMISSION_DELETE : AuditEventType.PERMISSION_UPDATE;
			}
		}
		if (action.equals("approve" + title + "UserAccessRequest")) {
			eventType = AuditEventType.ACCESS_REQUEST_APPROVE;
		}
		if (action.equals("deny" + title + "UserAccessRequest")) {
			eventType = AuditEventType.ACCESS_REQUEST_REJECT;
		}
		if (eventType == null || isDuplicateFailure(dedupeKey(contextValue(SemossLogUtils.REQUEST_ID), eventType.name(),
				action, Integer.toString(httpStatus), AuditSanitizer.sanitizeMessage(message)))) {
			return;
		}
		recordEvent(new AuditEvent().eventType(eventType).target(targetType, null, null)
				.status(httpStatus >= 500 ? AuditStatus.ERROR : AuditStatus.FAILURE).httpStatus(httpStatus)
				.error(httpStatus >= 500 ? AuditErrorCode.INTERNAL_ERROR : AuditErrorCode.OPERATION_FAILED,
						"Permission request did not complete")
				.details(Map.of("endpointAction", action)));
	}

	/**
	 * Classify an error into an audit status and a stable error code.
	 *
	 * @param error   thrown error, may be null
	 * @param message error message, may be null
	 * @return the status ({@code FAILURE}, {@code DENIED} or {@code ERROR}) and
	 *         error code
	 */
	public static FailureClassification classify(Throwable error, String message) {
		String text = message == null ? "" : message;
		if (hasCause(error, IllegalAccessException.class)) {
			return new FailureClassification(AuditStatus.DENIED, AuditErrorCode.PERMISSION_DENIED);
		}
		if (ADMIN_DENIAL_PATTERN.matcher(text).find()) {
			return new FailureClassification(AuditStatus.DENIED, AuditErrorCode.ADMIN_REQUIRED);
		}
		if (DENIAL_PATTERN.matcher(text).find()) {
			return new FailureClassification(AuditStatus.DENIED, AuditErrorCode.PERMISSION_DENIED);
		}
		if (LOGIN_REQUIRED_PATTERN.matcher(text).find()) {
			return new FailureClassification(AuditStatus.DENIED, AuditErrorCode.LOGIN_REQUIRED);
		}
		if (NOT_FOUND_PATTERN.matcher(text).find()) {
			return new FailureClassification(AuditStatus.FAILURE, AuditErrorCode.NOT_FOUND);
		}
		if (error == null || error instanceof IllegalArgumentException || error instanceof IllegalStateException
				|| error instanceof SemossPixelException) {
			return new FailureClassification(AuditStatus.FAILURE,
					error instanceof IllegalArgumentException ? AuditErrorCode.VALIDATION_FAILED
							: AuditErrorCode.OPERATION_FAILED);
		}
		return new FailureClassification(AuditStatus.ERROR, AuditErrorCode.INTERNAL_ERROR);
	}

	/**
	 * Audit status and stable error code for a failure.
	 *
	 * @param status    how the action ended
	 * @param errorCode why it did not succeed
	 */
	public record FailureClassification(AuditStatus status, AuditErrorCode errorCode) {

	}

	// ------------------------------------------------------------------------
	// maintenance
	// ------------------------------------------------------------------------

	/**
	 * Earlier builds of the audit trail stored the raw HTTP session id in a
	 * {@code SESSION_ID} column. Raw session ids can be replayed to hijack a
	 * session, so any values left in that legacy column are cleared on startup.
	 *
	 * @param userTrackingDb the user tracking database
	 */
	public static void clearLegacyRawSessionIds(IRDBMSEngine userTrackingDb) {
		if (userTrackingDb == null) {
			return;
		}
		java.sql.Connection conn = null;
		try {
			conn = userTrackingDb.getConnection();
			List<String> columns = userTrackingDb.getQueryUtil().getTableColumns(conn, TABLE,
					userTrackingDb.getDatabase(), userTrackingDb.getSchema());
			boolean hasLegacyColumn = columns != null
					&& columns.stream().anyMatch(c -> "SESSION_ID".equalsIgnoreCase(c));
			if (!hasLegacyColumn) {
				return;
			}
			try (PreparedStatement ps = conn
					.prepareStatement("UPDATE " + TABLE + " SET SESSION_ID = NULL WHERE SESSION_ID IS NOT NULL")) {
				int cleared = ps.executeUpdate();
				if (cleared > 0) {
					classLogger.info("Cleared {} legacy raw session ids from {}", cleared, TABLE);
				}
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (Exception e) {
			classLogger.warn("Unable to clear legacy raw session ids from {}", TABLE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(userTrackingDb, conn, null, null);
		}
	}

	/**
	 * Logging context entries an agent run puts on its threads so every audited
	 * action taken by the agent (directly or through tools) is attributed to the
	 * agent, with the delegating user kept in {@code DETAILS.onBehalfOf}.
	 *
	 * @param agentId   the agent (workspace) id; a generic id is used when null
	 * @param agentName display name or harness type
	 * @param runId     the agent run id
	 * @return context entries to add to the run thread
	 */
	public static Map<String, String> agentContext(String agentId, String agentName, String runId) {
		Map<String, String> context = new LinkedHashMap<>();
		context.put(AGENT_ID_CONTEXT_KEY, agentId == null || agentId.isBlank() ? "AGENT_RUN" : agentId);
		if (agentName != null && !agentName.isBlank()) {
			context.put(AGENT_NAME_CONTEXT_KEY, agentName);
		}
		if (runId != null && !runId.isBlank()) {
			context.put(AGENT_RUN_ID_CONTEXT_KEY, runId);
		}
		return context;
	}

	/**
	 * One-way hash of a session id so events from the same session can be
	 * correlated without storing a value that could be replayed.
	 *
	 * @param sessionId raw session id
	 * @return {@code sha256:<hex>} or null
	 */
	public static String hashSessionId(String sessionId) {
		if (sessionId == null || sessionId.isBlank() || PLACEHOLDER_CONTEXT_VALUES.contains(sessionId)) {
			return null;
		}
		return "sha256:" + sha256Hex(sessionId);
	}

	// ------------------------------------------------------------------------
	// internals
	// ------------------------------------------------------------------------

	private static void recordPermissionChange(User user, AuditEventType eventType, AuditTargetType targetType,
			String targetId, String targetName, String projectId, String engineId, String insightId, String granteeId,
			String granteeType, String oldPermission, String newPermission, Object details) {
		recordEvent(new AuditEvent().eventType(eventType).status(AuditStatus.SUCCESS).actorUser(user)
				.subject(granteeId, granteeType, null).target(targetType, targetId, targetName)
				.context(projectId, engineId, insightId, null)
				.oldValue(oldPermission == null ? null : Map.of("permission", oldPermission))
				.newValue(newPermission == null ? null : Map.of("permission", newPermission))
				.details(newDetails(details)));
	}

	static Map<String, Object> newDetails(Object details) {
		Map<String, Object> detailMap = new LinkedHashMap<>();
		if (details instanceof Map<?, ?> inputMap) {
			for (Map.Entry<?, ?> entry : inputMap.entrySet()) {
				if (entry.getKey() != null && entry.getValue() != null) {
					detailMap.put(String.valueOf(entry.getKey()), entry.getValue());
				}
			}
		} else if (details != null) {
			detailMap.put("details", details);
		}
		return detailMap;
	}

	private static void putIfPresent(Map<String, Object> map, String key, Object value) {
		if (value != null) {
			map.put(key, value);
		}
	}

	private static boolean hasCause(Throwable error, Class<? extends Throwable> type) {
		Throwable current = error;
		int depth = 0;
		while (current != null && depth++ < 10) {
			if (type.isInstance(current)) {
				return true;
			}
			current = current.getCause();
		}
		return false;
	}

	private static String findAuditedReactor(String reactorName, String pixel) {
		String normalized = normalizeReactorName(reactorName);
		if (normalized != null && AUDITED_REACTORS.containsKey(normalized)) {
			return normalized;
		}
		if (pixel == null) {
			return null;
		}
		Matcher matcher = REACTOR_CALL.matcher(pixel);
		while (matcher.find()) {
			String candidate = matcher.group(1);
			if (AUDITED_REACTORS.containsKey(candidate)) {
				return candidate;
			}
		}
		return null;
	}

	private static String firstReactor(String reactorName, String pixel) {
		String normalized = normalizeReactorName(reactorName);
		if (normalized != null) {
			return normalized;
		}
		if (pixel != null) {
			Matcher matcher = REACTOR_CALL.matcher(pixel);
			if (matcher.find()) {
				return matcher.group(1);
			}
		}
		return null;
	}

	private static String normalizeReactorName(String reactorName) {
		if (reactorName == null || reactorName.isBlank()) {
			return null;
		}
		String name = reactorName.trim();
		return name.endsWith("Reactor") ? name.substring(0, name.length() - "Reactor".length()) : name;
	}

	private static String dedupeKey(String requestId, String eventType, String status, String code, String message) {
		return requestId == null ? null : requestId + "|" + eventType + "|" + status + "|" + code + "|" + message;
	}

	private static boolean isDuplicateFailure(String key) {
		if (key == null) {
			return false;
		}
		long now = System.currentTimeMillis();
		if (RECENT_FAILURES.size() > FAILURE_DEDUPE_MAX_ENTRIES) {
			RECENT_FAILURES.entrySet().removeIf(e -> now - e.getValue() > FAILURE_DEDUPE_WINDOW_MS);
			if (RECENT_FAILURES.size() > FAILURE_DEDUPE_MAX_ENTRIES) {
				RECENT_FAILURES.clear();
			}
		}
		Long previous = RECENT_FAILURES.putIfAbsent(key, now);
		if (previous == null) {
			return false;
		}
		if (now - previous >= FAILURE_DEDUPE_WINDOW_MS) {
			return !RECENT_FAILURES.replace(key, previous, now);
		}
		return true;
	}

	/**
	 * The SHA-256 of a value's UTF-8 bytes as lower-case hex. The digest comes from
	 * the first JCA provider that has SHA-256, which in a FIPS image is BCFIPS, so
	 * the value is hashed by the validated module.
	 *
	 * @param value the value to hash
	 * @return 64 hex characters
	 * @throws IllegalStateException when no provider has SHA-256
	 */
	static String sha256Hex(String value) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
		} catch (Exception e) {
			throw new IllegalStateException("SHA-256 is not available", e);
		}
	}

	private static void bind(IRDBMSEngine engine, PreparedStatement ps, AuditEvent event) throws Exception {
		var queryUtil = engine.getQueryUtil();
		List<Object> values = event.columnValues();
		values.add(event.hashPrevious);
		values.add(event.hashCurrent);
		for (int i = 0; i < COLUMNS.size(); i++) {
			int index = i + 1;
			Object value = values.get(i);
			String column = COLUMNS.get(i);
			switch (column) {
			case "EVENT_TIME", "EVENT_OCCURRED_TIME" -> {
				if (value == null) {
					ps.setNull(index, Types.TIMESTAMP);
				} else {
					ps.setTimestamp(index, (Timestamp) value);
				}
			}
			case "ACTOR_IS_ADMIN" -> {
				if (value == null) {
					ps.setNull(index, Types.BOOLEAN);
				} else {
					ps.setBoolean(index, (Boolean) value);
				}
			}
			case "HTTP_STATUS" -> {
				if (value == null) {
					ps.setNull(index, Types.INTEGER);
				} else {
					ps.setInt(index, (Integer) value);
				}
			}
			case "OLD_VALUE", "NEW_VALUE", "DETAILS", "ERROR_MESSAGE" ->
				queryUtil.setNullableLargeText(ps, index, (String) value);
			default -> queryUtil.setNullableString(ps, index, (String) value);
			}
		}
	}

	private static StackWalker.StackFrame findCaller() {
		String utilsClass = UserAuditTrailUtils.class.getName();
		String sanitizerClass = AuditSanitizer.class.getName();
		return StackWalker.getInstance().walk(frames -> frames.filter(frame -> {
			String className = frame.getClassName();
			return !className.equals(utilsClass) && !className.startsWith(utilsClass + "$")
					&& !className.equals(sanitizerClass);
		}).findFirst()).orElse(null);
	}

	private static String contextValue(String key) {
		String value = ThreadContext.get(key);
		if (value == null || value.isBlank()
				|| PLACEHOLDER_CONTEXT_VALUES.contains(value.trim().toUpperCase(Locale.ROOT))) {
			return null;
		}
		return value;
	}

	private static Boolean isAdmin(User user) {
		if (user == null) {
			return null;
		}
		try {
			if (user.isAnonymous()) {
				return Boolean.FALSE;
			}
			Actor actor = getActor(user, null);
			String key = actor.userId == null ? null : actor.userId + "|" + actor.userType;
			long now = System.currentTimeMillis();
			CachedAdminFlag cached = key == null ? null : ADMIN_CACHE.get(key);
			if (cached != null && now - cached.timestamp() < ADMIN_CACHE_TTL_MS) {
				return cached.admin();
			}
			Boolean admin = SecurityAdminUtils.userIsAdmin(user);
			if (key != null && admin != null) {
				ADMIN_CACHE.put(key, new CachedAdminFlag(admin, now));
			}
			return admin;
		} catch (Exception e) {
			classLogger.debug("Unable to determine whether the audit actor is an admin", e);
			return null;
		}
	}

	private static String lookupUserName(String userId, String userType) {
		if (userId == null) {
			return null;
		}
		try {
			return QueryExecutionUtility.read(SystemEngineRegistry.getSecurityDb(), connection -> {
				try (PreparedStatement ps = connection.prepareStatement(
						"SELECT NAME FROM SMSS_USER WHERE ID=?" + (userType == null ? "" : " AND TYPE=?"))) {
					ps.setString(1, userId);
					if (userType != null) {
						ps.setString(2, userType);
					}
					ps.setMaxRows(2);
					try (ResultSet rs = ps.executeQuery()) {
						if (!rs.next()) {
							return null;
						}
						String name = rs.getString(1);
						return rs.next() ? null : name;
					}
				}
			});
		} catch (Exception e) {
			classLogger.debug("Unable to look up the audit subject name", e);
			return null;
		}
	}

	static Actor getActor(User user, AuthProvider provider) {
		if (user == null) {
			return new Actor(null, null, null);
		}
		try {
			if (user.isAnonymous()) {
				return new Actor(user.getAnonymousId(), "ANONYMOUS", "ANONYMOUS " + user.getAnonymousId());
			}

			AuthProvider resolvedProvider = provider == null ? user.getPrimaryLogin() : provider;
			AccessToken token = resolvedProvider == null ? null : user.getAccessToken(resolvedProvider);
			if (token == null) {
				return new Actor(User.getSingleLogginName(user), null, null);
			}
			String name = token.getName();
			if (name == null || name.isBlank()) {
				name = token.getResolvedDisplayName();
			}
			return new Actor(token.getId(), resolvedProvider.getLabel(), name);
		} catch (Exception e) {
			classLogger.debug("Unable to resolve the audit actor from the user", e);
			return new Actor(null, null, null);
		}
	}

	record Actor(String userId, String userType, String userName) {

	}

	private record CachedAdminFlag(Boolean admin, long timestamp) {

	}

	/**
	 * Builder for a single audit row. Unset values are resolved when the event is
	 * recorded: event id and time, category and severity (from the event type and
	 * status), the actor (from the given user, the thread user, or the request
	 * logging context), request context, and the emitting class.
	 */
	public static class AuditEvent {
		private String eventId;
		private Timestamp eventTime;
		private Timestamp eventOccurredTime;
		private AuditEventType eventType;
		private String action;
		private AuditStatus status;
		private AuditCategory category;
		private AuditSeverity severity;

		private User actorUser;
		private boolean actorSet;
		private String actorUserId;
		private String actorUserType;
		private String actorUserName;
		private Boolean actorIsAdmin;

		private String subjectUserId;
		private String subjectUserType;
		private String subjectUserName;
		private boolean subjectNameLookup = true;

		private String sessionId;
		private String requestId;
		private String ipAddr;
		private String userAgent;
		private String httpMethod;
		private String requestPath;
		private Integer httpStatus;

		private AuditTargetType targetType;
		private String targetId;
		private String targetName;

		private String projectId;
		private String engineId;
		private String insightId;
		private String roomId;

		private Object oldValueRaw;
		private Object newValueRaw;
		private Object detailsRaw;
		private String oldValue;
		private String newValue;
		private String details;

		private AuditErrorCode errorCode;
		private String errorMessage;

		private AuditSourceApp sourceApp;
		private String sourceModule;
		private String sourceClass;

		private String hashPrevious;
		private String hashCurrent;

		public AuditEvent eventId(String eventId) {
			this.eventId = eventId;
			return this;
		}

		public AuditEvent eventTime(Timestamp eventTime) {
			this.eventTime = eventTime;
			return this;
		}

		public AuditEvent occurredTime(Timestamp eventOccurredTime) {
			this.eventOccurredTime = eventOccurredTime;
			return this;
		}

		/**
		 * The kind of event, which also sets its category and, unless {@link #severity}
		 * sets one, its severity.
		 */
		public AuditEvent eventType(AuditEventType eventType) {
			this.eventType = eventType;
			return this;
		}

		/**
		 * What was done or tried, such as the reactor or endpoint called; the event
		 * type's name when left out.
		 */
		public AuditEvent action(String action) {
			this.action = action;
			return this;
		}

		/** How the action ended; {@link AuditStatus#SUCCESS} when left out. */
		public AuditEvent status(AuditStatus status) {
			this.status = status;
			return this;
		}

		/**
		 * A severity higher than the event type's, such as for a change that makes
		 * someone an admin; null keeps the event type's.
		 */
		public AuditEvent severity(AuditSeverity severity) {
			this.severity = severity;
			return this;
		}

		/**
		 * The SEMOSS user performing the action. Used to resolve the actor columns and
		 * {@code ACTOR_IS_ADMIN} when they are not set explicitly.
		 */
		public AuditEvent actorUser(User user) {
			this.actorUser = user;
			return this;
		}

		public AuditEvent actor(String userId, String userType, String userName) {
			this.actorSet = true;
			this.actorUserId = userId;
			this.actorUserType = userType;
			this.actorUserName = userName;
			return this;
		}

		/**
		 * Makes SEMOSS itself the actor, for what the platform does on its own, such as
		 * removing a grant whose end date passed.
		 */
		public AuditEvent systemActor() {
			return actor(ACTOR_TYPE_SYSTEM, ACTOR_TYPE_SYSTEM, ACTOR_TYPE_SYSTEM);
		}

		public AuditEvent actorIsAdmin(Boolean actorIsAdmin) {
			this.actorIsAdmin = actorIsAdmin;
			return this;
		}

		public AuditEvent subject(String userId, String userType, String userName) {
			this.subjectUserId = userId;
			this.subjectUserType = userType;
			this.subjectUserName = userName;
			return this;
		}

		/**
		 * @param lookup whether to look up the subject's display name by id when it is
		 *               not provided
		 */
		public AuditEvent subjectNameLookup(boolean lookup) {
			this.subjectNameLookup = lookup;
			return this;
		}

		/**
		 * @param sessionId raw session id; only its hash is stored
		 */
		public AuditEvent session(String sessionId, String requestId, String ipAddr) {
			this.sessionId = sessionId;
			this.requestId = requestId;
			this.ipAddr = ipAddr;
			return this;
		}

		public AuditEvent request(String httpMethod, String requestPath, String userAgent) {
			this.httpMethod = httpMethod;
			this.requestPath = requestPath;
			this.userAgent = userAgent;
			return this;
		}

		public AuditEvent httpStatus(Integer httpStatus) {
			this.httpStatus = httpStatus;
			return this;
		}

		public AuditEvent target(AuditTargetType targetType, String targetId, String targetName) {
			this.targetType = targetType;
			this.targetId = targetId;
			this.targetName = targetName;
			return this;
		}

		public AuditEvent context(String projectId, String engineId, String insightId, String roomId) {
			this.projectId = projectId;
			this.engineId = engineId;
			this.insightId = insightId;
			this.roomId = roomId;
			return this;
		}

		public AuditEvent oldValue(Object oldValue) {
			this.oldValueRaw = oldValue;
			return this;
		}

		public AuditEvent newValue(Object newValue) {
			this.newValueRaw = newValue;
			return this;
		}

		public AuditEvent details(Object details) {
			this.detailsRaw = details;
			return this;
		}

		public AuditEvent error(AuditErrorCode errorCode, String errorMessage) {
			this.errorCode = errorCode;
			this.errorMessage = errorMessage;
			return this;
		}

		public AuditEvent errorMessage(String errorMessage) {
			this.errorMessage = errorMessage;
			return this;
		}

		public AuditEvent source(AuditSourceApp sourceApp, String sourceModule, String sourceClass) {
			this.sourceApp = sourceApp;
			this.sourceModule = sourceModule;
			this.sourceClass = sourceClass;
			return this;
		}

		void resolveDefaults(StackWalker.StackFrame caller) {
			if (this.eventId == null || this.eventId.isBlank()) {
				this.eventId = UUID.randomUUID().toString();
			}
			if (this.eventTime == null) {
				this.eventTime = Utility.getCurrentSqlTimestampUTC();
			}
			if (this.eventOccurredTime == null) {
				this.eventOccurredTime = this.eventTime;
			}
			if (this.status == null) {
				this.status = AuditStatus.SUCCESS;
			}
			if (this.action == null && this.eventType != null) {
				this.action = this.eventType.name();
			}

			resolveActor();
			resolveRequest();
			resolveAccessRequestSubject();

			if (this.subjectNameLookup && this.subjectUserName == null && this.subjectUserId != null) {
				this.subjectUserName = lookupUserName(this.subjectUserId, this.subjectUserType);
			}
			this.category = this.eventType == null ? null : this.eventType.getCategory();
			if (this.severity == null) {
				this.severity = deriveSeverity(this.eventType, this.status);
			}

			if (this.sourceApp == null) {
				this.sourceApp = AuditSourceApp.SEMOSS;
			}
			if (caller != null) {
				String callerClass = caller.getClassName();
				int lastDot = callerClass.lastIndexOf('.');
				if (this.sourceModule == null) {
					this.sourceModule = lastDot > 0 ? callerClass.substring(0, lastDot) : null;
				}
				if (this.sourceClass == null) {
					String simple = lastDot >= 0 ? callerClass.substring(lastDot + 1) : callerClass;
					int nested = simple.indexOf('$');
					this.sourceClass = nested > 0 ? simple.substring(0, nested) : simple;
				}
			}
			if (this.sourceApp == AuditSourceApp.SEMOSS && this.sourceModule != null
					&& (this.sourceModule.startsWith("prerna.semoss.web")
							|| this.sourceModule.startsWith("prerna.web"))) {
				this.sourceApp = AuditSourceApp.MONOLITH;
			}

			this.oldValue = AuditSanitizer.toSanitizedJson(this.oldValueRaw);
			this.newValue = AuditSanitizer.toSanitizedJson(this.newValueRaw);
			this.details = AuditSanitizer.toSanitizedJson(emptyToNull(this.detailsRaw));
			this.errorMessage = AuditSanitizer.sanitizeMessage(this.errorMessage);
			this.targetName = AuditSanitizer.truncate(this.targetName, 1000);
			this.userAgent = AuditSanitizer.truncate(this.userAgent, 1000);
			this.requestPath = AuditSanitizer.truncate(this.requestPath, 2000);
		}

		private void resolveAccessRequestSubject() {
			if (this.eventType != AuditEventType.ACCESS_REQUEST_REJECT || this.subjectUserId != null
					|| !(this.detailsRaw instanceof Map<?, ?> details)
					|| !(details.get("requestId") instanceof String requestId) || this.targetId == null
					|| this.targetType == null) {
				return;
			}
			// access requests are kept per engine, whatever its kind, per project and per
			// insight
			String table = this.targetType.isEngine() ? AuditTargetType.ENGINE.name()
					: this.targetType == AuditTargetType.PROJECT || this.targetType == AuditTargetType.INSIGHT
							? this.targetType.name()
							: null;
			if (table == null) {
				return;
			}
			try {
				String sql = "SELECT REQUEST_USERID,REQUEST_TYPE FROM " + table + "ACCESSREQUEST WHERE ID=? AND "
						+ table + "ID=?" + (this.targetType == AuditTargetType.INSIGHT ? " AND PROJECTID=?" : "");
				String[] requester = QueryExecutionUtility.queryOne(SystemEngineRegistry.getSecurityDb(), sql, ps -> {
					ps.setString(1, requestId);
					ps.setString(2, this.targetId);
					if (this.targetType == AuditTargetType.INSIGHT) {
						ps.setString(3, this.projectId);
					}
				}, rs -> new String[] { rs.getString(1), rs.getString(2) });
				if (requester != null) {
					this.subjectUserId = requester[0];
					this.subjectUserType = requester[1];
				}
			} catch (Exception e) {
				classLogger.warn("Unable to resolve the affected user for an access-request audit event", e);
			}
		}

		private void resolveActor() {
			// the request logging context is refreshed on every request, unlike ThreadStore
			// which can hold a previous request's user on a pooled thread
			User user = this.actorUser;
			if (!this.actorSet) {
				if (user != null) {
					Actor actor = getActor(user, null);
					this.actorUserId = actor.userId;
					this.actorUserType = actor.userType;
					this.actorUserName = actor.userName;
				} else {
					this.actorUserId = contextValue(SemossLogUtils.USER_ID);
					this.actorUserType = contextValue(SemossLogUtils.USER_TYPE);
					this.actorUserName = contextValue(SemossLogUtils.USER_NAME);
				}
			}
			if (this.actorIsAdmin == null && user != null) {
				this.actorIsAdmin = isAdmin(user);
			}

			// an agent acting for a user is the actor; the delegating user is kept in
			// details
			String agentId = contextValue(AGENT_ID_CONTEXT_KEY);
			if (agentId != null && !ACTOR_TYPE_AGENT.equals(this.actorUserType)) {
				Map<String, Object> onBehalfOf = new LinkedHashMap<>();
				putIfPresent(onBehalfOf, "userId", this.actorUserId);
				putIfPresent(onBehalfOf, "userType", this.actorUserType);
				putIfPresent(onBehalfOf, "userName", this.actorUserName);
				Map<String, Object> detailMap = newDetails(this.detailsRaw);
				if (!onBehalfOf.isEmpty()) {
					if (this.actorIsAdmin != null) {
						onBehalfOf.put("isAdmin", this.actorIsAdmin);
					}
					detailMap.put("onBehalfOf", onBehalfOf);
				}
				putIfPresent(detailMap, "agentRunId", contextValue(AGENT_RUN_ID_CONTEXT_KEY));
				this.detailsRaw = detailMap;
				this.actorUserId = agentId;
				this.actorUserType = ACTOR_TYPE_AGENT;
				this.actorIsAdmin = null;
				this.actorUserName = Optional.ofNullable(contextValue(AGENT_NAME_CONTEXT_KEY)).orElse(agentId);
			}
		}

		private void resolveRequest() {
			if (this.requestId == null) {
				this.requestId = contextValue(SemossLogUtils.REQUEST_ID);
			}
			if (this.ipAddr == null) {
				this.ipAddr = contextValue(SemossLogUtils.CLIENT_IP);
			}
			if (this.httpMethod == null) {
				this.httpMethod = contextValue(SemossLogUtils.METHOD);
			}
			if (this.requestPath == null) {
				this.requestPath = contextValue(SemossLogUtils.ENDPOINT);
			}
			if (this.userAgent == null) {
				this.userAgent = contextValue(SemossLogUtils.USER_AGENT);
			}
			String rawSession = this.sessionId != null ? this.sessionId : contextValue(SemossLogUtils.SESSION_ID);
			this.sessionId = hashSessionId(rawSession);
		}

		/** The name an enum value is stored by, or null */
		private static String name(Enum<?> value) {
			return value == null ? null : value.name();
		}

		private static Object emptyToNull(Object value) {
			if (value instanceof Map<?, ?> map && map.isEmpty()) {
				return null;
			}
			return value;
		}

		/**
		 * @return every column value except the two hash columns, in
		 *         {@link UserAuditTrailUtils#COLUMNS} order
		 */
		List<Object> columnValues() {
			List<Object> values = new ArrayList<>(COLUMNS.size());
			values.add(this.eventId);
			values.add(this.eventTime);
			values.add(this.eventOccurredTime);
			values.add(name(this.eventType));
			values.add(this.action);
			values.add(name(this.status));
			values.add(name(this.category));
			values.add(name(this.severity));
			values.add(this.actorUserId);
			values.add(this.actorUserType);
			values.add(this.actorUserName);
			values.add(this.actorIsAdmin);
			values.add(this.subjectUserId);
			values.add(this.subjectUserType);
			values.add(this.subjectUserName);
			values.add(this.sessionId);
			values.add(this.requestId);
			values.add(this.ipAddr);
			values.add(this.userAgent);
			values.add(this.httpMethod);
			values.add(this.requestPath);
			values.add(this.httpStatus);
			values.add(name(this.targetType));
			values.add(this.targetId);
			values.add(this.targetName);
			values.add(this.projectId);
			values.add(this.engineId);
			values.add(this.insightId);
			values.add(this.roomId);
			values.add(this.oldValue);
			values.add(this.newValue);
			values.add(this.details);
			values.add(name(this.errorCode));
			values.add(this.errorMessage);
			values.add(name(this.sourceApp));
			values.add(this.sourceModule);
			values.add(this.sourceClass);
			for (int i = 0; i < values.size(); i++) {
				if (!(values.get(i) instanceof String value)) {
					continue;
				}
				int limit = switch (COLUMNS.get(i)) {
				case "OLD_VALUE", "NEW_VALUE", "DETAILS", "ERROR_MESSAGE" -> Integer.MAX_VALUE;
				case "STATUS", "SEVERITY" -> 50;
				case "CATEGORY", "SOURCE_APP" -> 100;
				case "HTTP_METHOD" -> 20;
				case "USER_AGENT", "TARGET_NAME" -> 1000;
				case "REQUEST_PATH" -> 2000;
				default -> 255;
				};
				values.set(i, AuditSanitizer.truncate(value, limit));
			}
			return values;
		}
	}

	/**
	 * The severity of an event that sets none itself: its type's, raised to
	 * {@link AuditSeverity#MEDIUM} when the action did not succeed.
	 *
	 * @param eventType the kind of event, or null when it was left out
	 * @param status    how the action ended
	 * @return the severity to record
	 */
	static AuditSeverity deriveSeverity(AuditEventType eventType, AuditStatus status) {
		AuditSeverity severity = eventType == null ? AuditSeverity.LOW : eventType.getSeverity();
		if (severity == AuditSeverity.LOW && status != null && status != AuditStatus.SUCCESS) {
			severity = AuditSeverity.MEDIUM;
		}
		return severity;
	}
}
