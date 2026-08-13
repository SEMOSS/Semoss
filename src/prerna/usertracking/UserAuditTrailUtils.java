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
 * 	MERCHANTIBILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.usertracking;

import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

public final class UserAuditTrailUtils {

	private static final Logger classLogger = LogManager.getLogger(UserAuditTrailUtils.class);
	private static final Gson GSON = new Gson();

	private static final String INSERT_AUDIT_EVENT = """
			INSERT INTO USER_AUDIT_EVENTS (
				EVENT_ID, EVENT_TIME, EVENT_TYPE, ACTION, STATUS,
				ACTOR_USER_ID, ACTOR_USER_TYPE, ACTOR_USER_NAME, SESSION_ID, REQUEST_ID, IP_ADDR,
				TARGET_TYPE, TARGET_ID, TARGET_NAME, PROJECT_ID, ENGINE_ID, INSIGHT_ID, ROOM_ID,
				OLD_VALUE, NEW_VALUE, DETAILS, ERROR_MESSAGE
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			""";

	private UserAuditTrailUtils() {

	}

	/**
	 * Records a business/security audit event in the user-tracking database. This
	 * method is intentionally no-op when user tracking is disabled so the audit
	 * trail follows the same deployment switch as the existing user activity tables.
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

		IRDBMSEngine userTrackingDb = SystemEngineRegistry.getUserTrackingDb();
		PreparedStatement ps = null;
		try {
			ps = userTrackingDb.getPreparedStatement(INSERT_AUDIT_EVENT);
			int index = 1;
			ps.setString(index++, valueOrGeneratedId(event.eventId));
			ps.setTimestamp(index++, event.eventTime == null ? Utility.getCurrentSqlTimestampUTC() : event.eventTime);
			setStringOrNull(ps, index++, event.eventType);
			setStringOrNull(ps, index++, event.action);
			setStringOrNull(ps, index++, event.status);
			setStringOrNull(ps, index++, event.actorUserId);
			setStringOrNull(ps, index++, event.actorUserType);
			setStringOrNull(ps, index++, event.actorUserName);
			setStringOrNull(ps, index++, event.sessionId);
			setStringOrNull(ps, index++, event.requestId);
			setStringOrNull(ps, index++, event.ipAddr);
			setStringOrNull(ps, index++, event.targetType);
			setStringOrNull(ps, index++, event.targetId);
			setStringOrNull(ps, index++, event.targetName);
			setStringOrNull(ps, index++, event.projectId);
			setStringOrNull(ps, index++, event.engineId);
			setStringOrNull(ps, index++, event.insightId);
			setStringOrNull(ps, index++, event.roomId);
			setClobOrNull(userTrackingDb, ps, index++, event.oldValue);
			setClobOrNull(userTrackingDb, ps, index++, event.newValue);
			setClobOrNull(userTrackingDb, ps, index++, event.details);
			setClobOrNull(userTrackingDb, ps, index++, event.errorMessage);

			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to record audit event type={} action={} targetType={} targetId={}",
					event.eventType, event.action, event.targetType, event.targetId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(userTrackingDb, ps, null);
		}
	}

	/**
	 * Records a successful login event.
	 *
	 * @param user      authenticated SEMOSS user
	 * @param provider  provider used for this login
	 * @param sessionId HTTP session id
	 * @param ipAddr    client IP address
	 */
	public static void recordLogin(User user, AuthProvider provider, String sessionId, String ipAddr) {
		Actor actor = getActor(user, provider);
		recordEvent(new AuditEvent()
				.eventType("LOGIN")
				.action("LOGIN")
				.status("SUCCESS")
				.actor(actor.userId, actor.userType, actor.userName)
				.session(sessionId, null, ipAddr)
				.target("USER", actor.userId, actor.userName)
				.details(provider == null ? null : Map.of("provider", provider.getLabel())));
	}

	/**
	 * Records a successful logout/session-end event.
	 *
	 * @param user      SEMOSS user ending the session
	 * @param sessionId HTTP session id
	 * @param reason    explicit logout, timeout, or other session cleanup reason
	 */
	public static void recordLogout(User user, String sessionId, String reason) {
		recordLogout(user, sessionId, reason, null);
	}

	/**
	 * Records a successful logout/session-end event.
	 *
	 * @param user      SEMOSS user ending the session
	 * @param sessionId HTTP session id
	 * @param reason    explicit logout, timeout, or other session cleanup reason
	 * @param ipAddr    client IP address when the logout was request-driven
	 */
	public static void recordLogout(User user, String sessionId, String reason, String ipAddr) {
		Actor actor = getActor(user, null);
		recordEvent(new AuditEvent()
				.eventType("LOGOUT")
				.action("LOGOUT")
				.status("SUCCESS")
				.actor(actor.userId, actor.userType, actor.userName)
				.session(sessionId, null, ipAddr)
				.target("USER", actor.userId, actor.userName)
				.details(reason == null ? null : Map.of("reason", reason)));
	}

	public static void recordPermissionAdd(User user, String targetType, String targetId, String targetName,
			String projectId, String engineId, String insightId, String granteeId, String granteeType,
			String permission, Object details) {
		recordPermissionChange(user, "PERMISSION_ADD", targetType, targetId, targetName, projectId, engineId,
				insightId, granteeId, granteeType, null, permission, details);
	}

	public static void recordPermissionUpdate(User user, String targetType, String targetId, String targetName,
			String projectId, String engineId, String insightId, String granteeId, String granteeType,
			String oldPermission, String newPermission, Object details) {
		recordPermissionChange(user, "PERMISSION_UPDATE", targetType, targetId, targetName, projectId, engineId,
				insightId, granteeId, granteeType, oldPermission, newPermission, details);
	}

	public static void recordPermissionDelete(User user, String targetType, String targetId, String targetName,
			String projectId, String engineId, String insightId, String granteeId, String granteeType,
			String oldPermission, Object details) {
		recordPermissionChange(user, "PERMISSION_DELETE", targetType, targetId, targetName, projectId, engineId,
				insightId, granteeId, granteeType, oldPermission, null, details);
	}

	public static void recordAccessRequestDecision(User user, String action, String targetType, String targetId,
			String targetName, String projectId, String engineId, String insightId, String requestId,
			String requesterId, String requesterType, String permission, Object details) {
		Actor actor = getActor(user, null);
		Map<String, Object> detailMap = newDetails(details);
		putIfPresent(detailMap, "requestId", requestId);
		putIfPresent(detailMap, "requesterId", requesterId);
		putIfPresent(detailMap, "requesterType", requesterType);
		putIfPresent(detailMap, "permission", permission);
		recordEvent(new AuditEvent()
				.eventType(action)
				.action(action)
				.status("SUCCESS")
				.actor(actor.userId, actor.userType, actor.userName)
				.target(targetType, targetId, targetName)
				.context(projectId, engineId, insightId, null)
				.details(detailMap));
	}

	public static void recordProjectLifecycle(User user, String action, String projectId, String projectName,
			Object details) {
		Actor actor = getActor(user, null);
		recordEvent(new AuditEvent()
				.eventType(action)
				.action(action)
				.status("SUCCESS")
				.actor(actor.userId, actor.userType, actor.userName)
				.target("PROJECT", projectId, projectName)
				.context(projectId, null, null, null)
				.details(details));
	}

	public static void recordWorkspaceLifecycle(User user, String action, String workspaceId, String workspaceName,
			Object details) {
		Actor actor = getActor(user, null);
		recordEvent(new AuditEvent()
				.eventType(action)
				.action(action)
				.status("SUCCESS")
				.actor(actor.userId, actor.userType, actor.userName)
				.target("WORKSPACE", workspaceId, workspaceName)
				.context(workspaceId, null, null, workspaceId)
				.details(details));
	}

	public static void recordEngineLifecycle(User user, String action, String targetType, String engineId,
			String engineName, Object details) {
		Actor actor = getActor(user, null);
		recordEvent(new AuditEvent()
				.eventType(action)
				.action(action)
				.status("SUCCESS")
				.actor(actor.userId, actor.userType, actor.userName)
				.target(targetType == null ? "ENGINE" : targetType, engineId, engineName)
				.context(null, engineId, null, null)
				.details(details));
	}

	private static void recordPermissionChange(User user, String action, String targetType, String targetId,
			String targetName, String projectId, String engineId, String insightId, String granteeId,
			String granteeType, String oldPermission, String newPermission, Object details) {
		Actor actor = getActor(user, null);
		Map<String, Object> detailMap = newDetails(details);
		putIfPresent(detailMap, "granteeId", granteeId);
		putIfPresent(detailMap, "granteeType", granteeType);
		recordEvent(new AuditEvent()
				.eventType(action)
				.action(action)
				.status("SUCCESS")
				.actor(actor.userId, actor.userType, actor.userName)
				.target(targetType, targetId, targetName)
				.context(projectId, engineId, insightId, null)
				.oldValue(oldPermission == null ? null : Map.of("permission", oldPermission))
				.newValue(newPermission == null ? null : Map.of("permission", newPermission))
				.details(detailMap));
	}

	private static Map<String, Object> newDetails(Object details) {
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

	private static String valueOrGeneratedId(String eventId) {
		if (eventId == null || eventId.trim().isEmpty()) {
			return UUID.randomUUID().toString();
		}
		return eventId;
	}

	private static void setStringOrNull(PreparedStatement ps, int index, String value) throws Exception {
		if (value == null) {
			ps.setNull(index, Types.VARCHAR);
		} else {
			ps.setString(index, value);
		}
	}

	private static void setClobOrNull(IRDBMSEngine engine, PreparedStatement ps, int index, String value)
			throws Exception {
		if (value == null) {
			ps.setNull(index, Types.CLOB);
			return;
		}
		if (engine.getQueryUtil().allowClobJavaObject()) {
			Clob clob = ps.getConnection().createClob();
			clob.setString(1, value);
			ps.setClob(index, clob);
		} else {
			ps.setString(index, value);
		}
	}

	private static Actor getActor(User user, AuthProvider provider) {
		if (user == null) {
			return new Actor(null, null, null);
		}
		if (user.isAnonymous()) {
			return new Actor(user.getAnonymousId(), "ANONYMOUS", "ANONYMOUS " + user.getAnonymousId());
		}

		AuthProvider resolvedProvider = provider == null ? user.getPrimaryLogin() : provider;
		AccessToken token = resolvedProvider == null ? null : user.getAccessToken(resolvedProvider);
		if (token == null) {
			return new Actor(User.getSingleLogginName(user), null, null);
		}
		return new Actor(token.getId(), resolvedProvider.getLabel(), token.getName());
	}

	private record Actor(String userId, String userType, String userName) {

	}

	public static class AuditEvent {
		private String eventId;
		private java.sql.Timestamp eventTime;
		private String eventType;
		private String action;
		private String status;
		private String actorUserId;
		private String actorUserType;
		private String actorUserName;
		private String sessionId;
		private String requestId;
		private String ipAddr;
		private String targetType;
		private String targetId;
		private String targetName;
		private String projectId;
		private String engineId;
		private String insightId;
		private String roomId;
		private String oldValue;
		private String newValue;
		private String details;
		private String errorMessage;

		public AuditEvent eventId(String eventId) {
			this.eventId = eventId;
			return this;
		}

		public AuditEvent eventTime(java.sql.Timestamp eventTime) {
			this.eventTime = eventTime;
			return this;
		}

		public AuditEvent eventType(String eventType) {
			this.eventType = eventType;
			return this;
		}

		public AuditEvent action(String action) {
			this.action = action;
			return this;
		}

		public AuditEvent status(String status) {
			this.status = status;
			return this;
		}

		public AuditEvent actor(String userId, String userType, String userName) {
			this.actorUserId = userId;
			this.actorUserType = userType;
			this.actorUserName = userName;
			return this;
		}

		public AuditEvent session(String sessionId, String requestId, String ipAddr) {
			this.sessionId = sessionId;
			this.requestId = requestId;
			this.ipAddr = ipAddr;
			return this;
		}

		public AuditEvent target(String targetType, String targetId, String targetName) {
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
			this.oldValue = toJson(oldValue);
			return this;
		}

		public AuditEvent newValue(Object newValue) {
			this.newValue = toJson(newValue);
			return this;
		}

		public AuditEvent details(Object details) {
			this.details = toJson(details);
			return this;
		}

		public AuditEvent errorMessage(String errorMessage) {
			this.errorMessage = errorMessage;
			return this;
		}

		private static String toJson(Object value) {
			if (value == null) {
				return null;
			}
			if (value instanceof String) {
				return (String) value;
			}
			return GSON.toJson(value);
		}
	}
}
