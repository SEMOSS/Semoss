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
package prerna.notifications;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import com.github.f4b6a3.uuid.alt.GUID;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityUserUtils;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.NotificationConstants;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class NotificationDbUtils {

	private static final Logger classLogger = LogManager.getLogger(NotificationDbUtils.class);

	static boolean initialized = false;

	private NotificationDbUtils() {

	}

	public static void loadNotificationDatabase() throws Exception {
		IRDBMSEngine notificationDb = SystemEngineRegistry.getNotificationDb();
		NotificationOwlCreator owlCreator = new NotificationOwlCreator(notificationDb.getQueryUtil());
		if (owlCreator.needsRemake(notificationDb)) {
			owlCreator.remakeOwl(notificationDb);
		}
		initialize(owlCreator.getDBSchema());
		initialized = true;
	}

	private static void initialize(List<Pair<String, List<Pair<String, String>>>> dbSchema) throws Exception {
		IRDBMSEngine notificationDb = SystemEngineRegistry.getNotificationDb();
		String database = notificationDb.getDatabase();
		String schema = notificationDb.getSchema();
		Connection conn = notificationDb.getConnection();
		try {
			AbstractSqlQueryUtil queryUtil = notificationDb.getQueryUtil();
			boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();
			boolean allowIfExistsIndexs = queryUtil.allowIfExistsIndexSyntax();

			for (Pair<String, List<Pair<String, String>>> tableSchema : dbSchema) {
				String tableName = tableSchema.getValue0();
				String[] colNames = tableSchema.getValue1().stream().map(Pair::getValue0).toArray(String[]::new);
				String[] types = tableSchema.getValue1().stream().map(Pair::getValue1).toArray(String[]::new);
				if (allowIfExistsTable) {
					String sql = queryUtil.createTableIfNotExists(tableName, colNames, types);
					classLogger.info("Running sql {}", sql);
					notificationDb.insertData(sql);
				} else if (!queryUtil.tableExists(conn, tableName, database, schema)) {
					String sql = queryUtil.createTable(tableName, colNames, types);
					classLogger.info("Running sql {}", sql);
					notificationDb.insertData(sql);
				}

				List<String> allCols = queryUtil.getTableColumns(conn, tableName, database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column {} missing from {} table; adding it. Existing columns: {}", col,
								tableName, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn(tableName, col, types[i]);
						classLogger.info("Running sql {}", addColumnSql);
						notificationDb.insertData(addColumnSql);
					}
				}
			}

			createIndexIfMissing(notificationDb, queryUtil, allowIfExistsIndexs, database, schema,
					"NOTIFICATION_EVENT_NOTIFICATION_ID_INDEX", "NOTIFICATION_EVENT", "NOTIFICATION_ID");
			createIndexIfMissing(notificationDb, queryUtil, allowIfExistsIndexs, database, schema,
					"NOTIFICATION_EVENT_SCOPE_INDEX", "NOTIFICATION_EVENT", Arrays.asList("SCOPE_TYPE", "SCOPE_ID"));
			createIndexIfMissing(notificationDb, queryUtil, allowIfExistsIndexs, database, schema,
					"NOTIFICATION_EVENT_AUDIENCE_INDEX", "NOTIFICATION_EVENT",
					Arrays.asList("AUDIENCE_TYPE", "AUDIENCE_ID"));
			createIndexIfMissing(notificationDb, queryUtil, allowIfExistsIndexs, database, schema,
					"NOTIFICATION_EVENT_TARGET_INDEX", "NOTIFICATION_EVENT", Arrays.asList("TARGET_TYPE", "TARGET_ID"));
			createIndexIfMissing(notificationDb, queryUtil, allowIfExistsIndexs, database, schema,
					"NOTIFICATION_EVENT_GROUP_INDEX", "NOTIFICATION_EVENT", "GROUP_ID");
			createIndexIfMissing(notificationDb, queryUtil, allowIfExistsIndexs, database, schema,
					"NOTIFICATION_USER_STATE_NOTIFICATION_USER_INDEX", "NOTIFICATION_USER_STATE",
					Arrays.asList("NOTIFICATION_ID", "USER_ID", "USER_TYPE"));
			createIndexIfMissing(notificationDb, queryUtil, allowIfExistsIndexs, database, schema,
					"NOTIFICATION_USER_STATE_USER_INDEX", "NOTIFICATION_USER_STATE",
					Arrays.asList("USER_ID", "USER_TYPE"));
			createIndexIfMissing(notificationDb, queryUtil, allowIfExistsIndexs, database, schema,
					"NOTIFICATION_DELIVERY_NOTIFICATION_INDEX", "NOTIFICATION_DELIVERY", "NOTIFICATION_ID");

			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} finally {
			if (conn != null && notificationDb.isConnectionPooling()) {
				conn.close();
			}
		}
	}

	private static void createIndexIfMissing(IRDBMSEngine notificationDb, AbstractSqlQueryUtil queryUtil,
			boolean allowIfExistsIndexs, String database, String schema, String indexName, String tableName,
			String columnName) throws Exception {
		createIndexIfMissing(notificationDb, queryUtil, allowIfExistsIndexs, database, schema, indexName, tableName,
				Arrays.asList(columnName));
	}

	private static void createIndexIfMissing(IRDBMSEngine notificationDb, AbstractSqlQueryUtil queryUtil,
			boolean allowIfExistsIndexs, String database, String schema, String indexName, String tableName,
			Collection<String> columns) throws Exception {
		if (allowIfExistsIndexs) {
			String sql = queryUtil.createIndexIfNotExists(indexName, tableName, columns);
			classLogger.info("Running sql {}", sql);
			notificationDb.insertData(sql);
		} else if (!queryUtil.indexExists(notificationDb, indexName, tableName, database, schema)) {
			String sql = queryUtil.createIndex(indexName, tableName, columns);
			classLogger.info("Running sql {}", sql);
			notificationDb.insertData(sql);
		}
	}

	/**
	 * Determine if the notification db is present.
	 * 
	 * @return
	 */
	public static boolean isInitalized() {
		return NotificationDbUtils.initialized;
	}

	/**
	 * Adapter for existing access-request notification producers.
	 */
	public static void createNotification(User loggedInUser, String affectedUserId, String affectedUserType,
			String catalogId, String notificationType, String notificationSource, String priority,
			String affectedUserPreviousRole, String affectedUserNewRole, String displaySurface) {
		IRDBMSEngine notificationDb = SystemEngineRegistry.getNotificationDb();
		List<Map<String, Object>> authorRecipients = NotificationConstants.APP_CATALOG
				.equalsIgnoreCase(notificationSource) ? SecurityProjectUtils.getProjectAuthors(catalogId)
						: SecurityEngineUtils.getEngineAuthors(catalogId);
		List<Map<String, Object>> recipients = authorRecipients == null ? new ArrayList<>()
				: new ArrayList<>(authorRecipients);

		boolean affectedUserFound = false;
		if (affectedUserId != null) {
			for (Map<String, Object> recipient : recipients) {
				if (recipient == null) {
					continue;
				}
				if (affectedUserId.equals(recipient.get("userId"))) {
					affectedUserFound = true;
					break;
				}
			}
		}

		if (!affectedUserFound && affectedUserId != null) {
			Map<String, Object> affectedUser = new HashMap<>();
			affectedUser.put("userId", affectedUserId);
			affectedUser.put("userType", affectedUserType);
			recipients.add(affectedUser);
		}

		String createdBy = loggedInUser.getAccessToken(loggedInUser.getLogins().get(0)).getId();
		Timestamp createdAt = Utility.getCurrentSqlTimestampUTC();
		String kind = deriveKind(notificationType);
		String type = deriveType(notificationType);
		String scopeType = deriveScopeType(notificationSource);
		String scopeId = NotificationConstants.Scope.APP.equals(scopeType) ? catalogId : null;
		String sourceType = deriveSourceType(notificationSource);
		String targetType = NotificationConstants.Scope.APP.equals(scopeType) ? NotificationConstants.Target.APP
				: NotificationConstants.Target.NONE;
		String metadataJson = buildLegacyMetadata(affectedUserId, affectedUserType, affectedUserPreviousRole,
				affectedUserNewRole, notificationType, notificationSource);

		String query = "INSERT INTO NOTIFICATION_EVENT (NOTIFICATION_ID,KIND,TYPE,SCOPE_TYPE,SCOPE_ID,AUDIENCE_TYPE,AUDIENCE_ID,AUDIENCE_USER_TYPE,TITLE,MESSAGE,PRIORITY,DISPLAY_SURFACE,SOURCE_TYPE,SOURCE_ID,TARGET_TYPE,TARGET_ID,TARGET_URL,ACTION_LABEL,STATUS,GROUP_ID,METADATA_JSON,CREATED_BY,CREATED_AT,RESOLVED_AT,EXPIRES_AT) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		for (Map<String, Object> recipient : recipients) {
			if (recipient == null || recipient.get("userId") == null) {
				continue;
			}
			PreparedStatement ps = null;
			try {
				ps = notificationDb.getPreparedStatement(query);
				int parameterIndex = 1;
				ps.setString(parameterIndex++, GUID.v7().toUUID().toString());
				ps.setString(parameterIndex++, kind);
				ps.setString(parameterIndex++, type);
				ps.setString(parameterIndex++, scopeType);
				ps.setString(parameterIndex++, scopeId);
				ps.setString(parameterIndex++, NotificationConstants.Audience.USER);
				ps.setString(parameterIndex++, String.valueOf(recipient.get("userId")));
				ps.setString(parameterIndex++,
						recipient.get("userType") == null ? null : String.valueOf(recipient.get("userType")));
				ps.setString(parameterIndex++, "NOTIFICATION");
				ps.setString(parameterIndex++, null);
				ps.setString(parameterIndex++, normalizePriority(priority));
				ps.setString(parameterIndex++, normalizeDisplaySurface(displaySurface));
				ps.setString(parameterIndex++, sourceType);
				ps.setString(parameterIndex++, catalogId);
				ps.setString(parameterIndex++, targetType);
				ps.setString(parameterIndex++, catalogId);
				ps.setString(parameterIndex++, null);
				ps.setString(parameterIndex++, NotificationConstants.Kind.ACTION.equals(kind) ? "Review" : null);
				ps.setString(parameterIndex++, NotificationConstants.Status.ACTIVE);
				ps.setString(parameterIndex++, null);
				ps.setString(parameterIndex++, metadataJson);
				ps.setString(parameterIndex++, createdBy);
				ps.setTimestamp(parameterIndex++, createdAt);
				ps.setTimestamp(parameterIndex++, null);
				ps.setTimestamp(parameterIndex++, null);

				ps.execute();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error("Failed to insert notification for recipient {} (type {}) on catalog {}",
						recipient.get("userId"), recipient.get("userType"), catalogId, e);
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(notificationDb, ps);
			}
		}
	}

	public static List<Map<String, Object>> fetchAllNotifications(User user, String limit, String offset) {
		List<Pair<String, String>> userIdAndTypeList = User.getUserIdAndType(user);
		if (userIdAndTypeList.isEmpty()) {
			return new ArrayList<>();
		}
		List<Object> audienceParameters = new ArrayList<>();
		List<Object> dismissedParameters = new ArrayList<>();
		List<Object> readParameters = new ArrayList<>();
		String audienceCondition = buildVisibleAudienceSqlCondition(userIdAndTypeList, audienceParameters);
		String dismissedCondition = buildStateExistsSqlCondition("us", userIdAndTypeList, dismissedParameters,
				"us.IS_DISMISSED = TRUE");
		String readCondition = buildStateExistsSqlCondition("urs", userIdAndTypeList, readParameters,
				"urs.IS_READ = TRUE");

		StringBuilder query = new StringBuilder();
		query.append("SELECT n.NOTIFICATION_ID, n.KIND, n.TYPE, n.SCOPE_TYPE, n.SCOPE_ID, n.AUDIENCE_TYPE, ")
				.append("n.AUDIENCE_ID, n.AUDIENCE_USER_TYPE, n.TITLE, n.MESSAGE, n.PRIORITY, n.DISPLAY_SURFACE, n.SOURCE_TYPE, ")
				.append("n.SOURCE_ID, n.TARGET_TYPE, n.TARGET_ID, n.TARGET_URL, n.ACTION_LABEL, n.STATUS, ")
				.append("n.GROUP_ID, n.METADATA_JSON, n.CREATED_BY, n.CREATED_AT, n.RESOLVED_AT, n.EXPIRES_AT, ")
				.append("CASE WHEN EXISTS (SELECT 1 FROM NOTIFICATION_USER_STATE urs WHERE urs.NOTIFICATION_ID = n.NOTIFICATION_ID AND ")
				.append(readCondition).append(") THEN TRUE ELSE FALSE END AS IS_READ ")
				.append("FROM NOTIFICATION_EVENT n WHERE (").append(audienceCondition).append(") ")
				.append("AND n.STATUS <> ? ").append("AND (n.EXPIRES_AT IS NULL OR n.EXPIRES_AT > CURRENT_TIMESTAMP) ")
				.append("AND NOT EXISTS (SELECT 1 FROM NOTIFICATION_USER_STATE us WHERE us.NOTIFICATION_ID = n.NOTIFICATION_ID AND ")
				.append(dismissedCondition).append(") ").append("ORDER BY n.CREATED_AT DESC");
		List<Object> parameters = new ArrayList<>();
		parameters.addAll(readParameters);
		parameters.addAll(audienceParameters);
		parameters.add(NotificationConstants.Status.EXPIRED);
		parameters.addAll(dismissedParameters);

		Long longLimit = parseLong(limit);
		Long longOffset = parseLong(offset);
		if (longLimit != null && longLimit >= 0) {
			query.append(" LIMIT ?");
			parameters.add(longLimit);
		}
		if (longOffset != null && longOffset >= 0) {
			query.append(" OFFSET ?");
			parameters.add(longOffset);
		}

		List<Map<String, Object>> notificationList = executeNotificationFetch(query.toString(), parameters);
		hydrateLegacyDisplayFields(notificationList);
		return notificationList;
	}

	public static int deleteNotification(String recipientId, String recipientType, String notificationId) {
		List<Pair<String, String>> recipientPairs = new ArrayList<>();
		if (recipientId != null && recipientType != null) {
			recipientPairs.add(Pair.with(recipientId, recipientType));
		}
		return deleteNotification(recipientPairs, notificationId);
	}

	public static int deleteNotification(List<Pair<String, String>> recipientPairs, String notificationId) {
		if (recipientPairs == null || recipientPairs.isEmpty()) {
			return 0;
		}
		List<String> notificationIds = new ArrayList<>();
		if (notificationId != null) {
			if (isNotificationVisibleToUser(notificationId, recipientPairs)) {
				notificationIds.add(notificationId);
			}
		} else {
			notificationIds.addAll(fetchVisibleNotificationIds(recipientPairs));
		}
		int count = 0;
		Timestamp dismissedAt = Utility.getCurrentSqlTimestampUTC();
		Pair<String, String> statePair = firstValidPair(recipientPairs);
		if (statePair == null) {
			return 0;
		}
		for (String id : notificationIds) {
			count += upsertNotificationState(id, statePair, true, dismissedAt, true, dismissedAt);
		}
		return count;
	}

	public static void resetNotificationActionType(User user) {
		List<Pair<String, String>> userIdAndTypeList = User.getUserIdAndType(user);
		if (userIdAndTypeList.isEmpty()) {
			return;
		}
		Pair<String, String> statePair = firstValidPair(userIdAndTypeList);
		if (statePair == null) {
			return;
		}
		Timestamp readAt = Utility.getCurrentSqlTimestampUTC();
		for (String notificationId : fetchVisibleNotificationIds(userIdAndTypeList)) {
			upsertNotificationState(notificationId, statePair, true, readAt, null, null);
		}
	}

	public static void markNotificationRead(String notificationId, Timestamp readDate) {
		markNotificationRead(notificationId, readDate, null);
	}

	public static int markNotificationRead(String notificationId, Timestamp readDate,
			List<Pair<String, String>> recipientPairs) {
		if (notificationId == null || recipientPairs == null || recipientPairs.isEmpty()
				|| !isNotificationVisibleToUser(notificationId, recipientPairs)) {
			return 0;
		}
		Pair<String, String> statePair = firstValidPair(recipientPairs);
		if (statePair == null) {
			return 0;
		}
		return upsertNotificationState(notificationId, statePair, true, readDate, null, null);
	}

	public static int fetchNewNotificationCount(String recipientId, String recipientType) {
		List<Pair<String, String>> recipientPairs = new ArrayList<>();
		if (recipientId != null && recipientType != null) {
			recipientPairs.add(Pair.with(recipientId, recipientType));
		}
		return fetchNewNotificationCount(recipientPairs);
	}

	public static int fetchNewNotificationCount(List<Pair<String, String>> recipientPairs) {
		if (recipientPairs == null || recipientPairs.isEmpty()) {
			return 0;
		}
		IRDBMSEngine notificationDb = SystemEngineRegistry.getNotificationDb();
		List<Object> audienceParameters = new ArrayList<>();
		List<Object> dismissedParameters = new ArrayList<>();
		List<Object> readParameters = new ArrayList<>();
		String audienceCondition = buildVisibleAudienceSqlCondition(recipientPairs, audienceParameters);
		String dismissedCondition = buildStateExistsSqlCondition("us", recipientPairs, dismissedParameters,
				"us.IS_DISMISSED = TRUE");
		String readCondition = buildStateExistsSqlCondition("urs", recipientPairs, readParameters,
				"urs.IS_READ = TRUE");
		List<Object> parameters = new ArrayList<>();
		parameters.addAll(audienceParameters);
		parameters.add(NotificationConstants.Status.EXPIRED);
		parameters.addAll(dismissedParameters);
		parameters.addAll(readParameters);
		String query = "SELECT COUNT(n.NOTIFICATION_ID) FROM NOTIFICATION_EVENT n WHERE (" + audienceCondition + ") "
				+ "AND n.STATUS <> ? " + "AND (n.EXPIRES_AT IS NULL OR n.EXPIRES_AT > CURRENT_TIMESTAMP) "
				+ "AND NOT EXISTS (SELECT 1 FROM NOTIFICATION_USER_STATE us WHERE us.NOTIFICATION_ID = n.NOTIFICATION_ID AND "
				+ dismissedCondition + ") "
				+ "AND NOT EXISTS (SELECT 1 FROM NOTIFICATION_USER_STATE urs WHERE urs.NOTIFICATION_ID = n.NOTIFICATION_ID AND "
				+ readCondition + ")";

		PreparedStatement ps = null;
		try {
			ps = notificationDb.getPreparedStatement(query);
			setParameters(ps, parameters);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					return rs.getInt(1);
				}
			}
		} catch (SQLException e) {
			classLogger.error("Failed to fetch new notification count for recipient pairs {}", recipientPairs, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(notificationDb, ps);
		}
		return 0;
	}

	private static List<Map<String, Object>> executeNotificationFetch(String query, List<Object> parameters) {
		IRDBMSEngine notificationDb = SystemEngineRegistry.getNotificationDb();
		PreparedStatement ps = null;
		List<Map<String, Object>> rows = new ArrayList<>();
		try {
			ps = notificationDb.getPreparedStatement(query);
			setParameters(ps, parameters);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					rows.add(mapNotificationRow(rs));
				}
			}
		} catch (SQLException e) {
			classLogger.error("Failed to fetch notifications", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(notificationDb, ps);
		}
		return rows;
	}

	private static Map<String, Object> mapNotificationRow(ResultSet rs) throws SQLException {
		Map<String, Object> row = new HashMap<>();
		JsonObject metadata = parseMetadata(getString(rs, "METADATA_JSON"));
		String legacyType = getMetadataString(metadata, "legacyNotificationType");
		String sourceType = getString(rs, "SOURCE_TYPE");
		String sourceId = getString(rs, "SOURCE_ID");
		String targetId = getString(rs, "TARGET_ID");
		String catalogId = targetId != null ? targetId : sourceId;

		row.put("notification_id", getString(rs, "NOTIFICATION_ID"));
		row.put("recipient_id", getString(rs, "AUDIENCE_ID"));
		row.put("recipient_type", getString(rs, "AUDIENCE_USER_TYPE"));
		row.put("notification_title", getString(rs, "TITLE"));
		row.put("notification_message", getString(rs, "MESSAGE"));
		row.put("notification_actiontype", Boolean.TRUE.equals(rs.getObject("IS_READ")) ? "NONE" : "NEW");
		row.put("notification_actiontarget", getString(rs, "TARGET_URL"));
		row.put("notification_isread", rs.getBoolean("IS_READ"));
		row.put("notification_priority", getString(rs, "PRIORITY"));
		row.put("display_surface", normalizeDisplaySurface(getString(rs, "DISPLAY_SURFACE")));
		row.put("notification_type", legacyType == null ? getString(rs, "TYPE") : legacyType);
		row.put("catalog_id", catalogId);
		row.put("notification_createddate", rs.getTimestamp("CREATED_AT"));
		row.put("notification_readdate", null);
		row.put("notification_source", deriveLegacyNotificationSource(sourceType, getString(rs, "SCOPE_TYPE")));
		row.put("recipient_user_id", getMetadataString(metadata, "affectedUserId"));
		row.put("notification_usertype", getMetadataString(metadata, "affectedUserType"));
		row.put("user_existingrole", getMetadataString(metadata, "affectedUserPreviousRole"));
		row.put("user_newrole", getMetadataString(metadata, "affectedUserNewRole"));
		row.put("notification_createdby", getString(rs, "CREATED_BY"));
		row.put("kind", getString(rs, "KIND"));
		row.put("type", getString(rs, "TYPE"));
		row.put("scope_type", getString(rs, "SCOPE_TYPE"));
		row.put("scope_id", getString(rs, "SCOPE_ID"));
		row.put("target_type", getString(rs, "TARGET_TYPE"));
		row.put("target_id", targetId);
		row.put("target_url", getString(rs, "TARGET_URL"));
		row.put("action_label", getString(rs, "ACTION_LABEL"));
		row.put("status", getString(rs, "STATUS"));
		row.put("group_id", getString(rs, "GROUP_ID"));
		return row;
	}

	private static void hydrateLegacyDisplayFields(List<Map<String, Object>> notificationList) {
		if (notificationList == null || notificationList.isEmpty()) {
			return;
		}
		Set<String> userIds = new HashSet<>();
		Set<String> catalogIds = new HashSet<>();
		for (Map<String, Object> row : notificationList) {
			if (row.get("recipient_user_id") != null) {
				userIds.add(String.valueOf(row.get("recipient_user_id")));
			}
			if (row.get("notification_createdby") != null) {
				userIds.add(String.valueOf(row.get("notification_createdby")));
			}
			if (row.get("catalog_id") != null) {
				catalogIds.add(String.valueOf(row.get("catalog_id")));
			}
		}
		Map<String, String> userIdToNameMap = SecurityUserUtils.getUserNamesByIds(userIds);
		Map<String, String> projectIdToNameMap = SecurityProjectUtils.getProjectNamesByIds(catalogIds);
		Map<String, String> engineIdToNameMap = SecurityEngineUtils.getEngineNamesByIds(catalogIds);

		for (Map<String, Object> row : notificationList) {
			String catalogId = row.get("catalog_id") == null ? null : String.valueOf(row.get("catalog_id"));
			String notificationSource = row.get("notification_source") == null ? null
					: String.valueOf(row.get("notification_source"));
			Object affectedUserId = row.get("recipient_user_id");
			row.put("recipient_user_name", affectedUserId == null ? "Unknown User"
					: userIdToNameMap.getOrDefault(affectedUserId, "Unknown User"));

			String projectName = catalogId == null ? null : projectIdToNameMap.get(catalogId);
			String engineName = catalogId == null ? null : engineIdToNameMap.get(catalogId);
			if (NotificationConstants.APP_CATALOG.equalsIgnoreCase(notificationSource)) {
				row.put("catalog_name", projectName != null && !projectName.isEmpty() ? projectName : engineName);
			} else {
				row.put("catalog_name", engineName != null && !engineName.isEmpty() ? engineName : projectName);
			}
		}
	}

	private static List<String> fetchVisibleNotificationIds(List<Pair<String, String>> recipientPairs) {
		List<Object> audienceParameters = new ArrayList<>();
		List<Object> dismissedParameters = new ArrayList<>();
		String audienceCondition = buildVisibleAudienceSqlCondition(recipientPairs, audienceParameters);
		String dismissedCondition = buildStateExistsSqlCondition("us", recipientPairs, dismissedParameters,
				"us.IS_DISMISSED = TRUE");
		List<Object> parameters = new ArrayList<>();
		parameters.addAll(audienceParameters);
		parameters.add(NotificationConstants.Status.EXPIRED);
		parameters.addAll(dismissedParameters);
		String query = "SELECT n.NOTIFICATION_ID FROM NOTIFICATION_EVENT n WHERE (" + audienceCondition + ") "
				+ "AND n.STATUS <> ? " + "AND (n.EXPIRES_AT IS NULL OR n.EXPIRES_AT > CURRENT_TIMESTAMP) "
				+ "AND NOT EXISTS (SELECT 1 FROM NOTIFICATION_USER_STATE us WHERE us.NOTIFICATION_ID = n.NOTIFICATION_ID AND "
				+ dismissedCondition + ")";
		List<String> ids = new ArrayList<>();
		IRDBMSEngine notificationDb = SystemEngineRegistry.getNotificationDb();
		PreparedStatement ps = null;
		try {
			ps = notificationDb.getPreparedStatement(query);
			setParameters(ps, parameters);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					ids.add(rs.getString(1));
				}
			}
		} catch (SQLException e) {
			classLogger.error("Failed to fetch visible notification ids for recipient pairs {}", recipientPairs, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(notificationDb, ps);
		}
		return ids;
	}

	private static boolean isNotificationVisibleToUser(String notificationId,
			List<Pair<String, String>> recipientPairs) {
		List<Object> audienceParameters = new ArrayList<>();
		List<Object> dismissedParameters = new ArrayList<>();
		String audienceCondition = buildVisibleAudienceSqlCondition(recipientPairs, audienceParameters);
		String dismissedCondition = buildStateExistsSqlCondition("us", recipientPairs, dismissedParameters,
				"us.IS_DISMISSED = TRUE");
		List<Object> parameters = new ArrayList<>();
		parameters.addAll(audienceParameters);
		parameters.add(notificationId);
		parameters.add(NotificationConstants.Status.EXPIRED);
		parameters.addAll(dismissedParameters);
		String query = "SELECT 1 FROM NOTIFICATION_EVENT n WHERE (" + audienceCondition + ") "
				+ "AND n.NOTIFICATION_ID = ? AND n.STATUS <> ? "
				+ "AND (n.EXPIRES_AT IS NULL OR n.EXPIRES_AT > CURRENT_TIMESTAMP) "
				+ "AND NOT EXISTS (SELECT 1 FROM NOTIFICATION_USER_STATE us WHERE us.NOTIFICATION_ID = n.NOTIFICATION_ID AND "
				+ dismissedCondition + ")";
		IRDBMSEngine notificationDb = SystemEngineRegistry.getNotificationDb();
		PreparedStatement ps = null;
		try {
			ps = notificationDb.getPreparedStatement(query);
			setParameters(ps, parameters);
			try (ResultSet rs = ps.executeQuery()) {
				return rs.next();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to check notification visibility [notificationId={}, recipientPairs={}]",
					notificationId, recipientPairs, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(notificationDb, ps);
		}
		return false;
	}

	private static int upsertNotificationState(String notificationId, Pair<String, String> userPair, Boolean isRead,
			Timestamp readAt, Boolean isDismissed, Timestamp dismissedAt) {
		IRDBMSEngine notificationDb = SystemEngineRegistry.getNotificationDb();
		PreparedStatement updatePs = null;
		PreparedStatement insertPs = null;
		try {
			List<String> sets = new ArrayList<>();
			List<Object> parameters = new ArrayList<>();
			if (isRead != null) {
				sets.add("IS_READ = ?");
				parameters.add(isRead);
				sets.add("READ_AT = ?");
				parameters.add(readAt);
			}
			if (isDismissed != null) {
				sets.add("IS_DISMISSED = ?");
				parameters.add(isDismissed);
				sets.add("DISMISSED_AT = ?");
				parameters.add(dismissedAt);
			}
			if (sets.isEmpty()) {
				return 0;
			}
			parameters.add(notificationId);
			parameters.add(userPair.getValue0());
			parameters.add(userPair.getValue1());
			String update = "UPDATE NOTIFICATION_USER_STATE SET " + String.join(", ", sets)
					+ " WHERE NOTIFICATION_ID = ? AND USER_ID = ? AND USER_TYPE = ?";
			updatePs = notificationDb.getPreparedStatement(update);
			setParameters(updatePs, parameters);
			int updated = updatePs.executeUpdate();
			if (updated == 0) {
				insertPs = notificationDb.getPreparedStatement(
						"INSERT INTO NOTIFICATION_USER_STATE (NOTIFICATION_ID,USER_ID,USER_TYPE,IS_READ,READ_AT,IS_DISMISSED,DISMISSED_AT) VALUES (?,?,?,?,?,?,?)");
				insertPs.setString(1, notificationId);
				insertPs.setString(2, userPair.getValue0());
				insertPs.setString(3, userPair.getValue1());
				insertPs.setBoolean(4, isRead != null && isRead.booleanValue());
				insertPs.setTimestamp(5, readAt);
				insertPs.setBoolean(6, isDismissed != null && isDismissed.booleanValue());
				insertPs.setTimestamp(7, dismissedAt);
				insertPs.executeUpdate();
			}
			Connection conn = updatePs.getConnection();
			if (conn != null && !conn.getAutoCommit()) {
				conn.commit();
			}
			return 1;
		} catch (SQLException e) {
			classLogger.error("Failed to upsert notification state [notificationId={}, userPair={}]", notificationId,
					userPair, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(notificationDb, insertPs);
			ConnectionUtils.closeAllConnectionsIfPooling(notificationDb, updatePs);
		}
		return 0;
	}

	private static String buildVisibleAudienceSqlCondition(List<Pair<String, String>> recipientPairs,
			List<Object> parameters) {
		List<String> conditions = new ArrayList<>();
		for (Pair<String, String> pair : recipientPairs) {
			if (pair == null || pair.getValue0() == null) {
				continue;
			}
			conditions.add(
					"(n.AUDIENCE_TYPE = ? AND n.AUDIENCE_ID = ? AND (n.AUDIENCE_USER_TYPE = ? OR n.AUDIENCE_USER_TYPE IS NULL))");
			parameters.add(NotificationConstants.Audience.USER);
			parameters.add(pair.getValue0());
			parameters.add(pair.getValue1());
		}
		conditions.add("(n.AUDIENCE_TYPE = ?)");
		parameters.add(NotificationConstants.Audience.GLOBAL);
		return String.join(" OR ", conditions);
	}

	private static String buildStateExistsSqlCondition(String alias, List<Pair<String, String>> recipientPairs,
			List<Object> parameters, String statePredicate) {
		List<String> pairConditions = new ArrayList<>();
		for (Pair<String, String> pair : recipientPairs) {
			if (pair == null || pair.getValue0() == null || pair.getValue1() == null) {
				continue;
			}
			pairConditions.add("(" + alias + ".USER_ID = ? AND " + alias + ".USER_TYPE = ?)");
			parameters.add(pair.getValue0());
			parameters.add(pair.getValue1());
		}
		if (pairConditions.isEmpty()) {
			pairConditions.add("1 = 0");
		}
		return "(" + String.join(" OR ", pairConditions) + ") AND " + statePredicate;
	}

	private static Pair<String, String> firstValidPair(List<Pair<String, String>> recipientPairs) {
		for (Pair<String, String> pair : recipientPairs) {
			if (pair != null && pair.getValue0() != null && pair.getValue1() != null) {
				return pair;
			}
		}
		return null;
	}

	private static void setParameters(PreparedStatement ps, List<Object> parameters) throws SQLException {
		for (int i = 0; i < parameters.size(); i++) {
			ps.setObject(i + 1, parameters.get(i));
		}
	}

	private static Long parseLong(String value) {
		if (value == null || value.trim().isEmpty()) {
			return null;
		}
		return ((Number) Double.parseDouble(value)).longValue();
	}

	private static String deriveKind(String notificationType) {
		if (NotificationConstants.Type.USER_REQUEST.equalsIgnoreCase(notificationType)) {
			return NotificationConstants.Kind.ACTION;
		}
		return NotificationConstants.Kind.INFO;
	}

	private static String deriveType(String notificationType) {
		if (NotificationConstants.Type.USER_REQUEST.equalsIgnoreCase(notificationType)) {
			return NotificationConstants.Type.ACCESS_REQUEST;
		}
		return notificationType;
	}

	private static String deriveScopeType(String notificationSource) {
		if (NotificationConstants.APP_CATALOG.equalsIgnoreCase(notificationSource)) {
			return NotificationConstants.Scope.APP;
		}
		return NotificationConstants.Scope.SYSTEM;
	}

	private static String deriveSourceType(String notificationSource) {
		if (NotificationConstants.APP_CATALOG.equalsIgnoreCase(notificationSource)) {
			return NotificationConstants.Source.PROJECT;
		}
		if (notificationSource == null || notificationSource.trim().isEmpty()) {
			return NotificationConstants.Source.SYSTEM;
		}
		return NotificationConstants.Source.ENGINE;
	}

	private static String deriveLegacyNotificationSource(String sourceType, String scopeType) {
		if (NotificationConstants.Scope.APP.equalsIgnoreCase(scopeType)
				|| NotificationConstants.Source.PROJECT.equalsIgnoreCase(sourceType)) {
			return NotificationConstants.APP_CATALOG;
		}
		return sourceType;
	}

	private static String normalizePriority(String priority) {
		if (priority == null || priority.trim().isEmpty()) {
			return NotificationConstants.Priority.NORMAL;
		}
		if (NotificationConstants.Priority.MEDIUM.equalsIgnoreCase(priority)) {
			return NotificationConstants.Priority.NORMAL;
		}
		return priority.toUpperCase();
	}

	private static String normalizeDisplaySurface(String displaySurface) {
		if (displaySurface == null || displaySurface.trim().isEmpty()) {
			return NotificationConstants.DisplaySurface.BELL;
		}
		String normalized = displaySurface.trim().toUpperCase();
		if (NotificationConstants.DisplaySurface.isValid(normalized)) {
			return normalized;
		}
		return NotificationConstants.DisplaySurface.BELL;
	}

	private static String buildLegacyMetadata(String affectedUserId, String affectedUserType,
			String affectedUserPreviousRole, String affectedUserNewRole, String legacyNotificationType,
			String legacyNotificationSource) {
		JsonObject metadata = new JsonObject();
		addJsonProperty(metadata, "affectedUserId", affectedUserId);
		addJsonProperty(metadata, "affectedUserType", affectedUserType);
		addJsonProperty(metadata, "affectedUserPreviousRole", affectedUserPreviousRole);
		addJsonProperty(metadata, "affectedUserNewRole", affectedUserNewRole);
		addJsonProperty(metadata, "legacyNotificationType", legacyNotificationType);
		addJsonProperty(metadata, "legacyNotificationSource", legacyNotificationSource);
		return metadata.toString();
	}

	private static void addJsonProperty(JsonObject metadata, String key, String value) {
		if (value == null) {
			return;
		}
		metadata.addProperty(key, value);
	}

	private static JsonObject parseMetadata(String metadataJson) {
		if (metadataJson == null || metadataJson.trim().isEmpty()) {
			return new JsonObject();
		}
		try {
			JsonElement element = JsonParser.parseString(metadataJson);
			return element != null && element.isJsonObject() ? element.getAsJsonObject() : new JsonObject();
		} catch (Exception e) {
			classLogger.warn("Failed to parse notification metadata json", e);
			return new JsonObject();
		}
	}

	private static String getMetadataString(JsonObject metadata, String key) {
		JsonElement element = metadata.get(key);
		if (element == null || element.isJsonNull()) {
			return null;
		}
		return element.getAsString();
	}

	private static String getString(ResultSet rs, String column) throws SQLException {
		Object value = rs.getObject(column);
		if (value == null) {
			return null;
		}
		if (value instanceof Clob) {
			Clob clob = (Clob) value;
			return clob.getSubString(1L, (int) clob.length());
		}
		return String.valueOf(value);
	}

}
