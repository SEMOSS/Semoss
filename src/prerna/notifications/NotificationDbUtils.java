package prerna.notifications;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityUserUtils;
import prerna.engine.api.IRDBMSEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;

public class NotificationDbUtils {

	private static final Logger classLogger = LogManager.getLogger(NotificationDbUtils.class);

	static IRDBMSEngine notificationDb;
	static boolean initialized = false;

	private NotificationDbUtils() {

	}

	public static void loadNotificationDatabase() throws Exception {
		notificationDb = (IRDBMSEngine) Utility.getDatabase(Constants.NOTIFICATION_DB);
		NotificationOwlCreator owlCreator = new NotificationOwlCreator(notificationDb);
		if (owlCreator.needsRemake()) {
			owlCreator.remakeOwl();
		}
		initialize();
		initialized = true;
	}

	private static void initialize() throws Exception {
		String database = notificationDb.getDatabase();
		String schema = notificationDb.getSchema();
		Connection conn = notificationDb.getConnection();
		try {
			String[] colNames = null;
			String[] types = null;

			AbstractSqlQueryUtil queryUtil = notificationDb.getQueryUtil();
			boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();
			boolean allowIfExistsIndexs = queryUtil.allowIfExistsIndexSyntax();
			final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
			final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
			final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();

			// notification
			colNames = new String[] { "NOTIFICATIONID", "RECIPIENTID", "RECIPIENTTYPE", "NOTIFICATIONTITLE", "MESSAGE",
					"ACTIONTYPE", "ACTIONTARGET", "ISREAD", "PRIORITY", "NOTIFICATIONTYPE", "CATALOGID", "CREATEDBY",
					"CREATEDDATE", "READDATE", "NOTIFICATIONSOURCE", "USERID", "USEREXISTINGROLE", "USERNEWROLE" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", CLOB_DATATYPE_NAME,
					"VARCHAR(50)", "VARCHAR(255)", BOOLEAN_DATATYPE_NAME, "VARCHAR(20)", "VARCHAR(255)", "VARCHAR(255)",
					"VARCHAR(255)", TIMESTAMP_DATATYPE_NAME, TIMESTAMP_DATATYPE_NAME, "VARCHAR(255)", "VARCHAR(255)",
					"VARCHAR(255)", "VARCHAR(255)" };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("NOTIFICATION", colNames, types);
				classLogger.info("Running sql " + sql);
				notificationDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "NOTIFICATION", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("NOTIFICATION", colNames, types);
					classLogger.info("Running sql " + sql);
					notificationDb.insertData(sql);
				}
			}
			if (allowIfExistsIndexs) {
				String sql = queryUtil.createIndexIfNotExists("NOTIFICATION_NOTIFICATIONID_INDEX", "NOTIFICATION",
						"NOTIFICATIONID");
				classLogger.info("Running sql " + sql);
				notificationDb.insertData(sql);
			} else {
				// see if index exists
				if (!queryUtil.indexExists(notificationDb, "NOTIFICATION_NOTIFICATIONID_INDEX", "NOTIFICATION",
						database, schema)) {
					String sql = queryUtil.createIndex("NOTIFICATION_NOTIFICATIONID_INDEX", "NOTIFICATION",
							"NOTIFICATIONID");
					classLogger.info("Running sql " + sql);
					notificationDb.insertData(sql);
				}
			}

			// check all the columns we want are there
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "NOTIFICATION", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '" + col + "' is not present in current list of columns: "
								+ allCols.toString());
						String addColumnSql = queryUtil.alterTableAddColumn("NOTIFICATION", col, types[i]);
						classLogger.info("Running sql " + addColumnSql);
						notificationDb.insertData(addColumnSql);
					}
				}
			}

			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} finally {
			// clean up the connection used for this method
			if (conn != null && notificationDb.isConnectionPooling()) {
				conn.close();
			}
		}
	}

	/**
	 * Determine if the theme db is present to be able to set custom themes
	 * 
	 * @return
	 */
	public static boolean isInitalized() {
		return NotificationDbUtils.initialized;
	}

	/**
	 * Add notification into database
	 * 
	 * @param userId
	 * @param catalogId
	 * @param notificationType
	 * @param priority
	 * @param userExistingPermission
	 * @param userNewPermission
	 */
	public static void addNotification(User member, String userId, String catalogId, String notificationType,
			String notificationSource, String priority, String userExistingRole, String userNewRole) {

		List<Map<String, Object>> usersOfCatalog = getAuthors(catalogId, notificationSource, userId);
		for (Map<String, Object> recipient : usersOfCatalog) {
			String recipientId = (String) recipient.get("userId");
			String recipientType = (String) recipient.get("userType");
			String actionTarget = "In-app";
			String createdBy = member.getAccessToken(member.getLogins().get(0)).getId();
			Timestamp createdDate = Utility.getCurrentSqlTimestampUTC();

			String query = "INSERT INTO NOTIFICATION (NOTIFICATIONID,RECIPIENTID,RECIPIENTTYPE,NOTIFICATIONTITLE,MESSAGE,ACTIONTYPE,ACTIONTARGET,ISREAD,PRIORITY,NOTIFICATIONTYPE,CATALOGID,CREATEDBY,CREATEDDATE,READDATE,NOTIFICATIONSOURCE,USERID,USEREXISTINGROLE,USERNEWROLE) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			PreparedStatement ps = null;
			try {
				ps = notificationDb.getPreparedStatement(query);
				int parameterIndex = 1;
				ps.setString(parameterIndex++, UUID.randomUUID().toString()); // notificationId
				ps.setString(parameterIndex++, recipientId);
				ps.setString(parameterIndex++, recipientType);
				ps.setString(parameterIndex++, "NOTIFICATION"); // notificationTitle
				ps.setString(parameterIndex++, null); // message
				ps.setString(parameterIndex++, "NEW"); // actionType
				ps.setString(parameterIndex++, actionTarget);
				ps.setBoolean(parameterIndex++, false); // isRead
				ps.setString(parameterIndex++, priority);
				ps.setString(parameterIndex++, notificationType);
				ps.setString(parameterIndex++, catalogId);
				ps.setString(parameterIndex++, createdBy);
				ps.setTimestamp(parameterIndex++, createdDate);
				ps.setTimestamp(parameterIndex++, null); // readDate
				ps.setString(parameterIndex++, notificationSource);
				ps.setString(parameterIndex++, userId);
				ps.setString(parameterIndex++, userExistingRole);
				ps.setString(parameterIndex++, userNewRole);

				ps.execute();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(notificationDb, ps);
			}
		}
	}

	/**
	 * Get all notifications for user
	 * 
	 * @param memberId
	 * @param limit
	 * @param offset
	 * @return list of notifications
	 */
	public static List<Map<String, Object>> getAllNotifications(String memberId, String limit, String offset) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("NOTIFICATION__NOTIFICATIONID", "notification_id"));
		qs.addSelector(new QueryColumnSelector("NOTIFICATION__RECIPIENTID", "recipient_id"));
		qs.addSelector(new QueryColumnSelector("NOTIFICATION__RECIPIENTTYPE", "recipient_type"));
		qs.addSelector(new QueryColumnSelector("NOTIFICATION__NOTIFICATIONTITLE", "notification_title"));
		qs.addSelector(new QueryColumnSelector("NOTIFICATION__MESSAGE", "notification_message"));
		qs.addSelector(new QueryColumnSelector("NOTIFICATION__ACTIONTYPE", "notification_actiontype"));
		qs.addSelector(new QueryColumnSelector("NOTIFICATION__ACTIONTARGET", "notification_actiontarget"));
		qs.addSelector(new QueryColumnSelector("NOTIFICATION__ISREAD", "notification_isread"));
		qs.addSelector(new QueryColumnSelector("NOTIFICATION__PRIORITY", "notification_priority"));
		qs.addSelector(new QueryColumnSelector("NOTIFICATION__NOTIFICATIONTYPE", "notification_type"));
		qs.addSelector(new QueryColumnSelector("NOTIFICATION__CATALOGID", "catalog_id"));
		qs.addSelector(new QueryColumnSelector("NOTIFICATION__CREATEDDATE", "notification_createddate"));
		qs.addSelector(new QueryColumnSelector("NOTIFICATION__READDATE", "notification_readdate"));
		qs.addSelector(new QueryColumnSelector("NOTIFICATION__NOTIFICATIONSOURCE", "notification_source"));
		qs.addSelector(new QueryColumnSelector("NOTIFICATION__USERID", "user_id"));
		qs.addSelector(new QueryColumnSelector("NOTIFICATION__USEREXISTINGROLE", "user_existingrole"));
		qs.addSelector(new QueryColumnSelector("NOTIFICATION__USERNEWROLE", "user_newrole"));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("NOTIFICATION__RECIPIENTID", "==", memberId));
		qs.addOrderBy("NOTIFICATION__CREATEDDATE", "desc");

		Long long_limit = -1L;
		Long long_offset = -1L;
		if (limit != null && !limit.trim().isEmpty()) {
			long_limit = ((Number) Double.parseDouble(limit)).longValue();
		}
		if (offset != null && !offset.trim().isEmpty()) {
			long_offset = ((Number) Double.parseDouble(offset)).longValue();
		}
		qs.setLimit(long_limit);
		qs.setOffSet(long_offset);

		List<Map<String, Object>> notificationList = QueryExecutionUtility.flushRsToMap(notificationDb, qs);
		if (notificationList == null || notificationList.isEmpty()) {
			return new ArrayList<>();
		}

		// Collect all unique IDs
		Set<String> userIds = new HashSet<>();
		Set<String> catalogIds = new HashSet<>();

		for (Map<String, Object> row : notificationList) {
			if (row.get("user_id") != null) {
				userIds.add(String.valueOf(row.get("user_id")));
			}
			if (row.get("catalog_id") != null) {
				catalogIds.add(String.valueOf(row.get("catalog_id")));
			}
		}
		// bulk fetch
		Map<String, String> userIdToNameMap = SecurityUserUtils.getUserNamesByIds(userIds);
		Map<String, String> projectIdToNameMap = SecurityProjectUtils.getProjectNamesByIds(catalogIds);
		Map<String, String> engineIdToNameMap = SecurityEngineUtils.getEngineNamesByIds(catalogIds);

		for (Map<String, Object> row : notificationList) {
			String catalogId = String.valueOf(row.get("catalog_id"));
			String userId = String.valueOf(row.get("user_id"));
			String notificationSource = String.valueOf(row.get("notification_source")).trim();

			// user name from cached map
			String userName = userIdToNameMap.getOrDefault(userId, "Unknown User");
			row.put("user_name", userName);

			// project and engine names from cached maps
			String projectName = projectIdToNameMap.get(catalogId);
			String engineName = engineIdToNameMap.get(catalogId);

			// final catalog name
			String finalCatalogName;
			if ("app".equalsIgnoreCase(notificationSource)) {
				finalCatalogName = (projectName != null && !projectName.isEmpty()) ? projectName
						: ((engineName != null && !engineName.isEmpty()) ? engineName : null);
			} else {
				finalCatalogName = (engineName != null && !engineName.isEmpty()) ? engineName
						: ((projectName != null && !projectName.isEmpty()) ? projectName : null);
			}
			row.put("catalog_name", finalCatalogName);
		}

		return notificationList;
	}

	/**
	 * Remove notification/ notifications
	 * 
	 * @param userId
	 * @param notificationId
	 */
	public static int removeNotifications(String memberId, String notificationId) {
		StringBuilder deleteQuery = new StringBuilder("DELETE FROM NOTIFICATION WHERE ");
		String conditionField = null;
		String conditionValue = null;

		if (memberId != null) {
			conditionField = "RECIPIENTID";
			conditionValue = memberId;
		} else if (notificationId != null) {
			conditionField = "NOTIFICATIONID";
			conditionValue = notificationId;
		} else {
			return 0;
		}

		deleteQuery.append(conditionField).append(" = ?");
		PreparedStatement ps = null;
		int rowCount = 0;
		try {
			ps = notificationDb.getPreparedStatement(deleteQuery.toString());
			ps.setString(1, conditionValue);
			rowCount = ps.executeUpdate();

			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(notificationDb, ps);
		}
		return rowCount;
	}

	/**
	 * Update notification actionType
	 * 
	 * @param userId
	 */
	public static void updateActiontypeForUserNotifications(String recipientId) {
		String updateQuery = "UPDATE NOTIFICATION SET ACTIONTYPE = 'NONE' WHERE ACTIONTYPE = 'NEW' AND RECIPIENTID=?";
		PreparedStatement ps = null;
		try {
			ps = notificationDb.getPreparedStatement(updateQuery);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, recipientId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(notificationDb, ps);
		}
	}

	/**
	 * Update the Read notifications for user
	 * 
	 * @param notificationId
	 * @param readDate
	 */
	public static void updateReadNotifications(String notificationId, Timestamp readDate) {
		String query = "UPDATE NOTIFICATION SET ISREAD = TRUE, READDATE=? WHERE NOTIFICATIONID=?";
		PreparedStatement ps = null;
		try {
			ps = notificationDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setTimestamp(parameterIndex++, readDate);
			ps.setString(parameterIndex++, notificationId);
			ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(notificationDb, ps);
		}
	}

	/**
	 * Get the new notification count for the logged in user
	 * 
	 * @param userId
	 * @return count
	 */
	public static int getNewNotificationCount(String memberId) {
		PreparedStatement ps = null;
		String query = "SELECT COUNT(NOTIFICATIONID) FROM NOTIFICATION WHERE RECIPIENTID = ? AND ACTIONTYPE='NEW'";
		try {
			ps = notificationDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, memberId);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					return rs.getInt(1);
				}
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(notificationDb, ps);
		}
		return 0;
	}

	/**
	 * Get authors of a catalog (Project or Engine) based on notification source.
	 *
	 * @param catalogId
	 * @param notificationSource (app = Project, otherwise = Engine)
	 * @param userId
	 * @return list of userIds
	 */
	public static List<Map<String, Object>> getAuthors(String catalogId, String notificationSource, String userId) {
		if ("app".equalsIgnoreCase(notificationSource)) {
			return SecurityProjectUtils.getProjectAuthors(catalogId, userId);
		} else {
			return SecurityEngineUtils.getEngineAuthors(catalogId, userId);
		}
	}

	/**
	 * Utility method to add the notification initiator (the current user)
	 */
	public static void addNotificationInitiator(List<Map<String, Object>> authorList, String userId) {
		if (userId != null) {
			Map<String, Object> notificationInitiator = new HashMap<>();
			notificationInitiator.put("userId", userId);
			notificationInitiator.put("userType", SecurityUserUtils.getUserTypeByUserId(userId));
			authorList.add(notificationInitiator);
		}
	}

}
