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
import org.javatuples.Pair;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityUserUtils;
import prerna.engine.api.IRDBMSEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

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
	 * @param createdUser          - The logged-in user performing the action
	 * @param initiatorUserId     - The user whose role or permission changed
	 * @param catalogId
	 * @param notificationType   - e.g. USER_REQUEST, REQUEST_APPROVAL
	 * @param notificationSource
	 * @param priority           - e.g. HIGH, MEDIUM, LOW
	 * @param userExistingRole
	 * @param userNewRole
	 */
	public static void createNotification(User createdUser, String initiatorUserId, String catalogId,
			String notificationType, String notificationSource, String priority, String userExistingRole,
			String userNewRole) {
		// Fetch all authors based on source
		List<Map<String, Object>> authors = Constants.APP_CATALOG.equalsIgnoreCase(notificationSource)
				? SecurityProjectUtils.getProjectAuthors(catalogId, initiatorUserId)
				: SecurityEngineUtils.getEngineAuthors(catalogId, initiatorUserId);

		// Check if initiator already present
		boolean initiatorFound = false;

		if (initiatorUserId != null && authors != null && !authors.isEmpty()) {

			for (int i = 0; i < authors.size(); i++) {
				Map<String, Object> user = authors.get(i);
				if (user == null) {
					continue;
				}
				String userId = (String) user.get("userId");
				if (userId == null) {
					continue;
				}

				if (initiatorUserId.equals(userId)) {
					initiatorFound = true;
					break;
				}
			}
		}

		if (!initiatorFound && initiatorUserId != null) {
			Map<String, Object> initiatorMap = new HashMap<>();
			initiatorMap.put("userId", initiatorUserId);
			initiatorMap.put("userType", SecurityUserUtils.getUserTypeByUserId(initiatorUserId));
			authors.add(initiatorMap);
		}
	    String createdBy = createdUser.getAccessToken(createdUser.getLogins().get(0)).getId();
		Timestamp createdDate = Utility.getCurrentSqlTimestampUTC();
		String actionTarget = "In-app";
		
		for (Map<String, Object> author : authors) {
			String recipientId = (String) author.get("userId");
			String recipientType = (String) author.get("userType");
			
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
				ps.setString(parameterIndex++, initiatorUserId);
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
	public static List<Map<String, Object>> fetchAllNotifications(User user, String limit, String offset) {
		 List<Pair<String, String>> userIdAndTypeList = User.getUserIdAndType(user);
		    if (userIdAndTypeList.isEmpty()) {
		        return new ArrayList<>();
		    }
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
		qs.addSelector(new QueryColumnSelector("NOTIFICATION__USERID", "recipient_user_id"));
		qs.addSelector(new QueryColumnSelector("NOTIFICATION__USEREXISTINGROLE", "user_existingrole"));
		qs.addSelector(new QueryColumnSelector("NOTIFICATION__USERNEWROLE", "user_newrole"));
		qs.addSelector(new QueryColumnSelector("NOTIFICATION__CREATEDBY", "notification_createdby"));

		
		Pair<String, String> userPair = userIdAndTypeList.get(0);
		String userId = userPair.getValue0();
		String userType = userPair.getValue1();

		List<IQueryFilter> andFilters = new ArrayList<>();
		andFilters.add(SimpleQueryFilter.makeColToValFilter("NOTIFICATION__RECIPIENTID", "==", userId));
		andFilters.add(SimpleQueryFilter.makeColToValFilter("NOTIFICATION__RECIPIENTTYPE", "==", userType));

		// (RECIPIENTID == userId AND RECIPIENTTYPE == userType)
		AndQueryFilter andCombined = new AndQueryFilter(andFilters);
		qs.addExplicitFilter(andCombined);
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
			if (row.get("recipient_user_id") != null) {
				userIds.add(String.valueOf(row.get("recipient_user_id")));
			}
			if (row.get("catalog_id") != null) {
				catalogIds.add(String.valueOf(row.get("catalog_id")));
			}
			if (row.get("notification_createdby") != null) {
				userIds.add(String.valueOf(row.get("notification_createdby")));
		    }
		}
		// bulk fetch
		Map<String, String> userIdToNameMap = SecurityUserUtils.getUserNamesByIds(userIds);
		Map<String, String> projectIdToNameMap = SecurityProjectUtils.getProjectNamesByIds(catalogIds);
		Map<String, String> engineIdToNameMap = SecurityEngineUtils.getEngineNamesByIds(catalogIds);

		for (Map<String, Object> row : notificationList) {
			String catalogId = String.valueOf(row.get("catalog_id"));
			String notificationSource = String.valueOf(row.get("notification_source")).trim();

			// user name from cached map
	        row.put("recipient_user_name", userIdToNameMap.getOrDefault((String) row.get("recipient_user_id"), "Unknown User"));
	        row.put("notification_createdby_name", userIdToNameMap.getOrDefault((String) row.get("notification_createdby"), "Unknown User"));


			// project and engine names from cached maps
			String projectName = projectIdToNameMap.get(catalogId);
			String engineName = engineIdToNameMap.get(catalogId);

			// final catalog name
			String finalCatalogName;
			if (Constants.APP_CATALOG.equalsIgnoreCase(notificationSource)) {
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
	 * 
	 * @param recipientId    -the ID of the recipient
	 * @param recipientType  -the type of the recipient (e.g., NATIVE, MS)
	 * @param notificationId -the ID of the notification
	 * @return
	 */
	public static int deleteNotification(String recipientId, String recipientType, String notificationId) {
		StringBuilder deleteQuery = new StringBuilder("DELETE FROM NOTIFICATION WHERE ");
		List<String> conditions = new ArrayList<>();
	    List<Object> parameters = new ArrayList<>();

		 if (notificationId != null) {
		        conditions.add("NOTIFICATIONID = ?");
		        parameters.add(notificationId);
		    } else if (recipientId != null && recipientType != null) {
		        conditions.add("RECIPIENTID = ?");
		        parameters.add(recipientId);
		        conditions.add("RECIPIENTTYPE = ?");
		        parameters.add(recipientType);
		    } else {
		        return 0; // nothing to delete
		    }

		 deleteQuery.append(String.join(" AND ", conditions));
		PreparedStatement ps = null;
		int deletedCount = 0;
		try {
			 ps = notificationDb.getPreparedStatement(deleteQuery.toString());
		        int index = 1;
		        for (Object param : parameters) {
		            ps.setObject(index++, param);
		        }

		        deletedCount = ps.executeUpdate();

		        if (!ps.getConnection().getAutoCommit()) {
		            ps.getConnection().commit();
		        }
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(notificationDb, ps);
		}
		return deletedCount;
	}

	/**
	 * Updates notification action type for a given user.
	 *
	 * @param user the user whose notifications need to be updated
	 */
	public static void resetNotificationActionType(User user) {
		List<Pair<String, String>> userIdAndTypeList = User.getUserIdAndType(user);
		if (userIdAndTypeList.isEmpty()) {
			return;
		}
		String updateQuery = "UPDATE NOTIFICATION SET ACTIONTYPE = 'NONE' WHERE ACTIONTYPE = 'NEW' AND RECIPIENTID=? AND RECIPIENTTYPE = ?";
		PreparedStatement ps = null;
		try {
			ps = notificationDb.getPreparedStatement(updateQuery);
			for (Pair<String, String> pair : userIdAndTypeList) {
				String recipientId = pair.getValue0();
				String recipientType = pair.getValue1();

				ps.setString(1, recipientId);
				ps.setString(2, recipientType);
				ps.executeUpdate();
			}

			Connection conn = (ps != null) ? ps.getConnection() : null;
			if (conn != null && !conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(notificationDb, ps);
		}
	}

	/**
	 * Marks a notification as read and updates the read date.
	 *
	 * @param notificationId -the ID of the notification
	 * @param readDate       -the timestamp when the notification was read
	 */
	public static void markNotificationRead(String notificationId, Timestamp readDate) {
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
	 * Retrieves the count of new notifications for a given user and type.
	 *
	 * @param recipientId   -the ID of the recipient
	 * @param recipientType -the type of the recipient (e.g., NATIVE, MS)
	 * @return the count of new notifications
	 */
	public static int fetchNewNotificationCount(String recipientId, String recipientType) {
		PreparedStatement ps = null;
		String query = "SELECT COUNT(NOTIFICATIONID) FROM NOTIFICATION " +
                "WHERE RECIPIENTID = ? AND RECIPIENTTYPE = ? AND ACTIONTYPE = 'NEW'";
		try {
			ps = notificationDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, recipientId);
			ps.setString(parameterIndex++, recipientType);
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
}
