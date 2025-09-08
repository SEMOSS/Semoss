package prerna.auth.utils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.joins.SubqueryRelationship;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;

public class SecurityProjectNotificationUtils extends AbstractSecurityUtils{

	private static final Logger classLogger = LogManager.getLogger(SecurityProjectUtils.class);
	
	/**
	 * Add notification into database
	 * 
	 * @param userId
	 * @param projectId
	 * @param notificationType
	 * @param priority
	 * @param userExistingPermission
	 * @param userNewPermission
	 */
	public static void addProjectNotification(User member, String userId, String projectId, String notificationType,
			String priority, String userExistingRole, String userNewRole) {

		List<Map<String, Object>> usersOfProject = getProjectAuthors(projectId, notificationType, userId);
		for (Map<String, Object> recipient : usersOfProject) {
			String recipientId = (String) recipient.get("userId");
			String recipientType = (String) recipient.get("userType");
			String actionTarget = "In-app";
			String createdBy = member.getAccessToken(member.getLogins().get(0)).getId();
			Timestamp createdAt = Utility.getCurrentSqlTimestampUTC();

			String query = "INSERT INTO PROJECT_NOTIFICATION (NOTIFICATIONID,RECIPIENTID,RECIPIENTTYPE,NOTIFICATIONTITLE,MESSAGE,ACTIONTYPE,ACTIONTARGET,ISREAD,PRIORITY,NOTIFICATIONTYPE,PROJECTID,CREATEDBY,CREATEDAT,READAT,NOTIFICATIONSOURCE,USERID,USEREXISTINGROLE,USERNEWROLE) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(query);
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
				ps.setString(parameterIndex++, projectId);
				ps.setString(parameterIndex++, createdBy);
				ps.setTimestamp(parameterIndex++, createdAt);
				ps.setTimestamp(parameterIndex++, null); // readAt
				ps.setString(parameterIndex++, "app"); // notificationSource
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
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
		}
	}
		
		/**
		 * Get authors of project
		 * 
		 * @param projectId
		 * @param notificationType
		 * @param userId
		 * @return list of userIds
		 */
		public static List<Map<String, Object>> getProjectAuthors(String projectId, String notificationType,
				String userId) {
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "userId"));
			qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE", "userType"));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PERMISSION", "==", 1));
			qs.addRelation("SMSS_USER", "PROJECTPERMISSION", "inner.join");
			List<Map<String, Object>> usersOfProject = QueryExecutionUtility.flushRsToMap(securityDb, qs);

			Map<String, Object> notificationInitiator = new HashMap<>();
			notificationInitiator.put("userId", userId);
			notificationInitiator.put("userType", getUserTypeByUserId(userId));
			usersOfProject.add(notificationInitiator);
			return usersOfProject;
		}
		
		/**
		 * Remove notification/ notifications
		 * 
		 * @param userId
		 * @param notificationId
		 */
		public static int removeNotifications(String memberId, String notificationId) {
			StringBuilder deleteQuery = new StringBuilder("DELETE FROM PROJECT_NOTIFICATION WHERE ");
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
				ps = securityDb.getPreparedStatement(deleteQuery.toString());
				ps.setString(1, conditionValue);
				rowCount = ps.executeUpdate();

				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
			return rowCount;
		}
	
	/**
	 * Get all notifications for user
	 * 
	 * @param userId
	 * @param limit
	 * @param offset
	 * @return list of notifications
	 */
	public static List<Map<String, Object>> getAllNotifications(String memberId, String limit, String offset) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT_NOTIFICATION__NOTIFICATIONID", "notification_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT_NOTIFICATION__RECIPIENTID", "recipient_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT_NOTIFICATION__RECIPIENTTYPE", "recipient_type"));
		qs.addSelector(new QueryColumnSelector("PROJECT_NOTIFICATION__NOTIFICATIONTITLE", "notification_title"));
		qs.addSelector(new QueryColumnSelector("PROJECT_NOTIFICATION__MESSAGE", "notification_message"));
		qs.addSelector(new QueryColumnSelector("PROJECT_NOTIFICATION__ACTIONTYPE", "notification_actiontype"));
		qs.addSelector(new QueryColumnSelector("PROJECT_NOTIFICATION__ACTIONTARGET", "notification_actiontarget"));
		qs.addSelector(new QueryColumnSelector("PROJECT_NOTIFICATION__ISREAD", "notification_isread"));
		qs.addSelector(new QueryColumnSelector("PROJECT_NOTIFICATION__PRIORITY", "notification_priority"));
		qs.addSelector(new QueryColumnSelector("PROJECT_NOTIFICATION__NOTIFICATIONTYPE", "notification_type"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT_NOTIFICATION__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT_NOTIFICATION__CREATEDAT", "notification_createdat"));
		qs.addSelector(new QueryColumnSelector("PROJECT_NOTIFICATION__READAT", "notification_readat"));
		qs.addSelector(new QueryColumnSelector("PROJECT_NOTIFICATION__NOTIFICATIONSOURCE", "notification_source"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "user_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT_NOTIFICATION__USEREXISTINGROLE", "user_existingrole"));
		qs.addSelector(new QueryColumnSelector("PROJECT_NOTIFICATION__USERNEWROLE", "user_newrole"));

		qs.addRelation("PROJECT_NOTIFICATION__PROJECTID", "PROJECT__PROJECTID", "inner.join");
		qs.addRelation("PROJECT_NOTIFICATION__USERID", "SMSS_USER__ID", "inner.join");
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			subQs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "ID"));
			subQs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "NAME"));
			IRelation subRel = new SubqueryRelationship(subQs, "CB", "left.outer.join",
					new String[] { "CB__ID", "PROJECT_NOTIFICATION__CREATEDBY", "=" });
			qs.addRelation(subRel);
			qs.addSelector(new QueryColumnSelector("CB__NAME", "notification_createdby"));
		}
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT_NOTIFICATION__RECIPIENTID", "==", memberId));
		qs.addOrderBy("PROJECT_NOTIFICATION__CREATEDAT", "desc");

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
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Update notification actionType
	 * 
	 * @param userId
	 */
	public static void updateActiontypeForUserNotifications(String recipientId) {
		String updateQuery = "UPDATE PROJECT_NOTIFICATION SET ACTIONTYPE = 'NONE' WHERE ACTIONTYPE = 'NEW' AND RECIPIENTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQuery);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, recipientId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * Update the Read notifications for user
	 * 
	 * @param notificationId
	 * @param readAt
	 */
	public static void updateReadNotifications(String notificationId, Timestamp readAt) {
		String query = "UPDATE PROJECT_NOTIFICATION SET ISREAD = TRUE, READAT=? WHERE NOTIFICATIONID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setTimestamp(parameterIndex++, readAt);
			ps.setString(parameterIndex++, notificationId);
			ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
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
		String query = "SELECT COUNT(NOTIFICATIONID) FROM PROJECT_NOTIFICATION WHERE RECIPIENTID = ? AND ACTIONTYPE='NEW'";
		try {
			ps = securityDb.getPreparedStatement(query);
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
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		return 0;
	}
	
	/**
	 * Get userDetails by using user's project access request
	 * 
	 * @param requestId
	 * @return List of user details
	 */
	public static List<Map<String, Object>> getUserDetailsFromProjectAccessRequest(String requestId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__REQUEST_USERID", "userId"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "permission"));
		qs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__PERMISSION", "permission"));
		qs.addRelation("PROJECTACCESSREQUEST__PERMISSION", "PERMISSION__ID", "inner.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTACCESSREQUEST__ID", "==", requestId));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Get the userType by using userId
	 * 
	 * @param userId
	 * @return userType
	 */
	public static String getUserTypeByUserId(String userId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE", "userType"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", userId));
		return QueryExecutionUtility.flushToString(securityDb, qs);
	}
	
}
