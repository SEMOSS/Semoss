package prerna.auth.utils;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;

public class SecurityGroupInsightsUtils extends AbstractSecurityUtils {
	
	private static final Logger logger = LogManager.getLogger(SecurityGroupInsightsUtils.class);
	
	/**
	 * Determine if group can view insight
	 * @param user
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static boolean userGroupCanViewInsight(User user, String projectId, String insightId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__ENDDATE"));
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__ID"));
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__TYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__PERMISSION", "!=", null, PixelDataType.CONST_INT));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		boolean anyUserGroups = false;
		for(AuthProvider login : logins) {
			if(user.getAccessToken(login).getUserGroups().isEmpty()) {
				continue;
			}
			
			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__TYPE", "==", user.getAccessToken(login).getUserGroupType()));
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__ID", "==", user.getAccessToken(login).getUserGroups()));
			orFilter.addFilter(andFilter);
		}
		
		if(!anyUserGroups) {
			return false;
		}
		
		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPINSIGHTPERMISSION__PERMISSION"));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				Object val = values[0];
				SemossDate endDate = (SemossDate) values[1];
				if (AbstractSecurityUtils.endDateIsExpired(endDate)) {
					// Need to delete expired permission here
					String groupId = (String) values[2];
					String groupType = (String) values[3];
					removeExpiredInsightGroupPermission(groupId, groupType, projectId, insightId);
					continue;
				}
				if(val != null) {
					// actually do not care what the value is - we have a record so that means we can at least view
					return true;
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to retrieve existing group project permissions for user", e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return false;
	}

	/**
	 * Determine if group can edit insight
	 * @param user
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static boolean userGroupCanEditInsight(User user, String projectId, String insightId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__ENDDATE"));
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__ID"));
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__TYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__PROJECTID", "==", projectId));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		boolean anyUserGroups = false;
		for(AuthProvider login : logins) {
			if(user.getAccessToken(login).getUserGroups().isEmpty()) {
				continue;
			}
			
			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__TYPE", "==", user.getAccessToken(login).getUserGroupType()));
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__ID", "==", user.getAccessToken(login).getUserGroups()));
			orFilter.addFilter(andFilter);
		}
		
		if(!anyUserGroups) {
			return false;
		}
		
		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPINSIGHTPERMISSION__PERMISSION"));
		IRawSelectWrapper wrapper = null;
		Integer bestGroupDatabasePermission = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				Object val = values[0];
				SemossDate endDate = (SemossDate) values[1];
				if (AbstractSecurityUtils.endDateIsExpired(endDate)) {
					// Need to delete expired permission here
					String groupId = (String) values[2];
					String groupType = (String) values[3];
					removeExpiredInsightGroupPermission(groupId, groupType, projectId, insightId);
					continue;
				}
				if(val != null) {
					bestGroupDatabasePermission  = ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to retrieve existing group project permissions for user", e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		if(bestGroupDatabasePermission != null) {
			return AccessPermissionEnum.isEditor(bestGroupDatabasePermission);
		}

		return false;
	}

	/**
	 * Determine if group is owner of insight
	 * @param user
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static boolean userGroupIsOwner(User user, String projectId, String insightId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__PROJECTID", "==", projectId));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		boolean anyUserGroups = false;
		for(AuthProvider login : logins) {
			if(user.getAccessToken(login).getUserGroups().isEmpty()) {
				continue;
			}
			
			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__TYPE", "==", user.getAccessToken(login).getUserGroupType()));
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__ID", "==", user.getAccessToken(login).getUserGroups()));
			orFilter.addFilter(andFilter);
		}
		
		if(!anyUserGroups) {
			return false;
		}
		
		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPINSIGHTPERMISSION__PERMISSION"));
		IRawSelectWrapper wrapper = null;
		Integer bestGroupDatabasePermission = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val != null) {
					bestGroupDatabasePermission  = ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to retrieve existing group project permissions for user", e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		if(bestGroupDatabasePermission != null) {
			return AccessPermissionEnum.isOwner(bestGroupDatabasePermission);
		}

		return false;
	}
	
//	/**
//	 * Determine if a user can view a insight including group permissions
//	 * @param user
//	 * @param insightId
//	 * @return
//	 */
//	public static boolean userCanViewInsight(User user, String projectId, String insightId) {
//		Integer bestUserInsightPermission = getBestInsightPermission(user, projectId, insightId);
//		return bestUserInsightPermission != null;
//	}
//	
//	/**
//	 * Determine if the user can modify the insight including group permissions
//	 * @param insightId
//	 * @param userId
//	 * @return
//	 */
//	public static boolean userCanEditInsight(User user, String projectId, String insightId) {
//		Integer bestUserInsightPermission = getBestInsightPermission(user, projectId, insightId);
//		return bestUserInsightPermission != null && AccessPermission.isEditor(bestUserInsightPermission);
//	}
//	
//	/**
//	 * Determine if the user is the owner of an insight including group permissions
//	 * @param userFilters
//	 * @param insightId
//	 * @return
//	 */
//	public static boolean userIsOwner(User user, String projectId, String insightId) {
//		Integer bestUserInsightPermission = getBestInsightPermission(user, projectId, insightId);
//		return bestUserInsightPermission != null && AccessPermission.isOwner(bestUserInsightPermission);
//	}
	
	/**
	 * Determine the strongest insight permission for the user/group
	 * @param userId
	 * @param insightId
	 * @return
	 */
	public static Integer getBestInsightPermission(User user, String projectId, String insightId) {
		// get best permission from user
		Integer bestUserInsightPermission = null;
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addOrderBy(new QueryColumnOrderBySelector("USERINSIGHTPERMISSION__PERMISSION"));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val != null) {
					bestUserInsightPermission = ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to retrieve existing insight permissions for user", e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}		
		
		// if they are the owner based on user, then skip the group check
		if(bestUserInsightPermission != null && AccessPermissionEnum.isOwner(bestUserInsightPermission)) {
			return bestUserInsightPermission;
		}
		
		// get best group permission
		Integer bestGroupInsightPermission = null;
		
		qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		for(AuthProvider login : logins) {
			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__TYPE", "==", user.getAccessToken(login).getUserGroupType()));
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__ID", "==", user.getAccessToken(login).getUserGroups()));
			orFilter.addFilter(andFilter);
		}
		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPINSIGHTPERMISSION__PERMISSION"));
		wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val != null) {
					bestGroupInsightPermission = ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to retrieve existing insight permissions for user", e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		if(bestGroupInsightPermission == null && bestUserInsightPermission == null) {
			if(SecurityInsightUtils.insightIsGlobal(projectId, insightId)) {
				return AccessPermissionEnum.READ_ONLY.getId();
			}
			return null;
		} else if(bestGroupInsightPermission == null || bestGroupInsightPermission.compareTo(bestUserInsightPermission) >= 0) {
			return bestUserInsightPermission;
		} else {
			return bestGroupInsightPermission;
		}
	}
	
	/**
	 * Create a insight group permission
	 * @param user
	 * @param groupId
	 * @param groupType
	 * @param insightId
	 * @param permission
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void addInsightGroupPermission(User user, String groupId, String groupType, String projectId, String insightId, String permission, String endDate) throws IllegalAccessException {
		if(!SecurityInsightUtils.userCanEditInsight(user, projectId, insightId)) {
			throw new IllegalAccessException("Insufficient privileges to modify this insight's permissions.");
		}
		
		if(getGroupInsightPermission(groupId, groupType, projectId, insightId) != null) {
			throw new IllegalArgumentException("This group already has access to this insight. Please edit the existing permission level.");
		}
		
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);
		
		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}
		
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("INSERT INTO GROUPINSIGHTPERMISSION (ID, TYPE, PROJECTID, INSIGHTID, PERMISSION, DATEADDED, ENDDATE, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE) VALUES(?,?,?,?,?,?,?,?,?)");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, groupType);
			ps.setString(parameterIndex++, projectId);
			ps.setString(parameterIndex++, insightId);
			ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(permission));
			ps.setTimestamp(parameterIndex++, startDate);
			ps.setTimestamp(parameterIndex++, verifiedEndDate);
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * Get the insight permission for a specific group
	 * @param groupId
	 * @param groupType
	 * @param insightId
	 * @return
	 */
	public static Integer getGroupInsightPermission(String groupId, String groupType, String projectId, String insightId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__ID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPINSIGHTPERMISSION__TYPE", "==", groupType));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val != null && val instanceof Number) {
					return ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return null;
	}
	
	/**
	 * Modify a group insight permission
	 * @param user
	 * @param groupId
	 * @param groupType
	 * @param insightId
	 * @param newPermission
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void editInsightGroupPermission(User user, String groupId, String groupType, String projectId, String insightId, String newPermission, String endDate) throws IllegalAccessException {
		// make sure user can edit the insight
		Integer userPermissionLvl = getBestInsightPermission(user, projectId, insightId);
		if(userPermissionLvl == null || !AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this insight's permissions.");
		}
		
		// make sure we are trying to edit a permission that exists
		Integer existingGroupPermission = getGroupInsightPermission(groupId, groupType, projectId, insightId);
		if(existingGroupPermission == null) {
			throw new IllegalArgumentException("Attempting to modify insight permission for a group who does not currently have access to the insight");
		}
		
		int newPermissionLvl = AccessPermissionEnum.getIdByPermission(newPermission);
		
		// if i am not an owner
		// then i need to check if i can edit this group permission
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermissionEnum.OWNER.getId() == existingGroupPermission) {
				throw new IllegalAccessException("The user doesn't have the high enough permissions to modify this group insight permission.");
			}
			
			// also, cannot give some owner permission if i am just an editor
			if(AccessPermissionEnum.OWNER.getId() == newPermissionLvl) {
				throw new IllegalAccessException("Cannot give owner level access to this insight since you are not currently an owner.");
			}
		}
		
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);
		
		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}
		
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE GROUPINSIGHTPERMISSION SET PERMISSION=?, DATEADDED=?, ENDDATE=?, PERMISSIONGRANTEDBY=?, PERMISSIONGRANTEDBYTYPE=? WHERE ID=? AND TYPE=? AND PROJECTID=? AND INSIGHTID=?");
			int parameterIndex = 1;
			ps.setInt(parameterIndex++, newPermissionLvl);
			ps.setTimestamp(parameterIndex++, startDate);
			ps.setTimestamp(parameterIndex++, verifiedEndDate);
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, groupType);
			ps.setString(parameterIndex++, projectId);
			ps.setString(parameterIndex++, insightId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * Delete a group insight permission
	 * @param user
	 * @param groupId
	 * @param groupType
	 * @param insightId
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void removeInsightGroupPermission(User user, String groupId, String groupType, String projectId, String insightId) throws IllegalAccessException {
		// make sure user can edit the insight
		Integer userPermissionLvl = getBestInsightPermission(user, projectId, insightId);
		if(userPermissionLvl == null || !AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this insight's permissions.");
		}
		
		// make sure we are trying to edit a permission that exists
		Integer existingGroupPermission = getGroupInsightPermission(groupId, groupType, projectId, insightId);
		if(existingGroupPermission == null) {
			throw new IllegalArgumentException("Attempting to modify group permission for a user who does not currently have access to the insight");
		}
		
		// if i am not an owner
		// then i need to check if i can remove this group permission
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermissionEnum.OWNER.getId() == existingGroupPermission) {
				throw new IllegalAccessException("The user doesn't have the high enough permissions to modify this group insight permission.");
			}
		}
		
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM GROUPINSIGHTPERMISSION WHERE ID=? AND TYPE=? AND PROJECTID=? AND INSIGHTID=?");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, groupType);
			ps.setString(parameterIndex++, projectId);
			ps.setString(parameterIndex++, insightId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * Delete a group insight permission
	 * @param groupId
	 * @param groupType
	 * @param insightId
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void removeExpiredInsightGroupPermission(String groupId, String groupType, String projectId, String insightId) throws IllegalAccessException {
		
		// make sure we are trying to edit a permission that exists
		Integer existingGroupPermission = getGroupInsightPermission(groupId, groupType, projectId, insightId);
		if(existingGroupPermission == null) {
			throw new IllegalArgumentException("Attempting to modify group permission for a user who does not currently have access to the insight");
		}
		
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM GROUPINSIGHTPERMISSION WHERE ID=? AND TYPE=? AND PROJECTID=? AND INSIGHTID=?");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, groupType);
			ps.setString(parameterIndex++, projectId);
			ps.setString(parameterIndex++, insightId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}



}
