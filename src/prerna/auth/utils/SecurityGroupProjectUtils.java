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
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;

public class SecurityGroupProjectUtils extends AbstractSecurityUtils {

	private static final Logger logger = LogManager.getLogger(SecurityGroupProjectUtils.class);

	/**
	 * Determine if a group can view a project
	 * @param user
	 * @param projectId
	 * @return
	 */
	public static boolean userGroupCanViewProject(User user, String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__ENDDATE"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__ID"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__TYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PERMISSION", "!=", null, PixelDataType.CONST_INT));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		boolean anyUserGroups = false;
		for(AuthProvider login : logins) {
			if(user.getAccessToken(login).getUserGroups().isEmpty()) {
				continue;
			}
			
			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==", user.getAccessToken(login).getUserGroupType()));
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", user.getAccessToken(login).getUserGroups()));
			orFilter.addFilter(andFilter);
		}
		
		if(!anyUserGroups) {
			return false;
		}
		
		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPPROJECTPERMISSION__PERMISSION"));
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
					removeExpiredProjectGroupPermission(groupId, groupType, projectId);
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
	 * Determine if the group can modify the project
	 * @param projectId
	 * @param userId
	 * @return
	 */
	public static boolean userGroupCanEditProject(User user, String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__ENDDATE"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__ID"));
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__TYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PROJECTID", "==", projectId));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		boolean anyUserGroups = false;
		for(AuthProvider login : logins) {
			if(user.getAccessToken(login).getUserGroups().isEmpty()) {
				continue;
			}
			
			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==", user.getAccessToken(login).getUserGroupType()));
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", user.getAccessToken(login).getUserGroups()));
			orFilter.addFilter(andFilter);
		}
		
		if(!anyUserGroups) {
			return false;
		}
		
		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPPROJECTPERMISSION__PERMISSION"));
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
					removeExpiredProjectGroupPermission(groupId, groupType, projectId);
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
	 * Determine if the group is the owner of a project
	 * @param userFilters
	 * @param projectId
	 * @return
	 */
	public static boolean userGroupIsOwner(User user, String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PROJECTID", "==", projectId));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		boolean anyUserGroups = false;
		for(AuthProvider login : logins) {
			if(user.getAccessToken(login).getUserGroups().isEmpty()) {
				continue;
			}
			
			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==", user.getAccessToken(login).getUserGroupType()));
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", user.getAccessToken(login).getUserGroups()));
			orFilter.addFilter(andFilter);
		}
		
		if(!anyUserGroups) {
			return false;
		}
		
		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPPROJECTPERMISSION__PERMISSION"));
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
//	 * Determine if a user can view a project including group permissions
//	 * @param user
//	 * @param projectId
//	 * @return
//	 */
//	public static boolean userCanViewProject(User user, String projectId) {
//		Integer bestUserProjectPermission = getBestProjectPermission(user, projectId);
//		return bestUserProjectPermission != null;
//	}
//
//	/**
//	 * Determine if the user can modify the project including group permissions
//	 * @param projectId
//	 * @param userId
//	 * @return
//	 */
//	public static boolean userCanEditProject(User user, String projectId) {
//		Integer bestUserProjectPermission = getBestProjectPermission(user, projectId);
//		return bestUserProjectPermission != null && AccessPermission.isEditor(bestUserProjectPermission);
//	}
//
//	/**
//	 * Determine if the user is the owner of an project including group permissions
//	 * @param userFilters
//	 * @param projectId
//	 * @return
//	 */
//	public static boolean userIsOwner(User user, String projectId) {
//		Integer bestUserProjectPermission = getBestProjectPermission(user, projectId);
//		return bestUserProjectPermission != null && AccessPermission.isOwner(bestUserProjectPermission);
//	}

	/**
	 * Determine the strongest project permission for the user/group
	 * @param userId
	 * @param projectId
	 * @return
	 */
	public static Integer getBestProjectPermission(User user, String projectId) {
		// get best permission from user
		Integer bestUserProjectPermission = null;

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addOrderBy(new QueryColumnOrderBySelector("PROJECTPERMISSION__PERMISSION"));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val != null) {
					bestUserProjectPermission = ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to retrieve existing project permissions for user", e);
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
		if(bestUserProjectPermission != null && AccessPermissionEnum.isOwner(bestUserProjectPermission)) {
			return bestUserProjectPermission;
		}

		// get best group permission
		Integer bestGroupProjectPermission = null;

		qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ENGINEID", "==", projectId));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		for(AuthProvider login : logins) {
			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==", user.getAccessToken(login).getUserGroupType()));
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", user.getAccessToken(login).getUserGroups()));
			orFilter.addFilter(andFilter);
		}
		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPPROJECTPERMISSION__PERMISSION"));
		wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val != null) {
					bestGroupProjectPermission = ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to retrieve existing project permissions for user", e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		if(bestGroupProjectPermission == null && bestUserProjectPermission == null) {
			if(SecurityProjectUtils.projectIsGlobal(projectId)) {
				return AccessPermissionEnum.READ_ONLY.getId();
			}
			return null;
		} else if(bestGroupProjectPermission == null || bestGroupProjectPermission.compareTo(bestUserProjectPermission) >= 0) {
			return bestUserProjectPermission;
		} else {
			return bestGroupProjectPermission;
		}
	}

	/**
	 * Create a project group permission
	 * @param user
	 * @param groupId
	 * @param groupType
	 * @param projectId
	 * @param permission
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void addProjectGroupPermission(User user, String groupId, String groupType, String projectId, String permission, String endDate) throws IllegalAccessException {
		if(!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}

		if(getGroupProjectPermission(groupId, groupType, projectId) != null) {
			throw new IllegalArgumentException("This group already has access to this project. Please edit the existing permission level.");
		}
		
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);
		
		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}
		
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("INSERT INTO GROUPPROJECTPERMISSION (ID, TYPE, PROJECTID, PERMISSION, DATEADDED, ENDDATE, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE) VALUES(?,?,?,?,?,?,?,?)");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, groupType);
			ps.setString(parameterIndex++, projectId);
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
	 * Get the project permission for a specific group
	 * @param groupId
	 * @param groupType
	 * @param projectId
	 * @return
	 */
	public static Integer getGroupProjectPermission(String groupId, String groupType, String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==", groupType));
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
	 * Modify a group project permission
	 * @param user
	 * @param groupId
	 * @param groupType
	 * @param projectId
	 * @param newPermission
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void editProjectGroupPermission(User user, String groupId, String groupType, String projectId, String newPermission, String endDate) throws IllegalAccessException {
		// make sure user can edit the project
		Integer userPermissionLvl = getBestProjectPermission(user, projectId);
		if(userPermissionLvl == null || !AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}

		// make sure we are trying to edit a permission that exists
		Integer existingGroupPermission = getGroupProjectPermission(groupId, groupType, projectId);
		if(existingGroupPermission == null) {
			throw new IllegalArgumentException("Attempting to modify project permission for a group who does not currently have access to the project");
		}

		int newPermissionLvl = AccessPermissionEnum.getIdByPermission(newPermission);

		// if i am not an owner
		// then i need to check if i can edit this group permission
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermissionEnum.OWNER.getId() == existingGroupPermission) {
				throw new IllegalAccessException("The user doesn't have the high enough permissions to modify this group project permission.");
			}

			// also, cannot give some owner permission if i am just an editor
			if(AccessPermissionEnum.OWNER.getId() == newPermissionLvl) {
				throw new IllegalAccessException("Cannot give owner level access to this project since you are not currently an owner.");
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
			ps = securityDb.getPreparedStatement("UPDATE GROUPPROJECTPERMISSION SET PERMISSION=?, DATEADDED=?, ENDDATE=?, PERMISSIONGRANTEDBY=?, PERMISSIONGRANTEDBYTYPE=? WHERE ID=? AND TYPE=? AND PROJECTID=?");
			int parameterIndex = 1;
			ps.setInt(parameterIndex++, newPermissionLvl);
			ps.setTimestamp(parameterIndex++, startDate);
			ps.setTimestamp(parameterIndex++, verifiedEndDate);
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, groupType);
			ps.setString(parameterIndex++, projectId);
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
	 * Delete a group project permission
	 * @param user
	 * @param groupId
	 * @param groupType
	 * @param projectId
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void removeProjectGroupPermission(User user, String groupId, String groupType, String projectId) throws IllegalAccessException {
		// make sure user can edit the project
		Integer userPermissionLvl = getBestProjectPermission(user, projectId);
		if(userPermissionLvl == null || !AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}

		// make sure we are trying to edit a permission that exists
		Integer existingGroupPermission = getGroupProjectPermission(groupId, groupType, projectId);
		if(existingGroupPermission == null) {
			throw new IllegalArgumentException("Attempting to modify group permission for a user who does not currently have access to the project");
		}

		// if i am not an owner
		// then i need to check if i can remove this group permission
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermissionEnum.OWNER.getId() == existingGroupPermission) {
				throw new IllegalAccessException("The user doesn't have the high enough permissions to modify this group project permission.");
			}
		}
		
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM GROUPPROJECTPERMISSION WHERE ID=? AND TYPE=? AND PROJECTID=?");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, groupType);
			ps.setString(parameterIndex++, projectId);
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
	 * Delete a group project permission
	 * @param groupId
	 * @param groupType
	 * @param projectId
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void removeExpiredProjectGroupPermission(String groupId, String groupType, String projectId) throws IllegalAccessException {

		// make sure we are trying to edit a permission that exists
		Integer existingGroupPermission = getGroupProjectPermission(groupId, groupType, projectId);
		if(existingGroupPermission == null) {
			throw new IllegalArgumentException("Attempting to modify group permission for a user who does not currently have access to the project");
		}
		
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM GROUPPROJECTPERMISSION WHERE ID=? AND TYPE=? AND PROJECTID=?");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, groupType);
			ps.setString(parameterIndex++, projectId);
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
	 * Determine if a group can view a project
	 * @param user
	 * @return
	 */
	public static List<String> getAllUserGroupProjects(User user) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PERMISSION", "!=", null, PixelDataType.CONST_INT));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		for(AuthProvider login : logins) {
			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==", user.getAccessToken(login).getUserGroupType()));
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", user.getAccessToken(login).getUserGroups()));
			orFilter.addFilter(andFilter);
		}
		qs.addExplicitFilter(orFilter);
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}
}
