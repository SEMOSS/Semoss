package prerna.auth.utils;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
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

public class SecurityGroupEngineUtils extends AbstractSecurityUtils {
	
	private static final Logger logger = LogManager.getLogger(SecurityGroupEngineUtils.class);
	
	/**
	 * Determine if a group can view a database
	 * @param user
	 * @param databaseId
	 * @return
	 */
	public static boolean userGroupCanViewEngine(User user, String engineId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__ENDDATE"));
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__ID"));
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__TYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__PERMISSION", "!=", null, PixelDataType.CONST_INT));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		boolean anyUserGroups = false;
		for(AuthProvider login : logins) {
			if(user.getAccessToken(login).getUserGroups().isEmpty()) {
				continue;
			}
			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==", user.getAccessToken(login).getUserGroupType()));
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==", user.getAccessToken(login).getUserGroups()));
			orFilter.addFilter(andFilter);
		}
		
		if(!anyUserGroups) {
			return false;
		}
		
		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPENGINEPERMISSION__PERMISSION"));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				Object val = values[0];
				// check if permission is expired
				SemossDate endDate = (SemossDate) values[1];
				if (AbstractSecurityUtils.endDateIsExpired(endDate)) {
					// need to delete expired permission
					String groupId = (String) values[2];
					String groupType = (String) values[3];
					removeExpiredEngineGroupPermission(groupId, groupType, engineId);
					continue;
				}
				if(val != null) {
					// actually do not care what the value is - we have a record so that means we can at least view
					return true;
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to retrieve existing group database permissions for user", e);
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
	 * Determine if the group can modify the database
	 * @param databaseId
	 * @param userId
	 * @return
	 */
	public static boolean userGroupCanEditEngine(User user, String engineId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__ENDDATE"));
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__ID"));
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__TYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ENGINEID", "==", engineId));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		boolean anyUserGroups = false;
		for(AuthProvider login : logins) {
			if(user.getAccessToken(login).getUserGroups().isEmpty()) {
				continue;
			}
			
			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==", user.getAccessToken(login).getUserGroupType()));
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==", user.getAccessToken(login).getUserGroups()));
			orFilter.addFilter(andFilter);
		}
		
		if(!anyUserGroups) {
			return false;
		}
		
		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPENGINEPERMISSION__PERMISSION"));
		IRawSelectWrapper wrapper = null;
		Integer bestGroupDatabasePermission = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while (wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				Object val = values[0];
				SemossDate endDate = (SemossDate) values[1];
				if (AbstractSecurityUtils.endDateIsExpired(endDate)) {
					// Need to delete expired permission here
					String permissionId = (String) values[2];
					String permissionType = (String) values[3];
					removeExpiredEngineGroupPermission(permissionId, permissionType, engineId);
					continue;
				}
				if(val != null) {
					bestGroupDatabasePermission  = ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to retrieve existing group database permissions for user", e);
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
	 * Determine if the group is the owner of a database
	 * @param userFilters
	 * @param databaseId
	 * @return
	 */
	public static boolean userGroupIsOwner(User user, String databaseId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ENGINEID", "==", databaseId));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		boolean anyUserGroups = false;
		for(AuthProvider login : logins) {
			if(user.getAccessToken(login).getUserGroups().isEmpty()) {
				continue;
			}
			
			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==", user.getAccessToken(login).getUserGroupType()));
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==", user.getAccessToken(login).getUserGroups()));
			orFilter.addFilter(andFilter);
		}
		
		if(!anyUserGroups) {
			return false;
		}
		
		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPENGINEPERMISSION__PERMISSION"));
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
			throw new IllegalArgumentException("Failed to retrieve existing group database permissions for user", e);
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
//	 * Determine if a user can view a database including group permissions
//	 * @param user
//	 * @param databaseId
//	 * @return
//	 */
//	public static boolean userCanViewDatabase(User user, String databaseId) {
//		Integer bestUserDatabasePermission = getBestDatabasePermission(user, databaseId);
//		return bestUserDatabasePermission != null;
//	}
//	
//	/**
//	 * Determine if the user can modify the database including group permissions
//	 * @param databaseId
//	 * @param userId
//	 * @return
//	 */
//	public static boolean userCanEditDatabase(User user, String databaseId) {
//		Integer bestUserDatabasePermission = getBestDatabasePermission(user, databaseId);
//		return bestUserDatabasePermission != null && AccessPermission.isEditor(bestUserDatabasePermission);
//	}
//	
//	/**
//	 * Determine if the user is the owner of an database including group permissions
//	 * @param userFilters
//	 * @param databaseId
//	 * @return
//	 */
//	public static boolean userIsOwner(User user, String databaseId) {
//		Integer bestUserDatabasePermission = getBestDatabasePermission(user, databaseId);
//		return bestUserDatabasePermission != null && AccessPermission.isOwner(bestUserDatabasePermission);
//	}
	
	/**
	 * Determine the strongest database permission for the user/group
	 * @param userId
	 * @param databaseId
	 * @return
	 */
	public static Integer getBestDatabasePermission(User user, String databaseId) {
		// get best permission from user
		Integer bestUserDatabasePermission = null;
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", databaseId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addOrderBy(new QueryColumnOrderBySelector("ENGINEPERMISSION__PERMISSION"));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val != null) {
					bestUserDatabasePermission = ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to retrieve existing database permissions for user", e);
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
		if(bestUserDatabasePermission != null && AccessPermissionEnum.isOwner(bestUserDatabasePermission)) {
			return bestUserDatabasePermission;
		}
		
		// get best group permission
		Integer bestGroupDatabasePermission = null;
		
		qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ENGINEID", "==", databaseId));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		for(AuthProvider login : logins) {
			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==", user.getAccessToken(login).getUserGroupType()));
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==", user.getAccessToken(login).getUserGroups()));
			orFilter.addFilter(andFilter);
		}
		qs.addExplicitFilter(orFilter);
		qs.addOrderBy(new QueryColumnOrderBySelector("GROUPENGINEPERMISSION__PERMISSION"));
		wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val != null) {
					bestGroupDatabasePermission = ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to retrieve existing database permissions for user", e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		if(bestGroupDatabasePermission == null && bestUserDatabasePermission == null) {
			if(SecurityEngineUtils.engineIsGlobal(databaseId)) {
				return AccessPermissionEnum.READ_ONLY.getId();
			}
			return null;
		} else if(bestGroupDatabasePermission == null || bestGroupDatabasePermission.compareTo(bestUserDatabasePermission) >= 0) {
			return bestUserDatabasePermission;
		} else {
			return bestGroupDatabasePermission;
		}
	}
	
	/**
	 * Create a database group permission
	 * @param user
	 * @param groupId
	 * @param groupType
	 * @param engineId
	 * @param permission
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void addEngineGroupPermission(User user, String groupId, String groupType, String engineId, String permission, String endDate) throws IllegalAccessException {
		if(!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalAccessException("Insufficient privileges to modify this engine's permissions.");
		}
		
		if(getGroupDatabasePermission(groupId, groupType, engineId) != null) {
			throw new IllegalArgumentException("This group already has access to this engine. Please edit the existing permission level.");
		}
		
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);
		
		Timestamp startDate = AbstractSecurityUtils.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}
		
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("INSERT INTO GROUPENGINEPERMISSION (ID, TYPE, ENGINEID, PERMISSION, DATEADDED, ENDDATE, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE) VALUES(?,?,?,?,?,?,?,?)");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, groupType);
			ps.setString(parameterIndex++, engineId);
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
	 * Get the database permission for a specific group
	 * @param groupId
	 * @param groupType
	 * @param databaseId
	 * @return
	 */
	public static Integer getGroupDatabasePermission(String groupId, String groupType, String databaseId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ENGINEID", "==", databaseId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==", groupType));
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
	 * Modify a group database permission
	 * @param user
	 * @param groupId
	 * @param groupType
	 * @param databaseId
	 * @param newPermission
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void editDatabaseGroupPermission(User user, String groupId, String groupType, String engineId, String newPermission, String endDate) throws IllegalAccessException {
		// make sure user can edit the database
		Integer userPermissionLvl = getBestDatabasePermission(user, engineId);
		if(userPermissionLvl == null || !AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this database's permissions.");
		}
		
		// make sure we are trying to edit a permission that exists
		Integer existingGroupPermission = getGroupDatabasePermission(groupId, groupType, engineId);
		if(existingGroupPermission == null) {
			throw new IllegalArgumentException("Attempting to modify database permission for a group who does not currently have access to the database");
		}
		
		int newPermissionLvl = AccessPermissionEnum.getIdByPermission(newPermission);
		
		// if i am not an owner
		// then i need to check if i can edit this group permission
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermissionEnum.OWNER.getId() == existingGroupPermission) {
				throw new IllegalAccessException("The user doesn't have the high enough permissions to modify this group database permission.");
			}
			
			// also, cannot give some owner permission if i am just an editor
			if(AccessPermissionEnum.OWNER.getId() == newPermissionLvl) {
				throw new IllegalAccessException("Cannot give owner level access to this database since you are not currently an owner.");
			}
		}
		
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);
		
		Timestamp startDate = AbstractSecurityUtils.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}
		
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE GROUPENGINEPERMISSION SET PERMISSION=?, DATEADDED=?, ENDDATE=?, PERMISSIONGRANTEDBY=?, PERMISSIONGRANTEDBYTYPE=? WHERE ID=? AND TYPE=? AND ENGINEID=?");
			int parameterIndex = 1;
			ps.setInt(parameterIndex++, newPermissionLvl);
			ps.setTimestamp(parameterIndex++, startDate);
			ps.setTimestamp(parameterIndex++, verifiedEndDate);
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, groupType);
			ps.setString(parameterIndex++, engineId);
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
	 * Delete a group database permission
	 * @param user
	 * @param groupId
	 * @param groupType
	 * @param databaseId
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void removeDatabaseGroupPermission(User user, String groupId, String groupType, String engineId) throws IllegalAccessException {
		// make sure user can edit the database
		Integer userPermissionLvl = getBestDatabasePermission(user, engineId);
		if(userPermissionLvl == null || !AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this database's permissions.");
		}
		
		// make sure we are trying to edit a permission that exists
		Integer existingGroupPermission = getGroupDatabasePermission(groupId, groupType, engineId);
		if(existingGroupPermission == null) {
			throw new IllegalArgumentException("Attempting to modify group permission for a user who does not currently have access to the database");
		}
		
		// if i am not an owner
		// then i need to check if i can remove this group permission
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermissionEnum.OWNER.getId() == existingGroupPermission) {
				throw new IllegalAccessException("The user doesn't have the high enough permissions to modify this group database permission.");
			}
		}
		
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM GROUPENGINEPERMISSION WHERE ID=? AND TYPE=? AND ENGINEID=?");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, groupType);
			ps.setString(parameterIndex++, engineId);
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
	 * Delete a group database permission
	 * @param user
	 * @param groupId
	 * @param groupType
	 * @param engineId
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void removeExpiredEngineGroupPermission(String groupId, String groupType, String engineId) throws IllegalAccessException {
		
		// make sure we are trying to edit a permission that exists
		Integer existingGroupPermission = getGroupDatabasePermission(groupId, groupType, engineId);
		if(existingGroupPermission == null) {
			throw new IllegalArgumentException("Attempting to modify group permission for a user who does not currently have access to the database");
		}
		
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM GROUPENGINEPERMISSION WHERE ID=? AND TYPE=? AND ENGINEID=?");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, groupType);
			ps.setString(parameterIndex++, engineId);
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
	 * Determine if a group can view a database
	 * @param user
	 * @return
	 */
	public static List<String> getAllUserGroupDatabases(User user) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__PERMISSION", "!=", null, PixelDataType.CONST_INT));
		OrQueryFilter orFilter = new OrQueryFilter();
		List<AuthProvider> logins = user.getLogins();
		for(AuthProvider login : logins) {
			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==", user.getAccessToken(login).getUserGroupType()));
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==", user.getAccessToken(login).getUserGroups()));
			orFilter.addFilter(andFilter);
		}
		qs.addExplicitFilter(orFilter);
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}
}
