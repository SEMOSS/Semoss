package prerna.auth.utils;

import java.sql.SQLException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.Constants;

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
			if(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
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
				wrapper.cleanUp();
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
				wrapper.cleanUp();
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
				wrapper.cleanUp();
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
				wrapper.cleanUp();
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
				wrapper.cleanUp();
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
	public static void addInsightGroupPermission(User user, String groupId, String groupType, String projectId, String insightId, String permission) throws IllegalAccessException {
		if(!SecurityInsightUtils.userCanEditInsight(user, projectId, insightId)) {
			throw new IllegalAccessException("Insufficient privileges to modify this insight's permissions.");
		}
		
		if(getGroupInsightPermission(groupId, groupType, projectId, insightId) != null) {
			throw new IllegalArgumentException("This group already has access to this insight. Please edit the existing permission level.");
		}
		
		String query = "INSERT INTO GROUPINSIGHTPERMISSION (ID, TYPE, PROJECTID, INSIGHTID, PERMISSION) VALUES('"
				+ RdbmsQueryBuilder.escapeForSQLStatement(groupId) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(groupType) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "', "
				+ RdbmsQueryBuilder.escapeForSQLStatement(insightId) + "', "
				+ AccessPermissionEnum.getIdByPermission(permission) + ");";
		
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred adding group permissions for this insight", e);
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
				wrapper.cleanUp();
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
	public static void editInsightGroupPermission(User user, String groupId, String groupType, String projectId, String insightId, String newPermission) throws IllegalAccessException {
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
		
		String query = "UPDATE GROUPINSIGHTPERMISSION SET PERMISSION=" + newPermissionLvl
				+ " WHERE ID='" + RdbmsQueryBuilder.escapeForSQLStatement(groupId) + "' "
				+ "AND TYPE='" + RdbmsQueryBuilder.escapeForSQLStatement(groupType) + "' "
				+ "AND PROJECTID='" + RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "' "
				+ "AND INSIGHTID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(insightId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred updating the group permissions for this insight", e);
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
		
		String query = "DELETE FROM GROUPINSIGHTPERMISSION WHERE ID='" 
				+ RdbmsQueryBuilder.escapeForSQLStatement(groupId) + "' "
				+ "AND TYPE='" + RdbmsQueryBuilder.escapeForSQLStatement(groupType) + "' "
				+ "AND PROJECTID='" + RdbmsQueryBuilder.escapeForSQLStatement(groupType) + "' "
				+ "AND INSIGHTID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(insightId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred removing the user permissions for this insight", e);
		}
	}



}
