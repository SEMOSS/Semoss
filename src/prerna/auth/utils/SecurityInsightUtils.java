package prerna.auth.utils;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import prerna.auth.AccessPermission;
import prerna.auth.User;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;

public class SecurityInsightUtils extends AbstractSecurityUtils {

	/**
	 * Get what permission the user has for a given insight
	 * @param userId
	 * @param engineId
	 * @param insightId
	 * @return
	 */
	public static String getUserInsightPermission(User user, String engineId, String insightId) {
		String userFilters = getUserFilters(user);

		// query the database
		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION  "
				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID IN " + userFilters;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			if(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val != null && val instanceof Number) {
					return AccessPermission.getPermissionValueById( ((Number) val).intValue() );
				}
			}
		} finally {
			wrapper.cleanUp();
		}
		
		if(SecurityQueryUtils.insightIsGlobal(engineId, insightId)) {
			return AccessPermission.READ_ONLY.getPermission();
		}
		
		return null;
	}
	
	private static Integer getUserInsightPermission(String singleUserId, String engineId, String insightId) {
		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION  "
				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID='" + singleUserId + "'";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			if(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val != null && val instanceof Number) {
					return ((Number) val).intValue();
				}
			}
		} finally {
			wrapper.cleanUp();
		}
		
		if(SecurityQueryUtils.insightIsGlobal(engineId, insightId)) {
			return AccessPermission.READ_ONLY.getId();
		}
		
		return null;
	}
	
	/**
	 * Determine if the user can edit the insight
	 * User must be database owner OR be given explicit permissions on the insight
	 * @param userId
	 * @param engineId
	 * @param insightId
	 * @return
	 */
	public static boolean userCanViewInsight(User user, String engineId, String insightId) {
		String userFilters = getUserFilters(user);

		// if user is owner
		// they can do whatever they want
		if(SecurityQueryUtils.userIsOwner(userFilters, engineId)) {
			return true;
		}
		
		//TODO: add this back when we have the UI to set insights to be not global
		if(SecurityQueryUtils.insightIsGlobal(engineId, insightId)) {
			return true;
		}
		
		// else query the database
		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION  "
				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID IN " + userFilters;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			if(wrapper.hasNext()) {
				// do not care if owner/edit/read
				return true;
			}
		} finally {
			wrapper.cleanUp();
		}
		
		return false;
	}
	
	/**
	 * Determine if the user can edit the insight
	 * User must be database owner OR be given explicit permissions on the insight
	 * @param userId
	 * @param engineId
	 * @param insightId
	 * @return
	 */
	public static boolean userCanEditInsight(User user, String engineId, String insightId) {
		String userFilters = getUserFilters(user);

		// if user is owner
		// they can do whatever they want
		if(SecurityQueryUtils.userIsOwner(userFilters, engineId)) {
			return true;
		}
		
		// else query the database
		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION "
				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID IN " + userFilters;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			while(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val == null) {
					return false;
				}
				int permission = ((Number) val).intValue();
				if(AccessPermission.isEditor(permission)) {
					return true;
				}
			}
		} finally {
			wrapper.cleanUp();
		}		
		return false;
	}
	
	/**
	 * Determine if the user can edit the insight
	 * User must be database owner OR be given explicit permissions on the insight
	 * @param userId
	 * @param engineId
	 * @param insightId
	 * @return
	 */
	public static int getMaxUserInsightPermission(User user, String engineId, String insightId) {
		String userFilters = getUserFilters(user);

		// if user is owner of the app
		// they can do whatever they want
		if(SecurityQueryUtils.userIsOwner(userFilters, engineId)) {
			// owner of engine is owner of all the insights
			return AccessPermission.OWNER.getId();
		}
		
		// else query the database
		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION "
				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID IN " + userFilters + " ORDER BY PERMISSION";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			while(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val == null) {
					return AccessPermission.READ_ONLY.getId();
				}
				int permission = ((Number) val).intValue();
				return permission;
			}
		} finally {
			wrapper.cleanUp();
		}		
		return AccessPermission.READ_ONLY.getId();
	}

	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Query for insight users
	 */
	
	/**
	 * Retrieve the list of users for a given insight
	 * @param user
	 * @param appId
	 * @param insightId
	 * @return
	 * @throws IllegalAccessException
	 */
	public static List<Map<String, Object>> getInsightUsers(User user, String appId, String insightId) throws IllegalAccessException {
		if(!userCanViewInsight(user, appId, insightId)) {
			throw new IllegalArgumentException("The user does not have access to view this insight");
		}
		
		String query = "SELECT USER.ID AS \"id\", "
				+ "USER.NAME AS \"name\", "
				+ "PERMISSION.NAME AS \"permission\" "
				+ "FROM USER "
				+ "INNER JOIN USERINSIGHTPERMISSION ON (USER.ID = USERINSIGHTPERMISSION.USERID) "
				+ "INNER JOIN PERMISSION ON (USERINSIGHTPERMISSION.PERMISSION = PERMISSION.ID) "
				+ "WHERE USERINSIGHTPERMISSION.ENGINEID='" + appId + "'"
				+ " AND USERINSIGHTPERMISSION.INSIGHTID='" + insightId + "'"
				;
		
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushRsToMap(wrapper);
	}
	
	/**
	 * 
	 * @param user
	 * @param newUserId
	 * @param engineId
	 * @param insightId
	 * @param permission
	 * @return
	 */
	public static void addInsightUser(User user, String newUserId, String engineId, String insightId, String permission) {
		if(!userCanEditInsight(user, engineId, insightId)) {
			throw new IllegalArgumentException("The user doesn't have the permission to modify the insight permissions.");
		}
		
		// make sure user doesn't already exist for this insight
		if(getUserInsightPermission(newUserId, engineId, insightId) != null) {
			// that means there is already a value
			throw new IllegalArgumentException("This user already has access to this insight. Please edit the existing permission level.");
		}
		
		String query = "INSERT INTO USERINSIGHTPERMISSION (USERID, ENGINEID, INSIGHTID, PERMISSION) VALUES('"
				+ RdbmsQueryBuilder.escapeForSQLStatement(newUserId) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(engineId) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(insightId) + "', "
				+ RdbmsQueryBuilder.escapeForSQLStatement(permission) + ");";

		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("An error occured adding user permissions for this insight");
		}
	}
	
	/**
	 * 
	 * @param user
	 * @param existingUserId
	 * @param engineId
	 * @param insightId
	 * @param newPermission
	 * @return
	 */
	public static void editInsightUserPermission(User user, String existingUserId, String engineId, String insightId, String newPermission) {
		// make sure user can edit the insight
		int userPermissionLvl = getMaxUserInsightPermission(user, engineId, insightId);
		if(!AccessPermission.isEditor(userPermissionLvl)) {
			throw new IllegalArgumentException("The user doesn't have the permission to modify the insight permissions.");
		}
		
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = getUserInsightPermission(existingUserId, engineId, insightId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the insight");
		}
		
		int newPermissionLvl = AccessPermission.getIdByPermission(newPermission);
		
		// if i am not an owner
		// then i need to check if i can edit this users permission
		if(!AccessPermission.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermission.OWNER.getId() == existingUserPermission) {
				throw new IllegalArgumentException("The user doesn't have the high enough permissions to modify this users insight permission.");
			}
			
			// also, cannot give some owner permission if i am just an editor
			if(AccessPermission.OWNER.getId() == newPermissionLvl) {
				throw new IllegalArgumentException("Cannot give owner level access to this insight since you are not currently an owner.");
			}
		}
		
		String query = "UPDATE USERINSIGHTPERMISSION SET PERMISSION=" + newPermissionLvl
				+ " WHERE USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND ENGINEID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(engineId) + "' "
				+ "AND INSIGHTID='" + RdbmsQueryBuilder.escapeForSQLStatement(insightId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("An error occured adding user permissions for this insight");
		}
	}
	
	/**
	 * 
	 * @param user
	 * @param editedUserId
	 * @param engineId
	 * @param insightId
	 * @return
	 */
	public static void removeInsightUser(User user, String existingUserId, String engineId, String insightId) {
		// make sure user can edit the insight
		int userPermissionLvl = getMaxUserInsightPermission(user, engineId, insightId);
		if(!AccessPermission.isEditor(userPermissionLvl)) {
			throw new IllegalArgumentException("The user doesn't have the permission to modify the insight permissions.");
		}
		
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = getUserInsightPermission(existingUserId, engineId, insightId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the insight");
		}
		
		// if i am not an owner
		// then i need to check if i can remove this users permission
		if(!AccessPermission.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermission.OWNER.getId() == existingUserPermission) {
				throw new IllegalArgumentException("The user doesn't have the high enough permissions to modify this users insight permission.");
			}
		}
		
		String query = "DELETE FROM USERINSIGHTPERMISSION WHERE "
				+ "USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND ENGINEID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(engineId) + "' "
				+ "AND INSIGHTID='" + RdbmsQueryBuilder.escapeForSQLStatement(insightId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("An error occured adding user permissions for this insight");
		}
	}
	
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Querying for insight lists
	 */
	
	/**
	 * User has access to specific insights within a database
	 * User can access if:
	 * 	1) Is Owner, Editer, or Reader of insight
	 * 	2) Insight is global
	 * 	3) Is Owner of database
	 * 
	 * @param engineId
	 * @param userId
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 */
	public static List<Map<String, Object>> searchUserInsights(User user, List<String> engineFilter, String searchTerm, String limit, String offset) {
		String userFilters = getUserFilters(user);

		String query = "SELECT DISTINCT "
				+ "INSIGHT.ENGINEID AS \"app_id\", "
				+ "ENGINE.ENGINENAME AS \"app_name\", "
				+ "INSIGHT.INSIGHTID as \"app_insight_id\", "
				+ "INSIGHT.INSIGHTNAME as \"name\", "
				+ "INSIGHT.LAYOUT as \"layout\", "
				+ "INSIGHT.CREATEDON as \"created_on\", "
				+ "INSIGHT.LASTMODIFIEDON as \"last_modified_on\", "
				+ "INSIGHT.CACHEABLE as \"cacheable\", "
				+ "INSIGHT.GLOBAL as \"insight_global\", "
				+ "LOWER(INSIGHT.INSIGHTNAME) as \"low_name\" "
				+ "FROM INSIGHT "
				+ "INNER JOIN ENGINE ON ENGINE.ENGINEID=INSIGHT.ENGINEID "
				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
				+ "LEFT JOIN USERINSIGHTPERMISSION ON USERINSIGHTPERMISSION.INSIGHTID=INSIGHT.INSIGHTID "
				+ "WHERE "
				+ "INSIGHT.ENGINEID " + createFilter(engineFilter)+ " "
				+ " AND (USERINSIGHTPERMISSION.USERID IN " + userFilters + " OR INSIGHT.GLOBAL=TRUE OR "
						+ "(ENGINEPERMISSION.PERMISSION=1 AND ENGINEPERMISSION.USERID IN " + userFilters + ") ) "
				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "AND REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ escapeRegexCharacters(searchTerm) + "', 'i')" : "")
				+ "ORDER BY LOWER(INSIGHT.INSIGHTNAME), \"last_modified_on\" DESC "
				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
				;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushRsToMap(wrapper);
	}
	
	public static List<Map<String, Object>> searchInsights(List<String> eFilters, String searchTerm, String limit, String offset) {
		String query = "SELECT DISTINCT "
				+ "INSIGHT.ENGINEID AS \"app_id\", "
				+ "ENGINE.ENGINENAME AS \"app_name\", "
				+ "INSIGHT.INSIGHTID as \"app_insight_id\", "
				+ "INSIGHT.INSIGHTNAME as \"name\", "
				+ "INSIGHT.LAYOUT as \"layout\", "
				+ "INSIGHT.CREATEDON as \"created_on\", "
				+ "INSIGHT.LASTMODIFIEDON as \"last_modified_on\", "
				+ "INSIGHT.CACHEABLE as \"cacheable\", "
				+ "INSIGHT.GLOBAL as \"insight_global\", "
				+ "LOWER(INSIGHT.INSIGHTNAME) as \"low_name\" "
				+ "FROM INSIGHT "
				+ "INNER JOIN ENGINE ON ENGINE.ENGINEID=INSIGHT.ENGINEID "
				+ "WHERE "
				+ "INSIGHT.ENGINEID " + createFilter(eFilters) + " "
				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "AND REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ escapeRegexCharacters(searchTerm) + "', 'i')" : "")
				+ "ORDER BY LOWER(INSIGHT.INSIGHTNAME), \"last_modified_on\" DESC "
				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
				;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushRsToMap(wrapper);
	}
	
	//////////////////////////////////////////////////////////////////
	
	/*
	 * For autocompletion of user searching
	 */
	
	/**
	 * User will see specific insight predictions for their searches
	 * User can see records if:
	 * 	1) Is Owner, Editer, or Reader of insight
	 * 	2) Insight is global
	 * 	3) Is Owner of database
	 * 
	 * @param userId
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 */
	public static List<String> predictUserInsightSearch(User user, String searchTerm, String limit, String offset) {
		String userFilters = getUserFilters(user);

		String query = "SELECT DISTINCT "
				+ "INSIGHT.INSIGHTNAME as \"name\", "
				+ "LOWER(INSIGHT.INSIGHTNAME) as \"low_name\" "
				+ "FROM INSIGHT "
				+ "LEFT JOIN ENGINEPERMISSION ON INSIGHT.ENGINEID=ENGINEPERMISSION.ENGINEID "
				+ "LEFT JOIN USERINSIGHTPERMISSION ON USERINSIGHTPERMISSION.ENGINEID=INSIGHT.ENGINEID "
				+ "WHERE "
				+ "(USERINSIGHTPERMISSION.USERID IN " + userFilters + " OR INSIGHT.GLOBAL=TRUE OR "
						+ "(ENGINEPERMISSION.PERMISSION=1 AND ENGINEPERMISSION.USERID IN " + userFilters + ") ) "
				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "AND REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ escapeRegexCharacters(searchTerm) + "', 'i')" : "")
				+ "ORDER BY LOWER(INSIGHT.INSIGHTNAME) "
				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
				;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushToListString(wrapper);
	}
	
	public static List<String> predictInsightSearch(String searchTerm, String limit, String offset) {
		String query = "SELECT DISTINCT "
				+ "INSIGHT.INSIGHTNAME as \"name\", "
				+ "LOWER(INSIGHT.INSIGHTNAME) as \"low_name\" "
				+ "FROM INSIGHT "
				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "WHERE REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ escapeRegexCharacters(searchTerm) + "', 'i')" : "")
				+ "ORDER BY LOWER(INSIGHT.INSIGHTNAME) "
				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
				;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushToListString(wrapper);
	}
	
	/*
	 * For searching in search bar
	 */
	
	/**
	 * 
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @param sortOrder
	 * @param sortField
	 * @param engineFilter
	 * @return
	 */
	public static List<Map<String, Object>> searchUserInsightDataByName(User user, String searchTerm, String limit, String offset) {
		String userFilters = getUserFilters(user);

		String query = "SELECT DISTINCT "
				+ "INSIGHT.ENGINEID AS \"app_id\", "
				+ "ENGINE.ENGINENAME AS \"app_name\", "
				+ "INSIGHT.INSIGHTID as \"app_insight_id\", "
				+ "INSIGHT.INSIGHTNAME as \"name\", "
				+ "INSIGHT.EXECUTIONCOUNT as \"view_count\", "
				+ "INSIGHT.LAYOUT as \"layout\", "
				+ "INSIGHT.CREATEDON as \"created_on\", "
				+ "INSIGHT.LASTMODIFIEDON as \"last_modified_on\", "
				+ "INSIGHT.CACHEABLE as \"cacheable\", "
				+ "INSIGHT.GLOBAL as \"insight_global\", "
				+ "LOWER(INSIGHT.INSIGHTNAME) AS \"low_name\" "
				+ "FROM INSIGHT "
				+ "INNER JOIN ENGINE ON ENGINE.ENGINEID=INSIGHT.ENGINEID "
				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
				+ "LEFT JOIN USER ON ENGINEPERMISSION.USERID=USER.ID "
				+ "LEFT JOIN USERINSIGHTPERMISSION ON USER.ID=USERINSIGHTPERMISSION.USERID "
				+ "WHERE "
				// engine is visible to me
				+ "( ENGINE.GLOBAL=TRUE "
				+ "OR ENGINEPERMISSION.USERID IN " + userFilters + " ) "
				+ "AND ENGINE.ENGINEID NOT IN (SELECT ENGINEID FROM ENGINEPERMISSION WHERE VISIBILITY=FALSE AND USERID IN " + userFilters + " "
				// have access to insight
				+ "AND (USERINSIGHTPERMISSION.USERID IN " + userFilters + " OR INSIGHT.GLOBAL=TRUE OR "
						// if i own this, i dont care what permissions you want to give me + i want to see this engine
						+ "(ENGINEPERMISSION.PERMISSION=1 AND ENGINEPERMISSION.USERID IN " + userFilters + " AND ENGINEPERMISSION.VISIBILITY=TRUE) )) "
				// and match what i search
				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "AND REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ escapeRegexCharacters(searchTerm) + "', 'i') " : "")
				+ "ORDER BY LOWER(INSIGHT.INSIGHTNAME), \"last_modified_on\" DESC "
				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
				;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushRsToMap(wrapper);
	}
	
	/**
	 * 
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @param sortOrder
	 * @param sortField
	 * @param engineFilter
	 * @return
	 */
	public static List<Map<String, Object>> searchAllInsightDataByName(String searchTerm, String limit, String offset) {
		String query = "SELECT DISTINCT "
				+ "INSIGHT.ENGINEID AS \"app_id\", "
				+ "ENGINE.ENGINENAME AS \"app_name\", "
				+ "INSIGHT.INSIGHTID as \"app_insight_id\", "
				+ "INSIGHT.INSIGHTNAME as \"name\", "
				+ "INSIGHT.EXECUTIONCOUNT as \"view_count\", "
				+ "INSIGHT.LAYOUT as \"layout\", "
				+ "INSIGHT.CREATEDON as \"created_on\", "
				+ "INSIGHT.LASTMODIFIEDON as \"last_modified_on\", "
				+ "INSIGHT.CACHEABLE as \"cacheable\", "
				+ "INSIGHT.GLOBAL as \"insight_global\", "
				+ "LOWER(INSIGHT.INSIGHTNAME) AS \"low_name\" "
				+ "FROM INSIGHT INNER JOIN ENGINE ON ENGINE.ENGINEID=INSIGHT.ENGINEID "
				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "WHERE REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ escapeRegexCharacters(searchTerm) + "', 'i')" : "")
				+ "ORDER BY LOWER(INSIGHT.INSIGHTNAME), \"last_modified_on\" DESC "
				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
				;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushRsToMap(wrapper);
	}
	
	/**
	 * 
	 * @param searchTerm
	 * @param engineFilter
	 * @return
	 */
	public static List<Map<String, Object>> getInsightFacetDataByName(String searchTerm, String[] engineFilter) {
		String filter = createFilter(engineFilter); 
		String query = "SELECT DISTINCT "
				+ "ENGINEID, "
				+ "LAYOUT, "
				+ "COUNT(ENGINEID) "
				+ "FROM INSIGHT LEFT JOIN ENGINE ON INSIGHT.ENGINEID=ENGINE.ENGINEID "
				+ "WHERE "
				+ "REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ escapeRegexCharacters(searchTerm) + "', 'i') " 
				+ "AND (INSIGHT.ENGINEID " + filter + " OR ENGINE.GLOBAL=TRUE) "
				+ "GROUP BY LAYOUT, ENGINEID;";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushRsToMap(wrapper);
	}


	
}
