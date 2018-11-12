package prerna.auth.utils;

import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;

public class SecurityAdminUtils extends AbstractSecurityUtils {

	private static SecurityAdminUtils instance = new SecurityAdminUtils();
	
	private SecurityAdminUtils() {
		
	}
	
	public static SecurityAdminUtils getInstance(User user) {
		if(user == null) {
			return null;
		}
		if(userIsAdmin(user)) {
			return instance;
		}
		return null;
	}

	/**
	 * Check if the user is an admin
	 * @param userId	String representing the id of the user to check
	 */
	public static Boolean userIsAdmin(User user) {
		String userFilters = getUserFilters(user);
		String query = "SELECT * FROM USER WHERE ADMIN=TRUE AND ID IN " + userFilters + " LIMIT 1;";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			return wrapper.hasNext();
		} finally {
			wrapper.cleanUp();
		}
	}
	
	/*
	 * all other methods should be on the instance
	 * so that we cannot bypass security easily
	 */
	
	/**
	 * Get all database users
	 * @param User
	 * @return
	 * @throws IllegalArgumentException
	 */
	public List<Map<String, Object>> getAllUsers() throws IllegalArgumentException{
		String query = "SELECT ID, NAME, USERNAME, EMAIL, TYPE, ADMIN FROM USER";
		return getSimpleQuery(query);
	}

	/**
	 * Update user information.
	 * @param adminId
	 * @param userInfo
	 * @return
	 * @throws IllegalArgumentException
	 */
	public boolean editUser(Map<String, Object> userInfo) {
		String userId = userInfo.remove("id").toString();
		if(userId == null || userId.toString().isEmpty()) {
			throw new IllegalArgumentException("Must define which user we are editing");
		}
		String name = userInfo.get("name") != null ? userInfo.get("name").toString() : "";
		String email = userInfo.get("email") != null ? userInfo.get("email").toString() : "";
		String password = userInfo.get("password") != null ? userInfo.get("password").toString() : "";
		
		// cannot edit a user to match another user when native... would cause some serious issues :/
		boolean isNative = SecurityQueryUtils.isUserType(userId, AuthProvider.NATIVE);
		if(isNative && SecurityQueryUtils.checkUserExist(name, email)){
			throw new IllegalArgumentException("The user name or email already exist");
		}
		String error = "";
		if(email != null && !email.isEmpty()){
			error = validEmail(email);
		}
		if(password != null && !password.isEmpty()){
            error += validPassword(password);
            if(error.isEmpty()){
                String newSalt = SecurityQueryUtils.generateSalt();
                userInfo.put("password", SecurityQueryUtils.hash(password, newSalt));
                userInfo.put("salt", newSalt);
            }
        }
		if(error != null && !error.isEmpty()) {
			throw new IllegalArgumentException(error);
		}
		
		boolean first = true;
		StringBuilder query = new StringBuilder("UPDATE USER SET ");
		Set<String> keys = userInfo.keySet();
		for(String k : keys) {
			Object value = userInfo.get(k);
			if(value == null || value.toString().isEmpty()) {
				continue;
			}
			if(!first) {
				query.append(", ");
			}
			query.append(k).append(" = '").append(RdbmsQueryBuilder.escapeForSQLStatement(value.toString())).append("'");
			first = false;
		}
		query.append(" WHERE ID='").append(userId).append("'");
		Statement stmt = securityDb.execUpdateAndRetrieveStatement(query.toString(), true);
		if(stmt != null){
			securityDb.commit();
			return true;
		}
		
		return false;
    }
	
	/**
	 * Delete a user and all its relationships.
	 * @param userId
	 * @param userDelete
	 */
	public boolean deleteUser(String userToDelete) {
		List<String> groups = SecurityQueryUtils.getGroupsOwnedByUser(userToDelete);
		for(String groupId : groups){
			removeGroup(userToDelete, groupId);
		}
		String query = "DELETE FROM ENGINEPERMISSION WHERE USERID = '?1'; DELETE FROM GROUPMEMBERS WHERE USERID = '?1'; DELETE FROM USER WHERE ID = '?1';";
		query = query.replace("?1", RdbmsQueryBuilder.escapeForSQLStatement(userToDelete));
		securityDb.execUpdateAndRetrieveStatement(query, true);
		securityDb.commit();
		return true;
	}
	
	/**
	 * Set if engine should be public or not
	 * @param engineId
	 * @param isPublic
	 */
	public boolean setDbGlobal(String engineId, boolean isPublic) {
		String query = "UPDATE ENGINE SET GLOBAL = " + isPublic + " WHERE ENGINEID ='" + engineId + "';";
		securityDb.execUpdateAndRetrieveStatement(query, true);
		securityDb.commit();
		return true;
	}
	
	/**
	 * Get all databases options
	 * @param usersId
	 * @param isAdmin
	 * @return
	 */
	public List<Map<String, Object>> getAllUserDatabaseSettings() {
		String query = "SELECT DISTINCT "
				+ "ENGINE.ENGINEID as \"app_id\", "
				+ "ENGINE.ENGINENAME as \"app_name\", "
				+ "LOWER(ENGINE.ENGINENAME) as \"low_app_name\", "
				+ "ENGINE.GLOBAL as \"app_global\" "
				+ "FROM ENGINE "
				+ "ORDER BY LOWER(ENGINE.ENGINENAME)";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushRsToMap(wrapper);
	}
	
	/**
	 * Remove a group
	 * @param userId
	 * @param groupId
	 * @return
	 */
	private Boolean removeGroup(String userId, String groupId) {
		String query = "DELETE FROM GROUPENGINEPERMISSION WHERE GROUPENGINEPERMISSION.GROUPID IN (SELECT USERGROUP.GROUPID FROM USERGROUP WHERE USERGROUP.GROUPID='" + groupId + "'); "
				+ "DELETE FROM GROUPMEMBERS WHERE GROUPMEMBERS.GROUPID IN (SELECT USERGROUP.GROUPID FROM USERGROUP WHERE USERGROUP.GROUPID='" + groupId + "'); "
				+ "DELETE FROM USERGROUP WHERE USERGROUP.GROUPID='" + groupId + "';";
		securityDb.execUpdateAndRetrieveStatement(query, true);
		securityDb.commit();
		return true;
	}

}
