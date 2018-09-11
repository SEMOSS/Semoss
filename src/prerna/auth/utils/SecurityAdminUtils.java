package prerna.auth.utils;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
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
	public List<Map<String, Object>> getAllUsers(User user) throws IllegalArgumentException{
		String query = "SELECT ID, NAME, USERNAME, EMAIL, TYPE, ADMIN FROM USER";
		return getSimpleQuery(query);
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
		query = query.replace("?1", userToDelete);
		securityDb.execUpdateAndRetrieveStatement(query, true);
		securityDb.commit();
		return true;
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
