package prerna.auth.utils;

import java.sql.SQLException;

import prerna.auth.AccessPermission;
import prerna.auth.User;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;

public class SecurityAppUtils extends AbstractSecurityUtils {

	/**
	 * Get what permission the user has for a given insight
	 * @param userId
	 * @param engineId
	 * @param insightId
	 * @return
	 */
	public static String getActualUserAppPermission(User user, String engineId) {
		String userFilters = getUserFilters(user);

		String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM ENGINEPERMISSION "
				+ "WHERE ENGINEID='" + engineId + "' AND USERID IN " + userFilters;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		while(wrapper.hasNext()) {
			Object val = wrapper.next().getValues()[0];
			if(val != null) {
				int permission = ((Number) val).intValue();
				return AccessPermission.getPermissionValueById(permission);
			}
		}
		
		// see if engine is public
		if(appIsGlobal(engineId)) {
			return AccessPermission.READ_ONLY.getPermission();
		}
				
		return null;
	}
	
	public static Integer getUserAppPermission(String singleUserId, String engineId) {
		String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM ENGINEPERMISSION  "
				+ "WHERE ENGINEID='" + engineId + "' AND USERID='" + singleUserId + "'";
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
		
		return null;
	}
	
	/**
	 * Get global engines
	 * @return
	 */
	public static boolean appIsGlobal(String engineId) {
		String query = "SELECT ENGINEID FROM ENGINE WHERE GLOBAL=TRUE and ENGINEID='" + engineId + "'";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			if(wrapper.hasNext()) {
				return true;
			}
		} finally {
			wrapper.cleanUp();
		}
		return false;
	}
	
	/**
	 * Determine if the user is the owner
	 * @param user
	 * @param engineId
	 * @return
	 */
	public static boolean userIsOwner(User user, String engineId) {
		String userFilters = getUserFilters(user);
		return userIsOwner(userFilters, engineId);
	}
	
	/**
	 * Determine if the user is the owner
	 * @param userFilters
	 * @param engineId
	 * @return
	 */
	static boolean userIsOwner(String userFilters, String engineId) {
		String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM ENGINEPERMISSION "
				+ "WHERE ENGINEID='" + engineId + "' AND USERID IN " + userFilters;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		while(wrapper.hasNext()) {
			Object val = wrapper.next().getValues()[0];
			if(val == null) {
				return false;
			}
			int permission = ((Number) val).intValue();
			if(AccessPermission.isOwner(permission)) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Determine if a user can view an engine
	 * @param user
	 * @param engineId
	 * @return
	 */
	public static boolean userCanViewEngine(User user, String engineId) {
		String userFilters = getUserFilters(user);
		String query = "SELECT * "
				+ "FROM ENGINE "
				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
				+ "WHERE ("
				+ "ENGINE.GLOBAL=TRUE "
				+ "OR ENGINEPERMISSION.USERID IN " + userFilters + ") AND ENGINE.ENGINEID='" + engineId + "'"
				;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			// if you are here, you can view
			while(wrapper.hasNext()) {
				return true;
			} 
		} finally {
			wrapper.cleanUp();
		}
		return false;
	}
	
	/**
	 * Determine if the user can modify the database
	 * @param engineId
	 * @param userId
	 * @return
	 */
	public static boolean userCanEditEngine(User user, String engineId) {
		String userFilters = getUserFilters(user);
		String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM ENGINEPERMISSION "
				+ "WHERE ENGINEID='" + engineId + "' AND USERID IN " + userFilters;
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
	 * Determine if the user can edit the app
	 * @param userId
	 * @param engineId
	 * @return
	 */
	static int getMaxUserAppPermission(User user, String engineId) {
		String userFilters = getUserFilters(user);

		// query the database
		String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM ENGINEPERMISSION "
				+ "WHERE ENGINEID='" + engineId + "' AND USERID IN " + userFilters + " ORDER BY PERMISSION";
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
	 * Query for app users
	 */
	
	
	/**
	 * 
	 * @param user
	 * @param newUserId
	 * @param engineId
	 * @param permission
	 * @return
	 */
	public static void addAppUser(User user, String newUserId, String engineId, String permission) {
		if(!userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException("Insufficient privileges to modify this app's permissions.");
		}
		
		// make sure user doesn't already exist for this insight
		if(getUserAppPermission(newUserId, engineId) != null) {
			// that means there is already a value
			throw new IllegalArgumentException("This user already has access to this app. Please edit the existing permission level.");
		}
		
		String query = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, VISIBILITY, PERMISSION) VALUES('"
				+ RdbmsQueryBuilder.escapeForSQLStatement(newUserId) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(engineId) + "', "
				+ "TRUE, "
				+ AccessPermission.getIdByPermission(permission) + ");";

		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("An error occured adding user permissions for this APP");
		}
	}
	
	/**
	 * 
	 * @param user
	 * @param existingUserId
	 * @param engineId
	 * @param newPermission
	 * @return
	 */
	public static void editAppUserPermission(User user, String existingUserId, String engineId, String newPermission) {
		// make sure user can edit the app
		int userPermissionLvl = getMaxUserAppPermission(user, engineId);
		if(!AccessPermission.isEditor(userPermissionLvl)) {
			throw new IllegalArgumentException("Insufficient privileges to modify this app's permissions.");
		}
		
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = getUserAppPermission(existingUserId, engineId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify app permission for a user who does not currently have access to the app");
		}
		
		int newPermissionLvl = AccessPermission.getIdByPermission(newPermission);
		
		// if i am not an owner
		// then i need to check if i can edit this users permission
		if(!AccessPermission.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermission.OWNER.getId() == existingUserPermission) {
				throw new IllegalArgumentException("The user doesn't have the high enough permissions to modify this users app permission.");
			}
			
			// also, cannot give some owner permission if i am just an editor
			if(AccessPermission.OWNER.getId() == newPermissionLvl) {
				throw new IllegalArgumentException("Cannot give owner level access to this insight since you are not currently an owner.");
			}
		}
		
		String query = "UPDATE ENGINEPERMISSION SET PERMISSION=" + newPermissionLvl
				+ " WHERE USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND ENGINEID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(engineId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("An error occured updating the user permissions for this insight");
		}
	}
	
	/**
	 * 
	 * @param user
	 * @param editedUserId
	 * @param engineId
	 * @return
	 */
	public static void removeAppUser(User user, String existingUserId, String engineId) {
		// make sure user can edit the app
		int userPermissionLvl = getMaxUserAppPermission(user, engineId);
		if(!AccessPermission.isEditor(userPermissionLvl)) {
			throw new IllegalArgumentException("Insufficient privileges to modify this app's permissions.");
		}
		
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = getUserAppPermission(existingUserId, engineId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the app");
		}
		
		// if i am not an owner
		// then i need to check if i can remove this users permission
		if(!AccessPermission.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermission.OWNER.getId() == existingUserPermission) {
				throw new IllegalArgumentException("The user doesn't have the high enough permissions to modify this users app permission.");
			}
		}
		
		String query = "DELETE FROM ENGINEPERMISSION WHERE "
				+ "USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND ENGINEID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(engineId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("An error occured removing the user permissions for this app");
		}
	}
	
}
