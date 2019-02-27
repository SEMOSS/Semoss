package prerna.auth.utils;

import prerna.auth.AccessPermission;
import prerna.auth.User;
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
	

	
}
