package prerna.auth.utils;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;

public class WorkspaceAssetUtils extends AbstractSecurityUtils {

	WorkspaceAssetUtils() {
		super();
	}
	
	/**
	 * Create the default user workspace app for the provided auth token
	 * @param user
	 * @param token
	 */
	public static void createUserWorkspaceApp(User user, AuthProvider token) {
		// look at GenerateEmptyAppReactor
		
	}
	
	/**
	 * Create the default user asset app for the provided auth token
	 * @param user
	 * @param token
	 */
	public static void createUserAssetApp(User user, AuthProvider token) {
		// look at GenerateEmptyAppReactor

	}

	/**
	 * Get the default user workspace app for the provided auth token
	 * @param user
	 * @param token
	 * @return
	 */
	public static String getUserWorkspaceApp(User user, AuthProvider token) {
		String query = "SELECT ENGINEID FROM WORKSPACEENGINE WHERE "
				+ "TYPE = '" + token.toString() + "' AND "
				+ "USERID = '" + user.getAccessToken(token).getId() + "'"
				;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			if(wrapper.hasNext()) {
				return wrapper.next().getValues()[0].toString();
			}
		} finally {
			wrapper.cleanUp();
		}
		return null;
	}
	
	/**
	 * Get the default user asset app for the provided auth token
	 * @param user
	 * @param token
	 * @return
	 */
	public static String getUserAssetApp(User user, AuthProvider token) {
		String query = "SELECT ENGINEID FROM ASSETENGINE WHERE "
				+ "TYPE = '" + token.toString() + "' AND "
				+ "USERID = '" + user.getAccessToken(token).getId() + "'"
				;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			if(wrapper.hasNext()) {
				return wrapper.next().getValues()[0].toString();
			}
		} finally {
			wrapper.cleanUp();
		}
		return null;
	}
	
}
