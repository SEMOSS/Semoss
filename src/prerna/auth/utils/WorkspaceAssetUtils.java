package prerna.auth.utils;

import java.sql.SQLException;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;

public class WorkspaceAssetUtils extends AbstractSecurityUtils {
	
	WorkspaceAssetUtils() {
		super();
	}
	
	
	//////////////////////////////////////////////////////////////////////
	// Creating workspace and asset metadata 
	//////////////////////////////////////////////////////////////////////
	
	/**
	 * Create the user workspace app for the provided access token
	 * @param token
	 * @return
	 */
	public static String createUserWorkspaceApp(AccessToken token) {
		String appId = null;
		// TODO >>>timb: WORKSPACE - look at GenerateEmptyAppReactor, use AppEngine
		registerUserWorkspaceApp(token, appId);
		return null;
	}
	
	/**
	 * Create the user workspace app for the provided user and auth token
	 * @param user
	 * @param token
	 * @return
	 */
	public static String createUserWorkspaceApp(User user, AuthProvider token) {
		return createUserWorkspaceApp(user.getAccessToken(token));
	}
	
	/**
	 * Create the user asset app for the provided access token
	 * @param token
	 * @return
	 */
	public static String createUserAssetApp(AccessToken token) {
		String appId = null;
		// TODO >>>timb: WORKSPACE - look at GenerateEmptyAppReactor, use AppEngine
		registerUserAssetApp(token, appId);
		return null;
	}
	
	/**
	 * Create the user asset app for the provided user and auth token
	 * @param user
	 * @param token
	 * @return
	 */
	public static String createUserAssetApp(User user, AuthProvider token) {
		return createUserAssetApp(user.getAccessToken(token));
	}
	
	
	//////////////////////////////////////////////////////////////////////
	// Updating workspace and asset metadata 
	//////////////////////////////////////////////////////////////////////
	// TODO >>>timb: WORKSPACE - DONE - register workspace

	/**
	 * Register the user workspace app for the provided access token and app id
	 * @param token
	 * @param appId
	 */
	public static void registerUserWorkspaceApp(AccessToken token, String appId) {
		String[] colNames = new String[] {"TYPE", "USERID", "ENGINEID"};
		String[] types = new String[] {"varchar(255)", "varchar(255)", "varchar(255)"};
		String insertQuery = RdbmsQueryBuilder.makeInsert("WORKSPACEENGINE", colNames, types, 
				new String[] {	token.getProvider().name(), 
								token.getId(), 
								appId});
		try {
			securityDb.insertData(insertQuery);
			securityDb.commit();
		} catch (SQLException e) {
			// TODO >>>timb: WORKSPACE - How to deal with this exception properly?
			e.printStackTrace();
		}
	}
	
	/**
	 * Register the user workspace app for the provided user, auth provider, and app id
	 * @param user
	 * @param provider
	 * @param appId
	 */
	public static void registerUserWorkspaceApp(User user, AuthProvider provider, String appId) {
		registerUserWorkspaceApp(user.getAccessToken(provider), appId);
	}
	
	/**
	 * Register the user asset app for the provided access token and app id
	 * @param token
	 * @param appId
	 */
	public static void registerUserAssetApp(AccessToken token, String appId) {
		String[] colNames = new String[] {"TYPE", "USERID", "ENGINEID"};
		String[] types = new String[] {"varchar(255)", "varchar(255)", "varchar(255)"};
		String insertQuery = RdbmsQueryBuilder.makeInsert("ASSETENGINE", colNames, types, 
				new String[] {	token.getProvider().name(), 
								token.getId(), 
								appId});
		try {
			securityDb.insertData(insertQuery);
			securityDb.commit();
		} catch (SQLException e) {
			// TODO >>>timb: WORKSPACE - How to deal with this exception properly?
			e.printStackTrace();
		}
	}
	
	/**
	 * Register the user asset app for the provided user, auth provider, and app id
	 * @param user
	 * @param provider
	 * @param appId
	 */
	public static void registerUserAssetApp(User user, AuthProvider provider, String appId) {
		registerUserAssetApp(user.getAccessToken(provider), appId);
	}

	
	//////////////////////////////////////////////////////////////////////
	// Querying workspace and asset metadata 
	//////////////////////////////////////////////////////////////////////
	
	/**
	 * Get the user workspace app for the provided access token; returns null if there is none
	 * @param token
	 * @return
	 */
	public static String getUserWorkspaceApp(AccessToken token) {
		String query = "SELECT ENGINEID FROM WORKSPACEENGINE WHERE "
				+ "TYPE = '" + token.getProvider().name() + "' AND "
				+ "USERID = '" + token.getId() + "'"
				;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			if (wrapper.hasNext()) {
				return wrapper.next().getValues()[0].toString();
			}
		} finally {
			wrapper.cleanUp();
		}
		return null;
	}
	
	/**
	 * Get the user workspace app for the provided user and auth provider; returns null if is there is none
	 * @param user
	 * @param provider
	 * @return
	 */
	public static String getUserWorkspaceApp(User user, AuthProvider provider) {
		return getUserWorkspaceApp(user.getAccessToken(provider));
	}
	
	/**
	 * Get the user asset app for the provided access token; returns null if there is none
	 * @param user
	 * @param token
	 * @return
	 */
	public static String getUserAssetApp(AccessToken token) {
		String query = "SELECT ENGINEID FROM ASSETENGINE WHERE "
				+ "TYPE = '" + token.getProvider().name() + "' AND "
				+ "USERID = '" + token.getId() + "'"
				;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			if (wrapper.hasNext()) {
				return wrapper.next().getValues()[0].toString();
			}
		} finally {
			wrapper.cleanUp();
		}
		return null;
	}
	
	/**
	 * Get the user asset app for the provided user and auth provider; returns null if there is none
	 * @param user
	 * @param provider
	 * @return
	 */
	public static String getUserAssetApp(User user, AuthProvider provider) {
		return getUserAssetApp(user.getAccessToken(provider));
	}
	
}
