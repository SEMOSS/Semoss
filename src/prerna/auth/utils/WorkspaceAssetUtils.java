package prerna.auth.utils;

import java.sql.SQLException;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.PixelRunner;

public class WorkspaceAssetUtils extends AbstractSecurityUtils {
	
	private static final String WORKSPACE_APP_NAME = "Workspace";
	private static final String ASSET_APP_NAME = "Asset";
	
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
		String appId = createEmptyApp(token, WORKSPACE_APP_NAME); 
		registerUserWorkspaceApp(token, appId);
		return appId;
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
		String appId = createEmptyApp(token, ASSET_APP_NAME); 
		registerUserAssetApp(token, appId);
		return appId;
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
	
	// TODO >>>timb: WORKSPACE - DONE - look at GenerateEmptyAppReactor, use AppEngine
	private static String createEmptyApp(AccessToken token, String appName) {
		
		// Make sure the app is created with the proper access token
		Insight insight = new Insight();
		User user = new User();
		user.setAccessToken(token);
		insight.setUser(user);
		
		// Run the pixel to import the data
		PixelRunner returnData = insight.runPixel("GenerateEmptyAppReactor(app=[\" + appName + \"])");
		
		@SuppressWarnings("unchecked")
		String appId = ((Map<String, String>) returnData.getResults().get(0).getValue()).get("app_id");
		return appId;
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
