package prerna.auth.utils;

import java.io.File;
import java.sql.SQLException;
import java.util.UUID;

import org.apache.commons.io.FileUtils;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.cluster.util.CloudClient;
import prerna.cluster.util.ClusterUtil;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.app.AppEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class WorkspaceAssetUtils extends AbstractSecurityUtils {
	
	private static final String FS = System.getProperty("file.separator");
	
	public static final String WORKSPACE_APP_NAME = "Workspace";
	public static final String ASSET_APP_NAME = "Asset";
	public static final String HIDDEN_FILE = ".semoss";
	
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
	 * @throws Exception 
	 */
	public static String createUserWorkspaceApp(AccessToken token) throws Exception {
		String appId = createEmptyApp(token, WORKSPACE_APP_NAME);
		registerUserWorkspaceApp(token, appId);
		return appId;
	}
	
	/**
	 * Create the user workspace app for the provided user and auth token
	 * @param user
	 * @param token
	 * @return
	 * @throws Exception 
	 */
	public static String createUserWorkspaceApp(User user, AuthProvider token) throws Exception {
		return createUserWorkspaceApp(user.getAccessToken(token));
	}
	
	/**
	 * Create the user asset app for the provided access token
	 * @param token
	 * @return
	 * @throws Exception 
	 */
	public static String createUserAssetApp(AccessToken token) throws Exception {
		String appId = createEmptyApp(token, ASSET_APP_NAME);
		registerUserAssetApp(token, appId);
		return appId;
	}
	
	/**
	 * Create the user asset app for the provided user and auth token
	 * @param user
	 * @param token
	 * @return
	 * @throws Exception 
	 */
	public static String createUserAssetApp(User user, AuthProvider token) throws Exception {
		return createUserAssetApp(user.getAccessToken(token));
	}
	
	// TODO >>>timb: WORKSPACE - DONE - look at GenerateEmptyAppReactor, use AppEngine
	private static String createEmptyApp(AccessToken token, String appName) throws Exception {
		
		// Create a new app id
		String appId = UUID.randomUUID().toString();

		// Create the app folder
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String appLocation = baseFolder + FS + "db" + FS + SmssUtilities.getUniqueName(appName, appId);
		File appFolder = new File(appLocation);
		appFolder.mkdirs();
		
		// Create the insights database
		IEngine insightDb = UploadUtilities.generateInsightsDatabase(appId, appName);

		// Add database into DIHelper so that the web watcher doesn't try to load as well
		File tempSmss = UploadUtilities.createTemporaryAppSmss(appId, appName);
		DIHelper.getInstance().getCoreProp().setProperty(appId + "_" + Constants.STORE, tempSmss.getAbsolutePath());
		
		// Add the app to security db
		SecurityUpdateUtils.addApp(appId, !AbstractSecurityUtils.securityEnabled());

		// Create the app engine
		AppEngine appEng = new AppEngine();
		appEng.setEngineId(appId);
		appEng.setEngineName(appName);
		appEng.setInsightDatabase(insightDb);
		
		// Only at end do we add to DIHelper
		DIHelper.getInstance().setLocalProperty(appId, appEng);
		String appNames = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
		appNames = appNames + ";" + appId;
		DIHelper.getInstance().setLocalProperty(Constants.ENGINES, appNames);
		
		// Rename .temp to .smss
		File smssFile = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
		FileUtils.copyFile(tempSmss, smssFile);
		tempSmss.delete();
		
		// Update engine smss file location
		appEng.setPropFile(smssFile.getAbsolutePath());
		DIHelper.getInstance().getCoreProp().setProperty(appId + "_" + Constants.STORE, smssFile.getAbsolutePath());
		DIHelper.getInstance().setLocalProperty(appId, appEng);
		
		// Even if no security, just add user as engine owner
		SecurityUpdateUtils.addEngineOwner(appId, token.getId());
		
		if (ClusterUtil.IS_CLUSTER) {
			CloudClient.getClient().pushApp(appId);
		}
		
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
	 * @throws SQLException 
	 */
	public static void registerUserWorkspaceApp(AccessToken token, String appId) throws SQLException {
		String[] colNames = new String[] {"TYPE", "USERID", "ENGINEID"};
		String[] types = new String[] {"varchar(255)", "varchar(255)", "varchar(255)"};
		String insertQuery = RdbmsQueryBuilder.makeInsert("WORKSPACEENGINE", colNames, types, 
				new String[] {	token.getProvider().name(), 
								token.getId(), 
								appId});
		securityDb.insertData(insertQuery);
		securityDb.commit();
	}
	
	/**
	 * Register the user workspace app for the provided user, auth provider, and app id
	 * @param user
	 * @param provider
	 * @param appId
	 * @throws SQLException 
	 */
	public static void registerUserWorkspaceApp(User user, AuthProvider provider, String appId) throws SQLException {
		registerUserWorkspaceApp(user.getAccessToken(provider), appId);
	}
	
	/**
	 * Register the user asset app for the provided access token and app id
	 * @param token
	 * @param appId
	 * @throws SQLException 
	 */
	public static void registerUserAssetApp(AccessToken token, String appId) throws SQLException {
		String[] colNames = new String[] {"TYPE", "USERID", "ENGINEID"};
		String[] types = new String[] {"varchar(255)", "varchar(255)", "varchar(255)"};
		String insertQuery = RdbmsQueryBuilder.makeInsert("ASSETENGINE", colNames, types, 
				new String[] {	token.getProvider().name(), 
								token.getId(), 
								appId});
		securityDb.insertData(insertQuery);
		securityDb.commit();
	}
	
	/**
	 * Register the user asset app for the provided user, auth provider, and app id
	 * @param user
	 * @param provider
	 * @param appId
	 * @throws SQLException 
	 */
	public static void registerUserAssetApp(User user, AuthProvider provider, String appId) throws SQLException {
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
				 Object rs = wrapper.next().getValues()[0];
				 if (rs == null){
					 return null;
				 }
				return rs.toString();
				//return wrapper.next().getValues()[0].toString();
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
				 Object rs = wrapper.next().getValues()[0];
				 if (rs == null){
					 return null;
				 }
				return rs.toString();
				//return wrapper.next().getValues()[0].toString();
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
