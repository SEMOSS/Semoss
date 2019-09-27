package prerna.util;

import java.io.File;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.om.Insight;
import prerna.util.git.GitRepoUtils;
import prerna.util.git.GitUtils;

public class AssetUtility {

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public static String USER_SPACE_KEY = "USER";
	public static String INSIGHT_SPACE_KEY = "INSIGHT";

	/**
	 * Grab the workspace to work with asset files
	 * 
	 * APP-ID: db/app/version/assets 
	 * USER: db/userApp/version/assets 
	 * INSIGHT: db/app/version/insightID
	 * 
	 * @param in
	 * @param space
	 * @return
	 */
	public static String getAssetBasePath(Insight in, String space, boolean editRequired) {
		String assetFolder = in.getInsightFolder();
		// find out what space the user wants to use to get the base asset path
		if (space != null) {
			if (USER_SPACE_KEY.equalsIgnoreCase(space)) {
				if (AbstractSecurityUtils.securityEnabled()) {
					User user = in.getUser();
					if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
						throw new IllegalArgumentException("Must be logged in to perform this operation");
					}
					AuthProvider provider = user.getPrimaryLogin();
					String appId = user.getAssetEngineId(provider);
					String appName = "Asset";
					assetFolder = getAppAssetFolder(appName, appId);
				}
			} else if (INSIGHT_SPACE_KEY.equalsIgnoreCase(space)) {
				// default
				// but need to perform check
				if(editRequired && !SecurityInsightUtils.userCanEditInsight(in.getUser(), in.getEngineId(), in.getRdbmsId())) {
					throw new IllegalArgumentException("User does not have permission for this insight");
				}
			} else {
				// user has passed an id
				String appId = space;
				// check if the user has permission for the app
				if (AbstractSecurityUtils.securityEnabled()) {
					if(editRequired) {
						if(!SecurityAppUtils.userCanEditEngine(in.getUser(), space)) {
							throw new IllegalArgumentException("User does not have permission for this app");
						}
					} else {
						// only read access
						if(!SecurityAppUtils.userCanViewEngine(in.getUser(), space)) {
							throw new IllegalArgumentException("User does not have permission for this app");
						}
					}
				}
				IEngine engine = Utility.getEngine(appId);
				String appName = engine.getEngineName();
				assetFolder = getAppAssetFolder(appName, appId);
			}
		}
		assetFolder = assetFolder.replace('\\', '/');
		return assetFolder;
	}
	
	/**
	 * Grab the git version base path
	 * 
	 * @param in
	 * @param space
	 * @return
	 */
	public static String getAssetVersionBasePath(Insight in, String space) {
		String assetFolder = getAppAssetVersionFolder(in.getEngineName(), in.getEngineId());
		// find out what space the user wants to use to get the base asset path
		if (space != null) {
			if (USER_SPACE_KEY.equalsIgnoreCase(space)) {
				if (AbstractSecurityUtils.securityEnabled()) {
					User user = in.getUser();
					if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
						throw new IllegalArgumentException("Must be logged in to perform this operation");
					}
					AuthProvider provider = user.getPrimaryLogin();
					String appId = user.getAssetEngineId(provider);
					String appName = "Asset";
					assetFolder = getAppAssetVersionFolder(appName, appId);
				}
			} else if (INSIGHT_SPACE_KEY.equalsIgnoreCase(space)) {
				// default
			} else {
				// user has passed an id
				String appId = space;
				String appName = MasterDatabaseUtility.getEngineAliasForId(appId);
				// check if the user has permission for the app
				if (AbstractSecurityUtils.securityEnabled()) {
					if (!SecurityAppUtils.userCanEditEngine(in.getUser(), space)) {
						throw new IllegalArgumentException("User does not have permission for this app");
					}
				}
				assetFolder = getAppAssetVersionFolder(appName, appId);
			}
		}
		assetFolder = assetFolder.replace('\\', '/');
		
		
		if(in.isSavedInsight() && !GitUtils.isGit(assetFolder)) {
			GitRepoUtils.init(assetFolder);
		}
		return assetFolder;
	}
	
	public static String getAppAssetFolder(String appName, String appId) {
		String appFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "db"
				+ DIR_SEPARATOR + SmssUtilities.getUniqueName(appName, appId) + DIR_SEPARATOR + "version"
				+ DIR_SEPARATOR + "assets";

		// if this folder does not exist create it
		File file = new File(appFolder);
		if (!file.exists()) {
			file.mkdir();
		}
		return appFolder;
		
	}
	
	public static String getAppAssetVersionFolder(String appName, String appId) {
		String gitFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "db"
				+ DIR_SEPARATOR + SmssUtilities.getUniqueName(appName, appId) + DIR_SEPARATOR + "version";

		// if this folder does not exist create it
		File file = new File(gitFolder);
		if (!file.exists()) {
			file.mkdir();
		}
		if(!GitUtils.isGit(gitFolder)) {
			GitRepoUtils.init(gitFolder);
		}
		return gitFolder;
		
	}
	
	public static String getInsightFolder(String appName, String appId, String rdbmsID) {
		String insightFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "db"
				+ DIR_SEPARATOR + SmssUtilities.getUniqueName(appName, appId) + DIR_SEPARATOR + "version" + DIR_SEPARATOR + rdbmsID;

		// if this folder does not exist create it
		File file = new File(insightFolder);
		if (!file.exists()) {
			file.mkdir();
		}

		return insightFolder;
		
	}
	
	public static String getAssetRelativePath(Insight in, String space) {
		String relativePath = "";
		if(space == null || space.equals(INSIGHT_SPACE_KEY)) {
			relativePath = in.getRdbmsId();
		} else {
			// user space or asset app
			relativePath = "assets";
		}	
		return relativePath;
	}
	
}
