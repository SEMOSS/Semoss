package prerna.util;

import java.io.File;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.engine.impl.SmssUtilities;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.om.Insight;
import prerna.util.git.GitRepoUtils;

public class AssetUtility {
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	/**
	 * Grab the workspace to work with assets
	 * 
	 * @param in
	 * @param space
	 * @return
	 */
	public static String getAssetBasePath(Insight in, String space) {
		String assetFolder = in.getInsightFolder();
		// find out what space the user wants to use to get the base asset path
		if (space != null) {
			if ("USER".equalsIgnoreCase(space)) {
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
			} else if ("INSIGHT".equalsIgnoreCase(space)) {
				assetFolder = in.getInsightFolder();
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
				assetFolder = getAppAssetFolder(appName, appId);
			}
		}
		assetFolder = assetFolder.replaceAll("\\\\", "/");
		return assetFolder;
	}
	
	public static String getAppAssetFolder(String appName, String appId) {
		String appFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "db"
				+ DIR_SEPARATOR + SmssUtilities.getUniqueName(appName, appId) + DIR_SEPARATOR + "version"
				+ DIR_SEPARATOR + "assets";

		// if this folder does not exist create it and git init it
		File file = new File(appFolder);
		if (!file.exists()) {
			file.mkdir();
			GitRepoUtils.init(appFolder);
		}
		return appFolder;
		
	}
}
