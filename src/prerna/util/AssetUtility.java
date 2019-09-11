package prerna.util;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.om.Insight;

public class AssetUtility {

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
				// TODO
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
				assetFolder = in.getAppFolder(appName, appId);
			}
		}
		assetFolder = assetFolder.replaceAll("\\\\", "/");
		return assetFolder;
	}
}
