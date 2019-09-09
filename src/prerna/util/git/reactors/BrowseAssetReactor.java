package prerna.util.git.reactors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.git.GitAssetUtils;

public class BrowseAssetReactor extends AbstractReactor {

	// pulls the latest for this project / asset
	// the asset is basically the folder where it sits
	// this can be used enroute in a pipeline

	public BrowseAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), "app" };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		
		// check if user is logged in
		User user = this.insight.getUser();
		if (AbstractSecurityUtils.securityEnabled()) {
			if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
				throwAnonymousUserError();
			}
		}

		// base asset folder path
		
		boolean app = keyValue.containsKey(keysToGet[1]) || (keyValue.containsKey(keysToGet[0]) && keyValue.get(keysToGet[0]).startsWith("app_assets"));
		String assetFolder = this.insight.getInsightFolder();
		String replacer = "";
		if(app)
		{
			assetFolder = this.insight.getAppFolder();
			replacer = "app_assets/";
			replacer = "";
		}
		assetFolder = assetFolder.replaceAll("\\\\", "/");

		// specific folder to browse
		String locFolder = assetFolder;
		if (keyValue.containsKey(keysToGet[0])) {
			locFolder = assetFolder + "/" + keyValue.get(keysToGet[0]);
			locFolder = locFolder.replaceAll("\\\\", "/");
		}
		// neutraize app_assets
		//locFolder = locFolder.replaceAll("/app_assets", "");
		
		// forcing so we dont add the app
		app = true;

		List <Map<String, Object>> output = GitAssetUtils.getAssetMetadata(locFolder, assetFolder, replacer, !app);
		
		
		return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

}
