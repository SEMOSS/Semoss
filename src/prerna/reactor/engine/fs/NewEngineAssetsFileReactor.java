package prerna.reactor.engine.fs;

import java.util.ArrayList;
import java.util.List;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class NewEngineAssetsFileReactor extends AbstractReactor {

	public NewEngineAssetsFileReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.COMMENT_KEY.getKey() };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Engine " + engineId + " does not exist or user does not have access to edit assets.");
		}
		IEngine engine = Utility.getEngine(engineId);

		String gitFolder = EngineUtility.getSpecificEngineVersionFolder(engine.getCatalogType(), engine.getEngineId(),
				engine.getEngineName());
		String assetFolder = EngineUtility.getSpecificEngineAssetsFolder(engine.getCatalogType(), engine.getEngineId(),
				engine.getEngineName());

		String filePath = Utility.normalizePath(this.keyValue.get(this.keysToGet[1]));
		if (filePath == null || filePath.isEmpty()) {
			throw new IllegalArgumentException("Must provide a valid filePath");
		}
		String comment = this.keyValue.get(this.keysToGet[2]);
		if (comment == null) {
			comment = "add: creating new directory";
		}

		FileSystemUtil.createNewAssetFile(assetFolder, filePath);

		List<String> gitRelativeFilePaths = new ArrayList<>();
		gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + "/" + filePath);

		// Get the user's email
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getUsername();

		GitRepoUtils.addSpecificFiles(gitFolder, gitRelativeFilePaths);
		// commit it
		GitRepoUtils.commitAddedFiles(gitFolder, comment, author, email);
		// handle synchronization to the cloud
		ClusterUtil.pushEngineFolder(engine, assetFolder);

		NounMetadata retNoun = NounMetadata.getSuccessNounMessage("Success!");
		return retNoun;
	}

	@Override
	public String getReactorDescription() {
		return "Create a new empty file in the engine folder";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The unique id for the engine";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Names of the file to create.";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while creating and saving the new file within the git repository for the engine";
		}
		return super.getDescriptionForKey(key);
	}

}
