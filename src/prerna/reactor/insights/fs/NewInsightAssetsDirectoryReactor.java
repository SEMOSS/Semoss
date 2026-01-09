package prerna.reactor.insights.fs;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;

public class NewInsightAssetsDirectoryReactor extends AbstractReactor {

	public NewInsightAssetsDirectoryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey() };
		this.keyRequired = new int[] { 1 };

//		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey() };
//		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String assetFolder = this.insight.getInsightFolder();

		String filePath = Utility.normalizePath(this.keyValue.get(this.keysToGet[0]));
		if (filePath == null || filePath.isEmpty()) {
			throw new IllegalArgumentException("Must provide a valid filePath");
		}
//		String comment = this.keyValue.get(this.keysToGet[1]);
//		if (comment == null) {
//			comment = "add: creating new directory";
//		}

		FileSystemUtil.createNewAssetDirectory(assetFolder, filePath);

//		List<String> gitRelativeFilePaths = new ArrayList<>();
//		gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + "/" + filePath);
//
//		// Get the user's email
//		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
//		String email = accessToken.getEmail();
//		String author = accessToken.getUsername();
//
//		GitRepoUtils.addSpecificFiles(gitFolder, gitRelativeFilePaths);
//		// commit it
//		GitRepoUtils.commitAddedFiles(gitFolder, comment, author, email);
//		// handle synchronization to the cloud
//		ClusterUtil.pushEngineFolder(engine, assetFolder);

		// push room to cloud storage
		if (this.insight.getRoomId() != null) {
			ClusterUtil.pushRoom(this.insight.getRoomId());
		}

		NounMetadata retNoun = NounMetadata.getSuccessNounMessage("Success!");
		return retNoun;
	}

	@Override
	public String getReactorDescription() {
		return "Create a new empty directory in the insight folder";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Names of the file to create.";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while creating and saving the new file within the git repository for the insight";
		}
		return super.getDescriptionForKey(key);
	}

}
