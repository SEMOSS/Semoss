package prerna.reactor.insights.fs;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;

public class RenameInsightAssetReactor extends AbstractReactor {

	public RenameInsightAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.NEW_VALUE.getKey() };
		this.keyRequired = new int[] { 1, 1 };

//		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(),
//				ReactorKeysEnum.NEW_VALUE.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey() };
//		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String currentFileName = Utility.normalizePath(this.keyValue.get(this.keysToGet[0]));
		String newFileName = Utility.normalizePath(this.keyValue.get(this.keysToGet[1]));

		if (currentFileName == null || currentFileName.trim().isEmpty() || newFileName == null
				|| newFileName.trim().isEmpty()) {
			throw new IllegalArgumentException("Must pass both existing name and new name");
		}

		String assetFolder = this.insight.getInsightFolder();
//		String comment = this.keyValue.get(this.keysToGet[2]);
//		if (comment == null) {
//			comment = "rename: Renaming " + currentFileName + " to " + newFileName;
//		}

		FileSystemUtil.renameAsset(assetFolder, currentFileName, newFileName);

		// handle pushing to git and the cloud

//		List<String> toAdd = new ArrayList<>();
//		toAdd.add(Constants.ASSETS_FOLDER + DIR_SEPARATOR + newFileName);
//
//		List<String> toRemove = new ArrayList<>();
//		toRemove.add(Constants.ASSETS_FOLDER + DIR_SEPARATOR + currentFileName);
//
//		// Get the user's email
//		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
//		String email = accessToken.getEmail();
//		String author = accessToken.getUsername();
//
//		GitRepoUtils.addSpecificFiles(gitFolder, toAdd);
//		GitDestroyer.removeSpecificFiles(gitFolder, true, toRemove);
//		// commit it
//		GitRepoUtils.commitAddedFiles(gitFolder, comment, author, email);
//		// handle synchronization to the cloud
//		ClusterUtil.pushEngineFolder(engine, assetFolder);

		NounMetadata retNoun = NounMetadata.getSuccessNounMessage("Success!");
		return retNoun;
	}

	@Override
	public String getReactorDescription() {
		return "Rename a file or directory in the insight assets folder";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Names of the file(s) to save. This relative path should assume the prefix of the insight folder.";
		} else if (key.equals(ReactorKeysEnum.NEW_VALUE.getKey())) {
			return "The new name for the file or directory. This cannot be an name for an existing file or directory and has the same character restrictions you would expect on typical file system.";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while saving the files within the git repository for the insight";
		}
		return super.getDescriptionForKey(key);
	}

}
