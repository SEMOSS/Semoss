package prerna.reactor.insights.fs;

import java.util.List;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;

public class SaveInsightAssetsReactor extends AbstractReactor {

	public SaveInsightAssetsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.CONTENT.getKey() };
		this.keyRequired = new int[] { 1, 1 };

//		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.CONTENT.getKey(),
//				ReactorKeysEnum.COMMENT_KEY.getKey() };
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

		// Retrieve all file paths and contents
		List<String> filePaths = getNounAsStringList(this.keysToGet[0]);
		List<String> contents = getNounAsStringList(this.keysToGet[1]);

		if (filePaths == null || filePaths.isEmpty() || contents == null || contents.isEmpty()) {
			throw new IllegalArgumentException("Must pass in at least one file name and content to save");
		}
		if (filePaths.size() != contents.size()) {
			throw new IllegalArgumentException("Number of file names and contents must match");
		}

		String assetFolder = this.insight.getInsightFolder();
//		String comment = this.keyValue.get(this.keysToGet[2]);
//		if (comment == null) {
//			comment = "add: SaveAppAssets executed";
//		}
		// Check strict script source settings once
		boolean strictScriptSource = Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.STRICT_SCRIPT_SOURCE));
		FileSystemUtil.validateAssetFiles(filePaths, strictScriptSource);
		FileSystemUtil.saveAssetFiles(assetFolder, filePaths, contents);

		// add file to git
//		List<String> gitRelativeFilePaths = new ArrayList<>();
//		for (int i = 0; i < filePaths.size(); i++) {
//			String rawFileName = filePaths.get(i).trim();
//			String fileName = Utility.normalizePath(rawFileName);
//			if (fileName == null || fileName.isEmpty()) {
//				continue;
//			}
//
//			// for git, we need to add the assets folder which is assumed in the path
//			gitRelativeFilePaths.add(Constants.ASSETS_FOLDER + DIR_SEPARATOR + fileName);
//		}
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
		return "Save a single or multiple files in the insight assets folder";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Names of the file(s) to save";
		} else if (key.equals(ReactorKeysEnum.CONTENT.getKey())) {
			return "Contents of the file(s) to save";
		} else if (key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while saving the files within the git repository for the insight";
		}
		return super.getDescriptionForKey(key);
	}

}
