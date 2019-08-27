package prerna.util.git.reactors;

import java.util.List;
import java.util.Vector;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.git.GitDestroyer;
import prerna.util.git.GitRepoUtils;

public class DeleteAssetReactor extends GitBaseReactor {

	public DeleteAssetReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// check if user is logged in
		User user = this.insight.getUser();
		if (AbstractSecurityUtils.securityEnabled() && user != null) {
			if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
				throwAnonymousUserError();
			}
		} else {
			throwAnonymousUserError();
		}
		
		// get asset base folder
		String assetFolder = this.insight.getInsightFolder();
		assetFolder = assetFolder.replaceAll("\\\\", "/");

		// get the file path to delete
		String fileName = keyValue.get(keysToGet[0]);
		String comment = this.keyValue.get(this.keysToGet[1]);

		List<String> files = new Vector<>();
		files.add(fileName);
		GitDestroyer.removeSpecificFiles(assetFolder, true, files);
		
		// Get the user's email
		String author = this.insight.getUserId();
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		// commit it
		GitRepoUtils.commitAddedFiles(assetFolder, comment, author, email);

		return NounMetadata.getSuccessNounMessage("Success!");
	}
}
