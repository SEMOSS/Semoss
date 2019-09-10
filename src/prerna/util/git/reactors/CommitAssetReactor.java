package prerna.util.git.reactors;

import java.util.List;
import java.util.Vector;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.git.GitRepoUtils;

public class CommitAssetReactor extends AbstractReactor {

	public CommitAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey(),
				ReactorKeysEnum.IN_APP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String author = null;
		String email = null;
		// check if user is logged in
		if (AbstractSecurityUtils.securityEnabled()) {
			if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
				throwAnonymousUserError();
			}
			// Get the user's email
			AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
			email = accessToken.getEmail();
			author = accessToken.getUsername();
		}
		String filePath = this.keyValue.get(this.keysToGet[0]);
		String comment = this.keyValue.get(this.keysToGet[1]);

		// get insight asset path
		boolean app = keyValue.containsKey(keysToGet[2])
				|| (keyValue.containsKey(keysToGet[0]) && keyValue.get(keysToGet[0]).startsWith("app_assets"));
		String assetFolder = this.insight.getInsightFolder();
		if (app) {
			assetFolder = this.insight.getAppFolder();
		}
		assetFolder = assetFolder.replaceAll("\\\\", "/");

		// add file to git
		List<String> files = new Vector<>();
		files.add(filePath);
		GitRepoUtils.addSpecificFiles(assetFolder, files);

		// commit it
		GitRepoUtils.commitAddedFiles(assetFolder, comment, author, email);
		return NounMetadata.getSuccessNounMessage("Success!");

	}
}
