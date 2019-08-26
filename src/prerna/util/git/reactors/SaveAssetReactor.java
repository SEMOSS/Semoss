package prerna.util.git.reactors;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.commons.io.FileUtils;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class SaveAssetReactor extends AbstractReactor {

	// pulls the latest for this project / asset
	// the asset is basically the folder where it sits
	// this can be used enroute in a pipeline

	public SaveAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_NAME.getKey(), ReactorKeysEnum.CONTENT.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey() };
		this.keyRequired = new int[] { 1, 1, 0 };
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
		
		String comment = this.keyValue.get(this.keysToGet[2]);

		// get asset base folder to create relative path
		String assetFolder = this.insight.getInsightFolder();
		assetFolder = assetFolder.replaceAll("\\\\", "/");

		String fileName = keyValue.get(keysToGet[0]);
		String filePath = assetFolder + "/" + fileName;
		String content = keyValue.get(keysToGet[1]);

		// write content to file
		content = Utility.decodeURIComponent(content);
		File file = new File(filePath);
		try {
			FileUtils.writeStringToFile(file, content);
		} catch (IOException e) {
			e.printStackTrace();
			NounMetadata error = NounMetadata.getErrorNounMessage("Unable to save file");
			SemossPixelException exception = new SemossPixelException(error);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		// add file to git
		List<String> files = new Vector<>();
		files.add(fileName);
		GitRepoUtils.addSpecificFiles(assetFolder, files);

		// Get the user's email
		String author = this.insight.getUserId();
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		// commit it
		GitRepoUtils.commitAddedFiles(assetFolder, comment, author, email);

		return NounMetadata.getSuccessNounMessage("Success!");
	}

}
