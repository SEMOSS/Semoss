package prerna.util.git.reactors;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Vector;
import java.util.regex.Matcher;

import org.codehaus.plexus.util.FileUtils;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.git.GitRepoUtils;

public class UploadAssetReactor extends AbstractReactor {

	public UploadAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), "tempPath",
				ReactorKeysEnum.COMMENT_KEY.getKey() };
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
		String tempPath = this.keyValue.get(this.keysToGet[1]);
		
		if(tempPath.startsWith("$IF")) {
			tempPath = tempPath.replaceFirst("\\$IF", Matcher.quoteReplacement(this.insight.getInsightFolder()));
		}
		
		String comment = this.keyValue.get(this.keysToGet[2]);

		// get insight asset path
		String assetFolder = this.insight.getInsightFolder();
		assetFolder = assetFolder.replaceAll("\\\\", "/");

		// create path where asset will be uploaded to
		String assetPath = assetFolder + "/" + filePath;
		assetPath = assetPath.replaceAll("\\\\", "/");

		// move temp file to asset file
		File tempFile = new File(tempPath);
		File newFile = new File(assetPath);
		try {
			FileUtils.copyFileToDirectory(tempPath, assetFolder);
			FileUtils.rename(tempFile, newFile);
			// TODO should I delete the old file?
			// tempFile.delete();
		} catch (IOException e) {
			e.printStackTrace();
			NounMetadata error = NounMetadata.getErrorNounMessage("Unable to Upload Asset");
			SemossPixelException exception = new SemossPixelException(error);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		// add file to git
		List<String> files = new Vector<>();
		files.add(filePath);
		GitRepoUtils.addSpecificFiles(assetFolder, files);


		// commit it
		GitRepoUtils.commitAddedFiles(assetFolder, comment, author, email);
		return NounMetadata.getSuccessNounMessage("Success!");
	}

}
