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
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_NAME.getKey(), ReactorKeysEnum.CONTENT.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey(), ReactorKeysEnum.IN_APP.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
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
		
		String comment = this.keyValue.get(this.keysToGet[2]);
		boolean app = keyValue.containsKey(keysToGet[3]) || (keyValue.containsKey(keysToGet[0]) && keyValue.get(keysToGet[0]).startsWith("app_assets"));

		// get asset base folder to create relative path
		String assetFolder = this.insight.getInsightFolder();
		
		if(app)
		{
			// create the asset folder if one doesn't exist
			assetFolder = this.insight.getAppFolder() ;
			File file = new File(assetFolder);
			if(!file.exists())
				file.mkdir();
			GitRepoUtils.init(assetFolder);
		}
		assetFolder = assetFolder.replaceAll("\\\\", "/");

		String fileName = keyValue.get(keysToGet[0]);
		String filePath = assetFolder + "/" + fileName;
		//filePath = filePath.replaceAll("/app_assets", "");
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

		// commit it
		GitRepoUtils.commitAddedFiles(assetFolder, comment, author, email);

		return NounMetadata.getSuccessNounMessage("Success!");
	}

}
