package prerna.util.git.reactors;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SaveAssetReactor extends AbstractReactor {

	public SaveAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_NAME.getKey(), ReactorKeysEnum.CONTENT.getKey(),
				ReactorKeysEnum.COMMENT_KEY.getKey(), ReactorKeysEnum.SPACE.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String space = this.keyValue.get(this.keysToGet[3]);
		// if security is enabled, you need proper permissions
		// this takes in the insight and does a user check that the user has access to perform the operations
		String assetFolder = AssetUtility.getAssetBasePath(this.insight, space, true);
		String fileName = Utility.normalizePath(keyValue.get(keysToGet[0]));
		
		// limit saving R/Py Files in prod - No new files can be created but they can be sourced
		boolean strict_script_source =  Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.STRICT_SCRIPT_SOURCE));
		if(strict_script_source) {
			 String extension = FilenameUtils.getExtension(fileName);
			 if(extension.equalsIgnoreCase("py") || extension.equalsIgnoreCase("R") ) {
					throw new IllegalArgumentException("User is not allowed to create or save R or Py scripts");
			 }
		}
		
		String filePath = assetFolder + "/" + fileName;
		String content = keyValue.get(keysToGet[1]);

		// you cannot save at root level if you are in user/project space
		if(space != null 
				&& !space.isEmpty() 
				&& !space.equals(AssetUtility.INSIGHT_SPACE_KEY) 
				&& !fileName.contains("/")) {
			return NounMetadata.getErrorNounMessage("You cannot create directory / files at this level");
		}
		
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

		return NounMetadata.getSuccessNounMessage("Success!");
	}

}
