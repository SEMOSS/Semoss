package prerna.util.git.reactors;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class SaveAssetReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SaveAssetReactor.class);

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
        String assetFolder = AssetUtility.getAssetBasePath(this.insight, space, true);

        // Retrieve all file names and contents
        List<String> fileNames = getNounAsStringList(this.keysToGet[0]);
        List<String> contents = getNounAsStringList(this.keysToGet[1]);

        if(fileNames == null || fileNames.isEmpty() || contents == null || contents.isEmpty()) {
            throw new IllegalArgumentException("Must pass in at least one file name and content to save");
        }
        if(fileNames.size() != contents.size()) {
            throw new IllegalArgumentException("Number of file names and contents must match");
        }

        // Check strict script source settings once
        boolean strictScriptSource = Boolean.parseBoolean(
            Utility.getDIHelperProperty(Constants.STRICT_SCRIPT_SOURCE)
        );

        // iterate each fileName/content pair
        for (int i = 0; i < fileNames.size(); i++) {
            String rawFileName = fileNames.get(i).trim();
            String fileName = Utility.normalizePath(rawFileName);
            if(fileName == null || fileName.isEmpty()) {
                continue;
            }

            // limit saving R/Py Files in prod - No new files can be created but they can be sourced
            if(strictScriptSource) {
                String extension = FilenameUtils.getExtension(fileName);
                if("py".equalsIgnoreCase(extension) || "R".equalsIgnoreCase(extension)) {
                    throw new IllegalArgumentException("User is not allowed to create or save R or Py scripts");
                }
            }

            // you cannot save at root level if you are in user/project space
            if(space != null && !space.isEmpty() && !space.equals(AssetUtility.INSIGHT_SPACE_KEY) && !fileName.contains("/")) {
                return NounMetadata.getErrorNounMessage("You cannot create directory / files at this level");
            }

            String filePath = assetFolder + "/" + fileName;
            String content = contents.get(i);
            content = Utility.decodeURIComponent(content);

            File file = new File(filePath);
            try {
                FileUtils.writeStringToFile(file, content);
            } catch (IOException e) {
                classLogger.error(Constants.STACKTRACE, e);
                NounMetadata error = NounMetadata.getErrorNounMessage("Unable to save file: " + fileName);
                SemossPixelException exception = new SemossPixelException(error);
                exception.setContinueThreadOfExecution(false);
                throw exception;
            }
        }

        return NounMetadata.getSuccessNounMessage("Success!");
    }
	
	@Override
	public String getReactorDescription() {
		return "This reactor saves single or multiple files for Files tab in notebook";
    }
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.FILE_NAME.getKey())) {
	    	return "Names of the files to save";
	    }else if(key.equals(ReactorKeysEnum.CONTENT.getKey())) {
	    	return "Contents of the files to save";
	    }else if(key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
	    	return "Comment to add while saving the file";
	    }else if(key.equals(ReactorKeysEnum.SPACE.getKey())) {
	    	return "Application ID";
	    }
		return super.getDescriptionForKey(key);
	}

}
