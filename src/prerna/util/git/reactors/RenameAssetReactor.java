package prerna.util.git.reactors;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
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

public class RenameAssetReactor extends AbstractReactor{

	private static final Logger classLogger = LogManager.getLogger(RenameAssetReactor.class);
	
	public RenameAssetReactor(){
		this.keysToGet = new String[] {
				ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.NEW_VALUE.getKey(),
				ReactorKeysEnum.SPACE.getKey()};
		this.keyRequired = new int[] {1,1,0};
				
		}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		 
        // check login user
        User user = this.insight.getUser();
        if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
            throwAnonymousUserError();
        }

        // input paths check
        String oldName = Utility.normalizePath(keyValue.get(this.keysToGet[0]));
        String newName = Utility.normalizePath(keyValue.get(this.keysToGet[1]));
        String space  = keyValue.get(this.keysToGet[2]);
 
        if (oldName == null || oldName.trim().isEmpty() || newName == null || newName.trim().isEmpty()){
            throw new IllegalArgumentException("Must pass both existing name and new name");
        }
 
        String baseFolder = AssetUtility.getAssetBasePath(this.insight, space, true);
        String oldAbs    = (baseFolder + "/" + oldName).replace("\\", "/");
        String newAbs    = (baseFolder + "/" + newName).replace("\\", "/");
        File oldFile = new File(oldAbs);
        File newFile = new File(newAbs);
 
        // validation checks
        if (!oldFile.exists()) {
            throw new IllegalArgumentException("Cannot find file/folder to rename: " + oldName);
        }
        if (newFile.exists()) {
            throw new IllegalArgumentException("Name already exists: " + newName);
        }
 

        try {
            FileUtils.forceMkdirParent(newFile);
        } catch (IOException e) {
            classLogger.error(Constants.STACKTRACE, e);
            throw new SemossPixelException(
                NounMetadata.getErrorNounMessage("Unable to create parent directory for " + newName));
        }
 
        // rename the file/folder
        try {
            if (oldFile.isDirectory()) {
                FileUtils.moveDirectory(oldFile, newFile);
            } else {
                FileUtils.moveFile(oldFile, newFile);
            }
        } catch (IOException e) {
            classLogger.error(Constants.STACKTRACE, e);
            SemossPixelException ex = new SemossPixelException(
            NounMetadata.getErrorNounMessage("Failed to rename " + oldName ));
            ex.setContinueThreadOfExecution(false);
            throw ex;
        }
 
        return NounMetadata.getSuccessNounMessage("Renamed successfully");
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor helps to rename file or folder";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Existing file/folder path to rename";
		}else if(key.equals(ReactorKeysEnum.NEW_VALUE.getKey())) {
			return "File/folder path with new name";
		}else if(key.equals(ReactorKeysEnum.SPACE.getKey())) {
			return "Application ID";
		}
		return super.getDescriptionForKey(key);
	}

}
