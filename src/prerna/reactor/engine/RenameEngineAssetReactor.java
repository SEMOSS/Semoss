package prerna.reactor.engine;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class RenameEngineAssetReactor extends AbstractReactor {

	/*
	 * TODO: expose Git at engine level as well
	 */
	
	private static final Logger classLogger = LogManager.getLogger(RenameEngineAssetReactor.class);

	public RenameEngineAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), 
				ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.NEW_VALUE.getKey() };
		this.keyRequired = new int[] {1,1,1};
//				,
//				ReactorKeysEnum.COMMENT_KEY.getKey() };
//		this.keyRequired = new int[] {1,1,1,0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException("Engine " + engineId + " does not exist or user does not have access to edit assets.");
		}
		// force to pull it from cloud if not in the container
		IEngine engine = Utility.getEngine(engineId);

		String currentFileName = Utility.normalizePath(this.keyValue.get(this.keysToGet[1]));
		String newFileName = Utility.normalizePath(this.keyValue.get(this.keysToGet[2]));

        if (currentFileName == null || currentFileName.trim().isEmpty() || newFileName == null || newFileName.trim().isEmpty()){
            throw new IllegalArgumentException("Must pass both existing name and new name");
        }

//		String gitFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), project.getProjectId());
		String assetFolder = EngineUtility.getSpecificEngineBaseFolder(engineId);
//		String comment = this.keyValue.get(this.keysToGet[3]);
//		if(comment == null) {
//        	comment = "rename: Renaming " + currentFileName + " to " + newFileName;
//        }
		
		String oldAbs = (assetFolder + "/" + currentFileName).replace("\\", "/");
        String newAbs = (assetFolder + "/" + newFileName).replace("\\", "/");
        File oldFile = new File(oldAbs);
        File newFile = new File(newAbs);
 
        // validation checks
        if (!oldFile.exists()) {
            throw new IllegalArgumentException("Cannot find file/folder to rename: " + currentFileName);
        }
        if (newFile.exists()) {
            throw new IllegalArgumentException("A file or directory exists with the new name: " + newFileName);
        }

        try {
            FileUtils.forceMkdirParent(newFile);
        } catch (IOException e) {
            classLogger.error(Constants.STACKTRACE, e);
            throw new SemossPixelException(
                NounMetadata.getErrorNounMessage("Unable to create parent directory for " + newFileName));
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
            NounMetadata.getErrorNounMessage("Failed to rename " + currentFileName ));
            ex.setContinueThreadOfExecution(false);
            throw ex;
        }
        
        // handle pushing to git and the cloud
        
//        List<String> toAdd = new ArrayList<>();
//		toAdd.add(Constants.ASSETS_FOLDER + DIR_SEPARATOR + newFileName);		
//
//        List<String> toRemove = new ArrayList<>();
//		toRemove.add(Constants.ASSETS_FOLDER + DIR_SEPARATOR + currentFileName);		
//
//		// Get the user's email
//		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
//		String email = accessToken.getEmail();
//		String author = accessToken.getUsername();
//		
//        GitRepoUtils.addSpecificFiles(gitFolder, toAdd);
//        GitDestroyer.removeSpecificFiles(gitFolder, true, toRemove);
//        // commit it
//		GitRepoUtils.commitAddedFiles(gitFolder, comment, author, email);
		// handle synchronization to the cloud
		ClusterUtil.pushEngineFolder(engine, assetFolder);

		NounMetadata retNoun = NounMetadata.getSuccessNounMessage("Success!");
		return retNoun;
	}

	@Override
	public String getReactorDescription() {
		return "Rename a file or directory in the projects assets folder";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the project/app";
		} else if(key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Names of the file(s) to save. This relative path should assume the prefix of '/version/assets/' and not include the prefix in the string value.";
		} else if(key.equals(ReactorKeysEnum.NEW_VALUE.getKey())) {
			return "The new name for the file or directory. This cannot be an name for an existing file or directory and has the same character restrictions you would expect on typical file system.";
		}  else if(key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add while saving the files within the git repository for the project";
		} 
		return super.getDescriptionForKey(key);
	}

}
