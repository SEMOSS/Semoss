package prerna.util.git.reactors;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.git.GitDestroyer;
import prerna.util.git.GitRepoUtils;

public class RenameAssetReactor extends AbstractReactor{

	private static final Logger classLogger = LogManager.getLogger(RenameAssetReactor.class);
	
	public RenameAssetReactor(){
		this.keysToGet = new String[] {
				ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.NEW_VALUE.getKey(),
				ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
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
 
        String assetFolder = AssetUtility.getRootFolderPath(this.insight, space, true);
		String relativePath = AssetUtility.getAssetRelativePath(this.insight, space);
		String comment = this.keyValue.get(this.keysToGet[3]);
		if(comment == null) {
        	comment = "rename: Renaming " + oldName + " to " + newName;
        }
		
        String oldAbs    = (assetFolder + "/" + oldName).replace("\\", "/");
        String newAbs    = (assetFolder + "/" + newName).replace("\\", "/");
        File oldFile = new File(oldAbs);
        File newFile = new File(newAbs);
 
        // validation checks
        if (!oldFile.exists()) {
            throw new IllegalArgumentException("Cannot find file/folder to rename: " + oldName);
        }
        if (newFile.exists()) {
            throw new IllegalArgumentException("A file or directory exists with the new name: " + newName);
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
        
        // handle pushing to git and the cloud
        
        List<String> toAdd = new ArrayList<>();
        List<String> toRemove = new ArrayList<>();

		// check the file to see if it is version/
		// if not add it here
		// make the asset folder to be the first piece of the file path
		// need to get the first piece of fileName
		// add it to the asset
		// and pass that as asset folder
		String [] fileTokens = oldName.split("/");
		String baseDir = fileTokens[0];
		assetFolder = assetFolder + "/" + baseDir;
		oldName = oldName.replace(baseDir, "");
		// we dont want to start with a "/"
		if(relativePath.isEmpty()) {
			if(oldName.startsWith(DIR_SEPARATOR)) {
				toRemove.add(oldName.substring(1));		
			} else {
				toRemove.add(oldName);
			}
		} else {
			toRemove.add(relativePath + DIR_SEPARATOR + oldName);		
		}
		newName = newName.replace(baseDir, "");
		// we dont want to start with a "/"
		if(relativePath.isEmpty()) {
			if(newName.startsWith(DIR_SEPARATOR)) {
				toAdd.add(newName.substring(1));		
			} else {
				toAdd.add(newName);
			}
		} else {
			toAdd.add(relativePath + DIR_SEPARATOR + newName);		
		}
        GitRepoUtils.addSpecificFiles(assetFolder, toAdd);
        GitDestroyer.removeSpecificFiles(assetFolder, true, toRemove);
		// Get the user's email
		AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String email = accessToken.getEmail();
		String author = accessToken.getUsername();
		
		// commit it
		GitRepoUtils.commitAddedFiles(assetFolder, comment, author, email);
		// handle synchronization to the cloud
		if (AssetUtility.USER_SPACE_KEY.equalsIgnoreCase(space)) {
			AuthProvider provider = user.getPrimaryLogin();
			String projectId = user.getAssetProjectId(provider);
			if(projectId!=null && !(projectId.isEmpty())) {
				ClusterUtil.pushUserWorkspace(projectId, true);
			}
		} else {
			// if space is null or it is in the insight, push using insight id to get engine
			if(space == null || space.trim().isEmpty() || space.equals(AssetUtility.INSIGHT_SPACE_KEY)) {
				IProject project = Utility.getProject(this.insight.getProjectId());
				ClusterUtil.pushProjectFolder(project, assetFolder);
			} else {
				// this is a project asset. space is the projectId
				IProject project = Utility.getProject(space);
				ClusterUtil.pushProjectFolder(project, assetFolder);
			}
		}
 
        return NounMetadata.getSuccessNounMessage("Renamed successfully");
	}
	
	@Override
	public String getReactorDescription() {
		return "Rename a file or directory";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Name of an existing file or directory";
		} else if(key.equals(ReactorKeysEnum.NEW_VALUE.getKey())) {
			return "The new name for the file or directory. This cannot be an name for an existing file or directory and has the same character restrictions you would expect on typical file system.";
		} else if(key.equals(ReactorKeysEnum.SPACE.getKey())) {
			return "This is an optional field to determine the space in which the relative file path exists (user project space, current insight space, project id space).";
		} else if(key.equals(ReactorKeysEnum.COMMENT_KEY.getKey())) {
			return "Comment to add with the rename into the git repository associated with the space";
		} 
		return super.getDescriptionForKey(key);
	}

}
