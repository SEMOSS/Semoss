package prerna.util.git.reactors;

import java.io.File;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.git.GitFetchUtils;

public class GitCloneReactor extends AbstractReactor {

	public GitCloneReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.URL.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.SPACE.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String repoURL = keyValue.get(keysToGet[0]);
		if (!repoURL.startsWith("http")) {
			SemossPixelException exception = new SemossPixelException(
					NounMetadata.getErrorNounMessage("Not a valid URL - " + repoURL));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		// get base asset folder path
		String filePath = Utility.normalizePath(this.keyValue.get(this.keysToGet[1]));
		String space = this.keyValue.get(this.keysToGet[2]);
		// if security is enabled, you need proper permissions
		// this takes in the insight and does a user check that the user has access to perform the operations
		String assetFolder = AssetUtility.getAssetBasePath(this.insight, space, true);
		// add relative path in asset folder if specified
		if (filePath != null && !filePath.isEmpty()) {
			assetFolder += filePath;
		}
		
		// clone repo
		String repoName = Utility.getInstanceName(repoURL);
		String repoPath = assetFolder + DIR_SEPARATOR + repoName;
		GitFetchUtils.cloneApp(repoURL, repoPath);
		
		//check if clone was successful
		File clonePath = new File(repoPath);
		if(!clonePath.exists()) {
			SemossPixelException exception = new SemossPixelException(
					NounMetadata.getErrorNounMessage("Unable to clone " + repoURL));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		
		return NounMetadata.getSuccessNounMessage("Successfully cloned " + repoURL);
	}

}
