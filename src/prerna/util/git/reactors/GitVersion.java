package prerna.util.git.reactors;

import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.git.GitPushUtils;
import prerna.util.git.GitRepoUtils;
import prerna.util.git.GitUtils;

public class GitVersion extends AbstractReactor {

	public GitVersion() {
		this.keysToGet = new String[]{"app"};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String appFolder = baseFolder + "db/" + keyValue.get(keysToGet[0]);	
		
		Logger logger = getLogger(this.getClass().getName());
		logger.info("Converting " + appFolder + " to a versionable app");
		logger.info("Checking to see if it is already versioned");

		if(GitUtils.isGit(appFolder)) {
			logger.info("App is already versionable");
		} else {
			logger.info("Creating initial version");
			GitRepoUtils.makeLocalDatabaseGitVersionFolder(appFolder);
			// we create a version folder
			String versionFolder = appFolder + "/version";
			GitPushUtils.addAllFiles(versionFolder, false);
			GitPushUtils.commitAddedFiles(versionFolder);
		}
		logger.info("Complete");

		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE);
	}

}
