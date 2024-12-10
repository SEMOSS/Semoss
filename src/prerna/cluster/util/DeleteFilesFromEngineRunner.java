package prerna.cluster.util;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Constants;

public class DeleteFilesFromEngineRunner implements Runnable {

	private static final Logger classLogger = LogManager.getLogger(DeleteFilesFromEngineRunner.class);

	private final String ENGINE_ID;
	private final IEngine.CATALOG_TYPE ENGINE_TYPE;
	private final String [] FILE_PATHS;
	
	public DeleteFilesFromEngineRunner(String engineId, IEngine.CATALOG_TYPE engineType, String [] filePaths) {
		this.ENGINE_ID = engineId;
		this.ENGINE_TYPE = engineType;
		this.FILE_PATHS = filePaths;
	}
	
	@Override
	public void run() {
		for (int i = 0; i < this.FILE_PATHS.length; i++) {
			try {
				ClusterUtil.deleteEngineCloudFile(ENGINE_ID, ENGINE_TYPE, FILE_PATHS[i]);
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}	
		}
		
		// once all the files have been deleted for that vec db, issue a pull command on ZK
		if(ClusterUtil.IS_CLUSTER_ZK) {
			try {
				ClusterUtil.getClusterSynchronizer().publishEngineChange(ENGINE_ID, "pullEngine", ENGINE_ID);
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				SemossPixelException err = new SemossPixelException("Failed to publish engine '"+ENGINE_ID+"' to sync with ZK cluster");
				err.setContinueThreadOfExecution(true);
				throw err;
			}
		}
	}
}
