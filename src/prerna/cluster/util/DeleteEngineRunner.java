package prerna.cluster.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabase;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IStorage;
import prerna.util.Constants;

public class DeleteEngineRunner implements Runnable {

	private static final Logger classLogger = LogManager.getLogger(DeleteEngineRunner.class);

	private final String ENGINE_ID;
	private final String ENGINE_TYPE;
	
	public DeleteEngineRunner(String engineId, String engineType) {
		this.ENGINE_ID = engineId;
		this.ENGINE_TYPE = engineType;
	}
	
	@Override
	public void run() {
		try {
			if(IDatabase.CATALOG_TYPE.equals(ENGINE_TYPE)) {
				ClusterUtil.deleteDatabase(ENGINE_ID);
			} else if(IStorage.CATALOG_TYPE.equals(ENGINE_TYPE)) {
				ClusterUtil.deleteStorage(ENGINE_ID);
			} else if(IModelEngine.CATALOG_TYPE.equals(ENGINE_TYPE)) {
				ClusterUtil.deleteModel(ENGINE_ID);
			} else {
				classLogger.warn("Unknown engine type '"+ENGINE_TYPE+"' with no method to delete from cloud");
				classLogger.warn("Unknown engine type '"+ENGINE_TYPE+"' with no method to delete from cloud");
				classLogger.warn("Unknown engine type '"+ENGINE_TYPE+"' with no method to delete from cloud");
				classLogger.warn("Unknown engine type '"+ENGINE_TYPE+"' with no method to delete from cloud");
				classLogger.warn("Unknown engine type '"+ENGINE_TYPE+"' with no method to delete from cloud");
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}		
	}

}
