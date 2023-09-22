package prerna.cluster.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.util.Constants;

public class DeleteEngineRunner implements Runnable {

	private static final Logger classLogger = LogManager.getLogger(DeleteEngineRunner.class);

	private final String ENGINE_ID;
	private final IEngine.CATALOG_TYPE ENGINE_TYPE;
	
	public DeleteEngineRunner(String engineId, IEngine.CATALOG_TYPE engineType) {
		this.ENGINE_ID = engineId;
		this.ENGINE_TYPE = engineType;
	}
	
	@Override
	public void run() {
		try {
			ClusterUtil.deleteEngine(ENGINE_ID, ENGINE_TYPE);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}		
	}

}
