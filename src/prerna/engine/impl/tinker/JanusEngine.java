package prerna.engine.impl.tinker;

import org.janusgraph.core.JanusGraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.util.Utility;

public class JanusEngine extends TinkerEngine {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(JanusEngine.class);

	@Override
	public void openDB(String propFile) {
		super.openDB(propFile);
		String janusConfFilePath = SmssUtilities.getJanusFile(smssProp).getAbsolutePath();
		try {
			LOGGER.info("Opening graph: " + Utility.cleanLogString(janusConfFilePath));
			g = JanusGraphFactory.open(janusConfFilePath);
			LOGGER.info("Done opening graph: " + Utility.cleanLogString(janusConfFilePath));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public DATABASE_TYPE getDatabaseType() {
		return IDatabaseEngine.DATABASE_TYPE.JANUS_GRAPH;
	}

}
