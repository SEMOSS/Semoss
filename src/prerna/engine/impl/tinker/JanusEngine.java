package prerna.engine.impl.tinker;

import org.janusgraph.core.JanusGraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;

public class JanusEngine extends TinkerEngine {
	private static final Logger LOGGER = LoggerFactory.getLogger(JanusEngine.class);

	@Override
	public void openDB(String propFile) {
		super.openDB(propFile);
		String janusConfFilePath = SmssUtilities.getJanusFile(prop).getAbsolutePath();
		try {
			LOGGER.info("Opening graph: " + janusConfFilePath);
			g = JanusGraphFactory.open(janusConfFilePath);
			LOGGER.info("Done opening graph: " + janusConfFilePath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public ENGINE_TYPE getEngineType() {
		return IEngine.ENGINE_TYPE.JANUS_GRAPH;
	}

}
