package prerna.engine.impl.tinker;

import java.util.Properties;

import org.janusgraph.core.JanusGraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.util.Utility;

public class JanusEngine extends TinkerEngine {
	
	private static final Logger classLogger = LoggerFactory.getLogger(JanusEngine.class);

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		String janusConfFilePath = SmssUtilities.getJanusFile(this.smssProp).getAbsolutePath();
		classLogger.info("Opening graph: " + Utility.cleanLogString(janusConfFilePath));
		g = JanusGraphFactory.open(janusConfFilePath);
		classLogger.info("Done opening graph: " + Utility.cleanLogString(janusConfFilePath));
	}
	
	@Override
	public DATABASE_TYPE getDatabaseType() {
		return IDatabaseEngine.DATABASE_TYPE.JANUS_GRAPH;
	}

}
