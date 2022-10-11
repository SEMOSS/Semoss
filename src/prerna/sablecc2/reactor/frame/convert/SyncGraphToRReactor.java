package prerna.sablecc2.reactor.frame.convert;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.engine.impl.tinker.TinkerUtilities;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.util.Constants;

public class SyncGraphToRReactor extends AbstractFrameReactor {

	private static final String CLASS_NAME = SyncGraphToRReactor.class.getName();
	
	public SyncGraphToRReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FRAME.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		ITableDataFrame frame = getFrame();
		if(!(frame instanceof TinkerFrame)) {
			throw new IllegalArgumentException("Frame must be a graph frame type");
		}
		Logger logger = getLogger(CLASS_NAME);

		TinkerFrame graph = (TinkerFrame) frame;
		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(CLASS_NAME);
		String wd = this.insight.getInsightFolder();
		synchronizeGraphToR(graph, rJavaTranslator, graph.getName(), wd, logger);
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	/**
	 * 
	 * @param graph
	 * @param rJavaTranslator
	 * @param graphName
	 * @param wd
	 * @param logger
	 */
	private void synchronizeGraphToR(TinkerFrame graph, AbstractRJavaTranslator rJavaTranslator, String graphName, String wd, Logger logger) {
		try {
			String fileName = TinkerUtilities.serializeGraph(graph, wd);
			wd = wd.replace("\\", "/");

			StringBuilder builder = new StringBuilder("library(\"igraph\");");
			builder.append(graphName + "<- read_graph(\"" + fileName + "\", \"graphml\");");
			// load the graph
			rJavaTranslator.executeEmptyR(builder.toString());
		} catch (Exception ex) {
			logger.error(Constants.STACKTRACE, ex);
			throw new IllegalArgumentException("ERROR ::: Could not convert TinkerFrame into iGraph.\nPlease make sure iGraph package is installed.");
		}
	}
}
