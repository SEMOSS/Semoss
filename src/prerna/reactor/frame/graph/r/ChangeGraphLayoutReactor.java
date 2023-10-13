package prerna.reactor.frame.graph.r;

import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.engine.impl.tinker.iGraphUtilities;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Run a layout in iGraph and store back into tinker objects Possible values:
 * Fruchterman - layout_with_fr KK - layout_with_kk sugiyama -
 * layout_with_sugiyama layout_as_tree layout_as_star layout.auto
 * http://igraph.org/r/doc/layout_with_fr.html
 *
 */
public class ChangeGraphLayoutReactor extends AbstractRFrameReactor {
	private static final String CLASS_NAME = ChangeGraphLayoutReactor.class.getName();

	public ChangeGraphLayoutReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.GRAPH_LAYOUT.getKey(), "yMin", "yMax", "xMin", "xMax" };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		String[] packages = new String[] { "igraph" };
		this.rJavaTranslator.checkPackages(packages);
		this.rJavaTranslator.executeEmptyR("library(igraph)");
		Logger logger = getLogger(CLASS_NAME);

		ITableDataFrame frame = getFrame();
		if(!(frame instanceof TinkerFrame)) {
			throw new IllegalArgumentException("Frame must be a graph frame type");
		}
		TinkerFrame graph = (TinkerFrame) frame;
		if(!graph.isIGraphSynched()) {
			AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(CLASS_NAME);
			String wd = this.insight.getInsightFolder();
			iGraphUtilities.synchronizeGraphToR(graph, rJavaTranslator, graph.getName(), wd, logger);
		}
		String graphName = graph.getName();
		
		String inputLayout = this.keyValue.get(this.keysToGet[0]);
		try {
			logger.info("Determining vertex positions...");
			String tempOutputLayout = "xy_layout" + Utility.getRandomString(8);
			this.rJavaTranslator.executeEmptyR("" + tempOutputLayout + " <- " + inputLayout + "(" + graphName + ")");
			// default normalization scale
			String yMin = getYMin();
			String yMax = getYMax();
			String xMin = getXMin();
			String xMax = getXMax();
			this.rJavaTranslator.executeEmptyR(tempOutputLayout + "<-layout.norm(" + tempOutputLayout + ", ymin=" + yMin
					+ ", ymax=" + yMax + ", xmin=" + xMin + ", xmax=" + xMax + ")");
			logger.info("Done calculating positions...");
			synchronizeXY(graph, graphName, tempOutputLayout);
			// clean up temp variable
			this.rJavaTranslator.executeEmptyR("rm(" + tempOutputLayout + ")");
			return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
		} catch (Exception ex) {
			logger.error(Constants.STACKTRACE, ex);
		}

		throw new IllegalArgumentException("Unable to change layout");
	}

	private void synchronizeXY(TinkerFrame graph, String graphName, String layout) {
		Logger logger = getLogger(CLASS_NAME);
		logger.info("Synchronizing vertex positions into frame...");
		double[][] memberships = this.rJavaTranslator.getDoubleMatrix(layout);
		String[] axis = null;
		if (memberships[0].length == 2) {
			axis = new String[] { "X", "Y" };
		} else if (memberships[0].length == 3) {
			axis = new String[] { "X", "Y", "Z" };
		}
		String[] ids = this.rJavaTranslator
				.getStringArray("vertex_attr(" + graphName + ", \"" + TinkerFrame.TINKER_ID + "\")");
		for (int memIndex = 0; memIndex < memberships.length; memIndex++) {
			String thisID = ids[memIndex];
			Vertex retVertex = null;
			GraphTraversal<Vertex, Vertex> gt = graph.g.traversal().V().has(TinkerFrame.TINKER_ID, thisID);
			if (gt.hasNext()) {
				retVertex = gt.next();
			}
			if (retVertex != null && axis != null) {
				for (int i = 0; i < axis.length; i++) {
					retVertex.property(axis[i], memberships[memIndex][i]);
				}
			}

			if (memIndex % 100 == 0) {
				logger.info("Done synchronizing graph vertex number " + memIndex + " out of " + memberships.length);
			}
		}
		logger.info("Done synchronizing vertex positions");
	}

	public String getYMin() {
		String yMin = this.keyValue.get(this.keysToGet[1]);
		if (yMin == null) {
			yMin = "-50";
		}
		return yMin;
	}

	private String getYMax() {
		String yMin = this.keyValue.get(this.keysToGet[2]);
		if (yMin == null) {
			yMin = "50";
		}
		return yMin;
	}

	private String getXMin() {
		String xMin = this.keyValue.get(this.keysToGet[3]);
		if (xMin == null) {
			xMin = "-50";
		}
		return xMin;
	}

	private String getXMax() {
		String xMax = this.keyValue.get(this.keysToGet[4]);
		if (xMax == null) {
			xMax = "50";
		}
		return xMax;
	}
}
