package prerna.sablecc2.reactor.frame.r.graph;

import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.ds.TinkerFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.Utility;

/**
 * Run a layout in iGraph and store back into tinker objects Possible values:
 * Fruchterman - layout_with_fr KK - layout_with_kk sugiyama -
 * layout_with_sugiyama layout_as_tree layout_as_star layout.auto
 * http://igraph.org/r/doc/layout_with_fr.html
 *
 */
public class GraphLayoutReactor extends AbstractRFrameReactor {

	private static final String CLASS_NAME = GraphLayoutReactor.class.getName();

	public GraphLayoutReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.GRAPH_LAYOUT.getKey(), "yMin", "yMax", "xMin", "xMax"};
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		String[] packages = new String[]{"igraph"};
		this.rJavaTranslator.checkPackages(packages);
		Logger logger = getLogger(CLASS_NAME);
		TinkerFrame frame = (TinkerFrame) getFrame();
		String graphName = (String) retrieveVariable("GRAPH_NAME");
		String inputLayout = this.keyValue.get(this.keysToGet[0]);
		
		try {
			logger.info("Determining vertex positions...");
			String tempOutputLayout = "xy_layout" + Utility.getRandomString(8);
			this.rJavaTranslator.executeR("" + tempOutputLayout + " <- " + inputLayout + "(" + graphName + ")");
			// default normalization scale
			String yMin = getYMin();
			String yMax = getYMax();
			String xMin = getXMin();
			String xMax = getXMax();
			this.rJavaTranslator.executeR(tempOutputLayout + "<-layout.norm("+tempOutputLayout+", ymin="+yMin+", ymax="+yMax+", xmin="+xMin+", xmax="+xMax+")");
			logger.info("Done calculating positions...");
			synchronizeXY(tempOutputLayout);
			// clean up temp variable
			this.rJavaTranslator.executeR("rm(" + tempOutputLayout + ")");
			return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		throw new IllegalArgumentException("Unable to change layout");
	}
	
	private void synchronizeXY(String layout) {
		Logger logger = getLogger(CLASS_NAME);
		TinkerFrame frame = (TinkerFrame) getFrame();
		logger.info("Synchronizing vertex positions into frame...");
		String graphName = (String) retrieveVariable("GRAPH_NAME");
		double[][] memberships = this.rJavaTranslator.getDoubleMatrix(layout);
		String[] axis = null;
		if (memberships[0].length == 2) {
			axis = new String[] { "X", "Y" };
		} else if (memberships[0].length == 3) {
			axis = new String[] { "X", "Y", "Z" };
		}
		String[] IDs = this.rJavaTranslator.getStringArray("vertex_attr(" + graphName + ", \"" + TinkerFrame.TINKER_ID + "\")");
		for (int memIndex = 0; memIndex < memberships.length; memIndex++) {
			String thisID = IDs[memIndex];
			Vertex retVertex = null;
			GraphTraversal<Vertex, Vertex> gt = frame.g.traversal().V().has(TinkerFrame.TINKER_ID, thisID);
			if (gt.hasNext()) {
				retVertex = gt.next();
			}
			if (retVertex != null) {
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
