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
import prerna.util.Utility;

public class ClusterGraphReactor extends AbstractRFrameReactor {

	private static final String CLASS_NAME = ClusterGraphReactor.class.getName();

	/**
	 * Example input for routine
	 * 
	 * previous method from AbstractBaseRClass -> Reactor input
	 * walk info 	-> 	cluster_walktrap
	 * cluster info -> 	clus
	 */
	
	public ClusterGraphReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ROUTINE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		String[] packages = new String[] {"igraph"};
		this.rJavaTranslator.checkPackages(packages);
		this.rJavaTranslator.executeEmptyR("library(igraph)");
		Logger logger = getLogger(CLASS_NAME);
		String routine = this.keyValue.get(this.keysToGet[0]);

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
		
		try {
			logger.info("Determining graph clusters...");
			String clusterName = "clus" + Utility.getRandomString(8);
			// run iGraph routine
			if (routine.toLowerCase().equals("clusters")) {
				this.rJavaTranslator.executeEmptyR(clusterName + " <- " + routine + "(" + graphName + ")");
			} else if (routine.toLowerCase().equals("cluster_walktrap")) {
				this.rJavaTranslator.executeEmptyR(clusterName + " <- " + routine + "(" + graphName + ", membership=TRUE)");
			} else {
				throw new IllegalArgumentException("Invalid igraph routine");
			}

			logger.info("Done calculating graph clusters...");
			colorClusters(graph, graphName, clusterName);
			// clean up temp variable
			this.rJavaTranslator.executeEmptyR("rm(" + clusterName + ")");
			return new NounMetadata(graph, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		throw new IllegalArgumentException("Unable to cluster graph");
	}

	private void colorClusters(TinkerFrame graph, String graphName, String clusterName) {
		Logger logger = getLogger(CLASS_NAME);

		logger.info("Synchronizing graph clusters into frame...");
		double[] memberships = this.rJavaTranslator.getDoubleArray(clusterName + "$membership");
		String[] IDs = this.rJavaTranslator
				.getStringArray("vertex_attr(" + graphName + ", \"" + TinkerFrame.TINKER_ID + "\")");
		for (int memIndex = 0; memIndex < memberships.length; memIndex++) {
			String thisID = IDs[memIndex];
			Vertex retVertex = null;
			GraphTraversal<Vertex, Vertex> gt = graph.g.traversal().V().has(TinkerFrame.TINKER_ID, thisID);
			if (gt.hasNext()) {
				retVertex = gt.next();
			}
			if (retVertex != null) {
				retVertex.property("CLUSTER", memberships[memIndex]);
			}
			if (memIndex % 100 == 0) {
				logger.info("Done synchronizing graph vertex number " + memIndex + " out of " + memberships.length);
			}
		}
		logger.info("Done synchronizing graph vertices");
	}

}
