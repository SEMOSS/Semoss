package prerna.sablecc2.reactor.frame.r.graph;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.ds.TinkerFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.reactor.task.constant.ConstantTaskCreationHelper;

public class NodeDetailsReactor extends AbstractRFrameReactor {

	private static final String CLASS_NAME = NodeDetailsReactor.class.getName();

	public NodeDetailsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.VALUE.getKey(), ReactorKeysEnum.PANEL.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		String[] packages = new String[] {"igraph"};
		this.rJavaTranslator.checkPackages(packages);
		Logger logger = getLogger(CLASS_NAME);
		String graphName = (String) retrieveVariable("GRAPH_NAME");
		TinkerFrame frame = (TinkerFrame) getFrame();

		String type = this.keyValue.get(this.keysToGet[0]);
		String instance = this.keyValue.get(this.keysToGet[1]);
		String panelId = this.keyValue.get(this.keysToGet[2]);
		if(panelId == null) {
			panelId = "Temp";
		}
		String uniqueNodeId = type + ":" + instance;
		
		GraphTraversal<Vertex, Vertex> it = frame.g.traversal().V().has(TinkerFrame.TINKER_ID, uniqueNodeId);
		Vertex v = null;
		if(it.hasNext()) {
			v = it.next();
		}
		
		if(v == null) {
			throw new IllegalArgumentException("Could not find vertex of type = " + type + " and value = " + instance);
		}
		
		List<Object[]> gridData = new Vector<Object[]>();
		
		long inE = 0;
		long outE = 0;
		long degree = 0;
		boolean isOrphan = false;
		long eigen = 0;
		
		logger.info("Calculating out edges");
		// count out E
		{
			Iterator<Edge> subIt = v.edges(Direction.OUT);
			while(subIt.hasNext()) {
				subIt.next();
				outE++;
			}
		}
		logger.info("Calculating in edges");
		// count in E
		{
			Iterator<Edge> subIt = v.edges(Direction.IN);
			while(subIt.hasNext()) {
				subIt.next();
				inE++;
			}
		}
		logger.info("Calculating degree");
		// get total degree
		degree = inE + outE;
		// determine if orphan
		if(degree == 0) {
			isOrphan = true;
		}
		logger.info("Calculating eigen value");
		// get eigen value
		eigen = frame.eigen(type, instance);
		
		// store these values
		gridData.add(new Object[]{"# in E", inE});
		gridData.add(new Object[]{"# out E", outE});
		gridData.add(new Object[]{"Degree", degree});
		gridData.add(new Object[]{"Is Orphan", isOrphan});
		gridData.add(new Object[]{"Eigen Value", eigen});

		if(graphName != null) {
			logger.info("Calculating articulation points");
			// get the articulation points
			boolean found = false;
			int [] vertices = this.rJavaTranslator.getIntArray("articulation.points(" + graphName + ")");
			for(int vertIndex = 0;vertIndex < vertices.length;  vertIndex++) {
				String thisNodeId = this.rJavaTranslator.getString("vertex_attr(" + graphName + ", \"" + TinkerFrame.TINKER_ID + "\", " + vertices[vertIndex] + ")");
				if(thisNodeId.equals(uniqueNodeId)) {
					found = true;
					gridData.add(new Object[]{"Is Articulation Node", true});
					break;
				}
			}
			
			if(!found) {
				gridData.add(new Object[]{"Is Articulation Node", false});
			}
		}

		ConstantDataTask taskData = ConstantTaskCreationHelper.getGridData(panelId, new String[]{"Metric", "Value"}, gridData);
		// store it in the insight
		if(panelId != "temp") {
			this.insight.getTaskStore().addTask(taskData);
		}
		
		// return the data
		return new NounMetadata(taskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
	}

}
