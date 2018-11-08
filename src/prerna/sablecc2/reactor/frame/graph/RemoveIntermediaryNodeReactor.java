package prerna.sablecc2.reactor.frame.graph;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.TinkerFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;

public class RemoveIntermediaryNodeReactor extends AbstractFrameReactor {

	public RemoveIntermediaryNodeReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.COLUMN.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		TinkerFrame tinker = (TinkerFrame) getFrame();
		OwlTemporalEngineMeta meta = tinker.getMetaData();
		String nodeTypeToRemove = getColumn();
		
		String physicalTypeToRemove = meta.getPhysicalName(nodeTypeToRemove);
		
		// we will iterate through the graph for all edges of this node type
		GraphTraversal<Vertex, Vertex> traversal = tinker.g.traversal().V().has(TinkerFrame.TINKER_TYPE, physicalTypeToRemove);
		while(traversal.hasNext()) {
			Vertex vert = traversal.next();
			
			List<Vertex> sourceVertices = new Vector<Vertex>();
			List<Vertex> targetVertices = new Vector<Vertex>();
			
			Iterator<Edge> inEdges = vert.edges(Direction.IN);
			while(inEdges.hasNext()) {
				Edge edge = inEdges.next();
				// since we allow for loops
				// we can determine if this is that type of node
				// based on the edge relationship
				if(edge.property(TinkerFrame.TINKER_ID).value().toString().contains(nodeTypeToRemove)) {
					sourceVertices.add(edge.outVertex());
				}
			}
			
			Iterator<Edge> outEdges = vert.edges(Direction.OUT);
			while(outEdges.hasNext()) {
				Edge edge = outEdges.next();
				// since we allow for loops
				// we can determine if this is that type of node
				// based on the edge relationship
				if(edge.property(TinkerFrame.TINKER_ID).value().toString().contains(nodeTypeToRemove)) {
					targetVertices.add(edge.inVertex());
				}
			}
			
			// we now need to connect every source vertex with every target vertex
			for(Vertex source : sourceVertices) {
				String sourceType = source.value(TinkerFrame.TINKER_TYPE);
				String sourceValue = source.value(TinkerFrame.TINKER_NAME);
				for(Vertex target : targetVertices) {
					String type = sourceType + TinkerFrame.EDGE_LABEL_DELIMETER + target.value(TinkerFrame.TINKER_TYPE);
					String edgeID = type + "/" + sourceValue + ":" + target.value(TinkerFrame.TINKER_NAME);
					
					// try to find the vertex
					GraphTraversal<Edge, Edge> gt = tinker.g.traversal().E().has(TinkerFrame.TINKER_ID, edgeID);
					if(gt.hasNext()) {
						Edge retEdge = gt.next();
						Integer count = (Integer)retEdge.value(TinkerFrame.TINKER_COUNT);
						count++;
						retEdge.property(TinkerFrame.TINKER_COUNT, count);
					} else {
						source.addEdge(edgeID, target, TinkerFrame.TINKER_COUNT, 1);
					}
				}
			}
		}
		
		// also need to update the meta of this frame
		
		
		return null;
	}
	
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////

	private String getColumn() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if(grs != null) {
			return (String) grs.get(0);
		}
		
		List<String> vals = this.curRow.getAllStrValues();
		if(!vals.isEmpty()) {
			return vals.get(0);
		}
		
		throw new IllegalArgumentException("Must define the node type to remove");
	}

}
