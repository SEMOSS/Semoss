package prerna.reactor.frame.graph;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.TinkerFrame;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

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
		
		List<String> upstream = new Vector<String>();
		List<String> upstreamPhysical = new Vector<String>();
		
		List<String> downstream = new Vector<String>();
		List<String> downstreamPhysical = new Vector<String>();
		
		List<String[]> upstreamRels = meta.getUpstreamRelationships(nodeTypeToRemove);
		List<String[]> downstreamRels = meta.getDownstreamRelationships(nodeTypeToRemove);
		
		for(String up[] : upstreamRels) {
			upstream.add(up[0]);
			upstreamPhysical.add(meta.getPhysicalName(up[0]));
		}
		for(String[] down : downstreamRels) {
			downstream.add(down[1]);
			downstreamPhysical.add(meta.getPhysicalName(down[1]));
		}
		
		// we will iterate through the graph for all edges of this node type
		GraphTraversal<Vertex, Vertex> traversal = tinker.g.traversal().V().has(TinkerFrame.TINKER_TYPE, physicalTypeToRemove);
		while(traversal.hasNext()) {
			Vertex vert = traversal.next();
			
			List<Vertex> sourceVertices = new Vector<Vertex>();
			List<String> sourceUniqueName = new Vector<String>();
			
			List<Vertex> targetVertices = new Vector<Vertex>();
			List<String> targetUniqueName = new Vector<String>();
			
			Iterator<Edge> inEdges = vert.edges(Direction.IN);
			while(inEdges.hasNext()) {
				Edge edge = inEdges.next();
				// since we allow for loops
				// we can determine if this is that type of node
				// based on the edge relationship
				if(edge.property(TinkerFrame.TINKER_ID).value().toString().contains(nodeTypeToRemove)) {
					Vertex outVertex = edge.outVertex();
					String outVertexPhysical = outVertex.value(TinkerFrame.TINKER_TYPE);
					
					sourceVertices.add(outVertex);
					sourceUniqueName.add(upstream.get(upstreamPhysical.indexOf(outVertexPhysical)));
				}
			}
			
			Iterator<Edge> outEdges = vert.edges(Direction.OUT);
			while(outEdges.hasNext()) {
				Edge edge = outEdges.next();
				// since we allow for loops
				// we can determine if this is that type of node
				// based on the edge relationship
				if(edge.property(TinkerFrame.TINKER_ID).value().toString().contains(nodeTypeToRemove)) {
					Vertex inVertex = edge.inVertex();
					String inVertexPhysical = inVertex.value(TinkerFrame.TINKER_TYPE);

					targetVertices.add(inVertex);
					targetUniqueName.add(downstream.get(downstreamPhysical.indexOf(inVertexPhysical)));
				}
			}
			
			// we now need to connect every source vertex with every target vertex
			int numSource = sourceVertices.size();
			int numTarget = targetVertices.size();
			for(int i = 0; i < numSource; i++) {
				Vertex source = sourceVertices.get(i);
				String sourceName = sourceUniqueName.get(i);
				Object sourceValue = source.value(TinkerFrame.TINKER_NAME);

				for(int j = 0; j < numTarget; j++) {
					Vertex target = targetVertices.get(j);
					String targetName = targetUniqueName.get(j);
					Object targetValue = target.value(TinkerFrame.TINKER_NAME);
					
					String type = sourceName + TinkerFrame.EDGE_LABEL_DELIMETER + targetName;
					String edgeID = type + "/" + sourceValue + ":" + targetValue;
					
					// try to find the vertex
					GraphTraversal<Edge, Edge> gt = tinker.g.traversal().E().has(TinkerFrame.TINKER_ID, edgeID);
					if(gt.hasNext()) {
						Edge retEdge = gt.next();
						Integer count = (Integer)retEdge.value(TinkerFrame.TINKER_COUNT);
						count++;
						retEdge.property(TinkerFrame.TINKER_COUNT, count);
					} else {
						source.addEdge(type, target, TinkerFrame.TINKER_ID, edgeID, TinkerFrame.TINKER_COUNT, 1);
					}
				}
			}
			
			// now drop the intermediary node
			vert.remove();
		}
		
		// when we drop the meta, we remove all 
		// the relationships it has as well
		meta.dropVertex(nodeTypeToRemove);
		
		// now add all the upstream to the downstream
		for(String up : upstream) {
			for(String down : downstream) {
				// now we will connect the 2 together
				meta.addRelationship(up, down, "inner.join");
			}
		}
		
		return new NounMetadata(tinker, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
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
