package prerna.reactor.planner;

import static prerna.reactor.PixelPlanner.TINKER_NAME;
import static prerna.reactor.PixelPlanner.TINKER_TYPE;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.reactor.AbstractReactor;
import prerna.reactor.PixelPlanner;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GraphPlanReactor extends AbstractReactor {
	
	public GraphPlanReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PLANNER.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Map map = createVertStores2(getPlanner());
		planner.addProperty("DATA", "DATA", map);
		return null;
	}
	
	private Map createVertStores2(PixelPlanner planner) {
		Map<String, SEMOSSVertex> vertStore = new HashMap<String, SEMOSSVertex>();
		Map<String, SEMOSSEdge> edgeStore = new HashMap<String, SEMOSSEdge>();
		
		//get all edges
		GraphTraversal<Edge, Edge> edgesIt = planner.g.traversal().E();//.not(__.or(__.has(TINKER_TYPE, TINKER_FILTER), __.bothV().in().has(TINKER_TYPE, TINKER_FILTER), __.V().has(PRIM_KEY, true)));
		while(edgesIt.hasNext()) {
			Edge e = edgesIt.next();
			Vertex outV = e.outVertex();
			
			Vertex inV = e.inVertex();
			SEMOSSVertex outVert = getSEMOSSVertex(vertStore, outV);
			SEMOSSVertex inVert = getSEMOSSVertex(vertStore, inV);
			
			String edgeString = "EDGE/"+e.property("ID").value() + "";
			SEMOSSEdge semossE = new SEMOSSEdge(outVert, inVert, edgeString);
			edgeStore.put(edgeString, semossE);
			
			// need to add edge properties
			Iterator<Property<Object>> edgeProperties = e.properties();
			while(edgeProperties.hasNext()) {
				Property<Object> prop = edgeProperties.next();
				String propName = prop.key();
				if(!propName.equals("ID") && !propName.equals(TINKER_NAME) && !propName.equals(TINKER_TYPE)) {
					semossE.propHash.put(propName, prop.value());
				}
			}
		}
		
		// now i just need to get the verts with no edges
		GraphTraversal<Vertex, Vertex> vertIt = planner.g.traversal().V();//.not(__.or(__.has(TINKER_TYPE, TINKER_FILTER), __.in().has(TINKER_TYPE, TINKER_FILTER), __.has(PRIM_KEY, true)));
		while(vertIt.hasNext()) {
			Vertex outV = vertIt.next();
			getSEMOSSVertex(vertStore, outV);
		}
		
		
		Map retHash = new HashMap();
		retHash.put("nodes", vertStore);
		retHash.put("edges", edgeStore.values());
		return retHash;
	}
	
	/**
     * 
     * @param vertStore
     * @param tinkerVert
     * @return
     */
	private SEMOSSVertex getSEMOSSVertex(Map<String, SEMOSSVertex> vertStore, Vertex tinkerVert){
		Object value = tinkerVert.property(TINKER_NAME).value();
		String type = tinkerVert.property(TINKER_TYPE).value() + "";
		
		// New logic to construct URI - don't need to take into account base URI beacuse it sits on OWL and is used upon query creation
		if(!(value instanceof String || value instanceof Number)) {
			System.out.println("here");
		}
		String newValue = Utility.getInstanceName(value.toString());
		String uri = "NODE/"+type+"/"+newValue;
//		String uri = "http://semoss.org/ontologies/Concept/" + type + "/" + newValue;
		
		SEMOSSVertex semossVert = vertStore.get(uri);
		if(semossVert == null){
			semossVert = new SEMOSSVertex(uri);
			// generic - move anything that is a property on the node
//			Iterator<VertexProperty<Object>> vertexProperties = tinkerVert.properties();
//			while(vertexProperties.hasNext()) {
//				VertexProperty<Object> prop = vertexProperties.next();
//				String propName = prop.key();
//				if(!propName.equals(TINKER_ID) && !propName.equals(TINKER_NAME) && !propName.equals(TINKER_TYPE)) {
//					semossVert.propHash.put(propName, prop.value());
//				}
//			}
			vertStore.put(uri, semossVert);
		}
		return semossVert;
	}
	
	private PixelPlanner getPlanner() {
		GenRowStruct allNouns = getNounStore().getNoun(PixelDataType.PLANNER.getKey());
		PixelPlanner planner = null;
		if(allNouns != null) {
			planner = (PixelPlanner) allNouns.get(0);
			return planner;
		} else {
			return this.planner;
		}
	}
	
}
