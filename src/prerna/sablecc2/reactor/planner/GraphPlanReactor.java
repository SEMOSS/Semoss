package prerna.sablecc2.reactor.planner;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.PKSLPlanner;
import static prerna.sablecc2.reactor.PKSLPlanner.*;
import prerna.util.Utility;


public class GraphPlanReactor extends AbstractReactor {

	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		return null;
	}
	
	@Override
	public NounMetadata execute() {
		Map map = createVertStores2(getPlanner());
		planner.addProperty("DATA", "DATA", map);
		return null;
	}
	
	private Map createVertStores2(PKSLPlanner planner) {
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
			
			SEMOSSEdge semossE = new SEMOSSEdge(outVert, inVert, "https://semoss.org/Relation/"+e.property("ID").value() + "");
			edgeStore.put("https://semoss.org/Relation/"+e.property("ID").value() + "", semossE);
			
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
		String newValue = Utility.getInstanceName(value.toString());
		String uri = "http://semoss.org/ontologies/Concept/" + type + "/" + newValue;
		
		SEMOSSVertex semossVert = vertStore.get(uri);
		if(semossVert == null){
			semossVert = new SEMOSSVertex(uri);
			// generic - move anything that is a property on the node
			Iterator<VertexProperty<Object>> vertexProperties = tinkerVert.properties();
			while(vertexProperties.hasNext()) {
				VertexProperty<Object> prop = vertexProperties.next();
				String propName = prop.key();
				if(!propName.equals(TINKER_ID) && !propName.equals(TINKER_NAME) && !propName.equals(TINKER_TYPE)) {
					semossVert.propHash.put(propName, prop.value());
				}
			}
			vertStore.put(uri, semossVert);
		}
		return semossVert;
	}
	
	private PKSLPlanner getPlanner() {
		GenRowStruct allNouns = getNounStore().getNoun(PkslDataTypes.PLANNER.toString());
		PKSLPlanner planner = null;
		if(allNouns != null) {
			planner = (PKSLPlanner) allNouns.get(0);
			return planner;
		} else {
			return this.planner;
		}
	}
	
}
