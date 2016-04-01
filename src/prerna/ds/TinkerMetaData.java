package prerna.ds;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import prerna.algorithm.api.IMetaData;
import prerna.util.Constants;

public class TinkerMetaData implements IMetaData {

	private static final Logger LOGGER = LogManager.getLogger(TinkerMetaData.class.getName());

	protected GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
	protected TinkerGraph g = null;

	public static final String ALIAS = "ALIAS";
	public static final String DB = "DB";
	public static final String ALIAS_TYPE = "ALIAS_TYPE";
	public static final String PHYSICAL_NAME = "PHYSICAL_NAME";
	public static final String PHYSICAL_URI = "PHYSICAL_URI";
	public static final String LOGICAL_NAME = "LOGICAL_NAME";
	public static final String PARENT = "PARENT";

	public static final String PRIM_KEY = "PRIM_KEY";
	public static final String META = "META";
	public static final String EMPTY = "_";
	
	
	public TinkerMetaData(TinkerGraph g) {
		this.g = g;
	}

	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}


	
	public void addEngineMeta(Vertex vert, String engineName, String logicalName, String instancesType, String physicalUri) {
		
		// now add the meta object if it doesn't already exist
		Map<Object, Object> metaData = null;
		if(vert.property(DB).isPresent()){
			metaData = new HashMap<Object, Object>();
			vert.property(engineName, metaData);
		}
		else{
			metaData = vert.value(engineName);
		}
		
		metaData.put(IMetaData.NAME_TYPE.DB_LOGICAL, logicalName);
		metaData.put(IMetaData.NAME_TYPE.DB_PHYSICAL_NAME, instancesType);
		metaData.put(IMetaData.NAME_TYPE.DB_PHYSICAL_URI, physicalUri);
	}




	@Override
	public Vertex upsertVertex(String type, String uniqueName, String logicalName, String instancesType, String physicalUri, String engineName, String parentIfProperty) {
		Vertex vert = upsertVertex(type, uniqueName, logicalName);
		
		// add aliases
		String[] curAl = new String[]{instancesType, physicalUri};
		addToMultiProperty(vert, ALIAS, logicalName, curAl);
		
		addAliasMeta(vert, logicalName, NAME_TYPE.DB_LOGICAL, engineName);
		addAliasMeta(vert, instancesType, NAME_TYPE.DB_PHYSICAL_NAME, engineName);
		addAliasMeta(vert, physicalUri, NAME_TYPE.DB_PHYSICAL_URI, engineName);
		
		// add engine
		addToMultiProperty(vert, DB, engineName);
		
		addEngineMeta(vert, engineName, logicalName, instancesType, physicalUri);
		
		// add parent if it is a property
		if(parentIfProperty != null){
			vert.property(PARENT, parentIfProperty);
		}
		
		return vert;
	}
	
	private void addToMultiProperty(Vertex vert, String propKey, String newValue, String... newVal){
		vert.property(VertexProperty.Cardinality.set, propKey, newValue);
		for(String val : newVal){
			vert.property(VertexProperty.Cardinality.set, propKey, val);
		}
	}
	
	private void addAliasMeta(Vertex vert, String name, NAME_TYPE type, String engineName){
		// now add the meta object if it doesn't already exist
		Map<String, Object> metaData = null;
		if(!vert.property(name).isPresent()){
			metaData = new HashMap<String, Object>();
			metaData.put(DB, new Vector<String>());
			metaData.put(ALIAS_TYPE, new Vector<IMetaData.NAME_TYPE>());
			vert.property(name, metaData);
		}
		else{
			metaData = vert.value(name);
		}
		
		if(engineName!=null){
			((Vector<String>)metaData.get(DB)).add(engineName);
		}
		
		((Vector<IMetaData.NAME_TYPE>)metaData.get(ALIAS_TYPE)).add(type);
	}

	// create or add vertex
	protected Vertex upsertVertex(String type, Object data, Object value)
	{
		if(data == null) data = EMPTY;
		if(value == null) value = EMPTY;
		// checks to see if the vertex is there already
		// if so retrieves the vertex
		// if not inserts the vertex and then returns that vertex
		Vertex retVertex = null;
		// try to find the vertex
//		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.TYPE, type).has(Constants.ID, type + ":" + data);
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.ID, type + ":" + data);
		if(gt.hasNext()) {
			retVertex = gt.next();
		} else {
			//retVertex = g.addVertex(Constants.ID, type + ":" + data, Constants.VALUE, value, Constants.TYPE, type, Constants.NAME, data, Constants.FILTER, false); //should we add a filter flag to each vertex?
			if(data instanceof Number) {
				// need to keep values as they are, not with XMLSchema tag
				retVertex = g.addVertex(Constants.ID, type + ":" + data, Constants.VALUE, data, Constants.TYPE, type, Constants.NAME, data);// push the actual value as well who knows when you would need it
			} else {
				LOGGER.debug(" adding vertex ::: " + Constants.ID + " = " + type + ":" + data+ " & " + Constants.VALUE+ " = " + value+ " & " + Constants.TYPE+ " = " + type+ " & " + Constants.NAME+ " = " + data);
				retVertex = g.addVertex(Constants.ID, type + ":" + data, Constants.VALUE, value, Constants.TYPE, type, Constants.NAME, data);// push the actual value as well who knows when you would need it
			}
			
			if(META.equals(type)) {
				// all new meta nodes are defaulted as unfiltered and not prim keys
				retVertex.property(Constants.FILTER, false);
				retVertex.property(PRIM_KEY, false);
			}
		}
		return retVertex;
	}




	@Override
	public Map<String, String> getProperties() {
		Map<String, String> retMap = new HashMap<String, String>();
		GraphTraversal<Vertex, Vertex> trav = g.traversal().V().has(Constants.TYPE, META).has(PARENT);
		while (trav.hasNext()){
			Vertex vert = trav.next();
			String prop = vert.property(Constants.NAME).value() +"";
			String parent = vert.property(PARENT).value()+"";
			retMap.put(prop, parent);
		}
		return retMap;
	}




	@Override
	public String getPhysicalUriForNode(String nodeUniqueName, String engineName) {
		GraphTraversal<Vertex, Vertex> trav = g.traversal().V().has(Constants.TYPE, META).has(Constants.NAME, nodeUniqueName);
		if(trav.hasNext()){
			Vertex node = trav.next();
			Map<Object, Object> engineMap = (Map<Object, Object>) node.property(engineName).value();
			String physUri = (String) engineMap.get(IMetaData.NAME_TYPE.DB_PHYSICAL_URI);
			return physUri;
		}
		else return null;
	}

}
