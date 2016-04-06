package prerna.ds;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
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
	public static final String DB_NAME = "DB_NAME";
//	public static final String DB_ARRAY = "DB_ARRAY";
	public static final String ALIAS_TYPE = "ALIAS_TYPE";
	public static final String PHYSICAL_NAME = "PHYSICAL_NAME";
	public static final String PHYSICAL_URI = "PHYSICAL_URI";
	public static final String LOGICAL_NAME = "LOGICAL_NAME";
	public static final String PARENT = "PARENT";
	public static final String DATATYPE = "DATATYPE";
	
	public static final String PRIM_KEY = "PRIM_KEY";
	public static final String META = "META";
	public static final String EMPTY = "_";
	
	/**
	 * CURRENT PROPERTY STRUCTURE::::::::::::::::::
	 * 		Key	Value	Description
			TYPE	META	Has type META to differentiate from instance nodes
			NAME	System_1	What the unique name for this column is
			VALUE	System	This aligns with the TYPE for the instance nodes
			ALIAS	[System_1, System, Application, etc]	This is a list for all different aliases that have been given to this node (can be user defined, db physical name, db logical name, etc)
			DERIVED	m:Sum(…)	String representing how to calc if it is derived (PKQL?) IF IT IS DERIVED
			PROPERTY	Interface	States what node (unique name) this node is a property of (IF IT IS A PROPERTY)
			System_1	"{
				Type: DB Logical Name or DB Physical Name or User Defined, etc
				DB: [TAP_Core, TAP_Site]
				}"	Holds all meta data around that alias -- where did it come from etc.
			System	"{
				Type: DB Logical Name or DB Physical Name or User Defined, etc
				DB: [TAP_Core, TAP_Site]
				}"	
			Application	"{
				Type: DB Logical Name or DB Physical Name or User Defined, etc
				DB: [TAP_Core, TAP_Site]
				}"	
			DATABASE	[TAP_Core, TAP_Site, etc]	List for all different databases that contributed to this column
			TAP_Core	"{
				Physical Name: System
				Physical URI: http://semoss.org/ontologies/Concept/System
				Logical Name: System
				Query Struct: ???
				Stringified OWL: ???
				}"	Holds all meta data around that database
			TAP_Site	"{
				Physical Name: System
				Physical URI: http://semoss.org/ontologies/Concept/System
				Logical Name: System
				Query Struct: ???
				Stringified OWL: ???
				}"	

	 * @param g
	 */
	public TinkerMetaData(TinkerGraph g) {
		this.g = g;
	}
	
	public TinkerMetaData() {
		TinkerGraph g = TinkerGraph.open();
		g.createIndex(Constants.TYPE, Vertex.class);
		g.createIndex(Constants.ID, Vertex.class);
		g.createIndex(T.label.toString(), Edge.class);
		g.createIndex(Constants.ID, Edge.class);
		this.g = g;
	}
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	/**
	 * 
	 * @param vert
	 * @param engineName
	 * @param logicalName
	 * @param instancesType
	 * @param physicalUri
	 * @param dataType
	 */
	public void addEngineMeta(Vertex vert, String engineName, String logicalName, String instancesType, String physicalUri, String dataType) {
		
		// now add the meta object if it doesn't already exist
		Map<Object, Object> metaData = null;
		if(!vert.property(engineName).isPresent()){
			metaData = new HashMap<Object, Object>();
			vert.property(engineName, metaData);
		}
		else{
			metaData = vert.value(engineName);
		}
		
		metaData.put(IMetaData.NAME_TYPE.DB_LOGICAL, logicalName);
		metaData.put(IMetaData.NAME_TYPE.DB_PHYSICAL_NAME, instancesType);
		metaData.put(IMetaData.NAME_TYPE.DB_PHYSICAL_URI, physicalUri);
		
		if(dataType != null) {
			metaData.put(DATATYPE, dataType);
		}
	}


	@Override
	/**
	 * 
	 */
	public Vertex upsertVertex(String type, String uniqueName, String logicalName, String instancesType, String physicalUri, String engineName, String dataType, String parentIfProperty) {
		Vertex vert = upsertVertex(type, uniqueName, logicalName);
		
		// add data type
		determineDataType(vert, dataType);
		
		// add aliases
		String[] curAl = new String[]{instancesType, physicalUri};
		addToMultiProperty(vert, ALIAS, logicalName, curAl);
		
		addAliasMeta(vert, logicalName, NAME_TYPE.DB_LOGICAL, engineName);
		addAliasMeta(vert, instancesType, NAME_TYPE.DB_PHYSICAL_NAME, engineName);
		addAliasMeta(vert, physicalUri, NAME_TYPE.DB_PHYSICAL_URI, engineName);
		
		// add engine
		addToMultiProperty(vert, DB_NAME, engineName);
		
		addEngineMeta(vert, engineName, logicalName, instancesType, physicalUri, dataType);
		
		// add parent if it is a property
		if(parentIfProperty != null){
			vert.property(PARENT, parentIfProperty);
		}
		
		return vert;
	}
	
//	public void addAlias(String type, String uniqueName, String logicalName, String aliasProperty, String newAlias) {
//		Vertex vert = upsertVertex(type, uniqueName, logicalName);
//		addToMultiProperty(vert, ALIAS, logicalName, newAlias);
//		addAliasMeta(vert, logicalName, );
//	}
	
	private void determineDataType(Vertex vert, String dataType) {
		if(dataType == null || dataType.isEmpty()) {
			return;
		}
		
		dataType = dataType.toUpperCase();
		
		String currType = null;
		if(vert.property(DATATYPE).isPresent()){
			currType = vert.value(DATATYPE);
		}
		
		if(currType == null) {
			if(dataType.equals("TYPE:STRING") || dataType.equals("TYPE:TEXT") || dataType.contains("TYPE:VARCHAR")) {
				vert.property(DATATYPE, "STRING");
			} 
			else if(dataType.contains("INT") || dataType.contains("DECIMAL") || dataType.contains("DOUBLE") || dataType.contains("FLOAT") || dataType.contains("LONG") || dataType.contains("BIGINT")
					|| dataType.contains("TINYINT") || dataType.contains("SMALLINT")){
				vert.property(DATATYPE, "NUMBER");
			} 
			else if(dataType.contains("DATE")) {
				vert.property(DATATYPE, "DATE");
			}
		} else {
			// if current is string or new col is string
			// column must now be a string
			if(currType.equals("STRING") || dataType.equals("TYPE:STRING") || dataType.equals("TYPE:TEXT") || dataType.contains("TYPE:VARCHAR")) {
				vert.property(DATATYPE, "STRING");
			}
			// if current is a number and new is a number
			// column is still number
			else if(currType.equals("NUMBER") && ( dataType.contains("INT") || dataType.contains("DECIMAL") || dataType.contains("DOUBLE") || dataType.contains("FLOAT") || dataType.contains("LONG") || dataType.contains("BIGINT")
					|| dataType.contains("TINYINT") || dataType.contains("SMALLINT") )){
				// no change
				// vert.property(DATATYPE, "NUMBER");
			}
			// if current is date and new is date
			// column is still date
			else if(currType.equals("DATE") && dataType.contains("DATE")) {
				// no change
				// vert.property(DATATYPE, "DATE");
			}
			// any other situation, you have mixed types or numbers and dates... declare it a string for now //TODO
			else {
				vert.property(DATATYPE, "STRING");
			}
			
		}
		
	}
	
	/**
	 * 
	 * @param vert
	 * @param propKey - property to add to
	 * @param newValue - value to add to propkey
	 * @param newVal - optional additional values to add to propKey
	 */
	private void addToMultiProperty(Vertex vert, String propKey, String newValue, String... newVal){
//		printNode(vert);
		vert.property(VertexProperty.Cardinality.set, propKey, newValue);
		for(String val : newVal){
			vert.property(VertexProperty.Cardinality.set, propKey, val);
		}
//		printNode(vert);
	}
	
	/**
	 * 
	 * @param vert - meta vertex to modify
	 * @param name - alias being added
	 * @param type - how the alias is defined e.g. DB_LOGICAL
	 * @param engineName - if db defined name, what db defined it
	 */
	private void addAliasMeta(Vertex vert, String name, NAME_TYPE type, String engineName){
		// now add the meta object if it doesn't already exist
		Map<String, Object> metaData = null;
		if(!vert.property(name).isPresent()){
			metaData = new HashMap<String, Object>();
			metaData.put(DB_NAME, new Vector<String>());
			metaData.put(ALIAS_TYPE, new Vector<IMetaData.NAME_TYPE>());
			vert.property(name, metaData);
		}
		else{
			metaData = vert.value(name);
		}
		
		if(engineName!=null){
			((Vector<String>)metaData.get(DB_NAME)).add(engineName);
		}
		
		((Vector<IMetaData.NAME_TYPE>)metaData.get(ALIAS_TYPE)).add(type);
	}

	// create or add vertex
	protected Vertex upsertVertex(String type, Object name, Object value)
	{
		if(name == null) name = EMPTY;
		if(value == null) value = EMPTY;
		// checks to see if the vertex is there already
		// if so retrieves the vertex
		// if not inserts the vertex and then returns that vertex
		Vertex retVertex = null;
		// try to find the vertex
//		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.TYPE, type).has(Constants.ID, type + ":" + data);
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.ID, type + ":" + name);
		if(gt.hasNext()) {
			retVertex = gt.next();
		} else {
			//retVertex = g.addVertex(Constants.ID, type + ":" + data, Constants.VALUE, value, Constants.TYPE, type, Constants.NAME, data, Constants.FILTER, false); //should we add a filter flag to each vertex?
			if(name instanceof Number) {
				// need to keep values as they are, not with XMLSchema tag
				retVertex = g.addVertex(Constants.ID, type + ":" + name, Constants.VALUE, name, Constants.TYPE, type, Constants.NAME, name);// push the actual value as well who knows when you would need it
			} else {
				LOGGER.debug(" adding vertex ::: " + Constants.ID + " = " + type + ":" + name+ " & " + Constants.VALUE+ " = " + value+ " & " + Constants.TYPE+ " = " + type+ " & " + Constants.NAME+ " = " + name);
				retVertex = g.addVertex(Constants.ID, type + ":" + name, Constants.VALUE, value, Constants.TYPE, type, Constants.NAME, name);// push the actual value as well who knows when you would need it
			}
			
			if(META.equals(type)) {
				// all new meta nodes are defaulted as unfiltered and not prim keys
				retVertex.property(Constants.FILTER, false);
				retVertex.property(PRIM_KEY, false);
			}
		}
		return retVertex;
	}

	public Map<String, String> getNodeTypesForUniqueAlias() {
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.TYPE, META);
		Map<String, String> retMap = new Hashtable<String, String>();
		while(gt.hasNext()) {
			Vertex vert = gt.next();
			String uniqueName = vert.value(Constants.NAME);
			if(vert.property(DATATYPE).isPresent()) {
				String type = vert.value(DATATYPE);
				retMap.put(uniqueName, type);
			} else {
				retMap.put(uniqueName, "TYPE NOT STORED IN OWL, NEED TO UPDATE DB");
			}
		}
		
		return retMap;
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

	@Override
	public Set<String> getEnginesForUniqueName(String nodeUniqueName) {
		GraphTraversal<Vertex, Vertex> trav = g.traversal().V().has(Constants.TYPE, META).has(Constants.NAME, nodeUniqueName);
		if(trav.hasNext()){
			Vertex node = trav.next();
			Iterator<VertexProperty<Object>> dbSet = node.properties(DB_NAME);
			Set<String> engines = new HashSet<String>();
			while(dbSet.hasNext()){
				engines.add(dbSet.next().value().toString());
			}
			return engines;
		}
		else return null;
	}
	
	
	private void printNode(Vertex v){
		System.out.println(v.toString());
		Iterator<VertexProperty<Object>> props = v.properties();
		while(props.hasNext()){
			VertexProperty<Object> prop = props.next();
			Iterator<Object> vals = prop.values();
			while(vals.hasNext()){
				System.out.println(vals.next());
			}
		}
	}

}
