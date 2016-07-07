package prerna.ds;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Io.Builder;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import prerna.algorithm.api.IMetaData;
import prerna.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.MyGraphIoRegistry;
import prerna.util.Utility;

public class TinkerMetaData implements IMetaData {

	private static final Logger LOGGER = LogManager.getLogger(TinkerMetaData.class.getName());

	protected GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
	protected TinkerGraph g = null;

	public static final String ALIAS = "ALIAS";
	public static final String DB_NAME = "DB_NAME";
	public static final String ALIAS_TYPE = "ALIAS_TYPE";
	public static final String PHYSICAL_NAME = "PHYSICAL_NAME";
	public static final String PHYSICAL_URI = "PHYSICAL_URI";
	public static final String LOGICAL_NAME = "LOGICAL_NAME";
	public static final String PARENT = "PARENT";
	public static final String DATATYPE = "DATATYPE";
	public static final String ORDER = "ORDER";
	public static final String DB_DATATYPE = "DB_DATATYPE";
	public static final String DERIVED = "DERIVED_COLUMN";
	public static final String DERIVED_CALCULATION = "DERIVED_CALCULATION";
	public static final String DERIVED_USING = "DERIVED_USING";

	private String latestPrimKey;

	public static final String PRIM_KEY = "PRIM_KEY";
	public static final String META = "META";
	public static final String edgeLabelDelimeter = "+++";
	private int orderIdx = -1;
	
	/**
	 * CURRENT PROPERTY STRUCTURE::::::::::::::::::
	 * 		Key	Value	Description
			TYPE	META	Has type META to differentiate from instance nodes
			NAME	System_1	What the unique name for this column is
			ORDER	1
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

	 */
	public TinkerMetaData() {
		TinkerGraph g = TinkerGraph.open();
		g.createIndex(Constants.TYPE, Vertex.class);
		g.createIndex(Constants.ID, Vertex.class);
		g.createIndex(T.label.toString(), Edge.class);
		g.createIndex(Constants.ID, Edge.class);
		this.g = g;
	}
	
	/*
	 * Return's nodes Contrant.Value/Physical logical name from the inputted physical name
	 */
	@Override
	public String getValueForUniqueName (String metaNodeName) {
		String metaNodeValue = null;
		// get metamodel info for metaModeName
		GraphTraversal<Vertex, Vertex> metaT = g.traversal().V().has(Constants.TYPE, META).has(Constants.NAME, metaNodeName);
		
		// if metaT has metaNodeName then find the value else return metaNodeName
		if (metaT.hasNext()) {
			Vertex startNode = metaT.next();
			metaNodeValue = startNode.property(Constants.VALUE).value() + "";
		}

		return metaNodeValue;
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
		
		metaData.put(IMetaData.NAME_TYPE.DB_LOGICAL_NAME, logicalName);
		metaData.put(IMetaData.NAME_TYPE.DB_PHYSICAL_NAME, instancesType);
		metaData.put(IMetaData.NAME_TYPE.DB_PHYSICAL_URI, physicalUri);
		
		if(dataType != null) {
			metaData.put(DATATYPE, dataType);
		}
	}
	
	@Override
	public void storeDataTypes(String[] uniqueNames, String[] dataTypes){
		for(int i = 0; i < uniqueNames.length; i++) {
			this.storeDataType(uniqueNames[i], dataTypes[i]);
		}
	}
	
	@Override
	public void storeDataType(String uniqueName, String dataType) {
		Vertex vert = getExistingVertex(uniqueName);
		if(dataType == null || dataType.isEmpty()) {
			vert.property(DB_DATATYPE, "VARCHAR(800)");
			return;
		}
		vert.property(DB_DATATYPE, dataType);
		
		dataType = dataType.toUpperCase();
		
		DATA_TYPES currType = null;
		if(vert.property(DATATYPE).isPresent()){
			currType = vert.value(DATATYPE);
		}
		
		if(currType == null) {
			if(dataType.contains("STRING") || dataType.contains("TEXT") || dataType.contains("VARCHAR")) {
				vert.property(DATATYPE, IMetaData.DATA_TYPES.STRING);
			} 
			else if(dataType.contains("INT") || dataType.contains("DECIMAL") || dataType.contains("DOUBLE") || dataType.contains("FLOAT") || dataType.contains("LONG") || dataType.contains("BIGINT")
					|| dataType.contains("TINYINT") || dataType.contains("SMALLINT") || dataType.contains("NUMBER")){
				vert.property(DATATYPE, IMetaData.DATA_TYPES.NUMBER);
			} 
			else if(dataType.contains("DATE")) {
				vert.property(DATATYPE, IMetaData.DATA_TYPES.DATE);
			}
		} else {
			// if current is string or new col is string
			// column must now be a string
			if(currType.equals(IMetaData.DATA_TYPES.STRING) || dataType.contains("STRING") || dataType.contains("TEXT") || dataType.contains("VARCHAR")) {
				vert.property(DATATYPE, IMetaData.DATA_TYPES.STRING);
			}
			// if current is a number and new is a number
			// column is still number
			else if(currType.equals(IMetaData.DATA_TYPES.NUMBER) && ( dataType.contains("INT") || dataType.contains("DECIMAL") || dataType.contains("DOUBLE") || dataType.contains("FLOAT") || dataType.contains("LONG") || dataType.contains("BIGINT")
					|| dataType.contains("TINYINT") || dataType.contains("SMALLINT") )){
				// no change
				// vert.property(DATATYPE, "NUMBER");
			}
			// if current is date and new is date
			// column is still date
			else if(currType.equals(IMetaData.DATA_TYPES.DATE) && dataType.contains("DATE")) {
				// no change
				// vert.property(DATATYPE, "DATE");
			}
			// any other situation, you have mixed types or numbers and dates... declare it a string for now //TODO
			else {
				vert.property(DATATYPE, IMetaData.DATA_TYPES.STRING);
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
	private Vertex upsertVertex(Object uniqueName, Object howItsCalledInDataFrame)
	{
		String type = META;
		// checks to see if the vertex is there already
		// if so retrieves the vertex
		// if not inserts the vertex and then returns that vertex
		Vertex retVertex = getExistingVertex(uniqueName);
		// if we were unable to get the existing vertex... time to create a new one
		if (retVertex == null){
			LOGGER.debug(" adding vertex ::: " + Constants.ID + " = " + type + ":" + uniqueName+ " & " + Constants.VALUE+ " = " + howItsCalledInDataFrame+ " & " + Constants.TYPE+ " = " + type+ " & " + Constants.NAME+ " = " + uniqueName);
			retVertex = g.addVertex(Constants.ID, type + ":" + uniqueName, Constants.VALUE, howItsCalledInDataFrame, Constants.TYPE, type, Constants.NAME, uniqueName);// push the actual value as well who knows when you would need it
			// all new meta nodes are defaulted as unfiltered and not prim keys
			retVertex.property(Constants.FILTER, false);
			if(uniqueName.toString().startsWith(TinkerFrame.PRIM_KEY)) {
				setPrimKey(uniqueName.toString(), true);
			}
			else {
				retVertex.property(PRIM_KEY, false);
			}
			retVertex.property(ORDER, getOrderIdx());
		}
		return retVertex;
	}

	private int getOrderIdx() {
		int retIdx = -1;
		if(this.orderIdx >= 0){ // this means that it has been set (either after deserializing or starting fresh)
			retIdx = this.orderIdx;
		}
		else {
			GraphTraversal<Vertex, Number> trav = g.traversal().V().has(Constants.TYPE, META).values(ORDER).max();
			if(trav.hasNext()){
				retIdx = (int) trav.next() + 1;
			}
			else {
				retIdx = 0;
			}
			this.orderIdx = retIdx;
		}
		this.orderIdx++; // set it up for next time
		return retIdx;
	}

	private Edge upsertEdge(Vertex fromVertex, Vertex toVertex)
	{
		Edge retEdge = null;
		String type = META + edgeLabelDelimeter + META;
		String edgeID = type + "/" + fromVertex.value(Constants.NAME) + ":" + toVertex.value(Constants.NAME);
		// try to find the vertex
		GraphTraversal<Edge, Edge> gt = g.traversal().E().has(Constants.ID, edgeID);
		if(gt.hasNext()) {
			retEdge = gt.next(); // COUNTS HAVE NO MEANING IN META. REMOVED.
		}
		else {
			retEdge = fromVertex.addEdge(type, toVertex, Constants.ID, edgeID);
		}

		return retEdge;
	}
	
	private Vertex getExistingVertex(Object uniqueName){
		Vertex retVertex = null;
		// try to find the vertex
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.ID, META + ":" + uniqueName);
		if(gt.hasNext()) {
			retVertex = gt.next();
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
			if(node.property(engineName).isPresent()) {
				Map<Object, Object> engineMap = (Map<Object, Object>) node.property(engineName).value();
				String physUri = (String) engineMap.get(IMetaData.NAME_TYPE.DB_PHYSICAL_URI);
				return physUri;
			} else {
				return nodeUniqueName;
			}
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
	
	@Override
	public void dropVertex(String uniqueName) {
		Vertex vert = getExistingVertex(uniqueName);
		vert.remove();
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

//	@Override
//	public void storeVertex(Object uniqueName, Object howItsCalledInDataFrame){
//		upsertVertex(uniqueName, howItsCalledInDataFrame);
//	}
//
//	@Override
//	public void storeProperty(Object uniqueName, Object howItsCalledInDataFrame, Object parentUniqueName){
//		Vertex vert = upsertVertex(uniqueName, howItsCalledInDataFrame);
//		vert.property(PARENT, parentUniqueName);
//	}

	@Override
	public void storeRelation(String uniqueName1, String uniqueName2) {
		Vertex outVert = getExistingVertex(uniqueName1);
		Vertex inVert = getExistingVertex(uniqueName2);
		upsertEdge(outVert, inVert);
	}

	@Override
	public void storeUserDefinedAlias(String uniqueName, String aliasName) {
		Vertex vert = getExistingVertex(uniqueName);
		addToMultiProperty(vert, ALIAS, aliasName);
		addAliasMeta(vert, aliasName, NAME_TYPE.USER_DEFINED, null);
	}

	//TODO: need to see how this is working and interacting now since new OWL is added
	//TODO: need to see how this is working and interacting now since new OWL is added
	//TODO: need to see how this is working and interacting now since new OWL is added
	@Override
	public void storeEngineDefinedVertex(String uniqueName, String uniqueParentNameIfProperty, String engineName, String queryStructName) {
		
		//get the rest of the needed information off the owl
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
		
		String physicalName = null;
		String physicalUri = null;
		//check if property
		if(queryStructName.contains("__")){
			physicalName = queryStructName.substring(queryStructName.indexOf("__")+2);
			physicalUri = engine.getTransformedNodeName(Constants.DISPLAY_URI+physicalName, false);
		}
		else{
			physicalName = Utility.getInstanceName(engine.getTransformedNodeName(Constants.DISPLAY_URI+queryStructName, false));
			physicalUri = engine.getConceptUri4PhysicalName(physicalName);
		}
		
		// stupid check
		if(engineName.equals(Constants.LOCAL_MASTER_DB_NAME)) {
			physicalUri = Constants.DISPLAY_URI + uniqueName;
		}
		
		//TODO: this needs to be updated - used to be getting the logical from the engine
		//TODO: this needs to be updated - used to be getting the logical from the engine
		String logicalName = Utility.getInstanceName(physicalUri);
//		String physicalName = Utility.getInstanceName(physicalUri);
		String dataType = engine.getDataTypes(physicalUri);

		Vertex vert = upsertVertex(uniqueName, logicalName);
		addToMultiProperty(vert, DB_NAME, engineName);
		
		//store it to the vertex
		addEngineMeta(vert, engineName, logicalName, physicalName, physicalUri, dataType);
		
		// add all of that information in terms of aliases as well
		String[] curAl = new String[]{physicalName, physicalUri};
		addToMultiProperty(vert, ALIAS, logicalName, curAl);
		
		addAliasMeta(vert, logicalName, NAME_TYPE.DB_LOGICAL_NAME, engineName);
		addAliasMeta(vert, physicalName, NAME_TYPE.DB_PHYSICAL_NAME, engineName);
		addAliasMeta(vert, queryStructName, NAME_TYPE.DB_QUERY_STRUCT_NAME, engineName);
		addAliasMeta(vert, physicalUri, NAME_TYPE.DB_PHYSICAL_URI, engineName);
		
		//store the datatype
		storeDataType(uniqueName, dataType);
		
		if(uniqueParentNameIfProperty != null){
			vert.property(PARENT, uniqueParentNameIfProperty);
		}
	}

	@Override
	public void setFiltered(String uniqueName, boolean filtered) {
		Vertex v = getExistingVertex(uniqueName);
		v.property(Constants.FILTER, filtered);
	}

	@Override
	public void setPrimKey(String uniqueName, boolean primKey) {
		Vertex v = getExistingVertex(uniqueName);
		v.property(PRIM_KEY, primKey);
		this.latestPrimKey = uniqueName;
	}
	
	@Override
	public void setDerived(String uniqueName, boolean derived) {
		Vertex v = getExistingVertex(uniqueName);
		v.property(DERIVED, derived);
	}
	

	@Override
	public void setDerivedCalculation(String uniqueName, String calculation) {
		Vertex v = getExistingVertex(uniqueName);
		v.property(DERIVED_CALCULATION, calculation);
	}

	@Override
	public void setDerivedUsing(String uniqueName, String... otherUniqueNames) {
		Vertex v = getExistingVertex(uniqueName);
		v.property(DERIVED_USING, otherUniqueNames);
	}
	
	
//////////////////::::::::::::::::::::::: GETTER METHODS :::::::::::::::::::::::::::::::://////////////////////////////

	@Override
	public Set<String> getAlias(String uniqueName) {
		Vertex vert = getExistingVertex(uniqueName);
		if(vert.properties(ALIAS).hasNext()) {
			Set<String> aliasSet = new TreeSet<String>();
			Iterator<VertexProperty<Object>> props = vert.properties(ALIAS);
			while(props.hasNext()) {
				aliasSet.add(props.next().value() + "");
			}
			return aliasSet;
		}
		return null;
	}

	@Override
	public Map<String, Map<String, Object>> getAliasMetaData(String uniqueName) {
		Vertex vert = getExistingVertex(uniqueName);
		if(vert.properties(ALIAS).hasNext()) {
			Map<String, Map<String, Object>> aliasMetaData = new TreeMap<String, Map<String, Object>>();
			Iterator<VertexProperty<Object>> props = vert.properties(ALIAS);
			while(props.hasNext()) {
				String prop = props.next().value() + "";
				if(vert.property(prop).isPresent()) {
					Map<String, Object> metaData = vert.value(prop);
					aliasMetaData.put(prop, metaData);
				}
			}
			return aliasMetaData;
		}
		return null;
	}

	@Override
	public IMetaData.DATA_TYPES getDataType(String uniqueName) {
		Vertex vert = getExistingVertex(uniqueName);
		if(vert.property(DATATYPE).isPresent()) {
			return vert.value(DATATYPE);
		}
		return IMetaData.DATA_TYPES.STRING;
	}

	public String getDBDataType(String uniqueName) {
		Vertex vert = getExistingVertex(uniqueName);
		if(vert.property(DB_DATATYPE).isPresent()) {
			return vert.value(DB_DATATYPE);
		}
		return "STRING";
	}
	@Override
	public boolean isFiltered(String uniqueName) {
		Vertex vert = getExistingVertex(uniqueName);
		if(vert.property(Constants.FILTER).isPresent()) {
			return vert.value(Constants.FILTER);
		}
		return false;
	}

	@Override
	public boolean isPrimKey(String uniqueName) {
		Vertex vert = getExistingVertex(uniqueName);
		if(vert.property(PRIM_KEY).isPresent()) {
			return vert.value(PRIM_KEY);
		}
		return false;
	}
	
	@Override
	public boolean isDerived(String uniqueName) {
		Vertex vert = getExistingVertex(uniqueName);
		if(vert.property(DERIVED).isPresent()) {
			return vert.value(DERIVED);
		}
		return false;
	}
	
	@Override
	public QueryStruct getQueryStruct(String startName) {
		QueryStruct qs = new QueryStruct();
		List<String> travelledEdges = new Vector<String>();

		Vertex startVert = null;
		if( startName != null && !startName.isEmpty()){
			startVert = getExistingVertex(startName);
		}
		else {
			startVert = g.traversal().V().next();
		}
		
		if(startVert != null) {
			visitNode(startVert, travelledEdges, qs);
			return qs;
		}
		return null;
	}
	
	public void visitNode(Vertex vert, List<String> travelledEdges, QueryStruct qs) {
		String origName = vert.value(Constants.NAME);
		String vertParent = null;
		if(vert.property(PARENT).isPresent()) {
			vertParent = vert.property(PARENT).value() + "";
		}

		GraphTraversal<Vertex, Vertex> downstreamIt = g.traversal().V().has(Constants.TYPE, META).has(Constants.ID, vert.property(Constants.ID).value()).out(META + TinkerFrame.edgeLabelDelimeter + META);
		while (downstreamIt.hasNext()) {
			Vertex nodeV = downstreamIt.next();
			if(nodeV.property(PRIM_KEY).isPresent()) {
				if((boolean) nodeV.property(PRIM_KEY).value()) {
					visitNode(nodeV, travelledEdges, qs);
					continue;
				}
			}
			String nameNode = nodeV.property(Constants.NAME).value() + "";
			
			String edgeKey = origName + TinkerFrame.edgeLabelDelimeter + nameNode;
			if(!travelledEdges.contains(edgeKey)) {
				travelledEdges.add(edgeKey);
				
				String nodeVParent = null;
				if(nodeV.property(PARENT).isPresent()) {
					nodeVParent = nodeV.property(PARENT).value() + "";
				}
				
				if(vert.value(Constants.NAME).equals(nodeVParent)) {
					qs.addSelector(vert.property(Constants.VALUE).value() + "", nodeV.property(Constants.VALUE).value() + "");
				} else if(nodeV.value(Constants.NAME).equals(vertParent)) {
					qs.addSelector(nodeV.property(Constants.VALUE).value() + "", vert.property(Constants.VALUE).value() + "");
				} else {
//					qs.addSelector(nodeVParent, nodeV.property(Constants.VALUE).value() + "");
//					qs.addSelector(vertParent, vert.property(Constants.VALUE).value() + "");
					qs.addRelation(vert.property(Constants.VALUE).value() + "", nodeV.property(Constants.VALUE).value() + "", "inner.join");
				}
				visitNode(nodeV, travelledEdges, qs);
			}
		}

		GraphTraversal<Vertex, Vertex> upstreamIt = g.traversal().V().has(Constants.TYPE, META).has(Constants.ID, vert.property(Constants.ID).value()).in(META+TinkerFrame.edgeLabelDelimeter+META);
		while(upstreamIt.hasNext()) {
			Vertex nodeV = upstreamIt.next();
			if(nodeV.property(PRIM_KEY).isPresent()) {
				if((boolean) nodeV.property(PRIM_KEY).value()) {
					visitNode(nodeV, travelledEdges, qs);
					continue;
				}
			}
			String nameNode = nodeV.property(Constants.NAME).value() + "";
			
			String edgeKey = nameNode + TinkerFrame.edgeLabelDelimeter + origName;
			if (!travelledEdges.contains(edgeKey)) {
				travelledEdges.add(edgeKey);
				
				String nodeVParent = null;
				if(nodeV.property(PARENT).isPresent()) {
					nodeVParent = nodeV.property(PARENT).value() + "";
				}
				if(vert.value(Constants.NAME).equals(nodeVParent)) {
					qs.addSelector(vert.property(Constants.VALUE).value() + "", nodeV.property(Constants.VALUE).value() + "");
				} else if(nodeV.value(Constants.NAME).equals(vertParent)) {
					qs.addSelector(nodeV.property(Constants.VALUE).value() + "", vert.property(Constants.VALUE).value() + "");
				} else {
//					qs.addSelector(nodeVParent, nodeV.property(Constants.VALUE).value() + "");
//					qs.addSelector(vertParent, vert.property(Constants.VALUE).value() + "");
					qs.addRelation(nodeV.property(Constants.VALUE).value() + "", vert.property(Constants.VALUE).value() + "", "inner.join");
				}
			}
		}
	}
	
	public List<String> getColumnNames(){
		List<String> uniqueList = new Vector<String>();
		GraphTraversal<Vertex, Object> trav = g.traversal().V().has(Constants.TYPE, META).has(PRIM_KEY, false).order().by(ORDER, Order.incr).values(Constants.NAME);
		while(trav.hasNext()){
			uniqueList.add(trav.next().toString());
		}
		return uniqueList;
	}
	
	private List<Vertex> getColumnVertices() {
		List<Vertex> uniqueList = new Vector<Vertex>();
		GraphTraversal<Vertex, Vertex> trav = g.traversal().V().has(Constants.TYPE, META).has(PRIM_KEY, false).order().by(ORDER, Order.incr);
		while(trav.hasNext()){
			uniqueList.add(trav.next());
		}
		return uniqueList;
	}

	/**
	 * edgeHash is all query struct names of things getting added this go
	 * e.g.
	 * {
	 * 		Title -> [Title__Budget, Studio]
	 * }
	 * 
	 * joins is list of all joins getting added this go
	 * e.g.
	 * {
	 * 		Title -> Title
	 * 		System -> System_1
	 * }
	 * 
	 * return is the unique name and unique parent name (if property) to be associated with each physical name in edgeHash
	 * e.g.
	 * {
	 * 		System -> [System_1, null]
	 * 		Budget -> [MovieBudget, Title_1]
	 * }
	 */
	@Override
	public Map<String, String[]> getPhysical2LogicalTranslations(Map<String, Set<String>> edgeHash,
			List<Map<String, String>> joins) {
		Map<String, String[]> retMap = new HashMap<String, String[]>();
		
		List<String> uniqueNames = getColumnNames();
		
		// create master set
		// master set is all of the query struct names of the things getting added this go
		Set<String> masterSet = new HashSet<String>();
		masterSet.addAll(edgeHash.keySet());
		for(Set<String> edgeHashSet : edgeHash.values()){
			masterSet.addAll(edgeHashSet);
		}
		
		// first go through just for the concepts
		for(String key: masterSet){
			String myParentsUniqueName = getUniqueName(key.contains("__")? key.substring(0, key.indexOf("__")): null, uniqueNames, joins);
			String myUniqueName = getUniqueName(key.contains("__")? key.substring(key.indexOf("__")+2): key, uniqueNames, joins);
            retMap.put(key, new String[]{myUniqueName, myParentsUniqueName});
		}
		return retMap;
	}
	
	private String getUniqueName(String name, List<String> uniqueNames, List<Map<String, String>> joins){
		if (name == null) return null;
		for(Map<String, String> join: joins) {
			if(join.containsKey(name)){
				return join.get(name);
			}
		}
		String correctName = name;
		int counter = 1;
        while (uniqueNames.contains(correctName)) {
        	correctName = name + "_" + counter;
            counter++;
        }
        return correctName;
	}

	@Override
	public String getLogicalNameForUniqueName(String uniqueName, String engineName) {
		Vertex vert = getExistingVertex(uniqueName);
		Map<Object, Object> engineProps = (Map<Object, Object>) vert.property(engineName).value();
		return engineProps.get(NAME_TYPE.DB_LOGICAL_NAME) + "";
	}

	@Override
	public Map<String, String> getAllUniqueNamesToValues() {
		Map<String, String> uniqueNamesToValue = new Hashtable<String, String>();
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.TYPE, META);
		while(gt.hasNext()) {
			Vertex v = gt.next();
			uniqueNamesToValue.put(v.value(Constants.NAME) + "", v.value(Constants.VALUE) + "");
		}
		
		return uniqueNamesToValue;
	}
	
	@Override
	public void storeVertex(String uniqueName, String howItsCalledInDataFrame, String uniqueParentNameIfProperty) {
		Vertex vertex = this.upsertVertex(uniqueName, howItsCalledInDataFrame);
		if(uniqueParentNameIfProperty != null && !uniqueParentNameIfProperty.isEmpty()) {
			vertex.property(PARENT, uniqueParentNameIfProperty);
		}
	}
	private String cleanValue(String value) {
		value = value.replaceAll("[#%!&()@#$'./-]*", ""); // replace all the useless shit in one go
    	value = value.replaceAll("\\s+","_");
    	value = value.replaceAll(",","_"); 
    	if(Character.isDigit(value.charAt(0)))
    		value = "c_" + value;
    	return value;
    }
	
	@Override
	public Map<String, Set<String>> getEdgeHash() {
		Map<String, Set<String>> retMap = new HashMap<String, Set<String>>();
		GraphTraversal<Vertex, Vertex> metaT = g.traversal().V().has(Constants.TYPE, META);
		while(metaT.hasNext()) {
			Vertex startNode = metaT.next();
			String startType = startNode.property(Constants.NAME).value()+"";
			Iterator<Vertex> downNodes = startNode.vertices(Direction.OUT);
			Set<String> downSet = new HashSet<String>();
			while(downNodes.hasNext()){
				Vertex downNode = downNodes.next();
				String downType = downNode.property(Constants.NAME).value()+"";
				downSet.add(downType);
			}
			retMap.put(startType, downSet);
		}
		return retMap;
	}
	
	@Override
	public void save(String baseFileName){
		String fileName = baseFileName + "_META.tg";
		try {
			long startTime = System.currentTimeMillis();
//			g.variables().set(Constants.HEADER_NAMES, headerNames);
			
			// create special vertex to save the order of the headers
//			Vertex specialVert = this.upsertVertex(ENVIRONMENT_VERTEX_KEY, ENVIRONMENT_VERTEX_KEY, ENVIRONMENT_VERTEX_KEY);
//			
//			Gson gson = new Gson();
//			Map<String, Object> varMap = g.variables().asMap();
//			for(String key : varMap.keySet()) {
//				specialVert.property(key, gson.toJson(varMap.get(key)));
//			}
			Builder<GryoIo> builder = IoCore.gryo();
			builder.graph(g);
			IoRegistry kryo = new MyGraphIoRegistry();
			builder.registry(kryo);
			GryoIo yes = builder.create();
			yes.writeGraph(fileName);
			
			long endTime = System.currentTimeMillis();
			LOGGER.info("Successfully saved TinkerFrame to file: "+fileName+ "("+(endTime - startTime)+" ms)");
			
			// now we need to remvoe the special vert after it is saved since the user might extend the viz even further
			// we dont want it to continue to show up
//			specialVert.remove();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	@Override
	public void open(String baseFileName){
		String fileName = baseFileName + "_META.tg";
		this.g = TinkerGraph.open();
		try {
			long startTime = System.currentTimeMillis();

			Builder<GryoIo> builder = IoCore.gryo();
			builder.graph(this.g);
			IoRegistry kryo = new MyGraphIoRegistry();
			builder.registry(kryo);
			GryoIo yes = builder.create();
			yes.readGraph(fileName);
			
			long endTime = System.currentTimeMillis();
			LOGGER.info("Successfully loaded TinkerFrame from file: "+fileName+ "("+(endTime - startTime)+" ms)");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//create new tinker frame and set its tinkergraph
		this.g.createIndex(Constants.TYPE, Vertex.class);
		this.g.createIndex(Constants.ID, Vertex.class);
		this.g.createIndex(Constants.ID, Edge.class);
		
	}

	public List<Map<String, Object>> getTableHeaderObjects(){
		List<Map<String, Object>> tableHeaders = new ArrayList<Map<String, Object>>();
		// get all the vertices
		List<Vertex> uniqueNonPrimKeyVertices = this.getColumnVertices();
		// get all the data types
		
		for(int i = 0; i < uniqueNonPrimKeyVertices.size(); i++) {
			Vertex vert = uniqueNonPrimKeyVertices.get(i);
			
			// store the normal vertex info
			Map<String, Object> innerMap = new HashMap<String, Object>();
			String name = vert.value(Constants.NAME); 
			innerMap.put("uri", name);
			innerMap.put("varKey", name);
			
			// store data type is present
			if(vert.property(DATATYPE).isPresent()) {
				IMetaData.DATA_TYPES type = vert.value(DATATYPE);
				innerMap.put("type", type + "");
			} else {
				innerMap.put("type", "TYPE NOT STORED IN OWL, NEED TO UPDATE DB");
			}
			
			//need to also store information if the vertex is a derived column
			if(vert.property(DERIVED).isPresent()) {
				boolean isDerived = vert.value(DERIVED);
				if(isDerived) {
					// need to determine if what points into this node is a prim key or not
					Vertex outVert = vert.edges(Direction.IN).next().outVertex();
					if(outVert.property(PRIM_KEY).isPresent() && (boolean) outVert.value(PRIM_KEY)) {
						// this is an example where you have a group by with 2 columns + math
						// the meta edgeHash for this example is {groupByCol1 -> [primKey] , groupByCol2 -> [primKey] , primKey -> [newCol]}
						List<String> inputs = new Vector<String>();
						Iterator<Edge> allEdges = outVert.edges(Direction.IN);
						while(allEdges.hasNext()) {
							inputs.add(allEdges.next().outVertex().value(Constants.NAME));
						}
						innerMap.put("derived", inputs.toArray(new String[]{}));
					} else {
						// the meta edgeHash for this example is {existingCol -> [newCol]}
						innerMap.put("derived", new String[]{outVert.value(Constants.NAME)});
					}
				}
			}
			
			
			tableHeaders.add(innerMap);
		}
		return tableHeaders;
	}

	
	
//	public List<String> getSelectors(String aliasKey) {
//		GraphTraversal<Vertex, Vertex> traversal = g.traversal().V().has(Constants.TYPE, META);
//		List<String> selectors = new ArrayList<String>();
//		while(traversal.hasNext()) {
//			Vertex nextVert = traversal.next();
//			
//			//if nextVert not a prim key
//			if(!nextVert.value(Constants.NAME).equals(PRIM_KEY)) {
//				selectors.add(nextVert.value(aliasKey));
//			}
//		}
//		return selectors;
//	}
	
//////////////////::::::::::::::::::::::: TESTING :::::::::::::::::::::::::::::::://////////////////////////////

	public static void main(String[] args) {
		TinkerMetaData meta = new TinkerMetaData();

		Vertex pkV = meta.upsertVertex("TABLE1","TABLE1");
		pkV.property(PRIM_KEY, true);

		Vertex v1 = meta.upsertVertex("TITLE","TITLE");
		v1.property(PARENT, "TABLE1");
		meta.upsertEdge(pkV, v1);

		Vertex v2 = meta.upsertVertex("STUDIO","STUDIO");
		v2.property(PARENT, "TABLE1");
		meta.upsertEdge(pkV, v2);
		
		Vertex v3 = meta.upsertVertex("DIRECTOR","DIRECTOR");
		v3.property(PARENT, "TABLE1");
		meta.upsertEdge(pkV, v3);
		
		// new sheet
		
		pkV = meta.upsertVertex("TABLE2","TABLE2");
		pkV.property(PRIM_KEY, true);

		Vertex newv1 = meta.upsertVertex("TABLE2__TITLE","TABLE2__TITLE");
		newv1.property(PARENT, "TABLE2");
		meta.upsertEdge(pkV, newv1);
		meta.upsertEdge(v1, newv1);

		v2 = meta.upsertVertex("BUDGET","BUDGET");
		v2.property(PARENT, "TABLE2");
		meta.upsertEdge(pkV, v2);
		
		v3 = meta.upsertVertex("REVENUE","REVENUE");
		v3.property(PARENT, "TABLE2");
		meta.upsertEdge(pkV, v3);

		QueryStruct qs = meta.getQueryStruct("DIRECTOR");
		qs.print();
	}

	@Override
	public void unfilterAll() {
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.TYPE, META);
		while(gt.hasNext()) {
			Vertex metaVertex = gt.next();
			metaVertex.property(Constants.FILTER, false);
		}
		
	}

	@Override
	public Map<String, String> getFilteredColumns() {
		Map<String, String> retMap = new HashMap<String, String>();
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(Constants.TYPE, META).has(Constants.FILTER, true);
		while(gt.hasNext()) {
			Vertex metaVertex = gt.next();
			String name = metaVertex.property(Constants.NAME).value()+"";
			String value = metaVertex.property(Constants.VALUE).value() +"";
			retMap.put(name, value);
		}
		return retMap;
		
	}

	@Override
	public boolean isConnectedInDirection(String outName, String inName) {
		String outValue = this.getValueForUniqueName(outName);
		String inValue = this.getValueForUniqueName(inName);
		GraphTraversal<Vertex, Vertex> metaT = g.traversal().V().has(Constants.TYPE, META).has(Constants.VALUE, outValue).out(META+edgeLabelDelimeter+META).has(Constants.TYPE, META).has(Constants.VALUE, inValue);
		if(metaT.hasNext()){
			return true;
		}
		else {
			return false;
		}
	}

	@Override
	public String getParentValueOfUniqueNode(String uniqueName) {
		Vertex v = getExistingVertex(uniqueName);
		if(v != null) {
			if(v.property(PARENT).isPresent()) {
				String parentUniqueName = v.property(PARENT).value() + "";
				Vertex parentV = getExistingVertex(parentUniqueName);
				return parentV.property(Constants.VALUE).value() + "";
			}
		}
		return null;
	}

	@Override
	public List<String> getPrimKeys() {
		List<String> uniqueList = new Vector<String>();
		GraphTraversal<Vertex, Object> trav = g.traversal().V().has(Constants.TYPE, META).has(PRIM_KEY, true).values(Constants.NAME);
		while(trav.hasNext()){
			uniqueList.add(trav.next().toString());
		}
		return uniqueList;
	}

	@Override
	public void setVertexValue(String uniqueName, String newValue) {
		GraphTraversal<Vertex, Vertex> trav = g.traversal().V().has(Constants.TYPE, META).has(Constants.NAME, uniqueName);
		while(trav.hasNext()){
			trav.next().property(Constants.VALUE, newValue);
		}
	}

	@Override
	public String getLatestPrimKey() {
		if(this.latestPrimKey!=null){
			return this.latestPrimKey;
		}
		else{
			List<String> keys = getPrimKeys();
			return keys.get(keys.size()-1);
		}
	}
}
