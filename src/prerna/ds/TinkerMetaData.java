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

	public static final String ALIAS = "_T_ALIAS";
	public static final String DB_NAME = "_T_DB_NAME";
	public static final String ALIAS_TYPE = "_T_ALIAS_TYPE";
	public static final String PHYSICAL_NAME = "_T_PHYSICAL_NAME";
	public static final String PHYSICAL_URI = "_T_PHYSICAL_URI";
	public static final String LOGICAL_NAME = "_T_LOGICAL_NAME";
	public static final String PARENT = "_T_PARENT";
	public static final String DATATYPE = "_T_DATATYPE";
	public static final String ORDER = "_T_ORDER";
	public static final String DB_DATATYPE = "_T_DB_DATATYPE";
	public static final String DERIVED = "_T_DERIVED_COLUMN";
	public static final String DERIVED_CALCULATION = "_T_DERIVED_CALCULATION";
	public static final String DERIVED_USING = "_T_DERIVED_USING";
	public static final String ALIAS_NAME = "_T_ALIAS_NAME";
	
	private String latestPrimKey;

	public static final String META = "META";
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
		g.createIndex(TinkerFrame.TINKER_TYPE, Vertex.class);
		g.createIndex(TinkerFrame.TINKER_ID, Vertex.class);
		g.createIndex(T.label.toString(), Edge.class);
		g.createIndex(TinkerFrame.TINKER_ID, Edge.class);
		this.g = g;
	}
	
	/*
	 * Return's nodes Contrant.Value/Physical logical name from the inputted physical name
	 */
	@Override
	public String getValueForUniqueName (String metaNodeName) {
		String metaNodeValue = null;
		// get metamodel info for metaModeName
		GraphTraversal<Vertex, Vertex> metaT = g.traversal().V().has(TinkerFrame.TINKER_TYPE, META).has(TinkerFrame.TINKER_NAME, metaNodeName);
		
		// if metaT has metaNodeName then find the value else return metaNodeName
		if (metaT.hasNext()) {
			Vertex startNode = metaT.next();
			metaNodeValue = startNode.property(TinkerFrame.TINKER_VALUE).value() + "";
		}

		return metaNodeValue;
	}
	
	
	
	@Override
	public String getAliasForUniqueName (String metaNodeName) {
		String metaNodeValue = null;
		// get metamodel info for metaModeName
		GraphTraversal<Vertex, Vertex> metaT = g.traversal().V().has(TinkerFrame.TINKER_TYPE, META).has(TinkerFrame.TINKER_NAME, metaNodeName);
		
		// if metaT has metaNodeName then find the value else return metaNodeName
		if (metaT.hasNext()) {
			Vertex startNode = metaT.next();
			metaNodeValue = startNode.property(ALIAS_NAME).value() + "";
		}

		return metaNodeValue;
	}
	
	@Override
	public String getUniqueNameForAlias (String aliasName) {
		String metaNodeValue = null;
		// get metamodel info for metaModeName
		GraphTraversal<Vertex, Vertex> metaT = g.traversal().V().has(TinkerFrame.TINKER_TYPE, META).has(ALIAS_NAME, aliasName);
		
		// if metaT has metaNodeName then find the value else return metaNodeName
		if (metaT.hasNext()) {
			Vertex startNode = metaT.next();
			metaNodeValue = startNode.property(TinkerFrame.TINKER_NAME).value() + "";
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
		if(dataType.contains("TYPE:")) {
			dataType = dataType.replace("TYPE:", "");
		}
		vert.property(DB_DATATYPE, dataType);
		
		dataType = dataType.toUpperCase();
		
		DATA_TYPES currType = null;
		if(vert.property(DATATYPE).isPresent()){
			currType = vert.value(DATATYPE);
		}
		
		IMetaData.DATA_TYPES metaDataType = Utility.convertStringToDataType(dataType);
		
		if(currType == null) {
//			if(dataType.contains("STRING") || dataType.contains("TEXT") || dataType.contains("VARCHAR")) {
			if(IMetaData.DATA_TYPES.STRING.equals(metaDataType)) {
				vert.property(DATATYPE, IMetaData.DATA_TYPES.STRING);
			} 
//			else if(dataType.contains("INT") || dataType.contains("DECIMAL") || dataType.contains("DOUBLE") || dataType.contains("FLOAT") || dataType.contains("LONG") || dataType.contains("BIGINT")
//					|| dataType.contains("TINYINT") || dataType.contains("SMALLINT") || dataType.contains("NUMBER")){
			else if(IMetaData.DATA_TYPES.NUMBER.equals(metaDataType)) {
				vert.property(DATATYPE, IMetaData.DATA_TYPES.NUMBER);
			} 
			else if(IMetaData.DATA_TYPES.DATE.equals(metaDataType)) {
				vert.property(DATATYPE, IMetaData.DATA_TYPES.DATE);
			}
		} else {
			// if current is string or new col is string
			// column must now be a string
			if(currType.equals(IMetaData.DATA_TYPES.STRING) || IMetaData.DATA_TYPES.STRING.equals(metaDataType)) {
				vert.property(DATATYPE, IMetaData.DATA_TYPES.STRING);
			}
			// if current is a number and new is a number
			// column is still number
			else if(currType.equals(IMetaData.DATA_TYPES.NUMBER) && IMetaData.DATA_TYPES.NUMBER.equals(metaDataType)){
				// no change
				// vert.property(DATATYPE, "NUMBER");
			}
			// if current is date and new is date
			// column is still date
			else if(currType.equals(IMetaData.DATA_TYPES.DATE) && IMetaData.DATA_TYPES.DATE.equals(metaDataType)) {
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
			LOGGER.debug(" adding vertex ::: " + TinkerFrame.TINKER_ID + " = " + type + ":" + uniqueName+ " & " + TinkerFrame.TINKER_VALUE+ " = " + howItsCalledInDataFrame+ " & " + TinkerFrame.TINKER_TYPE+ " = " + type+ " & " + TinkerFrame.TINKER_NAME+ " = " + uniqueName + " & " + ALIAS_NAME + " = " + uniqueName);
			retVertex = g.addVertex(TinkerFrame.TINKER_ID, type + ":" + uniqueName, TinkerFrame.TINKER_VALUE, howItsCalledInDataFrame, TinkerFrame.TINKER_TYPE, type, TinkerFrame.TINKER_NAME, uniqueName, ALIAS_NAME, uniqueName);// push the actual value as well who knows when you would need it
			// all new meta nodes are defaulted as unfiltered and not prim keys
			retVertex.property(TinkerFrame.TINKER_FILTER, false);
			if(uniqueName.toString().startsWith(TinkerFrame.PRIM_KEY)) {
				setPrimKey(uniqueName.toString(), true);
			}
			else {
				retVertex.property(TinkerFrame.PRIM_KEY, false);
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
			GraphTraversal<Vertex, Number> trav = g.traversal().V().has(TinkerFrame.TINKER_TYPE, META).values(ORDER).max();
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
		String type = META + TinkerFrame.EDGE_LABEL_DELIMETER + META;
		String edgeID = type + "/" + fromVertex.value(TinkerFrame.TINKER_NAME) + ":" + toVertex.value(TinkerFrame.TINKER_NAME);
		// try to find the vertex
		GraphTraversal<Edge, Edge> gt = g.traversal().E().has(TinkerFrame.TINKER_ID, edgeID);
		if(gt.hasNext()) {
			retEdge = gt.next(); // COUNTS HAVE NO MEANING IN META. REMOVED.
		}
		else {
			retEdge = fromVertex.addEdge(type, toVertex, TinkerFrame.TINKER_ID, edgeID);
		}

		return retEdge;
	}
	
	private Vertex getExistingVertex(Object uniqueName){
		Vertex retVertex = null;
		// try to find the vertex
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(TinkerFrame.TINKER_ID, META + ":" + uniqueName);
		if(gt.hasNext()) {
			retVertex = gt.next();
		}
		return retVertex;
	}

	public Map<String, String> getNodeTypesForUniqueAlias() {
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(TinkerFrame.TINKER_TYPE, META);
		Map<String, String> retMap = new Hashtable<String, String>();
		while(gt.hasNext()) {
			Vertex vert = gt.next();
			String uniqueName = vert.value(TinkerFrame.TINKER_NAME);
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
		GraphTraversal<Vertex, Vertex> trav = g.traversal().V().has(TinkerFrame.TINKER_TYPE, META).has(PARENT);
		while (trav.hasNext()){
			Vertex vert = trav.next();
			String prop = vert.property(TinkerFrame.TINKER_NAME).value() +"";
			String parent = vert.property(PARENT).value()+"";
			retMap.put(prop, parent);
		}
		return retMap;
	}


	@Override
	public String getPhysicalUriForNode(String nodeUniqueName, String engineName) {
		GraphTraversal<Vertex, Vertex> trav = g.traversal().V().has(TinkerFrame.TINKER_TYPE, META).has(TinkerFrame.TINKER_NAME, nodeUniqueName);
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
		GraphTraversal<Vertex, Vertex> trav = g.traversal().V().has(TinkerFrame.TINKER_TYPE, META).has(TinkerFrame.TINKER_NAME, nodeUniqueName);
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
		
		// variable we define from the engine
		
		//TODO: this needs to be updated - used to be getting the logical from the engine
		//TODO: this needs to be updated - used to be getting the logical from the engine
		String logicalName = null;
		String conceptualUri = null;
		String physicalName = null;
		String physicalUri = null;
		
		//check if property
		if(queryStructName.contains("__")){
			String parentName = queryStructName.substring(0, queryStructName.indexOf("__"));
			physicalName = queryStructName.substring(queryStructName.indexOf("__")+2);
			conceptualUri = "http://semoss.org/ontologies/Relation/Contains/" + physicalName + "/" + parentName;
			physicalUri = engine.getPhysicalUriFromConceptualUri(conceptualUri);
			logicalName = physicalName;
		}
		else{
			physicalName = queryStructName;
			conceptualUri = "http://semoss.org/ontologies/Concept/" + physicalName;
			physicalUri = engine.getPhysicalUriFromConceptualUri(conceptualUri);
			logicalName = physicalName;
		}
		
		// stupid check
		if(engineName.equals(Constants.LOCAL_MASTER_DB_NAME)) {
			physicalUri = "http://semoss.org/ontologies/Concept/" + uniqueName;
		}
		
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
		
		//store the data type
		storeDataType(uniqueName, dataType);
		
		if(uniqueParentNameIfProperty != null){
			vert.property(PARENT, uniqueParentNameIfProperty);
		}
	}

	@Override
	public void setFiltered(String uniqueName, boolean filtered) {
		Vertex v = getExistingVertex(uniqueName);
		v.property(TinkerFrame.TINKER_FILTER, filtered);
	}

	@Override
	public void setPrimKey(String uniqueName, boolean primKey) {
		Vertex v = getExistingVertex(uniqueName);
		v.property(TinkerFrame.PRIM_KEY, primKey);
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
	
	@Override
	public void modifyUniqueName(String uniqueName, String newName) {
		Vertex v = getExistingVertex(uniqueName);

		// set new ID
		v.property(VertexProperty.Cardinality.single, TinkerFrame.TINKER_ID, META + ":" + newName);
		// set new VALUE
		v.property(VertexProperty.Cardinality.single, TinkerFrame.TINKER_VALUE, newName);
		// set new NAME
		v.property(VertexProperty.Cardinality.single, TinkerFrame.TINKER_NAME, newName);
		// set new ALIAS
		v.property(VertexProperty.Cardinality.single, ALIAS_NAME, newName);
		
		
		Iterator<Edge> outEdges = v.edges(Direction.OUT);
		while(outEdges.hasNext()) {
			Edge outEdge = outEdges.next();
			outEdge.inVertex().property(VertexProperty.Cardinality.single, PARENT, newName);
		}
	}
	
	@Override
	public void addEngineForUniqueName(String uniqueName, String engineName) {
		Vertex v = getExistingVertex(uniqueName);
		v.property(VertexProperty.Cardinality.list, DB_NAME, engineName);
	}
	
//////////////////::::::::::::::::::::::: GETTER METHODS :::::::::::::::::::::::::::::::://////////////////////////////

	@Override
	/**
	 * Get the list of the vertices to their types
	 * Ignores Prim_Keys
	 * Returns the VALUE (i.e. what it is called in frame) to the type
	 * @return
	 */
	public Map<String, IMetaData.DATA_TYPES> getColumnTypes() {
		List<Vertex> verts = getColumnVertices();
		
		Map<String, IMetaData.DATA_TYPES> retMap = new Hashtable<String, IMetaData.DATA_TYPES>();
		for(Vertex vert : verts) {
			String name = vert.value(TinkerFrame.TINKER_NAME);
			System.out.println(name);
			IMetaData.DATA_TYPES dataType = vert.value(DATATYPE);
			retMap.put(name, dataType);
		}
		
		return retMap;
	}
	
	@Override
	/**
	 * Get the list of the vertices to their types
	 * Ignores Prim_Keys
	 * Returns the VALUE (i.e. what it is called in frame) to the type
	 * @return
	 */
	public Map<String, String> getDBColumnTypes() {
		List<Vertex> verts = getColumnVertices();
		
		Map<String, String> retMap = new Hashtable<>();
		for(Vertex vert : verts) {
			String name = vert.value(TinkerFrame.TINKER_NAME);
			System.out.println(name);
			String dataType = vert.value(DB_DATATYPE);
			retMap.put(name, dataType);
		}
		
		return retMap;
	}
	
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
		if(vert.property(TinkerFrame.TINKER_FILTER).isPresent()) {
			return vert.value(TinkerFrame.TINKER_FILTER);
		}
		return false;
	}

	@Override
	public boolean isPrimKey(String uniqueName) {
		Vertex vert = getExistingVertex(uniqueName);
		if(vert.property(TinkerFrame.PRIM_KEY).isPresent()) {
			return vert.value(TinkerFrame.PRIM_KEY);
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
		String origName = vert.value(TinkerFrame.TINKER_NAME);
		String vertParent = null;
		if(vert.property(PARENT).isPresent()) {
			vertParent = vert.property(PARENT).value() + "";
		}

		GraphTraversal<Vertex, Vertex> downstreamIt = g.traversal().V().has(TinkerFrame.TINKER_TYPE, META).has(TinkerFrame.TINKER_ID, vert.property(TinkerFrame.TINKER_ID).value()).out(META + TinkerFrame.EDGE_LABEL_DELIMETER + META);
		while (downstreamIt.hasNext()) {
			Vertex nodeV = downstreamIt.next();
			if(nodeV.property(TinkerFrame.PRIM_KEY).isPresent()) {
				if((boolean) nodeV.property(TinkerFrame.PRIM_KEY).value()) {
					visitNode(nodeV, travelledEdges, qs);
					continue;
				}
			}
			String nameNode = nodeV.property(TinkerFrame.TINKER_NAME).value() + "";
			
			String edgeKey = origName + TinkerFrame.EDGE_LABEL_DELIMETER + nameNode;
			if(!travelledEdges.contains(edgeKey)) {
				travelledEdges.add(edgeKey);
				
				String nodeVParent = null;
				if(nodeV.property(PARENT).isPresent()) {
					nodeVParent = nodeV.property(PARENT).value() + "";
				}
				
				if(vert.value(TinkerFrame.TINKER_NAME).equals(nodeVParent)) {
					qs.addSelector(vert.property(TinkerFrame.TINKER_VALUE).value() + "", nodeV.property(TinkerFrame.TINKER_VALUE).value() + "");
				} else if(nodeV.value(TinkerFrame.TINKER_NAME).equals(vertParent)) {
					qs.addSelector(nodeV.property(TinkerFrame.TINKER_VALUE).value() + "", vert.property(TinkerFrame.TINKER_VALUE).value() + "");
				} else {
//					qs.addSelector(nodeVParent, nodeV.property(TinkerFrame.TINKER_VALUE).value() + "");
//					qs.addSelector(vertParent, vert.property(TinkerFrame.TINKER_VALUE).value() + "");
					qs.addRelation(vert.property(TinkerFrame.TINKER_VALUE).value() + "", nodeV.property(TinkerFrame.TINKER_VALUE).value() + "", "inner.join");
				}
				visitNode(nodeV, travelledEdges, qs);
			}
		}

		GraphTraversal<Vertex, Vertex> upstreamIt = g.traversal().V().has(TinkerFrame.TINKER_TYPE, META).has(TinkerFrame.TINKER_ID, vert.property(TinkerFrame.TINKER_ID).value()).in(META+TinkerFrame.EDGE_LABEL_DELIMETER+META);
		while(upstreamIt.hasNext()) {
			Vertex nodeV = upstreamIt.next();
			if(nodeV.property(TinkerFrame.PRIM_KEY).isPresent()) {
				if((boolean) nodeV.property(TinkerFrame.PRIM_KEY).value()) {
					visitNode(nodeV, travelledEdges, qs);
					continue;
				}
			}
			String nameNode = nodeV.property(TinkerFrame.TINKER_NAME).value() + "";
			
			String edgeKey = nameNode + TinkerFrame.EDGE_LABEL_DELIMETER + origName;
			if (!travelledEdges.contains(edgeKey)) {
				travelledEdges.add(edgeKey);
				
				String nodeVParent = null;
				if(nodeV.property(PARENT).isPresent()) {
					nodeVParent = nodeV.property(PARENT).value() + "";
				}
				if(vert.value(TinkerFrame.TINKER_NAME).equals(nodeVParent)) {
					qs.addSelector(vert.property(TinkerFrame.TINKER_VALUE).value() + "", nodeV.property(TinkerFrame.TINKER_VALUE).value() + "");
				} else if(nodeV.value(TinkerFrame.TINKER_NAME).equals(vertParent)) {
					qs.addSelector(nodeV.property(TinkerFrame.TINKER_VALUE).value() + "", vert.property(TinkerFrame.TINKER_VALUE).value() + "");
				} else {
//					qs.addSelector(nodeVParent, nodeV.property(TinkerFrame.TINKER_VALUE).value() + "");
//					qs.addSelector(vertParent, vert.property(TinkerFrame.TINKER_VALUE).value() + "");
					qs.addRelation(nodeV.property(TinkerFrame.TINKER_VALUE).value() + "", vert.property(TinkerFrame.TINKER_VALUE).value() + "", "inner.join");
				}
			}
		}
	}
	
	public List<String> getColumnNames(){
		List<String> uniqueList = new Vector<String>();
		GraphTraversal<Vertex, Object> trav = g.traversal().V().has(TinkerFrame.TINKER_TYPE, META).has(TinkerFrame.PRIM_KEY, false).order().by(ORDER, Order.incr).values(TinkerFrame.TINKER_NAME);
		while(trav.hasNext()){
			uniqueList.add(trav.next().toString());
		}
		return uniqueList;
	}
	
	public List<String> getColumnAliasName(){
		List<String> uniqueList = new Vector<String>();
		GraphTraversal<Vertex, Object> trav = g.traversal().V().has(TinkerFrame.TINKER_TYPE, META).has(TinkerFrame.PRIM_KEY, false).order().by(ORDER, Order.incr).values(ALIAS_NAME);
		while(trav.hasNext()){
			uniqueList.add(trav.next().toString());
		}
		return uniqueList;
	}
	
	private List<Vertex> getColumnVertices() {
		List<Vertex> uniqueList = new Vector<Vertex>();
		GraphTraversal<Vertex, Vertex> trav = g.traversal().V().has(TinkerFrame.TINKER_TYPE, META).has(TinkerFrame.PRIM_KEY, false).order().by(ORDER, Order.incr);
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
	public Map<String, String[]> getPhysical2LogicalTranslations(Map<String, Set<String>> edgeHash, List<Map<String, String>> joins, Map<String, Boolean> makeUniqueNameMap) {
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
			String myParentName = null;
			String myNodeName = null;
			
			String parentName = key.contains("__") ? key.substring(0, key.indexOf("__")) : null;
			String nodeName = key.contains("__") ? key.substring(key.indexOf("__")+2) : key;
			
			/*
			 * Use the map to determine if we should make the name unique or use the existing value
			 */
			
			if(parentName != null) {
				if(makeUniqueNameMap != null && makeUniqueNameMap.get(parentName) != null && makeUniqueNameMap.get(parentName) ) {
					myParentName = getUniqueName(parentName, uniqueNames, joins);
				} else {
					myParentName = getName(parentName, joins);
				}
			}
			if(makeUniqueNameMap != null && makeUniqueNameMap.get(nodeName) != null && makeUniqueNameMap.get(nodeName) ) {
				myNodeName = getUniqueName(nodeName, uniqueNames, joins);
			} else {
				myNodeName = getName(nodeName, joins);
			}
			
            retMap.put(key, new String[]{myNodeName, myParentName});
		}
		return retMap;
	}
	
	private String getName(String name, List<Map<String, String>> joins) {
		if (name == null) return null;
		for(Map<String, String> join: joins) {
			if(join.containsKey(name)){
				return join.get(name);
			}
		}
		return name;
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
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(TinkerFrame.TINKER_TYPE, META);
		while(gt.hasNext()) {
			Vertex v = gt.next();
			uniqueNamesToValue.put(v.value(TinkerFrame.TINKER_NAME) + "", v.value(TinkerFrame.TINKER_VALUE) + "");
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
		GraphTraversal<Vertex, Vertex> metaT = g.traversal().V().has(TinkerFrame.TINKER_TYPE, META);
		while(metaT.hasNext()) {
			Vertex startNode = metaT.next();
			String startType = startNode.property(TinkerFrame.TINKER_NAME).value()+"";
			Iterator<Vertex> downNodes = startNode.vertices(Direction.OUT);
			Set<String> downSet = new HashSet<String>();
			while(downNodes.hasNext()){
				Vertex downNode = downNodes.next();
				String downType = downNode.property(TinkerFrame.TINKER_NAME).value()+"";
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
		this.g.createIndex(TinkerFrame.TINKER_TYPE, Vertex.class);
		this.g.createIndex(TinkerFrame.TINKER_ID, Vertex.class);
		this.g.createIndex(TinkerFrame.TINKER_ID, Edge.class);
		
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
			String name = vert.value(ALIAS_NAME); 
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
					if(outVert.property(TinkerFrame.PRIM_KEY).isPresent() && (boolean) outVert.value(TinkerFrame.PRIM_KEY)) {
						// this is an example where you have a group by with 2 columns + math
						// the meta edgeHash for this example is {groupByCol1 -> [primKey] , groupByCol2 -> [primKey] , primKey -> [newCol]}
						List<String> inputs = new Vector<String>();
						Iterator<Edge> allEdges = outVert.edges(Direction.IN);
						while(allEdges.hasNext()) {
							inputs.add(allEdges.next().outVertex().value(TinkerFrame.TINKER_NAME));
						}
						innerMap.put("derived", inputs.toArray(new String[]{}));
					} else {
						// the meta edgeHash for this example is {existingCol -> [newCol]}
						innerMap.put("derived", new String[]{outVert.value(TinkerFrame.TINKER_NAME)});
					}
				}
			}
			
			if(vert.property(PARENT).isPresent()) {
				String parent = vert.value(PARENT);
				innerMap.put("parent", parent);
			}
			
			tableHeaders.add(innerMap);
		}
		return tableHeaders;
	}

	
	
//	public List<String> getSelectors(String aliasKey) {
//		GraphTraversal<Vertex, Vertex> traversal = g.traversal().V().has(TinkerFrame.TINKER_TYPE, META);
//		List<String> selectors = new ArrayList<String>();
//		while(traversal.hasNext()) {
//			Vertex nextVert = traversal.next();
//			
//			//if nextVert not a prim key
//			if(!nextVert.value(TinkerFrame.TINKER_NAME).equals(PRIM_KEY)) {
//				selectors.add(nextVert.value(aliasKey));
//			}
//		}
//		return selectors;
//	}
	
//////////////////::::::::::::::::::::::: TESTING :::::::::::::::::::::::::::::::://////////////////////////////

	public static void main(String[] args) {
		TinkerMetaData meta = new TinkerMetaData();

		Vertex pkV = meta.upsertVertex("TABLE1","TABLE1");
		pkV.property(TinkerFrame.PRIM_KEY, true);

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
		pkV.property(TinkerFrame.PRIM_KEY, true);

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
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(TinkerFrame.TINKER_TYPE, META);
		while(gt.hasNext()) {
			Vertex metaVertex = gt.next();
			metaVertex.property(TinkerFrame.TINKER_FILTER, false);
		}
		
	}

	@Override
	public Map<String, String> getFilteredColumns() {
		Map<String, String> retMap = new HashMap<String, String>();
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(TinkerFrame.TINKER_TYPE, META).has(TinkerFrame.TINKER_FILTER, true);
		while(gt.hasNext()) {
			Vertex metaVertex = gt.next();
			String name = metaVertex.property(TinkerFrame.TINKER_NAME).value()+"";
			String value = metaVertex.property(TinkerFrame.TINKER_VALUE).value() +"";
			retMap.put(name, value);
		}
		return retMap;
		
	}

	@Override
	public boolean isConnectedInDirection(String outName, String inName) {
		String outValue = this.getValueForUniqueName(outName);
		String inValue = this.getValueForUniqueName(inName);
		GraphTraversal<Vertex, Vertex> metaT = g.traversal().V().has(TinkerFrame.TINKER_TYPE, META).has(TinkerFrame.TINKER_VALUE, outValue).out(META+TinkerFrame.EDGE_LABEL_DELIMETER+META).has(TinkerFrame.TINKER_TYPE, META).has(TinkerFrame.TINKER_VALUE, inValue);
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
				return parentV.property(TinkerFrame.TINKER_VALUE).value() + "";
			}
		}
		return null;
	}

	@Override
	public List<String> getPrimKeys() {
		List<String> uniqueList = new Vector<String>();
		GraphTraversal<Vertex, Object> trav = g.traversal().V().has(TinkerFrame.TINKER_TYPE, META).has(TinkerFrame.PRIM_KEY, true).values(TinkerFrame.TINKER_NAME);
		while(trav.hasNext()){
			uniqueList.add(trav.next().toString());
		}
		return uniqueList;
	}

	@Override
	public void setVertexValue(String uniqueName, String newValue) {
		GraphTraversal<Vertex, Vertex> trav = g.traversal().V().has(TinkerFrame.TINKER_TYPE, META).has(TinkerFrame.TINKER_NAME, uniqueName);
		while(trav.hasNext()){
			trav.next().property(TinkerFrame.TINKER_VALUE, newValue);
		}
	}

	@Override
	public void setVertexAlias (String uniqueName, String newValue) {
		GraphTraversal<Vertex, Vertex> trav = g.traversal().V().has(TinkerFrame.TINKER_TYPE, META).has(TinkerFrame.TINKER_NAME, uniqueName);
		while(trav.hasNext()){
			trav.next().property(ALIAS_NAME, newValue);
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

//	@Override
//	public void renameCol(String oldColumnHeader, String newColumnHeader) {
//		//change on a meta level
//		setVertexValue(oldColumnHeader, newColumnHeader);
//	//		
//	}
}
