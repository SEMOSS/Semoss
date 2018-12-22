package prerna.ds;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.script.ScriptContext;
import javax.script.ScriptException;

import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Io.Builder;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import prerna.cache.CachePropFileFrameObject;
import prerna.ds.shared.AbstractTableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.query.interpreters.GremlinInterpreter;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.RelationSet;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.evaluator.QueryStructExpressionIterator;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.sablecc2.reactor.imports.IImporter;
import prerna.sablecc2.reactor.imports.TinkerImporter;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.ui.components.playsheets.datamakers.JoinTransformation;
import prerna.util.ArrayUtilityMethods;
import prerna.util.MyGraphIoRegistry;
import prerna.util.Utility;

public class TinkerFrame extends AbstractTableDataFrame {
	
	public static final String DATA_MAKER_NAME = "TinkerFrame";

	public static final String PRIM_KEY = "_GEN_PRIM_KEY";
	public static final String EMPTY = "_";
	public static final String EDGE_LABEL_DELIMETER = "+++";
	public static final String EDGE_LABEL_DELIMETER_REGEX_SPLIT = "\\+\\+\\+";
	public static final String PRIM_KEY_DELIMETER = ":::";

	public static final String TINKER_ID = "_T_ID";
//	public static final String TINKER_VALUE = "_T_VALUE";
	public static final String TINKER_TYPE = "_T_TYPE";
	public static final String TINKER_NAME = "_T_NAME";
	public static final String TINKER_FILTER = "_T_FILTER";
	public static final String TINKER_COUNT = "_T_COUNT";


	//keeps the cache of whether a column is numerical or not, can this be stored on the meta model?
	protected Map<String, Boolean> isNumericalMap = new HashMap<String, Boolean>(); //store on meta
	
	public TinkerGraph g = null;

	/***********************************  CONSTRUCTORS  **********************************/
	
	public TinkerFrame(String[] headerNames) {
		this.qsNames = headerNames;
		g = TinkerGraph.open();
		g.createIndex(TINKER_TYPE, Vertex.class);
		g.createIndex(TINKER_ID, Vertex.class);
		g.createIndex(T.label.toString(), Edge.class);
		g.createIndex(TINKER_ID, Edge.class);
//		Map<String, Set<String>> primKeyEdgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(headerNames);
//		TinkerMetaHelper.mergeEdgeHash(this.metaData, primKeyEdgeHash, null);
	}
	
	public TinkerFrame(String[] headerNames, Map<String, Set<String>> edgeHash) {
		this.qsNames = headerNames;
		g = TinkerGraph.open();
		g.createIndex(TINKER_TYPE, Vertex.class);
		g.createIndex(TINKER_ID, Vertex.class);
		g.createIndex(T.label.toString(), Edge.class);
		g.createIndex(TINKER_ID, Edge.class);
//		TinkerMetaHelper.mergeEdgeHash(this.metaData, edgeHash, null);
	}			 

	public TinkerFrame() {
		g = TinkerGraph.open();
		g.createIndex(TINKER_TYPE, Vertex.class);
		g.createIndex(TINKER_ID, Vertex.class);
		g.createIndex(TINKER_ID, Edge.class);
		g.createIndex(T.label.toString(), Edge.class);
	}

	/*********************************  END CONSTRUCTORS  ********************************/


	/********************************  DATA MAKER METHODS ********************************/

//	private Map createVertStores(){
//		Map<String, SEMOSSVertex> vertStore = new HashMap<String, SEMOSSVertex>();
//		Map<String, SEMOSSEdge> edgeStore = new HashMap<String, SEMOSSEdge>();
//		
//		GraphTraversal<Edge, Edge> edgesIt = g.traversal().E().not(__.or(__.has(TINKER_TYPE, TINKER_FILTER), __.bothV().in().has(TINKER_TYPE, TINKER_FILTER)));
//		while(edgesIt.hasNext()){
//			Edge e = edgesIt.next();
//			Vertex outV = e.outVertex();
//			Vertex inV = e.inVertex();
//			SEMOSSVertex outVert = getSEMOSSVertex(vertStore, outV);
//			SEMOSSVertex inVert = getSEMOSSVertex(vertStore, inV);
//			
//			edgeStore.put("https://semoss.org/Relation/"+e.property(TINKER_ID).value() + "", new SEMOSSEdge(outVert, inVert, "https://semoss.org/Relation/"+e.property(TINKER_ID).value() + ""));
//		}
//		// now i just need to get the verts with no edges
//		GraphTraversal<Vertex, Vertex> vertIt = g.traversal().V().not(__.or(__.both(),__.has(TINKER_TYPE, TINKER_FILTER),__.in().has(TINKER_TYPE, TINKER_FILTER)));
//		while(vertIt.hasNext()){
//			Vertex outV = vertIt.next();
//			getSEMOSSVertex(vertStore, outV);
//		}
//		
//		Map retHash = new HashMap();
//		retHash.put("nodes", vertStore);
//		retHash.put("edges", edgeStore.values());
//		return retHash;
//	}
	
	private Map createVertStores2() {
		Map<String, SEMOSSVertex> vertStore = new HashMap<String, SEMOSSVertex>();
		Map<String, SEMOSSEdge> edgeStore = new HashMap<String, SEMOSSEdge>();
		
		//get all edges not attached to a filter node or is a filtered edge
		GraphTraversal<Edge, Edge> edgesIt = g.traversal().E().not(__.or(__.has(TINKER_TYPE, TINKER_FILTER), __.bothV().in().has(TINKER_TYPE, TINKER_FILTER), __.V().has(PRIM_KEY, true)));
		while(edgesIt.hasNext()) {
			Edge e = edgesIt.next();
			Vertex outV = e.outVertex();
			
			Vertex inV = e.inVertex();
			SEMOSSVertex outVert = getSEMOSSVertex(vertStore, outV);
			SEMOSSVertex inVert = getSEMOSSVertex(vertStore, inV);
			
			SEMOSSEdge semossE = new SEMOSSEdge(outVert, inVert, "https://semoss.org/Relation/"+e.property(TINKER_ID).value() + "");
			edgeStore.put("https://semoss.org/Relation/"+e.property(TINKER_ID).value() + "", semossE);
			
			// need to add edge properties
			Iterator<Property<Object>> edgeProperties = e.properties();
			while(edgeProperties.hasNext()) {
				Property<Object> prop = edgeProperties.next();
				String propName = prop.key();
				if(!propName.equals(TINKER_ID) && !propName.equals(TINKER_NAME) && !propName.equals(TINKER_TYPE)) {
					semossE.propHash.put(propName, prop.value());
				}
			}
		}
		
		// now i just need to get the verts with no edges
//		GraphTraversal<Vertex, Vertex> vertIt = g.traversal().V().not(__.or(__.both(),__.has(TINKER_TYPE, TINKER_FILTER),__.in().has(TINKER_TYPE, TINKER_FILTER)));
		
		//Not (has type filter or has in node type filter)  = not has type filter OR not has in node type filter
		GraphTraversal<Vertex, Vertex> vertIt = g.traversal().V().not(__.or(__.has(TINKER_TYPE, TINKER_FILTER), __.in().has(TINKER_TYPE, TINKER_FILTER), __.has(PRIM_KEY, true)));
//		GraphTraversal<Vertex, Vertex> vertIt = g.traversal().V().not(__.in().has(TINKER_TYPE, TINKER_FILTER));
		while(vertIt.hasNext()) {
			Vertex outV = vertIt.next();
//			if(!outV.property("TYPE").equals(TINKER_FILTER)) {
				getSEMOSSVertex(vertStore, outV);
//			}
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
	
	/******************************  END DATA MAKER METHODS ******************************/
	
	/******************************  GRAPH SPECIFIC METHODS ******************************/

	//TODO: need to update and remove uniqueName from method signature
	// create or add vertex
	protected Vertex upsertVertex(String type, Object data)
	{
		if(data == null) data = EMPTY;
		// checks to see if the vertex is there already
		// if so retrieves the vertex
		// if not inserts the vertex and then returns that vertex
		Vertex retVertex = null;
		// try to find the vertex
		//			GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(TINKER_TYPE, type).has(TINKER_ID, type + ":" + data);
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(TINKER_ID, type + ":" + data);
		if(gt.hasNext()) {
			retVertex = gt.next();
		} else {
			if(data instanceof Number) {
				// need to keep values as they are, not with XMLSchema tag
				retVertex = g.addVertex(TINKER_ID, type + ":" + data, TINKER_TYPE, type, TINKER_NAME, data);// push the actual value as well who knows when you would need it
			} else {
//				LOGGER.debug(" adding vertex ::: " + TINKER_ID + " = " + type + ":" + data+ " & " + TINKER_VALUE+ " = " + value+ " & " + TINKER_TYPE+ " = " + type+ " & " + TINKER_NAME+ " = " + data);
//				LOGGER.debug(" adding vertex ::: " + TINKER_ID + " = " + type + ":" + data+ " & " + " & " + TINKER_TYPE+ " = " + type+ " & " + TINKER_NAME+ " = " + data);
				retVertex = g.addVertex(TINKER_ID, type + ":" + data, TINKER_TYPE, type, TINKER_NAME, data.toString());// push the actual value as well who knows when you would need it
			}

		}
		return retVertex; 
	}
	
	/**
	 * 
	 * @param fromVertex
	 * @param fromVertexUniqueName
	 * @param toVertex
	 * @param toVertexUniqueName
	 * @return
	 */
	protected Edge upsertEdge(Vertex fromVertex, String fromVertexUniqueName, Vertex toVertex, String toVertexUniqueName)
	{
		Edge retEdge = null;
		String type = fromVertexUniqueName + EDGE_LABEL_DELIMETER + toVertexUniqueName;
		String edgeID = type + "/" + fromVertex.value(TINKER_NAME) + ":" + toVertex.value(TINKER_NAME);
		// try to find the vertex
		GraphTraversal<Edge, Edge> gt = g.traversal().E().has(TINKER_ID, edgeID);
		if(gt.hasNext()) {
			retEdge = gt.next();
			Integer count = (Integer)retEdge.value(TINKER_COUNT);
			count++;
			retEdge.property(TINKER_COUNT, count);
		}
		else {
			retEdge = fromVertex.addEdge(type, toVertex, TINKER_ID, edgeID, TINKER_COUNT, 1);
		}

		return retEdge;
	}
	
	/******************************  END GRAPH SPECIFIC METHODS **************************/
	
	/******************************  AGGREGATION METHODS *********************************/
	
	
	@Override
	public void addRow(Object[] rowCleanData, String[] headerNames) {
//		if(rowCleanData.length != headerNames.length) {
//			throw new IllegalArgumentException("Input row must have same dimensions as levels in dataframe."); // when the HELL would this ever happen ?
//		}
//		
//		String rowString = "";
//		Vertex[] toVertices = new Vertex[headerNames.length];
//		for(int index = 0; index < headerNames.length; index++) {
//			Vertex toVertex = upsertVertex(headerNames[index], rowCleanData[index]); // need to discuss if we need specialized vertices too		
//			toVertices[index] = toVertex;
//			rowString = rowString+rowCleanData[index]+":";
//		}
//		
//		String nextPrimKey = rowString.hashCode()+"";
//		Vertex primVertex = upsertVertex(this.metaData.getLatestPrimKey(), nextPrimKey);
//		
//		for(int i = 0; i < toVertices.length; i++) {
//			this.upsertEdge(primVertex, this.metaData.getLatestPrimKey(), toVertices[i], headerNames[i]);
//		}
//		
//		//Need to update Header Names if incoming headers is different from stored header names
//		if(this.headerNames == null) {
//			this.headerNames = headerNames;
//		} 
	}

	
	public void addRelationship(Map<String, Object> rowCleanData, Map<String, Set<String>> edgeHash, Map<String, String> logicalToTypeMap) {
		boolean hasRel = false;
		for(String startNode : rowCleanData.keySet()) {
			Set<String> set = edgeHash.get(startNode);
			if(set == null) continue;

			for(String endNode : set) {
				if(rowCleanData.keySet().contains(endNode)) {
					hasRel = true;
					
					//get from vertex
					Object startNodeValue = rowCleanData.get(startNode);
					String startNodeType = logicalToTypeMap.get(startNode);
					Vertex fromVertex = upsertVertex(startNodeType, startNodeValue);
					
					//get to vertex	
					Object endNodeValue = rowCleanData.get(endNode);
					String endNodeType = logicalToTypeMap.get(endNode);
					Vertex toVertex = upsertVertex(endNodeType, endNodeValue);
					
					upsertEdge(fromVertex, startNodeType, toVertex, endNodeType);
				}
			}
		}
		
		// this is to replace the addRow method which needs to be called on the first iteration
		// since edges do not exist yet
		if(!hasRel) {
			String singleColName = rowCleanData.keySet().iterator().next();
			String singleNodeType = logicalToTypeMap.get(singleColName);
			Object startNodeValue = rowCleanData.get(singleColName);
			upsertVertex(singleNodeType, startNodeValue);
		}
		
	}

	public void addRelationship(String[] headers, Object[] values, Map<Integer, Set<Integer>> cardinality) {
		boolean hasRel = false;
		
		for(Integer startIndex : cardinality.keySet()) {
			Set<Integer> endIndices = cardinality.get(startIndex);
			if(endIndices==null) continue;
			
			for(Integer endIndex : endIndices) {
				hasRel = true;
				
				//get from vertex
				String startNode = headers[startIndex];
				String startUniqueName = headers[startIndex];
				Object startNodeValue = values[startIndex];
				Vertex fromVertex = upsertVertex(startNode, startNodeValue);
				
				//get to vertex	
				String endNode = headers[endIndex];
				String endUniqueName = headers[endIndex];
				Object endNodeValue = values[endIndex];
				Vertex toVertex = upsertVertex(endNode, endNodeValue);
				
				upsertEdge(fromVertex, startUniqueName, toVertex, endUniqueName);
			}
		}
		
		// this is to replace the addRow method which needs to be called on the first iteration
		// since edges do not exist yet
		if(!hasRel) {
			String singleColName = headers[0];
			String singleNodeType = singleColName;
			Object startNodeValue = values[0];
			upsertVertex(singleNodeType, startNodeValue);
		}
	}
	
	public void addRelationship(String[] headers, Object[] values, Map<Integer, Set<Integer>> cardinality, Map<String, String> logicalToTypeMap) {
		boolean hasRel = false;
		
		for(Integer startIndex : cardinality.keySet()) {
			Set<Integer> endIndices = cardinality.get(startIndex);
			if(endIndices==null) continue;
			
			for(Integer endIndex : endIndices) {
				hasRel = true;
				
				//get from vertex
				String startNode = headers[startIndex];
				String startUniqueName = logicalToTypeMap.get(startNode);
				if(startUniqueName == null) {
					startUniqueName = startNode;
				}
				Object startNodeValue = values[startIndex];
				Vertex fromVertex = upsertVertex(startNode, startNodeValue);
				
				//get to vertex	
				String endNode = headers[endIndex];
				String endUniqueName = logicalToTypeMap.get(endNode);
				if(endUniqueName == null) {
					endUniqueName = endNode;
				}
				Object endNodeValue = values[endIndex];
				Vertex toVertex = upsertVertex(endNode, endNodeValue);
				
				upsertEdge(fromVertex, startUniqueName, toVertex, endUniqueName);
			}
		}
		
		// this is to replace the addRow method which needs to be called on the first iteration
		// since edges do not exist yet
		if(!hasRel) {
			String singleColName = headers[0];
			String singleNodeType = logicalToTypeMap.get(singleColName);
			Object startNodeValue = values[0];
			upsertVertex(singleNodeType, startNodeValue);
		}
	}

	public void addRelationship(String[] headers, Object[] values, Map<Integer, Set<Integer>> cardinality, String[] logicalToTypeMap) {
		boolean hasRel = false;
		
		for(Integer startIndex : cardinality.keySet()) {
			Set<Integer> endIndices = cardinality.get(startIndex);
			if(endIndices==null) continue;
			
			for(Integer endIndex : endIndices) {
				hasRel = true;
				
				//get from vertex
				String startNode = headers[startIndex];
				String startUniqueName = logicalToTypeMap[startIndex];
				Object startNodeValue = values[startIndex];
				Vertex fromVertex = upsertVertex(startNode, startNodeValue);
				
				//get to vertex	
				String endNode = headers[endIndex];
				String endUniqueName = logicalToTypeMap[endIndex];
				Object endNodeValue = values[endIndex];
				Vertex toVertex = upsertVertex(endNode, endNodeValue);
				
				upsertEdge(fromVertex, startUniqueName, toVertex, endUniqueName);
			}
		}
		
		// this is to replace the addRow method which needs to be called on the first iteration
		// since edges do not exist yet
		if(!hasRel) {
			String singleNodeType = logicalToTypeMap[0];
			Object startNodeValue = values[0];
			upsertVertex(singleNodeType, startNodeValue);
		}
	}
	
//	private void removeIncompletePaths() {
//		GraphTraversal deleteVertices = GremlinBuilder.getIncompleteVertices(getSelectors(), this.g);
//		while(deleteVertices.hasNext()) {
//			Vertex v = (Vertex)deleteVertices.next();
////			System.out.println(v.value(TINKER_NAME));
////			System.out.println(v.edges(Direction.OUT).hasNext());
//			if(!(v.value(TINKER_TYPE).equals(META))){
//				System.out.println(v.value(TINKER_NAME));
//				if(v.edges(Direction.IN).hasNext()) {
//					System.out.println("why?");
//				}
//				if(v.edges(Direction.OUT).hasNext()) {
//					System.out.println("why2?");
//				}
//				System.out.println(v.edges(Direction.OUT).hasNext());
//				System.out.println(v.edges(Direction.IN).hasNext()); // == false)
//				v.remove();
//			} else {
//				System.out.println("HERE!");
//			}
//		}
//		
//		System.out.println("*************************************");
//		GraphTraversal totalVertices = g.traversal().V();
//		while(totalVertices.hasNext()) {
//			Vertex v = (Vertex)totalVertices.next();
//			System.out.println(v.value(TINKER_NAME));
//		}
//	}

//	public void removeConnection(String outType, String inType) {
//		g.traversal().V().has(TINKER_TYPE, META).has(TINKER_VALUE, outType).outE();
//		
//		Iterator<Edge> it = g.traversal().V().has(TINKER_TYPE, META).has(TINKER_VALUE, outType).outE();
//		while(it.hasNext()) {
//			Edge e = it.next();
//			if(e.inVertex().value(TINKER_VALUE).equals(inType)) {
//				e.remove();
//				return;
//			}
//		}
//	}

	/****************************** END AGGREGATION METHODS *********************************/
	
	/**
	 * 
	 * @param columnHeader - column to remove values from
	 * @param removeValues - values to be removed
	 * 
	 * removes vertices from the graph that are associated with the column and values
	 */
	public void remove(String columnHeader, List<Object> removeValues) {
		//for each value
		for(Object val : removeValues) {
			String id = columnHeader +":"+ val.toString();

			//find the vertex
			GraphTraversal<Vertex, Vertex> fgt = g.traversal().V().has(TINKER_ID, id);
			Vertex nextVertex = null;
			if(fgt.hasNext()) {
				//remove
				nextVertex = fgt.next();
				nextVertex.remove();
			}
		}
	}
	
	/****************************** END FILTER METHODS ******************************************/
	
	
	/****************************** TRAVERSAL METHODS *******************************************/
	
//	@Override
//	public Iterator<List<Object[]>> scaledUniqueIterator(String columnHeader, List<String> attributeUniqueHeaderName) {
////		List<String> selectors = null;
////		Double[] max = null;
////		Double[] min = null;
////		if(options != null && options.containsKey(AbstractTableDataFrame.SELECTORS)) {
////			selectors = (List<String>) options.get(AbstractTableDataFrame.SELECTORS);
////			int numSelected = selectors.size();
////			max = new Double[numSelected];
////			min = new Double[numSelected];
////			for(int i = 0; i < numSelected; i++) {
////				//TODO: think about storing this value s.t. we do not need to calculate max/min with each loop
////				max[i] = getMax(selectors.get(i));
////				min[i] = getMin(selectors.get(i));
////			}
////		} else {
////			selectors = getSelectors();
////			max = getMax();
////			min = getMin();
////		}
////		
////		return new UniqueScaledTinkerFrameIterator(columnHeader, selectors, g, ((TinkerMetaData)this.metaData).g, max, min);
//		return null;
//	}

	@Override
	public Double[] getColumnAsNumeric(String columnHeader) {
		if(isNumeric(columnHeader)) {
			GremlinInterpreter interp = new GremlinInterpreter(this.g.traversal(), this.metaData);
			SelectQueryStruct qs = new SelectQueryStruct();
			// add selector
			QueryColumnSelector selector = new QueryColumnSelector();
			selector.setTable(columnHeader);
			qs.addSelector(selector);
			// add filters
			qs.mergeImplicitFilters(this.grf);
			interp.setQueryStruct(qs);
			RawGemlinSelectWrapper it = new RawGemlinSelectWrapper(interp, qs);
			it.execute();
			List<Object> columnList = new ArrayList<>();
			while(it.hasNext()) {
				columnList.add(it.next().getValues()[0]);
			}
			
			return columnList.toArray(new Double[]{});
		}
		return null;
	}

	@Override
	public void removeColumn(String columnHeader) {
		// if column header doesn't exist, do nothing
		if(!ArrayUtilityMethods.arrayContainsValue(this.qsNames, columnHeader)) {
			return;
		}
		
		// A couple of thoughts from Bill Sutton
		// there are quite a few interesting scenarios here
		// the first question is: do we want to maintain duplicate rows after a column is removed? I could see yes and no depending on the scenario
		// If yes, primary keys of some sort will have to be used. if the tinker already has PKs, we are good to go. Otherwise, we are probably best off just removing the column from the selectors since it will need PKs anyway
		// If no, primary keys cause a big issue--would have to remove the nodes of interest and then clean up extra PKs
		// If no and no primary keys we again have a couple scenarios. If the node is on the fringe of the tinker, good to go--just remove it. If the node is in the middle... not sure exactly what we can do--kind of similar to issue above (no and pks)

		// For now, the most common use for this will be through explore when clicking through the metamodel. This scenario will also be don't keep duplicates, no pk, node is on the fringe. I am handling that here:
		// Remove the actual nodes from tinker
		logger.info("REMOVING COLUMN :::: " + columnHeader);
		// delete from the instances
		
		//if columnHeader has incoming prim key with no other outgoing types, delete that prim key first, then delete the columnHeader
		String columnValue = columnHeader; //this.metaData.getValueForUniqueName(columnHeader);
		GraphTraversal<Vertex, Vertex> primKeyTraversal = g.traversal().V().has(TINKER_NAME, PRIM_KEY).as("PrimKey").out(PRIM_KEY+EDGE_LABEL_DELIMETER+columnValue).has(TINKER_TYPE, columnValue).in(PRIM_KEY+EDGE_LABEL_DELIMETER+columnValue).has(TINKER_NAME, PRIM_KEY).as("PrimKey2").where("PrimKey", P.eq("PrimKey2"));
		while(primKeyTraversal.hasNext()) {
			Vertex nextPrimKey = (Vertex)primKeyTraversal.next();
			Iterator<Vertex> verts = nextPrimKey.vertices(Direction.OUT);
			
			boolean delete = true;
			while(verts.hasNext()) {
				delete = verts.next().value(TINKER_TYPE).equals(columnHeader);
				if(!delete) {
					delete = false;
					break;
				}
			}
			if(delete) {
				nextPrimKey.remove();
			}
		}
		
		g.traversal().V().has(TINKER_TYPE, columnValue).drop().iterate();
		// remove the node from meta
		this.metaData.dropVertex(columnHeader);
		this.syncHeaders();
	}

	// Backdoor entry
	public void openBackDoor(){
		Thread thread = new Thread(){
			public void run()
			{
				openCommandLine();				
			}
		};
		thread.start();
	}
	
	public GraphTraversal runGremlin(String gremlinQuery){
		//instead of running the openCommandLine we are going to specify the query that we want to return data for. 
		GraphTraversal gt = null;
		try {
			if(gremlinQuery!=null){
				long start = System.currentTimeMillis();
				logger.info("Gremlin is " + gremlinQuery);
				try {
					GremlinGroovyScriptEngine mengine = new GremlinGroovyScriptEngine();
					mengine.getBindings(ScriptContext.ENGINE_SCOPE).put("g", g);

					gt = (GraphTraversal)mengine.eval(gremlinQuery);
					System.out.println("compiled gremlin :: " + gt);
				} catch (ScriptException e) {
					e.printStackTrace();
				}
//				if(gt.hasNext()) {
//					Object data = gt.next();
//
//					String node = "";
//					if(data instanceof Map) {
//						for(Object key : ((Map)data).keySet()) {
//							Map<String, Object> mapData = (Map<String, Object>)data; //cast to map
//							if(mapData.get(key) instanceof Vertex){
//								Iterator it = ((Vertex)mapData.get(key)).properties();
//								while (it.hasNext()){
//									node = node + it.next();
//								}
//							} else {
//								node = node + mapData.get(key);
//							}
//							node = node + "       ::::::::::::           ";
//						}
//					} else {
//						if(data instanceof Vertex){
//							Iterator it = ((Vertex)data).properties();
//							while (it.hasNext()){
//								node = node + it.next();
//							}
//						} else {
//							node = node + data;
//						}
//					}
//
//					LOGGER.warn(node);
//				}
//
//				long time2 = System.currentTimeMillis();
//				LOGGER.warn("time to execute : " + (time2 - start )+ " ms");
//				return gt;
//			}
			
		}} catch (RuntimeException e) {
			e.printStackTrace();
		}
		return gt;
		

	}
	
	public Object degree(String type, String data)
	{
		GraphTraversal <Vertex, Map<Object, Object>> gt = g.traversal().V().has(TINKER_ID, type + ":" + data).group().by().by(__.bothE().count());
		Object degree = null;
		if(gt.hasNext())
		{
			Map <Object, Object> map = gt.next();
			Iterator mapKeys = map.keySet().iterator();
			while(mapKeys.hasNext())
			{
				Object key = mapKeys.next();
				Object value = map.get(key);
				degree = value;
				
				System.out.println(((Vertex)key).value(TINKER_ID) + "<<>>" + value);				
			}			
		}
		return degree;
	}
	
	
	public Long eigen(String type, String data)
	{
		Long retLong = null;
		GraphTraversal<Vertex, Map<String, Object>> gt2 = g.traversal().V().repeat(__.groupCount("m").by(TINKER_ID).out()).times(5).cap("m")
				.V()
				//.has(TINKER_ID, type + ":" + data)
				.select("m");
				//.where("V);
		if(gt2.hasNext())
		{
			Map <String, Object> map = gt2.next();
			retLong = (Long)map.get(type + ":" +  data);
			System.out.println(retLong);
		}
		
		return retLong;
	}

	public void printEigenMatrix()
	{
		GraphTraversal <Vertex, Map<Object, Object>> gt = g.traversal().V().repeat(__.groupCount("m").by(TINKER_ID).out()).times(5).cap("m"); //. //(1)
        //order(Scope.local).by(__.values(), Order.decr).limit(Scope.local, 10); //.next(); //(2)
		if(gt.hasNext())
		{
			Map <Object, Object> map = gt.next();
			Iterator mapKeys = map.keySet().iterator();
			while(mapKeys.hasNext())
			{
				Object key = mapKeys.next();
				Object value = map.get(key);
				System.out.println(key + "<<>>" + value);				
				//System.out.println(((Vertex)key).value(TINKER_ID) + "<<>>" + value);
			}			
		}
	}

	@Override
	public long size(String tableName) {
		// get a flat QS
		// which contains all the selectors 
		// and all the joins as inner 
		SelectQueryStruct qs = this.metaData.getFlatTableQs();
		if(qs.getSelectors().isEmpty()) {
			return 0;
		}
		
		GremlinInterpreter interp = new GremlinInterpreter(this.g.traversal(), this.metaData);
		interp.setLogger(this.logger);
		interp.setQueryStruct(qs);
		
		// this will count all V's
		GraphTraversal it = interp.composeIterator().count();
		if(it.hasNext()) {
			return ((Number) it.next()).longValue() / qs.getSelectors().size();
		}

		return 0;
//		// this is an approximate
//		// i am using since it is faster
//		// than flushing fully
//		long numV = 0;
//		long numE = 0;
//		GraphTraversal<Vertex, Long> itV = g.traversal().V().count();
//		if(itV.hasNext()) {
//			numV = itV.next();
//		}
//		GraphTraversal<Edge, Long> itE = g.traversal().E().count();
//		if(itE.hasNext()) {
//			numE = itE.next();
//		}
//		
//		if(numE > 0) {
//			return numV + numE;
//		}
//		return numV;
	}
	
	public boolean isOrphan(String type, String data)
	{
		boolean retValue = false;
		
		GraphTraversal<Vertex, Edge> gt = g.traversal().V().has(TINKER_ID, type + ":" + data).bothE();
		if(gt.hasNext())
		{
			System.out.println(data + "  Not Orphan");
			retValue = false;
		}
		else
		{
			System.out.println(data + "  is Orphan");
			retValue = true;
		}
		
		return retValue;
	}
	

    /**
     * Method printAllRelationship.
     */
    public void openCommandLine()
    {
          logger.warn("<<<<");
          String end = "";
         
                while(!end.equalsIgnoreCase("end"))
                {
                      try {
	                      BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
	                      logger.info("Enter Gremlin");
	                      String query2 = reader.readLine();   
	                      if(query2!=null && !query2.isEmpty()) {
	                    	  long start = System.currentTimeMillis();
		                      end = query2;
		                      logger.info("Gremlin is " + query2);
		                      GraphTraversal gt = null;
		                      try {
		                    	  GremlinGroovyScriptEngine mengine = new GremlinGroovyScriptEngine();
		                    	  mengine.getBindings(ScriptContext.ENGINE_SCOPE).put("g", g);

		                    	  gt = (GraphTraversal)mengine.eval(end);
		                    	  System.out.println("compiled gremlin :: " + gt);
		                      } catch (ScriptException e) {
		                    	  e.printStackTrace();
		                      }
		                      while(gt.hasNext())
		                      {
		              			Object data = gt.next();

		              			String node = "";
		            			if(data instanceof Map) {
		            				for(Object key : ((Map)data).keySet()) {
		            					Map<String, Object> mapData = (Map<String, Object>)data; //cast to map
		            					if(mapData.get(key) instanceof Vertex){
					              			Iterator it = ((Vertex)mapData.get(key)).properties();
					              			while (it.hasNext()){
					              				node = node + it.next();
					              			}
		            					} else {
		            						node = node + mapData.get(key);
		            					}
				              			node = node + "       ::::::::::::           ";
		            				}
		            			} else {
	            					if(data instanceof Vertex){
				              			Iterator it = ((Vertex)data).properties();
				              			while (it.hasNext()){
				              				node = node + it.next();
				              			}
	            					} else {
	            						node = node + data;
	            					}
		            			}
		            			
		                        logger.warn(node);
		                      }

		                      long time2 = System.currentTimeMillis();
		                      logger.warn("time to execute : " + (time2 - start )+ " ms");
	                      } else {
	                    	  end = "end";
	                      }
                      } catch (RuntimeException e) {
                            e.printStackTrace();
                      } catch (IOException e) {
                             e.printStackTrace();
                      }
                             
                }
    }

	public Map<? extends String, ? extends Object> getGraphOutput() {
		return createVertStores2();
	}
	
	/*
	 * a. Adding Data - nodes / relationships
	 * b. Doing some analytical routine on top of the data
	 * 	2 types here
	 * 	Map - which does for every row some calculation i.e. transformation
	 *  Reduce / Fold - which runs for all the rows i.e. Action
	 *  Or some combination of it thereof.. 
	 * c. Getting a particular set of data - some particular set of columns
	 * d. Deriving a piece of data to be added
	 * e. Getting a particular column
	 * f. Getting the rows for a particular column of data selected - special case of c for all intents and purposes
	 * g. Joining / adding a new piece of data based on existing piece of data
	 * h. Save / Read - May be we even keep this somewhere outside
	 * Given this.. can we see why we need so many methods ?
	 * 
	 */	

	/**
	 * This method will remove all nodes that are not META and are not part of the main query return
	 * This is to keep the graph as small as possible as we are making joins
	 * Blank nodes must be used to keep nodes in the tinker that do not connect to every type
	 */
	public void removeExtraneousNodes() {
//		LOGGER.info("removing extraneous nodes");
//		GremlinBuilder builder = new GremlinBuilder(g);
//		List<String> selectors = builder.generateFullEdgeTraversal();
//		builder.addSelector(selectors);
//		GraphTraversal gt = builder.executeScript();
//		if(selectors.size()>1){
//			gt = gt.mapValues();
//		}
//		GraphTraversal metaT = g.traversal().V().has(TINKER_TYPE, META).outE();
//		while(metaT.hasNext()){
//			gt = gt.inject(metaT.next());
//		}
//		
//		TinkerGraph newG = (TinkerGraph) gt.subgraph("subGraph").cap("subGraph").next();
//		this.g = newG;
//		LOGGER.info("extraneous nodes removed");
	}
	
	@Override
	public CachePropFileFrameObject save(String folderDir) throws IOException {
		CachePropFileFrameObject cf = new CachePropFileFrameObject();
		String randFrameName = "Tinker" + Utility.getRandomString(6);
		cf.setFrameName(randFrameName);

		String frameFileName = folderDir + DIR_SEPARATOR + randFrameName + ".tg";
		// save frame
		Builder<GryoIo> builder = IoCore.gryo();
		builder.graph(g);
		IoRegistry kryo = new MyGraphIoRegistry();;
		builder.registry(kryo);
		GryoIo yes = builder.create();
		try {
			yes.writeGraph(frameFileName);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IOException("Error occured attempting to cache graph frame");
		}
		cf.setFrameCacheLocation(frameFileName);

		// also save the meta details
		this.saveMeta(cf, folderDir, randFrameName);
		return cf;
	}
	
	@Override
	public void open(CachePropFileFrameObject cf) {
		// load the frame
		try {
			Builder<GryoIo> builder = IoCore.gryo();
			builder.graph(this.g);
			IoRegistry kryo = new MyGraphIoRegistry();
			builder.registry(kryo);
			GryoIo yes = builder.create();
			yes.readGraph(cf.getFrameCacheLocation());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// open the meta details
		this.openCacheMeta(cf);
	}
	
	public void insertBlanks(String colName, List<String> addedColumns) {
//		// for each node in colName
//		// if it does not have a relationship to any node in any of the addedColumns
//		// add that node to a blank
//		LOGGER.info("PERFORMING inserting of blanks.......");
//		for(String addedType : addedColumns){
//			Vertex emptyV = null;
//			String colValue = colName; //this.metaData.getValueForUniqueName(colName);
//			String addedValue = addedType; //this.metaData.getValueForUniqueName(addedType);
//			boolean forward = false;
//			GraphTraversal<Vertex, Vertex> gt = null;
//			if(this.metaData.isConnectedInDirection(colValue, addedType)){
//				forward = true;
//				gt = g.traversal().V().has(TINKER_TYPE, colValue).not(__.out(colValue+EDGE_LABEL_DELIMETER+addedValue).has(TINKER_TYPE, addedValue));
//			}
//			else {
//				gt = g.traversal().V().has(TINKER_TYPE, colValue).not(__.in(addedValue+EDGE_LABEL_DELIMETER+colValue).has(TINKER_TYPE, addedValue));
//			}
//			while(gt.hasNext()){ // these are the dudes that need an empty
//				if(emptyV == null){
//					emptyV = this.upsertVertex(addedValue, EMPTY);
//				}
//				
//				Vertex existingVert = gt.next();
//				if(forward){
//					this.upsertEdge(existingVert, colName, emptyV, addedType); 
//				}
//				else {
//					this.upsertEdge(emptyV, addedType, existingVert, colName); 
//				}
//			}
//		}
//		LOGGER.info("DONE inserting of blanks.......");
	}

	@Override
	public Map<String, String> getScriptReactors() {
		Map<String, String> reactorNames = super.getScriptReactors();
		reactorNames.put(PKQLEnum.EXPR_TERM, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLEnum.EXPR_SCRIPT, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLReactor.MATH_FUN.toString(), "prerna.sablecc.MathReactor");
		reactorNames.put(PKQLEnum.COL_CSV, "prerna.sablecc.ColCsvReactor"); // it almost feels like I need a way to tell when to do this and when not but let me see
		reactorNames.put(PKQLEnum.ROW_CSV, "prerna.sablecc.RowCsvReactor");
//		reactorNames.put(PKQLEnum.API, "prerna.sablecc.ApiReactor");
		reactorNames.put(PKQLEnum.PASTED_DATA, "prerna.sablecc.PastedDataReactor");
		reactorNames.put(PKQLEnum.WHERE, "prerna.sablecc.ColWhereReactor");
		reactorNames.put(PKQLEnum.REL_DEF, "prerna.sablecc.RelReactor");
		reactorNames.put(PKQLEnum.COL_ADD, "prerna.sablecc.TinkerColAddReactor");
		reactorNames.put(PKQLEnum.COL_SPLIT, "prerna.sablecc.TinkerColSplitReactor");
		reactorNames.put(PKQLEnum.IMPORT_DATA, "prerna.sablecc.TinkerImportDataReactor");
		reactorNames.put(PKQLEnum.REMOVE_DATA, "prerna.sablecc.RemoveDataReactor");
		reactorNames.put(PKQLEnum.FILTER_DATA, "prerna.sablecc.ColFilterReactor");
		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.VizReactor");
		reactorNames.put(PKQLEnum.UNFILTER_DATA, "prerna.sablecc.ColUnfilterReactor");
		reactorNames.put(PKQLEnum.DATA_FRAME, "prerna.sablecc.DataFrameReactor");
		reactorNames.put(PKQLEnum.DATA_TYPE, "prerna.sablecc.DataTypeReactor");
		reactorNames.put(PKQLEnum.DATA_CONNECT, "prerna.sablecc.DataConnectReactor");
		reactorNames.put(PKQLEnum.JAVA_OP, "prerna.sablecc.JavaReactorWrapper");
		reactorNames.put(PKQLEnum.DATA_FRAME_DUPLICATES, "prerna.sablecc.TinkerDuplicatesReactor");
		reactorNames.put(PKQLEnum.COL_FILTER_MODEL, "prerna.sablecc.TinkerColFilterModelReactor");
		reactorNames.put(PKQLEnum.DATA_FRAME_CHANGE_TYPE, "prerna.sablecc.TinkerChangeTypeReactor");
		reactorNames.put(PKQLEnum.QUERY_API, "prerna.sablecc.QueryApiReactor");
		reactorNames.put(PKQLEnum.CSV_API, "prerna.sablecc.CsvApiReactor");
		reactorNames.put(PKQLEnum.EXCEL_API, "prerna.sablecc.ExcelApiReactor");
		reactorNames.put(PKQLEnum.WEB_API, "prerna.sablecc.WebApiReactor");
		
//		switch(reactorType) {
//			case IMPORT_DATA : return new TinkerImportDataReactor();
//			case COL_ADD : return new TinkerColAddReactor();
//		}
		
		return reactorNames;
	}
	
	@Override
	public String getDataMakerName() {
		return TinkerFrame.DATA_MAKER_NAME;
	}

	public void changeDataType(String columnName, String newType) {
		String typeName = columnName; // getValueForUniqueName(columnName);
		GraphTraversal<Vertex, Vertex> traversal = this.g.traversal().V().has(TinkerFrame.TINKER_TYPE, typeName);
		if(newType.equalsIgnoreCase("NUMBER")) {
			// convert the string to a number
			// if it cannot be cast to a number, make it into an empty node
			while(traversal.hasNext()) {
				Vertex v = traversal.next();
				String currName = v.value(TinkerFrame.TINKER_NAME);
				try {
					double numName = Double.parseDouble(currName);
					v.property(TinkerFrame.TINKER_NAME, numName);
				} catch(NumberFormatException ex) {
					// get the empty vertex, and do all the edge connections
					Vertex emptyVertex = upsertVertex(columnName, TinkerFrame.EMPTY);
					// first do the out connections
					Iterator<Edge> outEdges = v.edges(Direction.OUT);
					while(outEdges.hasNext()) {
						Edge e = outEdges.next();
						Vertex inVertex = e.inVertex();
						String[] label = e.label().split(EDGE_LABEL_DELIMETER_REGEX_SPLIT);
						upsertEdge(emptyVertex, label[0], inVertex, label[1]);
					}
					// now do the in connections
					Iterator<Edge> inEdges = v.edges(Direction.IN);
					while(inEdges.hasNext()) {
						Edge e = inEdges.next();
						Vertex outVertex = e.inVertex();
						String[] label = e.label().split(EDGE_LABEL_DELIMETER_REGEX_SPLIT);
						upsertEdge(outVertex, label[0], emptyVertex, label[1]);
					}
					// and now drop the vertex
					// that was an invalid type
					v.remove();
				}
			}
		} else if(newType.equalsIgnoreCase("STRING")) {
			// if converting to a string
			// just loop through and get the current value and make it into a string type
			while(traversal.hasNext()) {
				Vertex v = traversal.next();
				v.property(TinkerFrame.TINKER_NAME, v.value(TinkerFrame.TINKER_NAME) + "");
			}
		} else if(newType.equalsIgnoreCase("DATE")) {
			logger.info("TINKER FRAME DOES NOT SUPPORT DATE TYPES!!!");
			throw new IllegalArgumentException("Graphs do not support date as a data type!");
		}
	}
	
	@Override
	public IRawSelectWrapper query(String query) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public IRawSelectWrapper query(SelectQueryStruct qs) {
//		qs.mergeRelations(flushRelationships(this.metaData.getAllRelationships()));
		qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, this.metaData);
		GremlinInterpreter interp = new GremlinInterpreter(this.g.traversal(), this.metaData);
		interp.setLogger(this.logger);
		interp.setQueryStruct(qs);
		logger.info("Generating Gremlin query...");
		RawGemlinSelectWrapper gdi = new RawGemlinSelectWrapper(interp, qs);
		gdi.execute();
		logger.info("Done generating query...");
		
		logger.info("Executing query...");
		QueryStructExpressionIterator qsd = new QueryStructExpressionIterator(gdi, qs);
		qsd.execute();
		logger.info("Done executing query");
		return qsd;
	}
	
	private Map<String, Map<String, List>> flushRelationships(List<String[]> rels) {
		Map<String, Map<String, List>> relMap = new HashMap<String, Map<String, List>>();
		// iterate through list
		// and make it into a map
		for(String[] rel : rels) {
			String start = rel[0];
			String end = rel[1];
			String relType = rel[2];
			
			Map<String, List> nodeComparatorMap = new HashMap<String, List>();
			if(relMap.containsKey(start)) {
				nodeComparatorMap = relMap.get(start);
			} else {
				// add it to the map
				relMap.put(start, nodeComparatorMap);
			}
			
			List values = new ArrayList();
			if(nodeComparatorMap.containsKey(relType)) {
				values = nodeComparatorMap.get(relType);
			} else {
				nodeComparatorMap.put(relType, values);
			}
			
			values.add(end);
		}
		return relMap;
	}
	
	@Override
	public boolean isEmpty() {
		GraphTraversal<Vertex, Long> tv = g.traversal().V().count();
		if(tv.hasNext()) {
			Long count = tv.next();
			if(count > 0) {
				return false;
			}
		}
		return true;
	}

	@Override
	protected Boolean calculateIsUnqiueColumn(String columnName) {
		// This reactor checks for duplicates
		boolean isUnique = false;

		// we need to know how to traverse
		List<String[]> tinkerRelationships = this.metaData.getAllRelationships();

		SelectQueryStruct qs1 = new SelectQueryStruct();
		qs1.setDistinct(false);
		qs1.mergeImplicitFilters(getFrameFilters());
		for(String[] rel : tinkerRelationships) {
			qs1.addRelation(rel[0], rel[1], rel[2]);

		}
		// as long as i have the correct joins
		// i can just query a count for any single column
		// and call it a day
		{
			QueryFunctionSelector countSelector = new QueryFunctionSelector();
			countSelector.setFunction(QueryFunctionHelper.COUNT);
			QueryColumnSelector innerSelector = new QueryColumnSelector();
			if(columnName.contains("__")) {
				String[] split = columnName.split("__");
				innerSelector.setTable(split[0]);
				innerSelector.setColumn(split[1]);
			} else {
				innerSelector.setTable(columnName);
				innerSelector.setColumn(SelectQueryStruct.PRIM_KEY_PLACEHOLDER);
			}
			countSelector.addInnerSelector(innerSelector);
			qs1.addSelector(countSelector);
		}
		Iterator<IHeadersDataRow> nRowIt = query(qs1);
		long nRow = ((Number) nRowIt.next().getValues()[0]).longValue();

		SelectQueryStruct qs2 = new SelectQueryStruct();
		qs1.setDistinct(true);
		for(String[] rel : tinkerRelationships) {
			qs2.addRelation(rel[0], rel[1], rel[2]);
		}
		qs2.mergeImplicitFilters(getFrameFilters());
		{
			// TODO: WHY CAN'T I DO A UNIQUE COUNT!!! ???
			QueryColumnSelector innerSelector = new QueryColumnSelector();
			if(columnName.contains("__")) {
				String[] split = columnName.split("__");
				innerSelector.setTable(split[0]);
				innerSelector.setColumn(split[1]);
			} else {
				innerSelector.setTable(columnName);
				innerSelector.setColumn(SelectQueryStruct.PRIM_KEY_PLACEHOLDER);
			}
			qs2.addSelector(innerSelector);
		}

		Iterator<IHeadersDataRow> uniqueNRowIt = query(qs2);
		Set<String> uniqueSet = new HashSet<String>();
		while(uniqueNRowIt.hasNext()) {
			uniqueSet.add(Arrays.toString(uniqueNRowIt.next().getValues()));
		}
		long uniqueNRow = uniqueSet.size();

		isUnique = (long) nRow == (long) uniqueNRow;
		return isUnique;
	}
	
	@Override
	public void close() {
		super.close();
		this.g.clear();
		this.g.close();
	}
	
	
	
	
	
	
	
	
	
	/////////////////////////////////////////////////////////////////////////
	
	/*
	 * Deprecated stuff
	 */
	
	@Override
	@Deprecated
	public void processDataMakerComponent(DataMakerComponent component) {
		long startTime = System.currentTimeMillis();
		logger.info("Processing Component..................................");

		List<ISEMOSSTransformation>  preTrans = component.getPreTrans();
		List<Map<String,String>> joinColList= new ArrayList<Map<String,String>> ();
		String joinType = null;
		List<prerna.sablecc2.om.Join> joins = new ArrayList<prerna.sablecc2.om.Join>();
		for (ISEMOSSTransformation transformation : preTrans) {
			if (transformation instanceof JoinTransformation) {
				Map<String, String> joinMap = new HashMap<String, String>();
				String joinCol1 = (String) ((JoinTransformation) transformation).getProperties()
						.get(JoinTransformation.COLUMN_ONE_KEY);
				String joinCol2 = (String) ((JoinTransformation) transformation).getProperties()
						.get(JoinTransformation.COLUMN_TWO_KEY);
				joinType = (String) ((JoinTransformation) transformation).getProperties()
						.get(JoinTransformation.JOIN_TYPE);
				joinMap.put(joinCol2, joinCol1); // physical in query struct
				// ----> logical in existing
				// data maker
				prerna.sablecc2.om.Join colJoin = new prerna.sablecc2.om.Join(joinCol1, joinType, joinCol2);
				joins.add(colJoin);
				joinColList.add(joinMap);
			}
		}

		// logic to flush out qs -> qs2
		QueryStruct qs = component.getQueryStruct();
		// the component will either have a qs or a query string, account for
		// that here
		SelectQueryStruct qs2 = null;
		if (qs == null) {
			String query = component.getQuery();
			qs2 = new HardSelectQueryStruct();
			((HardSelectQueryStruct) qs2).setQuery(query);
			qs2.setQsType(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
		} else {
			qs2 = new SelectQueryStruct();
			// add selectors
			Map<String, List<String>> qsSelectors = qs.getSelectors();
			for (String key : qsSelectors.keySet()) {
				for (String prop : qsSelectors.get(key)) {
					qs2.addSelector(key, prop);
				}
			}
			// add relations
			Set<String[]> rels = new RelationSet();
			Map<String, Map<String, List>> curRels = qs.getRelations();
			for(String up : curRels.keySet()) {
				Map<String, List> innerMap = curRels.get(up);
				for(String jType : innerMap.keySet()) {
					List downs = innerMap.get(jType);
					for(Object d : downs) {
						rels.add(new String[]{up, jType, d.toString()});
					}
				}
			}
			qs2.mergeRelations(rels);
			qs2.setQsType(QUERY_STRUCT_TYPE.ENGINE);
		}

		long time1 = System.currentTimeMillis();
		// set engine on qs2
		qs2.setEngineId(component.getEngineName());
		// instantiate h2importer with frame and qs
		IImporter importer = new TinkerImporter(this, qs2);
		if (joins.isEmpty()) {
			importer.insertData();
		} else {
			importer.mergeData(joins);
		}

		long time2 = System.currentTimeMillis();
		logger.info(" Processed Merging Data: " + (time2 - time1) + " ms");
//
//      processPreTransformations(component, component.getPreTrans() );
//      long time1 = System.currentTimeMillis();
//      LOGGER.info(" Processed Pretransformations: " +(time1 - startTime)+" ms");
//
//      IEngine engine = component.getEngine();
//      // automatically created the query if stored as metamodel
//      // fills the query with selected params if required
//      // params set in insightcreatrunner
//      String query = component.fillQuery();
//
//      String[] displayNames = null;
//       if(query.trim().toUpperCase().startsWith("CONSTRUCT")){
//             TinkerGraphDataModel tgdm = new TinkerGraphDataModel();
//             tgdm.fillModel(query, engine, this);
//      } else if (!query.equals(Constants.EMPTY)){
//             ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
//             //if component has data from which we can construct a meta model then construct it and merge it
//             boolean hasMetaModel = component.getQueryStruct() != null;
//             if(hasMetaModel) {
//                   
//                   // sometimes, the query returns no data
//                   // and we update the headers resulting in no information
//                   // so only add it once
//                   boolean addedMeta = false;
//                   Map[] mergedMaps = null;
//                   while(wrapper.hasNext()){
//                          if(!addedMeta) {
//                                Map<String, Set<String>> edgeHash = component.getQueryStruct().getReturnConnectionsHash();
//                                mergedMaps = TinkerMetaHelper.mergeQSEdgeHash(this.metaData, edgeHash, engine, joinColList, null);
//                                this.headerNames = this.metaData.getColumnNames().toArray(new String[]{});
//                                addedMeta = true;
//                          }
//                          
//                          ISelectStatement ss = wrapper.next();
//                          this.addRelationship(ss.getPropHash(), mergedMaps[0], mergedMaps[1]);
//                   }
//             } 
//
//             //else default to primary key tinker graph
//             else {
//                   displayNames = wrapper.getDisplayVariables();
//                   if(displayNames.length == 1) {
//                          // dont create the prim key for a single column being pulled in a query
//                          // example of this is SQL queries in explore an instance
//                          // for create queries that flush into the TinkerGraphDataModel we already take this into consideration
//                          String header = displayNames[0];
//                          
//                          Map<String, Set<String>> edgeHash = new Hashtable<String, Set<String>>();
//                          edgeHash.put(header, new HashSet<String>());
//                          Map<String, String> dataTypeMap = new Hashtable<String, String>();
//                          dataTypeMap.put(header, "STRING");
//                          mergeEdgeHash(edgeHash, dataTypeMap);
//                          
//                          List<String> fullNames = this.metaData.getColumnNames();
//                          this.headerNames = fullNames.toArray(new String[fullNames.size()]);
//                          
//                          // need to pass in a map
//                          // this would be where we would take advantage of using display names
//                          Map<String, String> logicalToTypeMap = new HashMap<String, String>();
//                          logicalToTypeMap.put(header, header);
//                          
//                          // clear the edge hash so the tinker frame knows right away
//                          // to just add this as a single vertex
//                          edgeHash.clear();
//                          
//                          // actually go through and add the data
//                          while(wrapper.hasNext()) {
//                                 addRelationship(wrapper.next().getPropHash(), edgeHash, logicalToTypeMap);
//                          }
//                   } else {
//                          // need to make a prim key
//                          TinkerMetaHelper.mergeEdgeHash(this.metaData, TinkerMetaHelper.createPrimKeyEdgeHash(displayNames));
//                          List<String> fullNames = this.metaData.getColumnNames();
//                          this.headerNames = fullNames.toArray(new String[fullNames.size()]);
//                          
//                          // actually go through and add the data
//                          while(wrapper.hasNext()) {
//                                 this.addRow(wrapper.next());
//                          }
//                   }
//             }
//      }
//      //           g.variables().set(TINKER_HEADER_NAMES, this.headerNames); // I dont know if i even need this moving forward.. but for now I will assume it is
//      //           redoLevels(this.headerNames);
//
//      long time2 = System.currentTimeMillis();
//      LOGGER.info(" Processed Wrapper: " +(time2 - time1)+" ms");
//
//      processPostTransformations(component, component.getPostTrans());
//      processActions(component, component.getActions());
//
//      long time4 = System.currentTimeMillis();
//      LOGGER.info("Component Processed: " +(time4 - startTime)+" ms");
  }


	
	
	
	
	
	
	
	
	
	/**********************    TESTING PLAYGROUND  ******************************************/
	
//	public static void main(String [] args) throws Exception
//	{
////		testDeleteRows();
////		TinkerFrame t3 = new TinkerFrame();
////		testPaths();
//		
//		//tinkerframe to test on
//		String fileName = "C:\\Users\\rluthar\\Documents\\Movie_Data.csv";
//		Map<String, Map<String, String>> dataTypeMap = new HashMap<>();
//		Map<String, String> innerMap = new LinkedHashMap<>();
//		
//		innerMap.put("Title", "VARCHAR");
//		innerMap.put("Genre", "VARCHAR");
//		innerMap.put("Studio", "VARCHAR");
//		innerMap.put("Director", "VARCHAR");
//		
//		dataTypeMap.put("CSV", innerMap);
//		TinkerFrame t = (TinkerFrame) TableDataFrameFactory.generateDataFrameFromFile(fileName, ",", "Tinker", dataTypeMap, new HashMap<>());
////		TinkerFrame t = load2Graph4Testing(fileName);
//		
//		Iterator<Object[]> it = t.iterator();
//		int count = 0; 
//		while(it.hasNext()) {
////			System.out.println(it.next());
//			it.next();
//			count++;
//		}
//		System.out.println("COUNT IS: "+count);
//		
//		List<Object> list = new ArrayList<>();
//		list.add("Drama");
////		list.add("Gravity");
////		list.add("Her");
////		list.add("Admission");
//		t.remove("Genre", list);
//		
//		it = t.iterator();
//		count = 0; 
//		while(it.hasNext()) {
////			System.out.println(it.next());
//			it.next();
//			count++;
//		}
//		System.out.println("COUNT IS: "+count);
////		t.openSandbox();
////		testGroupBy();
////		testFilter();
////		testCleanup();
////		new TinkerFrame().doTest();
//		//t3.writeToFile();
//		//t3.readFromFile();
//		//t3.doTest();
////		t3.tryCustomGraph();
////		t3.tryFraph();
//		/*
//		Configuration config = new BaseConfiguration();
//		config.setProperty("gremlin.tinkergraph.graphLocation", "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\tinker.persist");
//		config.setProperty("gremlin.tinkergraph.graphFormat", "gryo");
//		
//		
//		Graph g = TinkerGraph.open(); TinkerFactory.createModern();
//		
//		Vertex b = g.addVertex("y");
//		b.property("name", "y");
//		for(int i = 0;i < 1000000;i++)
//		{
//			Vertex a = g.addVertex("v" + i);
//			a.property("name", "v"+i);
//			b.addEdge("generic", a);
//			//Edge e = g.(null, a, b, "sample");
//		}	
//		System.out.println("here.. ");
//		Vertex v = g.traversal().V().has("name", "v1").next();
//		//Vertex v = g.("name", "y").iterator().next();
//		System.out.println("done.. ");
//		
//		
//		g.close();
//		
//		
//		
//		g = TinkerFactory.createModern();
//		
//		
//		// experiments
//		*/
//		
//	}
//	
//	public static void testDeleteRows() {
//		String fileName = "C:\\Users\\rluthar\\Documents\\Movie Results.xlsx";
//		TinkerFrame t = load2Graph4Testing(fileName);
//		
//		//what i want to delete
//		Map<String, Object> deleteMap = new HashMap<>();
//		deleteMap.put("Studio", "Fox");
//		deleteMap.put("Year", 2007.0);
//		
//		Iterator<Object[]> iterator = t.iterator();
//		List<Object[]> deleteSet = new ArrayList<>();
//		List<String> selectors = t.getSelectors();
//		while(iterator.hasNext()) {
//			boolean addRow = true;
//			Object[] row = iterator.next();
//			for(String column : deleteMap.keySet()) {
//				int index = selectors.indexOf(column);
//				if(!row[index].equals(deleteMap.get(column))) {
//					addRow = false;
//					break;
//				}
//			}
//			
//			if(addRow) {
//				deleteSet.add(row);
//				System.out.println(Arrays.toString(row));
//			}
//		}
//		
//		Set<Integer> indexes =  new HashSet<>();
//		for(String column : deleteMap.keySet()) {
//			indexes.add(selectors.indexOf(column));
//		}
//		
//		for(Object[] row : deleteSet) {
//			//delete all the edges necessary
//			for(int i = 0; i < row.length; i++) {
//				if(!indexes.contains(i)) {
//					String column = selectors.get(i);
//					//we have the column
//					Vertex v = t.upsertVertex(column, row[i]);
//					
//					//we have the instance
//					//how to determine what edges to delete
//				}
//			}
//		}
//	}
//
//	public static void testGroupBy() {
//		String fileName = "C:\\Users\\rluthar\\Documents\\Movie Results.xlsx";
//		TinkerFrame tinker = load2Graph4Testing(fileName);
//		
//		TinkerFrameStatRoutine tfsr = new TinkerFrameStatRoutine();
//		Map<String, Object> functionMap = new HashMap<String, Object>();
//		functionMap.put("math", "count");
//		functionMap.put("name", "Studio");
//		functionMap.put("calcName", "NewCol");
//		functionMap.put("GroupBy", new String[]{"Studio"});
//		
//		tfsr.setSelectedOptions(functionMap);
//		tfsr.runAlgorithm(tinker);
//	}
//	
//	public static void testPaths() {
//		String fileName = "C:\\Users\\bisutton\\Desktop\\pregnancy.xlsx";
//		TinkerFrame tinker = load2Graph4Testing(fileName);
//		tinker.printTinker();
//		
////		GremlinBuilder builder = GremlinBuilder.prepareGenericBuilder(Arrays.asList(tinker.getColumnHeaders()), tinker.g);
////		GraphTraversal paths = builder.executeScript().path();
//////		Object o = paths.next();
////		int count = 0;
////		while(paths.hasNext()){
////		System.out.println(paths.next());
////		count++;
////		}
////		System.out.println(count);
//	}
//	
//	public static void testFilter() {
//		String fileName = "C:\\Users\\rluthar\\Documents\\Movie Results.xlsx";
//		TinkerFrame tinker = load2Graph4Testing(fileName);
//		tinker.printTinker();
//		
//		Iterator<Object> uniqIterator = tinker.uniqueValueIterator("Studio", false);
//		List<Object> uniqValues = new Vector<Object>();
//		while(uniqIterator.hasNext()) {
//			uniqValues.add(uniqIterator.next());
//		}
//		
//		List<Object> filterValues = new ArrayList<Object>();
//		for(Object o : uniqValues) {
//			if(!(o.toString().equals("CBS"))) {
//				filterValues.add(o);
//			}
//		}
////		tinker.filter("Studio", filterValues);		
//		tinker.printTinker();
//		
//		uniqIterator = tinker.uniqueValueIterator("Genre Updated", false);
//		uniqValues = new Vector<Object>();
//		while(uniqIterator.hasNext()) {
//			uniqValues.add(uniqIterator.next());
//		}
//		
//		filterValues = new ArrayList<Object>();
//		filterValues.add("Drama");
////		tinker.filter("Genre Updated", filterValues);
//		tinker.printTinker();
//		//print tinker
//		
////		tinker.unfilter("Title");
//		
//		//print tinker
//		
//		tinker.unfilter();
//		tinker.printTinker();
//		//print tinker
//	}
//	
//	public static void testCleanup() {
//		String fileName = "C:\\Users\\rluthar\\Documents\\Movie Results.xlsx";
//		TinkerFrame tinker = load2Graph4Testing(fileName);
//		GremlinBuilder builder = GremlinBuilder.prepareGenericBuilder(tinker.getSelectors(), tinker.g, ((TinkerMetaData)tinker.metaData).g, null);
//		GraphTraversal traversal = (GraphTraversal)builder.executeScript();
//		traversal = traversal.V();
//		GraphTraversal traversal2 = tinker.g.traversal().V();
//		Set<Vertex> deleteVertices = new HashSet<Vertex>();
//		while(traversal2.hasNext()) {
//			deleteVertices.add((Vertex)traversal2.next());
//		}
//		while(traversal.hasNext()) {
//			deleteVertices.remove((Vertex)traversal.next());
//		}
//		
//		for(Vertex v : deleteVertices) {
//			v.remove();
//		}
////		System.out.println(t.size());
////		for(Object key : t.keySet()) {
////			Object o = t.get(key);
////			System.out.println(key instanceof Vertex);
////			System.out.println(o.toString());
////		}
//		System.out.println("Done");
//	}
//
//	public void printTinker() {
//		Iterator<Object[]> iterator = this.iterator();
//		while(iterator.hasNext()) {
//			System.out.println(Arrays.toString(iterator.next()));
//		}
//	}
//	
//	public void tryCustomGraph()
//	{
//		g = TinkerGraph.open();
//		
//		long now = System.nanoTime();
//		System.out.println("Time now.. " + now);
//		
//		String [] types = {"Capability", "Business Process", "Activity", "DO", "System"};
////		String[] types = {"Capability"};
//		int [] nums = new int[5];
////		int [] nums = new int[1];
//		nums[0] = 2;
//		nums[1] = 5;
//		nums[2] = 8;
//		nums[3] = 1;
//		nums[4] = 1;
//		
//		for(int typeIndex = 0;typeIndex < types.length;typeIndex++)
//		{
//			String parentTypeName = types[typeIndex];
//			if(typeIndex + 1 < types.length)
//			{
//				String childTypeName = types[typeIndex + 1];
//				int numParent = nums[typeIndex];
//				int numChild = nums[typeIndex+1];
//			
//				for(int parIndex = 0;parIndex < numParent;parIndex++)
//				{
//					Vertex parVertex = upsertVertex(parentTypeName, parentTypeName + parIndex);
//					parVertex.property("DATA", parIndex);
//					for(int childIndex = 0;childIndex < numChild;childIndex++)
//					{
//						Vertex childVertex = upsertVertex(childTypeName, childTypeName + childIndex);
//						Object data = childIndex;
//						childVertex.property("DATA", data);
//						upsertEdge(parVertex, parentTypeName, childVertex, childTypeName);	
//					}
//				}
//				Map <String, Set<String>> edges = new Hashtable <String, Set<String>>();
//				Set set = new HashSet();
//				set.add(childTypeName);
//				edges.put(parentTypeName, set);
//				TinkerMetaHelper.mergeEdgeHash(this.metaData, edges, null);
//			}
//			
//			else {
//				//just add a vertex
//				Vertex vert = upsertVertex(types[0], types[0]+"1");
//				Vertex vert2 = upsertVertex(types[0], types[0]+"2");
//			}
//		}
//		
//		long graphTime = System.nanoTime();
//		System.out.println("Time taken.. " + ((graphTime - now) / 1000000000) + " secs");
//
//		
//		System.out.println("Graph Complete.. ");
//		
//		System.out.println("Total Number of vertices... ");
//		GraphTraversal gtCount = g.traversal().V().count();
//		if(gtCount.hasNext())
//			System.out.println("Vertices...  " + gtCount.next());
//
//		GraphTraversal gtECount = g.traversal().E().values("COUNT").sum();
//		if(gtECount.hasNext())
//			System.out.println("Edges...  " + gtECount.next());
//
//		
//		System.out.println("Trying group by on the custom graph");
//		GraphTraversal<Vertex, Map<Object, Object>> gt = g.traversal().V().group().by("TYPE").by(__.count());
////		GraphTraversal<Vertex, Map<Object, Object>> gt = g.traversal().V().group().by("TYPE").by(__.);
//		if(gt.hasNext())
//			System.out.println(gt.next());
//		System.out.println("Completed group by");
//		
//		System.out.println("Trying max");
//		GraphTraversal<Vertex, Number> gt2 = g.traversal().V().has("TYPE", "Activity").values(TINKER_NAME).max();
////		if(gt2.hasNext())
////			System.out.println(gt2.next());
//		System.out.println("Trying max - complete");
//
//		System.out.println("Trying max group");
//		GraphTraversal  gt3 = g.traversal().V().group().by("TYPE").by(__.values("DATA").max());
//		if(gt3.hasNext())
//			System.out.println(gt3.next());
//		System.out.println("Trying max group - complete");
//		
//		
//		GraphTraversal<Vertex, Map<String, Object>> gtAll = g.traversal().V().has("TYPE", "Capability").as("Cap").
//				out().V().as("BP").
//				out().as("Activity").
//				out().as("DO").
//				out().as("System").
//				range(4, 10).
//				select("Cap", "BP", "Activity", "DO", "System").by("VALUE");
//		int count = 0;
//		while(gtAll.hasNext())
//		{
//			//Map output = gtAll.next();
//			System.out.println(gtAll.next());
//			count++;
//		}
//		System.out.println("Count....  " + count);
//		
//		/*
//		GraphTraversal<Vertex, Long> gtAllC = g.traversal().V().has("TYPE", "Capability").as("Cap").out().as("BP").
//				out().as("Activity").
//				out().as("DO").out().as("System").
//				select("Cap", "BP", "Activity", "DO", "System").count();
//		
//		while(gtAllC.hasNext())
//		{
//			System.out.println("Total..  " + gtAllC.next());
//		}*/
//
//
//		long opTime = System.nanoTime();
//		System.out.println("Time taken.. for ops" + ((opTime - graphTime) / 1000000000) + " secs");
//		
//		headerNames = types;
//
//		Vector <String> cols = new Vector<String>();
//		cols.add("Capability"); 
//		cols.add("Business Process");
//		//, "Activity", "DO", "System"};
//
//		//getRawData();
//		Iterator out = getIterator(cols);
//		if(out.hasNext())
//			System.out.println("Output is..  " + out.next());
//	}
//
//	private void tryBuilder()
//	{
//		
//		
//		
//	}
//	
//	public void openSandbox() {
//		Thread thread = new Thread(){
//			public void run()
//			{
//				openCommandLine();				
//			}
//		};
//	
//		thread.start();
//	}
//	
//	public void writeToFile()
//	{
//		Configuration config = new BaseConfiguration();
//		config.setProperty("gremlin.tinkergraph.graphLocation", "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\tinker.persist");
//		config.setProperty("gremlin.tinkergraph.graphFormat", "gryo");
//		
//		
//		Graph g = TinkerGraph.open(config); 
//		
//		Vertex b = g.addVertex("y");
//		b.property("name", "y");
//		for(int i = 0;i < 1000000;i++)
//		{
//			Vertex a = g.addVertex("v" + i);
//			a.property("name", "v"+i);
//			b.addEdge("generic", a);
//			//Edge e = g.(null, a, b, "sample");
//		}	
//		System.out.println("here.. ");
//		Vertex v = g.traversal().V().has("name", "v1").next();
//		//Vertex v = g.("name", "y").iterator().next();
//		System.out.println("done.. ");
//		
//		
//		try {
//			System.out.println("Writing to file... ");
//			g.close();
//			System.out.println("written");
//
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//	}
//	
//	public void readFromFile(){
//		Configuration config = new BaseConfiguration();
//		config.setProperty("gremlin.tinkergraph.graphLocation", "C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\tinker.persist");
//		config.setProperty("gremlin.tinkergraph.graphFormat", "gryo");
//		
//		
//		Graph g = TinkerGraph.open(); 
//		try {
//			long time = System.nanoTime();
//
//			System.out.println("reading from file... ");
//
//			g.io(IoCore.gryo()).readGraph("C:\\Users\\pkapaleeswaran\\workspacej3\\Exp\\tinker.persist");
//			long delta = System.nanoTime() - time;
//			System.out.println("Search time in nanos " + (delta/1000000000));
//			
//			System.out.println("complte");
//			Vertex v = g.traversal().V().has("name", "v1").next();
//			//Vertex v = g.("name", "y").iterator().next();
//			System.out.println("done.. " + v);
//			
//			
//			g.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//
//	}
//	
//	public void testCount() {
//		GraphTraversal<Vertex, Map<String, Object>> gtAll = g.traversal().V().has("TYPE", "Capability").as("Cap").
//				out().V().as("BP").
//				out().as("Activity").
//				out().as("DO").
//				out().as("System").
//				range(4, 10).
//				select("Cap", "BP", "Activity", "DO", "System").by("VALUE");
//		
////		g.traversal().V().ou
//	}
//	
//	public void doTest()
//	{
//		Graph g = TinkerFactory.createModern();
//		
//		// trying to see the path
//		GraphTraversal  <Vertex,Path> gt = g.traversal().V().as("a").out().as("b").out().values("name").path(); //.select("a","b");
//		while(gt.hasNext())
//		{
//			Path thisPath = gt.next();
//			for(int index = 0;index < thisPath.size();index++)
//			{
//				//Vertex v = (Vertex)thisPath.get()
//				System.out.print(thisPath.get(index) + "");
//			}
//			System.out.println("\n--");
//		}
//		
//
//		System.out.println("Trying select.. ");
//		
//		
//		GraphTraversal<Vertex, Map<String, Object>> gt2 = g.traversal().V().as("a").out().as("b").out().as("c").select("a","b","c");
//		
//		while(gt2.hasNext())
//		{
//			Map<String, Object> map = gt2.next();
//			Iterator <String> keys = map.keySet().iterator();
//			while(keys.hasNext())
//			{
//				Vertex v = (Vertex)map.get(keys.next());
//				System.out.print(v.value("name") + "-");
//			}
//			System.out.println("\n--");
//		}
//
//		System.out.println("Trying Group By.. ");
//		
//		GraphTraversal<Vertex,Map<Object,Object>> gt3 = g.traversal().V().as("a").group().by("name").by(__.count());
//		
//		while(gt3.hasNext())
//		{
//			Map<Object, Object> map = gt3.next();
//			Iterator <Object> keys = map.keySet().iterator();
//			while(keys.hasNext())
//			{
//				Object key = keys.next();
//				System.out.print(key + "<>" + map.get(key));
//			}
//			System.out.println("\n--");
//		}
//		
//		System.out.println("Trying coalesce.. ");
//		
//		GraphTraversal<Vertex, Object> gt4 = g.traversal().V().coalesce(__.values("lang"), __.values("name"));
//		while(gt4.hasNext())
//		{
//			System.out.println(gt4.next());
//			/*
//			Map<Object, Object> map = gt4.next();
//			Iterator <Object> keys = map.keySet().iterator();
//			while(keys.hasNext())
//			{
//				Object key = keys.next();
//				System.out.print(key + "<>" + map.get(key));
//			}
//			System.out.println("\n--");*/
//		}
//
//		System.out.println("Trying choose.. with constant");
//		GraphTraversal<Vertex, Map<Object, Object>> gt5 = g.traversal().V().choose(__.has("lang"),__.values("lang"), __.constant("c#")).as("lang").group();
//		while(gt5.hasNext())
//		{
//			System.out.println(gt5.next());
//		}
//
//		System.out.println("Trying choose.. with vertex");
//		GraphTraversal<Vertex, Map<Object, Object>> gt6 = g.traversal().V().choose(__.has("lang"),__.as("a"), __.as("b")).group();
//		while(gt6.hasNext())
//		{
//			System.out.println(gt6.next());
//		}
//		
//		System.out.println("testing repeat.. ");
//		GraphTraversal<Vertex, Path> gt7 = g.traversal().V(1).repeat(__.out()).times(2).path().by("name");
//		while(gt7.hasNext())
//		{
//			Path thisPath = gt7.next();
//			for(int index = 0;index < thisPath.size();index++)
//			{
//				//Vertex v = (Vertex)thisPath.get()
//				System.out.print(thisPath.get(index) + "");
//			}
//			System.out.println("\n--");
//		}
//
//		System.out.println("Trying.. until.. ");
//		//GraphTraversal<Vertex, Path> gt8 = g.traversal().V().as("a").until(__.has("name", "ripple")).as("b").repeat(__.out()).path().by("name");
//		GraphTraversal<Vertex, Map<String, Object>> gt8 = g.traversal().V().as("a").where(__.has("name", "marko")).until(__.has("name", "ripple")).as("b").repeat(__.out()).select("a","b");
//		while(gt8.hasNext())
//		{
//			Map thisPath = gt8.next();
//			System.out.println(thisPath);
//			/*for(int index = 0;index < thisPath.size();index++)
//			{
//				//Vertex v = (Vertex)thisPath.get()
//				System.out.println(thisPath);
//				System.out.print(thisPath.get(index));
//			}*/
//			System.out.println("\n--");
//		}
//
//		System.out.println("Trying arbitrary selects.. ");
//		GraphTraversal<Vertex, Vertex> gt9 = g.traversal().V().as("a").out().as("b").out().as("c");
//		GraphTraversal<Vertex, Vertex> gt10 = gt9.select("c");
//		while(gt10.hasNext())
//		{
//			System.out.println(gt10.next());
//			System.out.println("\n--");
//		}
//
//		//GroovyShell shell = new GroovyShell();
//		
//		System.out.println("Testing subgraph.. ");
//		Graph sg = (Graph)g.traversal().E().hasLabel("knows").subgraph("subGraph").cap("subGraph").next();
//		org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource source =  sg.traversal(sg.traversal().standard());
//		GraphTraversal <Edge, Edge> output = source.E();
//	
//		while(output.hasNext())
//		{
//			System.out.println(output.next().inVertex().id());
//		}
//		System.out.println("It is a subgraph now.. ");
//		
//		System.out.println("Testing partition"); // useful when I want to roll back and move forward // can also be used for filtering..
//		SubgraphStrategy stratA = SubgraphStrategy.build().vertexCriterion(__.hasId(1)).create(); // putting id 1 into a separate subgraph
//		//GraphTraversal<Vertex, Vertex> gt11 = g.traversal().V().has("name", P.not(P.within("marko", "ripple")));
//		String [] names = {"marko", "ripple"};
//		GraphTraversal<Vertex, Vertex> gt11 = g.traversal().V().has("name", P.without(names));
//		
//		while(gt11.hasNext())
//		{
//			System.out.println(gt11.next());
//		}
//		System.out.println("Now printing from partition");
//		GraphTraversalSource newGraph = GraphTraversalSource.build().with(stratA).create(g);
//		gt11 = newGraph.V().has("name", P.within("marko"));
//		//gt11 = newGraph.V();
//		
//		while(gt11.hasNext())
//		{
//			System.out.println(gt11.next());
//		}
//		
//		
//		System.out.println("Testing.. variables.. ");
//		
//		String [] values = {"Hello World", "too"};
//		g.variables().set("Data",values);
//		
//		System.out.println("Column Count ? ...  " + (
//				(String[])(g.variables().get("Data").get())
//				).length
//				);
//		
//		System.out.println("Getting max values and min values.. ");
//		System.out.println(g.traversal().V().values("age").max().next());
//		System.out.println("Getting Sum.... ");
//		System.out.println(g.traversal().V().values("age").sum().next());
//		
//		
//		System.out.println("Trying count on vertex.. ");
//		System.out.println(g.traversal().V().count().next());
//		
//		// Things to keep in the variables
//		// String array of all the different nodes that are being added - Not sure given the graph
//		// For every column - what is the base URI it came with, database which it came from, possibly host and server as well
//		// For every column - also keep the logical name it is being referred to as vs. the logical name that it is being referred to in its native database
//		// this helps in understanding how the user relates these things together
//		// There is a default partition where the graph is added
//		// Need to find how to adjust when a node gets taken out - use with a match may be ?
//		//
//		
//		// counting number of occurences
//		// I need to pick the metamodel partition
//		// from this partition I need to find what are the connections
//		// from one point to another
//		
//		
//
//		// things I still need to try
//		// 1. going from one node to a different arbitrary node
//		// and then getting that subgraph
//		
//		// 2. Ability to find arbitrary set of nodes based on a filter on top of the 1
//		
//	}
//	
//	public void tryFraph()
//    {
//           Hashtable <String, Vector<String>> hash = new Hashtable<String, Vector<String>>();
//           
//           
//           Vector <String> TV = new Vector<String>();
//           TV.add("S");
//           TV.add("G");
//           
//           Vector <String> SV = new Vector<String>();
//           SV.add("A");
//           SV.add("D");
//           
//           hash.put("T", TV);
//           hash.put("S",SV);
//           
//           //Hashtable <String, Integer> outCounter = 
//           
//           // get the starting point
//           String start = "T";
//           String output = "";
//           output = output + ".traversal().V()";
//           Vector <String> nextRound = new Vector<String>();
//
//           nextRound.add(start);
//           
//           boolean firstTime = true;
//           
//           while(nextRound.size() > 0)
//           {
//                  Vector <String> realNR = new Vector<String>();
//                  System.out.println("Came in here.. ");
//                  for(int nextIndex = 0;nextIndex < nextRound.size();nextIndex++)
//                  {
//                        String element = nextRound.remove(nextIndex);
//                        if(hash.containsKey(element))
//                        {
//                               
//                               output = output + ".has('" + "TYPE" + "','" + element + "')";
//                               if(firstTime)
//                               {
//                                      output = output + ".as('" + element + "')";
//                                      firstTime = false;
//                               }
//                               Vector <String> child = hash.remove(element);
//                               output = addChilds(child, realNR, hash, output, element);
//                        }
//                        else
//                        {
//                               // no need to do anything it has already been added
//                        }
//                  }
//                  
//                  nextRound = realNR;
//                  
//           }
//           
//           System.out.println(output);
//    }
//    
//    // adds all the childs and leaves it in the same state as before
//    private String addChilds(Vector <String> inputVector, Vector <String> outputVector, Hashtable <String, Vector<String>> allHash, String inputString, String inputType)
//    {
//           for(int childIndex = 0;childIndex < inputVector.size();childIndex++)
//           {
//                  String child = inputVector.get(childIndex);
//                  inputString = inputString + ".out().has('" + "TYPE" + "','" + child + "').as('" + child + "').in().has('" + "TYPE" + "', '" + inputType + "')";
//                  if(allHash.containsKey(child))
//                        outputVector.add(child);
//           }
//           if(outputVector.size() > 0)
//                  inputString = inputString + ".out()";
//           return inputString;
//    }
//
//	public GraphTraversal <Vertex, Vertex> getLastVerticesforType(String type, String instanceName)
//	{
//	
//		// get the type for the last levelname
//		String lastLevelType = headerNames[headerNames.length-1];
//		GraphTraversal<Vertex, Vertex> gt8 = g.traversal().V().has(TINKER_TYPE, type).has(TINKER_NAME, instanceName).until(__.has(TINKER_TYPE, lastLevelType)).as("b").repeat(__.out()).select("b");
//		
//		return gt8;
//	}
//	
//	private static TinkerFrame load2Graph4Testing(String fileName){
//		XSSFWorkbook workbook = null;
//		FileInputStream poiReader = null;
//		try {
//			poiReader = new FileInputStream(fileName);
//			workbook = new XSSFWorkbook(poiReader);
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//		XSSFSheet lSheet = workbook.getSheet("Sheet1");
//		
//		int lastRow = lSheet.getLastRowNum();
//		XSSFRow headerRow = lSheet.getRow(0);
//		List<String> headerList = new ArrayList<String>();
//		int totalCols = 0;
//		while(headerRow.getCell(totalCols)!=null && !headerRow.getCell(totalCols).getStringCellValue().isEmpty()){
//			headerList.add(headerRow.getCell(totalCols).getStringCellValue());
//			totalCols++;
//		}
//		Map<String, Object> rowMap = new HashMap<>();
//		TinkerFrame tester = new TinkerFrame();
//		for (int rIndex = 1; rIndex <= lastRow; rIndex++) {
//			XSSFRow row = lSheet.getRow(rIndex);
//			Object[] nextRow = new Object[totalCols];
//			for(int cIndex = 0; cIndex<totalCols ; cIndex++)
//			{
//				Object v1;
//				if(row.getCell(cIndex)!=null){
//	
//					int cellType = row.getCell(cIndex).getCellType();
//					
//					if(cellType == Cell.CELL_TYPE_NUMERIC) {
//						 v1 = row.getCell(cIndex).getNumericCellValue();
//						 nextRow[cIndex] = v1;
//					} else if(cellType == Cell.CELL_TYPE_BOOLEAN) {
//						v1 = row.getCell(cIndex).getBooleanCellValue();
//						nextRow[cIndex] = v1;
//					} else {			
//						 v1 = row.getCell(cIndex).toString();
//						 nextRow[cIndex] = v1;
//					}
//					nextRow[cIndex] = v1;
//					rowMap.put(headerList.get(cIndex), v1);
//				}
//				else {
//					nextRow[cIndex] = EMPTY;
//					v1 = EMPTY;
//					rowMap.put(headerList.get(cIndex), v1);
//				}
//			}
//			tester.addRow(nextRow, headerList.toArray(new String[headerList.size()]));
//			System.out.println("added row " + rIndex);
//			System.out.println(rowMap.toString());
//		}
//		System.out.println("loaded file " + fileName);
//		
//		// 2 lines are used to create primary key table metagraph
//		Map<String, Set<String>> primKeyEdgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(headerList.toArray(new String[headerList.size()]));
//		TinkerMetaHelper.mergeEdgeHash(tester.metaData, primKeyEdgeHash, null);
//		return tester;
//	}

	/**********************   END TESTING PLAYGROUND  **************************************/
}
