package prerna.ds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.Dashboard;
import prerna.om.Insight;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ds.AbstractTableDataFrame.Comparator;
import prerna.ds.h2.H2Frame;
import prerna.ds.util.H2FilterHash;

/**
 * 
 *
 */
public class DataFrameJoiner {

	private static final Logger LOGGER = LogManager.getLogger(DataFrameJoiner.class.getName());

	private Dashboard dashboard;
	
	protected GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
	protected TinkerGraph dataFrameMetaGraph = null;
	
	private static final String VERTEX_ID = "VERTEX_ID";
	private static final String EDGE_ID = "EDGE_ID";
	private static final String EDGE_DELIMITER = ":::";
	private static final String FILTERHASH = "FILTERHASH";
	private static final String INSIGHT_KEY = "INSIGHT_KEY";
	private static final String JOIN_COLUMNS = "JOIN_COLUMNS";
	
	
	public DataFrameJoiner(Dashboard dashboard) {
		this.dashboard = dashboard;
		this.dataFrameMetaGraph = TinkerGraph.open();
	}
	
	/****************************************
	 * PUBLIC GETTERS
	 ****************************************/
	
	
	public Dashboard getDashboard() {
		return this.dashboard;
	}
	
	/**
	 * 
	 * @return
	 * 
	 * Returns all the insights in the graph
	 */
	public List<Insight> getInsights() {
		
		List<Insight> insights = new ArrayList<>();
		GraphTraversal<Vertex, Vertex> gt = dataFrameMetaGraph.traversal().V();
		
		while(gt.hasNext()) {
			Insight nextInsight = gt.next().value(INSIGHT_KEY);
			insights.add(nextInsight);
		}
		return insights;
	}
	
	/**
	 * 
	 * @param insight
	 * @return
	 * returns if the vertex associated with the node has any edges
	 * 		i.e. connections to other vertices (joins)
	 */
	public boolean isJoined(Insight insight) {
		Vertex vert = getVertex(insight);
		if(vert == null) {
			return false;
		} else {
			return vert.edges(Direction.BOTH).hasNext();
		}
	}
	
	/**
	 * 
	 * @return
	 * 
	 * returns a map in the form:
	 * 		String -> List<String>
	 * 			String key is an insight Id
	 * 			List is list of insight id's joined to the key
	 */
	public Map<String, List<String>> getJoinedInsightMap() {
		Map<String, List<String>> joinedInsightMap = new HashMap<>();
		GraphTraversal<Vertex, Vertex> vertexTraversal = this.dataFrameMetaGraph.traversal().V();
		while(vertexTraversal.hasNext()) {
			Vertex startVertex = vertexTraversal.next();
			String insightId = ((Insight)startVertex.value(INSIGHT_KEY)).getInsightID();
			if(!joinedInsightMap.containsKey(insightId)) {
				Set<Vertex> vertexIsland = new HashSet<>();
				vertexIsland.add(startVertex);
				grabIsland(startVertex, vertexIsland);
				addIslandToJoinedInsightMap(joinedInsightMap, vertexIsland);
			}
		}
		return joinedInsightMap;
	}
	
	private void grabIsland(Vertex startVert, Set<Vertex> vertexIsland) {
		// for each downstream node of this meta node
		Iterator<Vertex> nextVerts = startVert.vertices(Direction.BOTH);
		while(nextVerts.hasNext()) {
			Vertex nextVert = nextVerts.next();
			if(!vertexIsland.contains(nextVert)) {
				vertexIsland.add(nextVert);
				grabIsland(nextVert, vertexIsland);
			}
		}
	}
	
	private void addIslandToJoinedInsightMap(Map<String, List<String>> joinedInsightMap, Set<Vertex> vertexIsland) {
		
		Set<String> vertexIslandIds = vertexIsland.stream()
													.map(v -> {return ((Insight)v.value(INSIGHT_KEY)).getInsightID();})
													.collect(Collectors.toSet());
		for(String insightId : vertexIslandIds) {
			List<String> joinedInsights = new ArrayList<>(vertexIslandIds);
			joinedInsights.remove(insightId);
			joinedInsightMap.put(insightId, joinedInsights);										
		}
	}
	
	
	/****************************************
	 * PUBLIC GETTERS
	 ****************************************/
	
	
	
	
	/****************************************
	 * ADDING, REMOVING, JOINING METHODS
	 ****************************************/
	
	
	public void addInsight(Insight insight) {
		addVertex(insight);
	}
	
	public void removeInsight(Insight insight) {
		removeVertex(insight);
	}
	
	/**
	 * 
	 * @param joinCols - columns to join on
	 * @param insights - insights to join
	 * 
	 * Joins the insights based on the join columns
	 */
	public void joinInsights(List<List<String>> joinCols, List<Insight> insights) {
		
		if(insights.size() < 2) {
			throw new IllegalArgumentException("Must Pass in at least 2 Frames!!");
		}
		
		//two by two we join the frames
		for(int i = 1; i < insights.size(); i++) {
			List<List<String>> nextNewJoinCols = new ArrayList<>(2);
			for(int index = 0; index < joinCols.size(); index++) {
				List<String> cols = joinCols.get(index);
				List<String> nextCols = Arrays.asList(new String[]{cols.get(i-1), cols.get(i)});
				nextNewJoinCols.add(nextCols);
			}
			Insight insight1 = insights.get(i-1);
			Insight insight2 = insights.get(i);
			joinInsights(insight1, insight2, nextNewJoinCols);
		}
	}
	
	public void joinInsights(Insight insight1, Insight insight2, List<List<String>>  joinCols) {
		if(isJoined(insight1) && isJoined(insight2)) {
			throw new IllegalArgumentException("Frames already joined");
		}
		
		Vertex v1 = addVertex(insight1);
		Vertex v2 = addVertex(insight2);
		addEdge(v1, v2, joinCols);
		
		insight1.getDataMaker().updateDataId();
		insight2.getDataMaker().updateDataId();
		
		H2Frame frame1 = (H2Frame)insight1.getDataMaker();
//		frame1.setJoiner(this);
		H2Frame frame2 = (H2Frame)insight2.getDataMaker();
//		frame2.setJoiner(this);
	}
	
	
	/****************************************
	 * END ADDING, REMOVING, JOINING METHODS
	 ****************************************/
	
	
	
	
	/****************************************
	 * FILTER METHODS
	 ****************************************/
	
	
	public void filter(ITableDataFrame startFrame, Set<String> travelledEdges) {
		Vertex startNode = null;
		GraphTraversal<Vertex, Vertex> metaT = this.dataFrameMetaGraph.traversal().V().has(VERTEX_ID, getVertexId(startFrame));
		if(metaT.hasNext()) { //note: this is an if statement, not a while loop
			startNode = metaT.next();// the purpose of this is to just get the start node for the traversal
			chainFilter(startNode, travelledEdges);
		}
	}
	
	/**
	 * 
	 * @param frame1
	 * @param columnHeader
	 * @param filterValues
	 */
	public void filter(ITableDataFrame frame1, String columnHeader, Map<String, List<Object>> filterValues) {
		frame1.filter(columnHeader, filterValues);
		Vertex startVert = null;
		GraphTraversal<Vertex, Vertex> metaT = this.dataFrameMetaGraph.traversal().V().has(VERTEX_ID, getVertexId(frame1));
		if(metaT.hasNext()) { //note: this is an if statement, not a while loop
			startVert = metaT.next();// the purpose of this is to just get the start node for the traversal
			// for each downstream node of this meta node
			Iterator<Vertex> adjacentVerts = startVert.vertices(Direction.BOTH);
			ITableDataFrame origTable = getDataFrame(startVert);
			Set<String> travelledEdges = new HashSet<>();
			while(adjacentVerts.hasNext()) {
				Vertex nextVert = adjacentVerts.next();
				
				Edge nextEdge = getEdge(nextVert, startVert);
				String edgeId = nextEdge.value(EDGE_ID);
				travelledEdges.add(edgeId);
				
				String filterID = nextVert.value(VERTEX_ID);
				List<String> joinCols = (List<String>)nextEdge.value(JOIN_COLUMNS);
				for(String joinCol : joinCols) {
					String[] joins = joinCol.split(EDGE_DELIMITER);
					String filterColumn;
					String sourceColumn;
					if(joins[0].equals(filterID)) {
						filterColumn = joins[1];
						sourceColumn = joins[3];
					} else {
						filterColumn = joins[3];
						sourceColumn = joins[1];
					}
					
					if(sourceColumn.equals(columnHeader)) {
						ITableDataFrame nextFrame = getDataFrame(nextVert);
						nextFrame.filter(filterColumn, filterValues);
						nextFrame.updateDataId();
					} else {
						Iterator<Object> iterator = frame1.uniqueValueIterator(sourceColumn, false);
						List<Object> filters = new ArrayList<>();
						while(iterator.hasNext()) {
							filters.add(iterator.next());
						}
						addFilters(nextVert, filterColumn, filters);
					}
				}
				
				chainFilter(nextVert, travelledEdges);
			}
		}
	}
	
	/**
	 * 
	 * @param frame1
	 * @param columnHeader
	 * @param filterValues
	 */
	public void unfilter(ITableDataFrame frame1, String columnHeader) {
		frame1.unfilter(columnHeader);
		Vertex startVert = null;
		GraphTraversal<Vertex, Vertex> metaT = this.dataFrameMetaGraph.traversal().V().has(VERTEX_ID, getVertexId(frame1));
		if(metaT.hasNext()) { //note: this is an if statement, not a while loop
			startVert = metaT.next();// the purpose of this is to just get the start node for the traversal
			// for each downstream node of this meta node
			Iterator<Vertex> adjacentVerts = startVert.vertices(Direction.BOTH);
			ITableDataFrame origTable = getDataFrame(startVert);
			Set<String> travelledEdges = new HashSet<>();
			while(adjacentVerts.hasNext()) {
				Vertex nextVert = adjacentVerts.next();
				
				Edge nextEdge = getEdge(nextVert, startVert);
				String edgeId = nextEdge.value(EDGE_ID);
				travelledEdges.add(edgeId);
				
				String filterID = nextVert.value(VERTEX_ID);
				List<String> joinCols = (List<String>)nextEdge.value(JOIN_COLUMNS);
				for(String joinCol : joinCols) {
					String[] joins = joinCol.split(EDGE_DELIMITER);
					String filterColumn;
					String sourceColumn;
					if(joins[0].equals(filterID)) {
						filterColumn = joins[1];
						sourceColumn = joins[3];
					} else {
						filterColumn = joins[3];
						sourceColumn = joins[1];
					}
					
					if(sourceColumn.equals(columnHeader)) {
						ITableDataFrame nextFrame = getDataFrame(nextVert);
						nextFrame.unfilter(filterColumn);
						nextFrame.updateDataId();
						chainUnfilter(nextVert, travelledEdges, filterColumn);
						//bounce back filter
						if(isSoftFiltered(nextFrame) || isHardFiltered(nextFrame)) {
							Iterator<Object> iterator = nextFrame.uniqueValueIterator(filterColumn, false);
							List<Object> filters = new ArrayList<>();
							while(iterator.hasNext()) {
								filters.add(iterator.next());
							}
							addFilters(startVert, sourceColumn, filters);
						}
					} else {
						if(isHardFiltered(frame1)) {
							Iterator<Object> iterator = frame1.uniqueValueIterator(sourceColumn, false);
							List<Object> filters = new ArrayList<>();
							while(iterator.hasNext()) {
								filters.add(iterator.next());
							}
							addFilters(nextVert, filterColumn, filters);
							chainFilter(nextVert, travelledEdges);
						} else {
							removeFilters(nextVert, filterColumn);
							chainUnfilter(nextVert, travelledEdges, filterColumn);
						}
					}
				}
				
//				chainFilter(nextVert, travelledEdges);
			}
		}
		cleanUp();
	}
	
	private void chainFilter(Vertex orig, Set<String> travelledEdges) {
		// for each downstream node of this meta node
		Iterator<Vertex> nextVerts = orig.vertices(Direction.BOTH);
		ITableDataFrame origTable = getDataFrame(orig);
		while(nextVerts.hasNext()) {
			Vertex nextVert = nextVerts.next();
			ITableDataFrame nextFrame = getDataFrame(nextVert);
			String edgeId = getEdgeId(origTable, nextFrame);
			if(!travelledEdges.contains(edgeId)) {
				travelledEdges.add(edgeId);
				filterVertex(nextVert, orig, getEdge(nextVert, orig));
				chainFilter(nextVert, travelledEdges);
			}
		}
	}
	
	private void chainUnfilter(Vertex orig, Set<String> travelledEdges, String columnHeader) {
		// for each downstream node of this meta node
		Iterator<Vertex> nextVerts = orig.vertices(Direction.BOTH);
		ITableDataFrame origTable = getDataFrame(orig);
		while(nextVerts.hasNext()) {
			
			Vertex nextVert = nextVerts.next();
			
			Edge nextEdge = getEdge(nextVert, orig);
			String edgeId = nextEdge.value(EDGE_ID);
			
			String filterID = nextVert.value(VERTEX_ID);
			
			ITableDataFrame nextFrame = getDataFrame(nextVert);
			List<String> joinCols = (List<String>)nextEdge.value(JOIN_COLUMNS);
			if(!travelledEdges.contains(edgeId)) {
				travelledEdges.add(edgeId);
				for(String joinCol : joinCols) {
					String[] joins = joinCol.split(EDGE_DELIMITER);
					String filterColumn;
					String sourceColumn;
					if(joins[0].equals(filterID)) {
						filterColumn = joins[1];
						sourceColumn = joins[3];
					} else {
						filterColumn = joins[3];
						sourceColumn = joins[1];
					}
					
					if(sourceColumn.equals(columnHeader)) {
						removeFilters(nextVert, filterColumn);
						chainUnfilter(nextVert, travelledEdges, filterColumn);
					} else {
						Iterator<Object> iterator = origTable.uniqueValueIterator(sourceColumn, false);
						List<Object> filters = new ArrayList<>();
						while(iterator.hasNext()) {
							filters.add(iterator.next());
						}
						addFilters(nextVert, filterColumn, filters);
						chainFilter(nextVert, travelledEdges);
					}
				}
			}
		}
	}
	
	
	private void filterVertex(Vertex filterVert, Vertex sourceVert, Edge edge) {
		ITableDataFrame sourceFrame = getDataFrame(sourceVert);
		
		String filterID = filterVert.value(VERTEX_ID);
		String sourceID = sourceVert.value(VERTEX_ID);
		List<String> joinCols = (List<String>)edge.value(JOIN_COLUMNS);
		for(String joinCol : joinCols) {
			String[] joins = joinCol.split(EDGE_DELIMITER);
			if(joins[0].equals(filterID)) {
				String filterColumn = joins[1];
				String sourceColumn = joins[3];
//				Object[] filterValues = sourceFrame.getColumn(sourceColumn);//TODO : use unique value iterator
//				addFilters(filterVert, filterColumn, Arrays.asList(filterValues));
				
				Iterator<Object> iterator = sourceFrame.uniqueValueIterator(sourceColumn, false);
				List<Object> filters = new ArrayList<>();
				while(iterator.hasNext()) {
					filters.add(iterator.next());
				}
				addFilters(filterVert, filterColumn, filters);
			} else {
				String filterColumn = joins[3];
				String sourceColumn = joins[1];
//				Object[] filterValues = sourceFrame.getColumn(sourceColumn);
//				addFilters(filterVert, filterColumn, Arrays.asList(filterValues));
				Iterator<Object> iterator = sourceFrame.uniqueValueIterator(sourceColumn, false);
				List<Object> filters = new ArrayList<>();
				while(iterator.hasNext()) {
					filters.add(iterator.next());
				}
				addFilters(filterVert, filterColumn, filters);
			}			
		}
	}
	
	private void addFilters(Vertex filterVert, String columnHeader, List<Object> filterValues) {
		H2FilterHash filters = (H2FilterHash)filterVert.value(FILTERHASH);
		H2Frame filterFrame = (H2Frame)getDataFrame(filterVert);
		filterFrame.updateDataId();
		filters.setFilters(columnHeader, filterValues, "=");
	}
	
	private void removeFilters(Vertex filterVert, String columnHeader) {
		H2FilterHash filters = (H2FilterHash)filterVert.value(FILTERHASH);
		H2Frame filterFrame = (H2Frame)getDataFrame(filterVert);
		filterFrame.updateDataId();
		filters.removeFilter(columnHeader);
	}
	
	public H2FilterHash getFilters(ITableDataFrame frame1) {
		Vertex vert = getVertex(frame1);
		return (H2FilterHash)vert.value(FILTERHASH);
	}
	
	public H2FilterHash getFilters(String key) {
		Vertex vert = getVertex(key);
		return (H2FilterHash)vert.value(FILTERHASH);
	}
	
	public boolean isHardFiltered(ITableDataFrame frame) {
//		Map<String, Map<Comparator, Set<Object>>> obj = ((H2Frame)frame).getBuilder().getHardFilterHash();
//		if(obj == null || obj.isEmpty()) {
//			return false;
//		} return true;
		return false;
	}
	
	public boolean isSoftFiltered(ITableDataFrame frame) {
		H2FilterHash filterHash = (H2FilterHash)getVertex(frame).value(FILTERHASH);
		if(filterHash.getFilterHash().isEmpty()) {
			return false;
		} else {
			return true;
		}
	}
	
	private void cleanUp() {
		List<Insight> insights = getInsights();
		for(Insight insight : insights) {
			if(isHardFiltered((ITableDataFrame)insight.getDataMaker())) {
				return;
			}
		}
		
		//if nothing is hard filtered we should remove soft filters
		GraphTraversal<Vertex, Vertex> traversal = this.dataFrameMetaGraph.traversal().V();
		while(traversal.hasNext()) {
			Vertex v = traversal.next();
			v.property(FILTERHASH, new H2FilterHash());
		}
	}
	
	/****************************************
	 * END FILTER METHODS
	 ****************************************/
	
	
	/****************************************
	 * GRAPH METHODS
	 ****************************************/
	/**
	 * 
	 * @param frame
	 * @return
	 * 
	 * Gets the vertex associated with the frame
	 */
	private Vertex getVertex(Insight insight) {
		return getVertex(getVertexId(insight));
	}
	
	private Vertex getVertex(ITableDataFrame frame) {
		return getVertex(getVertexId(frame));
	}
	
	private Vertex getVertex(String vertexId) {
		GraphTraversal<Vertex, Vertex> gt = dataFrameMetaGraph.traversal().V().has(VERTEX_ID, vertexId);
		if(gt.hasNext()) {
			return gt.next();
		} 
		return null;
	}
	
	/**
	 * 
	 * @param frame
	 * @return
	 * 
	 * Adds a vertex to the meta graph which is associated with the insight argument
	 */
	private Vertex addVertex(Insight insight) {
		
		//if vertex already exists return that
		Vertex newVert = getVertex(insight);
		if(newVert != null) {
			return newVert;
		} 
		
		//otherwise create a new one
		else {
			String frameId = getVertexId(insight);
			newVert = dataFrameMetaGraph.addVertex(VERTEX_ID, frameId);
			newVert.property(INSIGHT_KEY, insight);
			newVert.property(FILTERHASH, new H2FilterHash());
		}
		
		return newVert;
	}
	
	/**
	 * 
	 * @param insight
	 * 
	 * Removes the vertex associated with the insight argument from the meta graph
	 */
	private void removeVertex(Insight insight) {
		Vertex remVert = getVertex(insight);
		if(remVert != null) {
			remVert.remove();
		}
	}
	
	/**
	 * @param v1
	 * @param v2
	 * @param joinCols
	 * @return
	 * 
	 * Adds an edge between vertex v1 and v2
	 * Adds the joinCols as a property on the edge, this is needed to know how to do the chain filtering
	 * 		joinCols is saved as the following structure:
	 * 		
	 */
	private Edge addEdge(Vertex v1, Vertex v2, List<List<String>>  joinCols) {
		Edge retEdge = null;
		String edgeId = getEdgeId(v1, v2);
		GraphTraversal<Edge, Edge> gt = dataFrameMetaGraph.traversal().E().has(EDGE_ID, edgeId);
		if(gt.hasNext()) {
			//should update the join cols here in case they have changed...but maybe the whole edge should be deleted first?
			//Not sure...will come back to this
			//Also not allowed anyways since can't join two frames which are already joined
		} else {
			Edge newEdge = v1.addEdge(edgeId, v2);
			//add the join columns
			List<String> joinStructure = getJoinStructure(v1, v2, joinCols);
			newEdge.property(JOIN_COLUMNS, joinStructure);
			newEdge.property(EDGE_ID, edgeId);
		}

		return retEdge;
	}
	
	private Edge getEdge(Vertex v1, Vertex v2) {
		Edge retEdge = null;
		String edgeId = getEdgeId(v1, v2);
		GraphTraversal<Edge, Edge> gt = dataFrameMetaGraph.traversal().E().has(EDGE_ID, edgeId);
		if(gt.hasNext()) {
			retEdge = gt.next();
		} 
		return retEdge;
	}
	
	private List<String> getJoinStructure(Vertex v1, Vertex v2, List<List<String>> joinCols) {
		List<String> joinStructure = new ArrayList<>(joinCols.size());
		for(List<String> nextJoinCols : joinCols) {
			String newJoinId = v1.value(VERTEX_ID) + EDGE_DELIMITER + nextJoinCols.get(0) + EDGE_DELIMITER + v2.value(VERTEX_ID) + EDGE_DELIMITER + nextJoinCols.get(1);
			joinStructure.add(newJoinId);
		}
		return joinStructure;
	}
	
	/**
	 * 
	 * @param insight
	 * @return
	 * 
	 * Returns the unique ID associated with the Insight
	 */
	private String getVertexId(Insight insight) {
		IDataMaker dm = insight.getDataMaker();
		if(dm instanceof ITableDataFrame) {
			return getVertexId((ITableDataFrame)dm);
		}
		return insight.getInsightID();
	}
	
	/**
	 * 
	 * @param frame
	 * @return
	 * 
	 * returns a unique id associated with the frame
	 * TODO : come up with better solution, just using table name for now
	 * 			this is not ideal because it requires the assumption the table name for the frame won't change
	 */
	private String getVertexId(ITableDataFrame frame) {
		if(frame instanceof H2Frame) {
			return ((H2Frame)frame).getTableName();
		}
		return "";
	}
	
	private ITableDataFrame getDataFrame(Vertex vert) {
		Insight insight = (Insight)vert.value(INSIGHT_KEY);
		return (ITableDataFrame)insight.getDataMaker();
	}
	
	/**
	 * 
	 * @param v1
	 * @param v2
	 * @return
	 * 
	 * Generates the Id associated between two vertices
	 * 		ID will be unique among all edges in the graph
	 * 		ID will be consistent based on the Vertices, regardless of order of arguments
	 */
	private String getEdgeId(Vertex v1, Vertex v2) {
		return getEdgeId(getDataFrame(v1), getDataFrame(v2));
	}
	
	/**
	 * 
	 * @param v1
	 * @param v2
	 * @return
	 * 
	 * Helper method of 'getEdgeId(Vertex v1, Vertex v2)'
	 * 
	 * Generates the Id associated between two frames
	 * 		ID will be unique among all edges in the graph
	 * 		ID will be consistent based on the Vertices, regardless of order of arguments
	 */
	private String getEdgeId(ITableDataFrame frame1, ITableDataFrame frame2) {
		String id1 = getVertexId(frame1);
		String id2 = getVertexId(frame2);
		//maintain stability of id regardless of order
		if(id1.compareTo(id2) > 0) {
			return id1+EDGE_DELIMITER+id2;
		} else {
			return id2+EDGE_DELIMITER+id1;
		}
	}
	

	/****************************************
	 * END GRAPH METHODS
	 ****************************************/
	
}