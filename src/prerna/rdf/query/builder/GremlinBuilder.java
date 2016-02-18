package prerna.rdf.query.builder;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.ds.TinkerFrame;
import prerna.util.Constants;

/**
 * Responsible for building a gremlin string script that can be executed on graph
 */
public class GremlinBuilder {

	private static final Logger LOGGER = LogManager.getLogger(GremlinBuilder.class.getName());
	
	public Graph g;
    public GraphTraversal gt;
	public List<String> selector = new Vector<String>();	
	private GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
	private Hashtable nodeHash = new Hashtable();
	//the range of the graph to execute on
	int startRange = -1;
	int endRange = -1;
	public String groupBySelector;
	
	
	/**
	 * Constructor for the GremlinBuilder class
	 * @param g					The graph for the gremlin script to be executed on
	 */
	public GremlinBuilder(Graph g){
		this.g = g;
		this.gt = g.traversal().V();
	}
	
	/**
	 * Adds the node for the gremlin traversal and assigns it an alias
	 * @param type					The type becomes the alias for the node if an alias is not provided
	 * @param alias					The varags parameter is only used for the first index if present
	 */
	public void addNode(String type, String...alias)
	{
		if(!nodeHash.containsKey(type))
		{
			// adds a node as 
			gt = gt.has(Constants.TYPE, type);
			String selector = type;
			if(alias.length > 0) {
				selector = alias[0];
			}
			gt = gt.as(selector);
			
			nodeHash.put(type,  type);
		}
	}
	
	/**
	 * Adds the path for the gremlin builder based on the metamodel data in the graph
	 */
	public void addEdge(String fromType, String toType)
	{
		addNode(fromType);
//		script = script + ".out()";
		gt = gt.out();
		addNode(toType);
	}

	/**
	 * This method handles the traversing of all meta nodes in a tinker graph to create a traversal that flattens the graph appropriately
	 * The basic idea is
	 *  1. Pick a starting meta node
	 *  2. Travel as far downstream as possible, adding those to our main traversal
	 *  3. Return back home to our starting meta node
	 *  4. Do the same thing going upstream
	 *  
	 * Each meta node is checked for the filter boolean property to determine whether we need to add the edge to the filter node to our traversal
	 */
	public void addNodeEdge() {
		List<String> travelledEdges = new Vector<String>();
		
		Vertex startNode;
		// get the metamodel information from the graph
		GraphTraversal<Vertex, Vertex> metaT = g.traversal().V().has(Constants.TYPE, TinkerFrame.META);
		if(metaT.hasNext()) { //note: this is an if statement, not a while loop
			// the purpose of this is to just get the start node for the traversal
			// start with any meta node
			startNode = metaT.next();
			String startType = startNode.property(Constants.NAME).value()+"";
			gt = gt.has(Constants.TYPE, startType).as(startType);
			
			// there is a boolean at the metamodel level if this type has any filters
			Object filtered = startNode.value(Constants.FILTER);
			if((Boolean)filtered == true) {
				// filtered edges have a type of filter
				gt = gt.not(__.in().has(Constants.TYPE, Constants.FILTER));
			}
			// add the logic to traverse
			gt = visitNode(startNode, gt, travelledEdges, new Integer(0));
		}
	}
	
	/**
	 * 
	 * @param orig
	 * @param gt1
	 * @param travelledEdges
	 * @param recursionCount
	 * @return
	 */
	private GraphTraversal visitNode(Vertex orig, GraphTraversal gt1, List<String> travelledEdges, Integer recursionCount) {
		recursionCount++;
		String origName = orig.property(Constants.NAME).value()+"";
		GraphTraversal<Vertex, Vertex> downstreamIt = g.traversal().V().has(Constants.TYPE, TinkerFrame.META).has(Constants.ID, orig.property(Constants.ID).value()).out(TinkerFrame.META);
		while (downstreamIt.hasNext()) {
			// for each downstream node of this meta node
			Vertex nodeV = downstreamIt.next();
			String node = nodeV.property(Constants.NAME).value()+"";
			String edgeKey = origName + ":::" + node;
			if(!travelledEdges.contains(edgeKey)) {
				LOGGER.debug("travelling down to " + node);
				gt1 = gt1.out().has(Constants.TYPE, node).as(node);
				
				Object filtered = nodeV.value(Constants.FILTER);
				if((Boolean)filtered == true) {
					gt1 = gt1.not(__.in().has(Constants.TYPE, Constants.FILTER));
				}

				travelledEdges.add(edgeKey);
				// travel as far downstream as possible
				gt1 = visitNode(nodeV, gt1, travelledEdges, recursionCount);
				// when we can go no further, we must return home to original meta node
				LOGGER.debug("returning home to " + origName);
				gt1 = gt1.in().has(Constants.TYPE, origName).as(origName + (recursionCount));
				gt1 = gt1.where(origName, P.eq(origName + (recursionCount)));
				recursionCount++;
			}
		}
		// do the same thing for upstream
		GraphTraversal<Vertex, Vertex> upstreamIt = g.traversal().V().has(Constants.TYPE, TinkerFrame.META).has(Constants.ID, orig.property(Constants.ID).value()).in(TinkerFrame.META);
		while(upstreamIt.hasNext()) {
			Vertex nodeV = upstreamIt.next();
			String node = nodeV.property(Constants.NAME).value()+"";
			String edgeKey = node + ":::" + origName;
			if(!travelledEdges.contains(edgeKey)){
				LOGGER.info("travelling up to " + node);

				gt1 = gt1.in().has(Constants.TYPE, node).as(node);
				
				
				Object filtered = nodeV.value(Constants.FILTER);
				if((Boolean)filtered == true) {
					gt1 = gt1.not(__.in().has(Constants.TYPE, Constants.FILTER));
				}
				
				travelledEdges.add(edgeKey);
				gt1 = visitNode(nodeV, gt1, travelledEdges, recursionCount);
				
				LOGGER.info("returning home to " + origName);
				gt1 = gt1.out().has(Constants.TYPE, origName).as(origName + (recursionCount));
				gt1 = gt1.where(origName, P.eq(origName + (recursionCount)));
				recursionCount++;
			}
		}
		return gt1;
	}
	
	/**
	 * Use this to gather the vertices that do not have an edge to a vertex but is expected to have one based on the metamodel
	 * 
	 * Example:
	 * 		Metamodel: a -> b, b -> c, a -> d
	 * 		Method will return all a's without b's, all b's without a's, all b's without c's, and so forth
	 */
	public void addIncompleteVertices() {
		Vertex startNode;
		
		//get a random start node
		GraphTraversal<Vertex, Vertex> metaT = g.traversal().V().has(Constants.TYPE, TinkerFrame.META);
		if(metaT.hasNext()) {
			startNode = metaT.next();
			
			//the list of the orTraversals (all a's without b, or all b's without a, ...)
			List<GraphTraversal> orTraversals = new ArrayList<>();
			
			//keep track of which edges have already been checked
			List<String> travelledEdges = new ArrayList<>();
			
			orTraversals = getIncompleteVertices(orTraversals, startNode, travelledEdges);
			gt = this.g.traversal().V().or(orTraversals.toArray(new GraphTraversal[0])).as("extraVerts").select("extraVerts");
		}
	}
	
	/**
	 * 
	 * @param orTraversals
	 * @param orig
	 * @return
	 */
	private List<GraphTraversal> getIncompleteVertices(List<GraphTraversal> orTraversals, Vertex orig, List<String> travelledEdges) {
		
		GraphTraversal<Vertex, Vertex> downstreamIt = g.traversal().V().has(Constants.TYPE, TinkerFrame.META).has(Constants.ID, orig.property(Constants.ID).value()).out(TinkerFrame.META);
		String origName = orig.property(Constants.NAME).value()+"";
		while(downstreamIt.hasNext()) {
			Vertex nodeV = downstreamIt.next();
			String node = nodeV.property(Constants.NAME).value()+"";
			
			String edgeKey = origName + ":::" + node;
			if(!travelledEdges.contains(edgeKey)) {
				GraphTraversal g = __.has(Constants.TYPE, origName).not(__.out().has(Constants.TYPE, node));
				orTraversals.add(g);
				travelledEdges.add(edgeKey);
				getIncompleteVertices(orTraversals, nodeV, travelledEdges);
			}
		}
		
		GraphTraversal<Vertex, Vertex> upstreamIt = g.traversal().V().has(Constants.TYPE, TinkerFrame.META).has(Constants.ID, orig.property(Constants.ID).value()).in(TinkerFrame.META);
		while(upstreamIt.hasNext()) {
			Vertex nodeV = upstreamIt.next();
			String node = nodeV.property(Constants.NAME).value()+"";
			
			String edgeKey = origName + ":::" + node;
			if(!travelledEdges.contains(edgeKey)) {
				GraphTraversal g = __.has(Constants.TYPE, origName).not(__.in().has(Constants.TYPE, node));
				orTraversals.add(g);
				travelledEdges.add(edgeKey);
				getIncompleteVertices(orTraversals, nodeV, travelledEdges);
			}
		}
		return orTraversals;
	}
	 
	/**
	 * 
	 * @param selectors
	 */
	public void addSelector(List selectors)
	{
		if(selectors != null) {
			this.selector.addAll(selectors);
		}
	}
	
	/**
	 * Get the script that the graph traversal is executing
	 * @return					The gremlin script for the traversal
	 */
	public String getScript()
	{
		return gt.toString();
	}
	
	/**
	 * Appends the selectors onto the graph traversal
	 */
	private void appendSelectors()
    {
       if(selector.size() == 1){
              gt = gt.select(selector.get(0));
       }
       else if(selector.size() == 2){
              gt = gt.select(selector.get(0), selector.get(1));
       }
       else if(selector.size() >= 3) {
    	   String[] selectorArr = new String[selector.size() - 2];
           for(int i = 2; i < selector.size(); i++) {
        	   selectorArr[i-2] = selector.get(i);
           }
           gt = gt.select(selector.get(0), selector.get(1), selectorArr);
       }
    }

	/**
	 * Generates the limit for the iterator with the offset being 0
	 * @param endRange
	 */
	public void setRange(int endRange)
	{
		setRange(0, endRange);
	}
	
	/**
	 * Determine the offset and limit for the iterator
	 * @param startRange				The offset for the iterator
	 * @param endRange					The limit for the iterator
	 */
	public void setRange(int startRange, int endRange)
	{
		this.startRange = startRange;
		this.endRange = endRange;
	}
	
	
	/**
	 * 
	 * @param g					The graph on which the script is to be executed
	 * @return					The graph traversal that is the result of executing the script on g
	 */
	public GraphTraversal executeScript()
	{
		// add the range
		if(startRange != -1) {
			gt = gt.range(startRange, endRange);
		}
		
		// add the projections
		if(selector.size() > 0) { 
			appendSelectors();
		}
		
		addGroupBy();
		
		LOGGER.info("Returning the graph traversal");
		LOGGER.info("Script being executed...  " + gt);
		return gt;
	}	
	
	public void addGroupBy() {
		if(groupBySelector != null) {
			gt.group().by(__.select(groupBySelector).values(Constants.NAME)).as("GROUP_BY");
		}
	}
	
	public String getGroupBySelector() {
		return groupBySelector;
	}

	public void setGroupBySelector(String groupBySelector) {
		this.groupBySelector = groupBySelector;
	}

	/**
	 * 
	 * @param selectors
	 * @param g
	 * @return
	 * 
	 * Method to traverse the graph based on the metamodel and return rows in a table format
	 * 
	 * Example:
	 * 		Metamodel: a -> b, a -> c, b -> d
	 * 		return traversal which returns {a -> a1, b -> b1, c -> c1, d -> d1} for each row in the table represented by graph g
	 */
	public static GremlinBuilder prepareGenericBuilder(List<String> selectors, Graph g){
		// get all the levels
		GremlinBuilder builder = new GremlinBuilder(g);

		
		//add edges if edges exist
		//if(selectors.size() > 1) {
		builder.addNodeEdge();
		//} 
		
//		else {
//			//no edges exist, add single node to builder
//			builder.addNode(selectors.get(0));
//		}

		// now add the projections
		builder.addSelector(selectors);

		return builder;
	}
	

	/**
	 * This method creates a traversal that names each edge that it traverses
	 * The path that it traverses is the same as the generic builder but makes a stop at each edge to add as variable
	 * The returned list of edge names can be used as selectors in the traversal to return the edges
	 * 
	 * @return Names of all of the edge variables that have been created
	 */
	public List<String> generateFullEdgeTraversal() {
		List<String> travelledEdges = new Vector<String>();
		List<String> edgeSelectors = new Vector<String>();
		
		Vertex startNode;
		GraphTraversal<Vertex, Vertex> metaT = g.traversal().V().has(Constants.TYPE, TinkerFrame.META);
		
		// pick any meta node as a starting point
		if(metaT.hasNext()) {
			startNode = metaT.next();
			String startType = startNode.property(Constants.NAME).value()+"";
			
			// add that to our traversal and check the filter
			gt = gt.has(Constants.TYPE, startType).as(startType);
			Object filtered = startNode.value(Constants.FILTER);
			if((Boolean)filtered == true) {
				gt = gt.not(__.in().has(Constants.TYPE, Constants.FILTER));
			}
			// begin recursion
			gt = visitNodeForEdgeTraversal(startNode, gt, travelledEdges, new Integer(0), edgeSelectors);
		}
		return edgeSelectors;
	}
	
	private GraphTraversal visitNodeForEdgeTraversal(Vertex orig, GraphTraversal gt1, List<String> travelledEdges, Integer recursionCount, List<String> edgeSelectors) {
		recursionCount++;
		String origName = orig.property(Constants.NAME).value()+"";
		GraphTraversal<Vertex, Vertex> downstreamIt = g.traversal().V().has(Constants.TYPE, TinkerFrame.META).has(Constants.ID, orig.property(Constants.ID).value()).out(TinkerFrame.META);
		// for each meta vertex downstream of this meta vertex
		while (downstreamIt.hasNext()){
			Vertex nodeV = downstreamIt.next();
			String node = nodeV.property(Constants.NAME).value()+"";
			String edgeKey = origName + ":::" + node;
			// if we have never travelled that edge before
			if(!travelledEdges.contains(edgeKey)) {
				LOGGER.debug("travelling down to " + node);
				
				String edgeSelector = "Edge"+recursionCount;
				edgeSelectors.add(edgeSelector);
				// travel the edge in our graph traversal, adding our edge selector to keep track
				gt1 = gt1.outE().as(edgeSelector).inV().has(Constants.TYPE, node).as(node);
				
				Object filtered = nodeV.value(Constants.FILTER);
				if((Boolean)filtered == true) {
					gt1 = gt1.not(__.in().has(Constants.TYPE, Constants.FILTER));
				}

				travelledEdges.add(edgeKey);
				
				// continue travelling downstream as far as we can
				gt1 = visitNodeForEdgeTraversal(nodeV, gt1, travelledEdges, recursionCount, edgeSelectors);
				
				// when we can't go any further downstream, need to return home
				LOGGER.debug("returning home to " + origName);
				gt1 = gt1.in().has(Constants.TYPE, origName).as(origName + (recursionCount));
				gt1 = gt1.where(origName, P.eq(origName + (recursionCount)));
				recursionCount++;
			}
		}
		
		// do the same for upstream
		GraphTraversal<Vertex, Vertex> upstreamIt = g.traversal().V().has(Constants.TYPE, TinkerFrame.META).has(Constants.ID, orig.property(Constants.ID).value()).in(TinkerFrame.META);
		while(upstreamIt.hasNext()) {
			Vertex nodeV = upstreamIt.next();
			String node = nodeV.property(Constants.NAME).value()+"";
			String edgeKey = node + ":::" + origName;
			if(!travelledEdges.contains(edgeKey)){
				LOGGER.debug("travelling up to " + node);

				String edgeSelector = "Edge"+recursionCount;
				edgeSelectors.add(edgeSelector);
				gt1 = gt1.inE().as(edgeSelector).outV().has(Constants.TYPE, node).as(node);
				
				
				Object filtered = nodeV.value(Constants.FILTER);
				if((Boolean)filtered == true) {
					gt1 = gt1.not(__.in().has(Constants.TYPE, Constants.FILTER));
				}
				
				travelledEdges.add(edgeKey);
				gt1 = visitNodeForEdgeTraversal(nodeV, gt1, travelledEdges, recursionCount, edgeSelectors);
				
				LOGGER.debug("returning home to " + origName);
				gt1 = gt1.out().has(Constants.TYPE, origName).as(origName + (recursionCount));
				gt1 = gt1.where(origName, P.eq(origName + (recursionCount)));
				recursionCount++;
			}
		}
		return gt1;
	}
	
//	public static GraphTraversal getIncompleteVertices(List<String> selectors, Graph g) {
//		GremlinBuilder builder = new GremlinBuilder(g);
//		GraphTraversal gt = builder.addNodeEdge2();
//		GraphTraversal gt2 = __.where(gt).path().V();
////		selectors.remove("Role");
////		builder.addSelector(selectors);
////		builder.appendSelectors(gt);
//		
//		builder.gt = builder.gt.not(gt2).V().as("deleteVertices").select("deleteVertices");
////		builder.appendSelectors();
//		LOGGER.debug("Script being executed...  " + builder.gt);
//		return builder.gt;
//	}
	/**
	 * 
	 * @param selectors
	 * @param g
	 * @return
	 * 
	 * Use this method to gather all the vertices in graph g such that each vertex has a missing edge based on the metamodel
	 * 
	 * Example:
	 * 		Metamodel: a -> b, a -> c, b -> d
	 * 		return traversal that returns all a's without b's, all b's without a's, all a's without out c's, etc.
	 */
	public static GraphTraversal getIncompleteVertices(Graph g) {
		GremlinBuilder builder = new GremlinBuilder(g);
		builder.addIncompleteVertices();
		return builder.gt;
	}
	
	
	
	
}