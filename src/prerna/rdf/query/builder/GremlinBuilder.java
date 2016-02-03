package prerna.rdf.query.builder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
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
	public void addNodeEdge() {
		List<String> travelledEdges = new Vector<String>();
		
		Vertex startNode;
		// get the metamodel information from the graph
		GraphTraversal<Vertex, Vertex> metaT = g.traversal().V().has(Constants.TYPE, TinkerFrame.META);
		if(metaT.hasNext()) { //note: this is an if statement, not a while loop
			// the purpose of this is to just get the start node for the traversal
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
	
//	public GraphTraversal addNodeEdge2() {
//		List<String> travelledEdges = new Vector<String>();
//		GraphTraversal graphtraversal = null;
//		Vertex startNode;
//		GraphTraversal<Vertex, Vertex> metaT = g.traversal().V().has(Constants.TYPE, TinkerFrame.META);
//		if(metaT.hasNext()) {
//			startNode = metaT.next();
//			String startType = startNode.property(Constants.NAME).value()+"";
//			
//			graphtraversal = __.has(Constants.TYPE, startType).as(startType);
//			Object filtered = startNode.value(Constants.FILTER);
//			if((Boolean)filtered == true) {
//				graphtraversal = graphtraversal.not(__.in().has(Constants.TYPE, Constants.FILTER));
//			}
//
//			graphtraversal = visitNode(startNode, graphtraversal, travelledEdges, new Integer(0));
//		}
//		return graphtraversal;
//	}
	
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
				gt1 = visitNode(nodeV, gt1, travelledEdges, recursionCount);
				
				LOGGER.debug("returning home to " + origName);
				gt1 = gt1.in().has(Constants.TYPE, origName).as(origName + (recursionCount));
				gt1 = gt1.where(origName, org.apache.tinkerpop.gremlin.process.traversal.P.eq(origName + (recursionCount)));
				recursionCount++;
			}
		}
		
		GraphTraversal<Vertex, Vertex> upstreamIt = g.traversal().V().has(Constants.TYPE, TinkerFrame.META).has(Constants.ID, orig.property(Constants.ID).value()).in(TinkerFrame.META);
		while(upstreamIt.hasNext()) {
			Vertex nodeV = upstreamIt.next();
			String node = nodeV.property(Constants.NAME).value()+"";
			String edgeKey = node + ":::" + origName;
			if(!travelledEdges.contains(edgeKey)){
				LOGGER.debug("travelling up to " + node);
				gt1 = gt1.in().has(Constants.TYPE, node).as(node);
				
				Object filtered = nodeV.value(Constants.FILTER);
				if((Boolean)filtered == true) {
					gt1 = gt1.not(__.in().has(Constants.TYPE, Constants.FILTER));
				}
				
				travelledEdges.add(edgeKey);
				gt1 = visitNode(nodeV, gt1, travelledEdges, recursionCount);
				
				LOGGER.debug("returning home to " + origName);
				gt1 = gt1.out().has(Constants.TYPE, origName).as(origName + (recursionCount));
				gt1 = gt1.where(origName, org.apache.tinkerpop.gremlin.process.traversal.P.eq(origName + (recursionCount)));
				recursionCount++;
			}
		}
		return gt1;
	}
	
	/**
	 * 
	 */
	public void addIncompleteVertices() {
		Vertex startNode;
		GraphTraversal<Vertex, Vertex> metaT = g.traversal().V().has(Constants.TYPE, TinkerFrame.META);
		if(metaT.hasNext()) {
			startNode = metaT.next();
			List<GraphTraversal> orTraversals = new ArrayList<>();
			orTraversals = getIncompleteVertices(orTraversals, startNode);
			gt = gt.or(orTraversals.toArray(new GraphTraversal[0])).as("deleteVerts");
		}
	}
	
	/**
	 * 
	 * @param orTraversals
	 * @param orig
	 * @return
	 */
	private List<GraphTraversal> getIncompleteVertices(List<GraphTraversal> orTraversals, Vertex orig) {
		
		GraphTraversal<Vertex, Vertex> downstreamIt = g.traversal().V().has(Constants.TYPE, TinkerFrame.META).has(Constants.ID, orig.property(Constants.ID).value()).out(TinkerFrame.META);
		String origName = orig.property(Constants.NAME).value()+"";
		while(downstreamIt.hasNext()) {
			Vertex nodeV = downstreamIt.next();
			String node = nodeV.property(Constants.NAME).value()+"";
				
			GraphTraversal g = __.has(Constants.TYPE, origName).out().not(__.has(Constants.TYPE, node));
			orTraversals.add(g);
			getIncompleteVertices(orTraversals, nodeV);
		}
		
		GraphTraversal<Vertex, Vertex> upstreamIt = g.traversal().V().has(Constants.TYPE, TinkerFrame.META).has(Constants.ID, orig.property(Constants.ID).value()).in(TinkerFrame.META);
		while(upstreamIt.hasNext()) {
			Vertex nodeV = upstreamIt.next();
			String node = nodeV.property(Constants.NAME).value()+"";
			
			GraphTraversal g = __.has(Constants.TYPE, origName).in().not(__.has(Constants.TYPE, node));
			orTraversals.add(g);
			getIncompleteVertices(orTraversals, nodeV);
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
		
		LOGGER.debug("Returning the graph traversal");
		return this.gt;
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
	 */
	public static GremlinBuilder prepareGenericBuilder(List<String> selectors, Graph g){
		// get all the levels
		GremlinBuilder builder = new GremlinBuilder(g);

		//add edges if edges exist
		if(selectors.size() > 1) {
			builder.addNodeEdge();
		} else {
			//no edges exist, add single node to builder
			builder.addNode(selectors.get(0));
		}

		// now add the projections
		builder.addSelector(selectors);

		return builder;
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
//		LOGGER.info("Script being executed...  " + builder.gt);
//		return builder.gt;
//	}
	
	
	
	
}