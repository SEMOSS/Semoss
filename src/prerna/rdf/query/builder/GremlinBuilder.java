package prerna.rdf.query.builder;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import javax.script.ScriptContext;

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
	
//	String script = "g.traversal().V()";
	Graph g;
    GraphTraversal gt;

	public List <String> selector = new Vector<String>();
	String where = "";
	
	GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
	Hashtable nodeHash = new Hashtable();
	
	//the range of the graph to execute on
	int startRange = -1;
	int endRange = -1;
	
	public GremlinBuilder(Graph g){
		this.g = g;
		this.gt = g.traversal().V();
	}
	
	/**
	 * 
	 * @param type
	 * @param alias
	 */
	public void addNode(String type, String...alias)
	{
		
		
		if(!nodeHash.containsKey(type))
		{
//			script = script + ".has('" + Constants.TYPE + "', '" + type +"')";
			gt = gt.has(Constants.TYPE, type);
			
			String selector = type;
			if(alias.length > 0)
				selector = alias[0];
			
//			script = script + ".as('" + selector + "')";
			gt = gt.as(selector);
			
			//nodeHash = type --> type?
			nodeHash.put(type,  type);
		}
	}
	
	/**
	 * 
	 * @param fromType
	 * @param toType
	 */
	public void addEdge(String fromType, String toType)
	{
		addNode(fromType);
//		script = script + ".out()";
		gt = gt.out();
		addNode(toType);
	}

	/**
	 * 
	 * @param startNode
	 * @param edgeHash
	 */
	public void addNodeEdge() {
		List<String> travelledEdges = new Vector<String>();
		
		Vertex startNode;
		GraphTraversal<Vertex, Vertex> metaT = g.traversal().V().has(Constants.TYPE, TinkerFrame.META);
		if(metaT.hasNext()) {
			startNode = metaT.next();
			String startType = startNode.property(Constants.NAME).value()+"";
			
			gt = gt.has(Constants.TYPE, startType);
			gt = gt.as(startType);
//			travelledEdges.add(startType);
			gt = visitNode(startNode, gt, travelledEdges, new Integer(0));
		}
	}
	
	private GraphTraversal visitNode(Vertex orig, GraphTraversal gt1, List<String> travelledEdges, Integer recursionCount) {
		recursionCount++;
		String origName = orig.property(Constants.NAME).value()+"";
		GraphTraversal<Vertex, Vertex> downstreamIt = g.traversal().V().has(Constants.TYPE, TinkerFrame.META).has(Constants.ID, orig.property(Constants.ID).value()).out(TinkerFrame.META);
		while (downstreamIt.hasNext()){
			Vertex nodeV = downstreamIt.next();
			String node = nodeV.property(Constants.NAME).value()+"";
			String edgeKey = origName + ":::" + node;
			if(!travelledEdges.contains(edgeKey)) {
				System.out.println("travelling down to " + node);
				gt1 = gt1.out().has(Constants.TYPE, node).as(node);
//				gt1 = gt1.out().has(Constants.TYPE, node).as(node).choose(__.as(node).in().has(Constants.TYPE, Constants.FILTER), //.out().has("TYPE", "Business Process"), // if this is true
//						__.as(node).in().has(Constants.TYPE, "DUMMY"), //.out().has("TYPE", "Business Prcess"), // then do this
//						__.as(node));  

				travelledEdges.add(edgeKey);
				gt1 = visitNode(nodeV, gt1, travelledEdges, recursionCount);
				System.out.println("returning home to " + origName);
				gt1 = gt1.in().has(Constants.TYPE, origName).as(origName + (recursionCount));
				gt1 = gt1.where(origName, org.apache.tinkerpop.gremlin.process.traversal.P.eq(origName + (recursionCount)));
				recursionCount++;
			}
		}
		
		GraphTraversal<Vertex, Vertex> upstreamIt = g.traversal().V().has(Constants.TYPE, TinkerFrame.META).has(Constants.ID, orig.property(Constants.ID).value()).in(TinkerFrame.META);
		while(upstreamIt.hasNext()){
			Vertex nodeV = upstreamIt.next();
			String node = nodeV.property(Constants.NAME).value()+"";
			String edgeKey = node + ":::" + origName;
			if(!travelledEdges.contains(edgeKey)){
				System.out.println("travelling up to " + node);
				gt1 = gt1.in().has(Constants.TYPE, node).as(node);
				travelledEdges.add(edgeKey);
				gt1 = visitNode(nodeV, gt1, travelledEdges, recursionCount);
				
				System.out.println("returning home to " + origName);
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
	private Set<String> getUpstreamVerts(String nodeType, Map<String, Set<String>> edgeHash){
		Set<String> retSet = new HashSet<String>();
		
		for(String key: edgeHash.keySet()){
			Set<String> value = edgeHash.get(key);
			if(value.contains(nodeType)){
				retSet.add(key);
			}
		}
		
		return retSet;
	}
	
	/**
	 * 
	 * @param selectors
	 */
	public void addSelector(List selectors)
	{
		this.selector.addAll(selectors);
	}
	
	/**
	 * 
	 * @param alias
	 * @param itemsToFilter
	 */
	public void addFilter(String alias, List itemsToFilter)
	{
		engine.getBindings(ScriptContext.ENGINE_SCOPE).put(alias + Constants.FILTERS, itemsToFilter.toArray()); // set it into the context
		gt = gt.where(__.as(alias)).has(Constants.NAME, org.apache.tinkerpop.gremlin.process.traversal.P.without(alias + Constants.FILTERS ));
	}
	
	public void addFilter(String alias)
	{
//		engine.getBindings(ScriptContext.ENGINE_SCOPE).put(alias + Constants.FILTERS, "filterNode"); // set it into the context
//		gt = gt.filter(__.as(alias)).in().has(Constants.TYPE, org.apache.tinkerpop.gremlin.process.traversal.P.without(alias + Constants.FILTERS));
//		gt = gt.filter(__.as(alias).in().has(Constants.TYPE, org.apache.tinkerpop.gremlin.process.traversal.P.eq(alias + Constants.FILTERS)));
//		gt = gt.filter(__.as(alias).inE().has(Constants.ID, "filterNode"));
//		gt = gt.filter(g.traversal().V().in().has(Constants.TYPE, "filterNode"));
//		gt = gt.choose(__.as(alias).in().has(Constants.TYPE, Constants.FILTER), //.out().has("TYPE", "Business Process"), // if this is true
//					__.as(alias).in().has(Constants.TYPE, "DUMMY"), //.out().has("TYPE", "Business Prcess"), // then do this
//					__.as(alias));  
	}
	
	/**
	 * 
	 * @param alias
	 * @param itemsToKeep
	 * 
	 * This adds to the script the values on which to traverse, i.e. traverse only when value is contained in itemsToKeep
	 */
	//TODO : give better name or integrate with addFilter method
	public void addRestriction(String alias, List itemsToKeep) {
		engine.getBindings(ScriptContext.ENGINE_SCOPE).put(alias + Constants.FILTERS, itemsToKeep.toArray()); // set it into the context
		//need to change Constants.FILTERS
//		script = script + ".where(__.as('" + alias + "').has('" + Constants.NAME + "', org.apache.tinkerpop.gremlin.process.traversal.P.within('" + alias + Constants.FILTERS + "')))";
		gt = gt.where(__.as(alias)).has(Constants.NAME, org.apache.tinkerpop.gremlin.process.traversal.P.within(alias + Constants.FILTERS ));
	}
	
	public String getScript()
	{
		return gt.toString();
	}
	
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

	
	public void setRange(int endRange)
	{
		setRange(0, endRange);
	}
	
	public void setRange(int startRange, int endRange)
	{
		this.startRange = startRange;
		this.endRange = endRange;
	}
	
	
	/**
	 * 
	 * @param g - the graph on which the script will be executed
	 * @return
	 * 
	 * returns the graph traversal that is the result of executing the script on g
	 */
	public Iterator executeScript(Graph g)
	{
		long startTime = System.currentTimeMillis();
		
		if(startRange != -1) // add the range
//			script = script +".range(" + startRange + "," + endRange + ")";
			gt = gt.range(startRange, endRange);
		
		if(selector.size() > 0) // add the projections
			appendSelectors();
		
		LOGGER.info("Script being executed...  " + gt);
		GraphTraversal gtR = this.gt;
//		try {
//			engine.getBindings(ScriptContext.ENGINE_SCOPE).put("g", g);
//			
//			gt = (GraphTraversal)engine.eval(gt);
//		} catch (ScriptException e) {
//			e.printStackTrace();
//		}
		
//		LOGGER.info("Script executed: "+(System.currentTimeMillis() - startTime)+" ms");
		return gtR;
	}	
	
	public static GremlinBuilder prepareGenericBuilder(String[] headerNames, List<String> columnsToSkip, Graph g){
		// get all the levels
		Vector <String> finalColumns = new Vector<String>();
		GremlinBuilder builder = new GremlinBuilder(g);

		//add edges if edges exist
		if(headerNames.length > 1) {
			builder.addNodeEdge();
		} else {
			//no edges exist, add single node to builder
			builder.addNode(headerNames[0]);
		}

		// add everything that you need
		for(int colIndex = 0;colIndex < headerNames.length;colIndex++) // add everything you want first
		{
			if(!columnsToSkip.contains(headerNames[colIndex])) {
				finalColumns.add(headerNames[colIndex]);
			}
		}

		// now add the projections
		builder.addSelector(finalColumns);

		// add the filters next
//		for(int colIndex = 0;colIndex < headerNames.length;colIndex++)
//		{
//			if(filterHash.containsKey(headerNames[colIndex]))
//				builder.addFilter(headerNames[colIndex], filterHash.get(headerNames[colIndex]));
//		}
		return builder;
	}
}