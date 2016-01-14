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

import prerna.util.Constants;

/**
 * Responsible for building a gremlin string script that can be executed on graph
 */
public class GremlinBuilder {

	private static final Logger LOGGER = LogManager.getLogger(GremlinBuilder.class.getName());
	
//	String script = "g.traversal().V()";
	Graph g;
    GraphTraversal gt;

	List <String> selector = new Vector<String>();
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
	public void addNodeEdge(Map<String, Set<String>> edgeHash) {
//		String output = "";
//		output = output + "g.traversal().V()";
		List<String> travelledEdges = new Vector<String>();
		
		String startNode;
		Set<String> set = edgeHash.keySet();
		if(set.size() > 0) {
			startNode = set.iterator().next();
			List<String> start = new Vector<String>();
			start.add(startNode);
			Set<String> vertsToVisit = edgeHash.remove(startNode);
			
//			output = output + ".has('" + "TYPE" + "','" + startNode + "')";
//			output = output + ".as('" + startNode + "')";
			
			gt = gt.has(Constants.TYPE, startNode);
			gt = gt.as(startNode);
			travelledEdges.add(startNode);
			gt = visitNode(startNode, vertsToVisit, getUpstreamVerts(startNode, edgeHash), edgeHash, gt, travelledEdges, new Integer(0));
		}
	}
	
	private GraphTraversal visitNode(String orig, Set<String> downVertsToVisit, Set<String> upVertsToVisit, Map<String, Set<String>> edgeHash, GraphTraversal gt1, List<String> travelledEdges, Integer recursionCount) {
		recursionCount++;
		if(downVertsToVisit!=null && !downVertsToVisit.isEmpty()){
			Iterator<String> downIt = downVertsToVisit.iterator();
			for(int i = 0; i < downVertsToVisit.size(); i++, recursionCount++) {
				String node = downIt.next();
				if(!travelledEdges.contains(node)){
					System.out.println("travelling down to " + node);
//					String s = ".out().has('" + "TYPE" + "','" + node + "').as('" + node + "')";
//					query = query + s;
					gt1 = gt1.out().has(Constants.TYPE, node).as(node);
					travelledEdges.add(node);
					gt1 = visitNode(node, edgeHash.remove(node), getUpstreamVerts(node, edgeHash), edgeHash, gt1, travelledEdges, recursionCount);
					System.out.println("returning home to " + orig);
//					String s2 = ".in().has('" + "TYPE" + "','" + orig + "').as('" + orig + i + "')";
//					s2 = s2 + ".where('" + orig + "', org.apache.tinkerpop.gremlin.process.traversal.P.eq('" + orig + i + "'))";
//					query = query + s2;
					gt1 = gt1.in().has(Constants.TYPE, orig).as(orig + (i + recursionCount));
					gt1 = gt1.where(orig, org.apache.tinkerpop.gremlin.process.traversal.P.eq(orig + (i + recursionCount)));
				}
			}
		}

		if(upVertsToVisit!=null && !upVertsToVisit.isEmpty()){
			Iterator<String> upIt = upVertsToVisit.iterator();
			for(int i = 0; i < upVertsToVisit.size(); i++, recursionCount++) {
				String node = upIt.next();
				if(!travelledEdges.contains(node)){
					System.out.println("travelling up to " + node);
//					String s = ".in().has('" + "TYPE" + "','" + node + "').as('" + node + "')";
//					query = query + s;
					gt1 = gt1.in().has(Constants.TYPE, node).as(node);
					travelledEdges.add(node);
					gt1 = visitNode(node, edgeHash.remove(node), getUpstreamVerts(node, edgeHash), edgeHash, gt1, travelledEdges, recursionCount);
					System.out.println("returning home to " + orig);
//					String s2 = ".out().has('" + "TYPE" + "','" + orig + "').as('" + orig + i + "')";
//					s2 = s2 + ".where('" + orig + "', org.apache.tinkerpop.gremlin.process.traversal.P.eq('" + orig + i + "'))";
//					query = query + s2;
					gt1 = gt1.out().has(Constants.TYPE, orig).as(orig + (i + recursionCount));
					gt1 = gt1.where(orig, org.apache.tinkerpop.gremlin.process.traversal.P.eq(orig + (i + recursionCount)));
				}
			}
		}

		return gt1;
	}
	
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
//		script = script + ".where(__.as('" + alias + "').has('" + Constants.NAME + "', org.apache.tinkerpop.gremlin.process.traversal.P.without('" + alias + Constants.FILTERS + "')))";
		gt = gt.where(__.as(alias)).has(Constants.NAME, org.apache.tinkerpop.gremlin.process.traversal.P.without(alias + Constants.FILTERS ));
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
//         String selectorStr = "";
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
//         for(int selIndex = 0;selIndex < selector.size();selIndex++)
//         {
//                if(selIndex == 0)
//                      selectorStr = "'" + selector.get(selIndex) + "'";
//                else
//                      selectorStr = selectorStr + ", '" + selector.get(selIndex) + "'";
//         }
//         script = script + ".select(" + selectorStr + ")"; 
//         gt.select(selectorStr);
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
		
		LOGGER.info("Script executed: "+(System.currentTimeMillis() - startTime)+" ms");
		return gtR;
	}	
}