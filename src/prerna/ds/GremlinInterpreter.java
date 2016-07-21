package prerna.ds;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.Insight;
import prerna.rdf.query.builder.IQueryInterpreter;
import prerna.sablecc.PKQLRunner;
import prerna.test.TestUtilityMethods;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.ui.components.playsheets.datamakers.PKQLTransformation;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class GremlinInterpreter implements IQueryInterpreter {

	private static final Logger LOGGER = LogManager.getLogger(GremlinInterpreter.class.getName());

	private Graph g;
	private Graph metaGraph;
	private GraphTraversal gt;
	private QueryStruct qs = null;
	
	private List<String> selector;
	private Hashtable<String, Hashtable<String, Vector>> filters;
	
	public GremlinInterpreter(Graph g, Graph metaGraph) {
		this.g = g;
		this.metaGraph = metaGraph;
		this.gt = g.traversal().V();
	}
	
	@Override
	public void setQueryStruct(QueryStruct qs) {
		this.qs = qs;
	}

	/**
	 * ugh... other interpreters return a string... but that is super inefficient on gremlin
	 */
	@Override
	public String composeQuery() {
		return null;
	}
	
	/**
	 * screw returning a string.. i'm going to go ahead and return an iterator.. 
	 * @return
	 */
	public Iterator composeIterator() {
		addFilters();
		addJoins();
		addSelectors();
		
		return gt;
	}
	
	/**
	 * gets the selectors and adds it to the traversal
	 */
	private void addSelectors() {
		List<String> selector = getSelector(); // get the selectors
		// cause gremlin interface is weird... 
		// need to determine which method to use based on size of selectors
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
	 * Need to clean the filter headers
	 */
	private void addFilters() {
		if(this.filters == null) {
			this.filters = new Hashtable<String, Hashtable<String, Vector>>();
			Hashtable<String, Hashtable<String, Vector>> qsFilters = qs.andfilters;
			for(String key : qsFilters.keySet()) {
				Hashtable<String, Vector> filterMap = qsFilters.get(key);
				Hashtable<String, Vector> cleanFilterMap = new Hashtable<String, Vector>();
				
				// need to loop through and make sure everything is clean
				for(String comp: filterMap.keySet()) {
					Vector filterValues = filterMap.get(comp);
					Vector cleanFilterValues = new Vector();
					
					for(Object object : filterValues) {
						Object myobject = object.toString().replace("\"", "");
						String type = Utility.findTypes(myobject + "")[0] + "";
						if(type.equalsIgnoreCase("Date")) {
							myobject = Utility.getDate(myobject + "");
						} else if(type.equalsIgnoreCase("Double")) {
		    				myobject = Utility.getDouble(myobject + "");
						}
						cleanFilterValues.add(myobject);
					}
					
					cleanFilterMap.put(comp, cleanFilterValues);
				}
				
				if(key.contains("__")) {
					this.filters.put(key.substring(key.indexOf("__")+2), cleanFilterMap);
				} else {
					this.filters.put(key,cleanFilterMap);
				}
				
			}
		}
	}

	private void addJoins() {
		// process the specific joins wanted in the traversal
		// this utilizes the previously definted filters

		// might want to consider doing some optimization in how i choose
		// the first node, similar to what is done in gremlin builder
		// but this will be a TODO

		Map<String, Set<String>> edgeMap = generateEdgeMap();
		addNodeEdge(edgeMap);
	}
	
	/**
	 * This is the bulk of the class
	 * Uses the edgeMap to figure out what things are connected
	 * 
	 * @param edgeMap
	 */
	public void addNodeEdge(Map<String, Set<String>> edgeMap) {
		if(edgeMap.isEmpty()) {
			return;
		}
		
		List<String> travelledEdges = new Vector<String>();
		List<GraphTraversal<Object, Vertex>> traversals = new Vector<GraphTraversal<Object, Vertex>>();
		
		String startUniqueName = edgeMap.keySet().iterator().next();
		
		Vertex startNode = this.metaGraph.traversal().V().has(Constants.NAME, startUniqueName).next();

		//Constants.NAME changes while Constants.VALUE stays constant
		String nameType = startNode.property(Constants.NAME).value()+""; 
		String valueType = startNode.property(Constants.VALUE).value()+""; 

		//remove prim_key when making a heatMap
		if(valueType.equals(TinkerFrame.PRIM_KEY)){
			valueType = startNode.property(Constants.NAME).value() + "";
		}
		
		gt = gt.has(Constants.TYPE, valueType).as(nameType);
		// there is a boolean at the metamodel level if this type has any filters
		Object filtered = startNode.value(Constants.FILTER); 
		if((Boolean)filtered == true) {
			// filtered edges have a type of filter
			gt = gt.not(__.in(Constants.FILTER + TinkerFrame.edgeLabelDelimeter + nameType).has(Constants.TYPE, Constants.FILTER));
		}
		if(this.filters.containsKey(nameType)) {
			addFilterInPath(gt, nameType, this.filters.get(nameType));
		}
					
		// add the logic to traverse
		traversals = visitNode(startNode, travelledEdges, edgeMap, traversals);
		
		if(traversals.size()>0){
			GraphTraversal[] array = new GraphTraversal[traversals.size()];
			gt = gt.match(traversals.toArray(array));
		}
		
	}
	
	private List<GraphTraversal<Object, Vertex>> visitNode(Vertex orig, List<String> travelledEdges, Map<String, Set<String>> edgeMap, List<GraphTraversal<Object, Vertex>> traversals) {
		//Constants.NAME changes while Constants.VALUE stays constant
		String origName = orig.value(Constants.NAME);  
		String origValue = orig.value(Constants.VALUE);
		
		//remove prim_key when making a heatMap
		if(origValue.equals(TinkerFrame.PRIM_KEY)){
			origValue = orig.property(Constants.NAME).value() + "";
		}
		
		Set<String> edgesToTraverse = edgeMap.get(origName);
		if(edgesToTraverse != null) {
			// for each downstream node of this meta node
			
			//TODO: this can be optimized, using the edgeMap do determine the traversal instead of iterating and guessing
			GraphTraversal<Vertex, Vertex> downstreamIt = this.metaGraph.traversal().V().has(Constants.TYPE, TinkerMetaData.META).has(Constants.ID, orig.property(Constants.ID).value()).out(TinkerMetaData.META + TinkerFrame.edgeLabelDelimeter + TinkerMetaData.META);
			while (downstreamIt.hasNext()) {
				// for each downstream node of this meta node
				Vertex nodeV = downstreamIt.next();
				
				//Constants.NAME changes while Constants.VALUE stays constant
				String nameNode = nodeV.property(Constants.NAME).value() + "";
				String valueNode = nodeV.property(Constants.VALUE).value() + "";
				
				//remove prim_key when making a heatMap
				if(valueNode.equals(TinkerFrame.PRIM_KEY)){
					valueNode = nodeV.property(Constants.NAME).value() + "";
				}
				
				String edgeKey = origName + TinkerFrame.edgeLabelDelimeter + nameNode;
	
				if (!travelledEdges.contains(edgeKey) && edgesToTraverse.contains(nameNode)) {
					LOGGER.info("travelling down to " + nameNode);
	
					GraphTraversal<Object, Vertex> twoStepT = __.as(origName).out(edgeKey).has(Constants.TYPE, valueNode);
	
					Object filtered = nodeV.value(Constants.FILTER);
					if ((Boolean) filtered == true) {
						twoStepT = twoStepT.not(__.in(Constants.FILTER + TinkerFrame.edgeLabelDelimeter + nameNode).has(Constants.TYPE, Constants.FILTER));
					}
					if(this.filters.containsKey(nameNode)) {
						addFilterInPath(twoStepT, nameNode, this.filters.get(nameNode));
					}
	
					twoStepT = twoStepT.as(nameNode);
					LOGGER.info("twoStepT downstream : " + twoStepT);
					traversals.add(twoStepT);
	
					travelledEdges.add(edgeKey);
					// travel as far downstream as possible
					traversals = visitNode(nodeV, travelledEdges, edgeMap, traversals);
				}
			}
			
			
			// do the same thing for upstream
			//TODO: this can be optimized, using the edgeMap do determine the traversal instead of iterating and guessing
			GraphTraversal<Vertex, Vertex> upstreamIt = this.metaGraph.traversal().V().has(Constants.TYPE, TinkerMetaData.META).has(Constants.ID, orig.property(Constants.ID).value()).in(TinkerMetaData.META+TinkerFrame.edgeLabelDelimeter+TinkerMetaData.META);
			while(upstreamIt.hasNext()) {
				Vertex nodeV = upstreamIt.next();
				
				//Constants.NAME changes while Constants.VALUE stays constant
				String nameNode = nodeV.property(Constants.NAME).value() + "";
				String valueNode = nodeV.property(Constants.VALUE).value() + "";
				
				//remove prim_key when making a heatMap
				if(valueNode.equals(TinkerFrame.PRIM_KEY)){
					valueNode = nodeV.property(Constants.NAME).value() + "";
				}
				
				String edgeKey = nameNode + TinkerFrame.edgeLabelDelimeter + origName;
				if (!travelledEdges.contains(edgeKey) && edgesToTraverse.contains(nameNode)) {
					LOGGER.info("travelling down to " + nameNode);
	
					GraphTraversal<Object, Vertex> twoStepT = __.as(origName).in(edgeKey).has(Constants.TYPE, valueNode);
	
					Object filtered = nodeV.value(Constants.FILTER);
					if ((Boolean) filtered == true) {
						twoStepT = twoStepT.not(__.in(Constants.FILTER + TinkerFrame.edgeLabelDelimeter + nameNode).has(Constants.TYPE, Constants.FILTER));
					}
					if(this.filters.containsKey(nameNode)) {
						addFilterInPath(twoStepT, nameNode, this.filters.get(nameNode));
					}
					
					twoStepT = twoStepT.as(nameNode);
					LOGGER.info("twoStepT upstream : " + twoStepT);
					traversals.add(twoStepT);
	
					travelledEdges.add(edgeKey);
					// travel as far upstream as possible
					traversals = visitNode(nodeV, travelledEdges, edgeMap, traversals);
				}
			}
		}
		
		return traversals;
	}
	
	void addFilterInPath(GraphTraversal<Object, Vertex> gt, String nameType, Hashtable<String, Vector> filterInfo) {
		// TODO: right now, if its a math, assumption that vector only contains one value
		for(String filterType : filterInfo.keySet()) {
			Vector filterVals = filterInfo.get(filterType);
			if(filterType.equals("=")) {
				if(filterVals.get(0) instanceof Number) {
					gt = gt.has(Constants.NAME, P.eq(filterVals.get(0) ));
				} else {
					gt = gt.has(Constants.NAME, P.within(filterVals.toArray(new String[]{})));
				}
			} else if(filterType.equals("<")) {
				gt = gt.has(Constants.NAME, P.lt(filterVals.get(0)));
			} else if(filterType.equals(">")) {
				gt = gt.has(Constants.NAME, P.gt(filterVals.get(0)));
			} else if(filterType.equals("<=")) {
				gt = gt.has(Constants.NAME, P.lte(filterVals.get(0)));
			} else if(filterType.equals(">=")) {
				gt = gt.has(Constants.NAME, P.gte(filterVals.get(0)));
			} else if(filterType.equals("!=")) {
				if(filterVals.get(0) instanceof Number) {
					gt = gt.has(Constants.NAME, P.neq(filterVals.get(0) ));
				} else {
					gt = gt.has(Constants.NAME, P.without(filterVals.toArray(new String[]{})));
				}
			}
		}
	}
	
	/**
	 * Generates the edgeMap to determine what to traverse based on the relations
	 * Assumes all the joins are inner.joins... at the moment, not sure how to/what it would mean to do
	 * 		something like a left join or right join since there is only one graph backing a insight
	 * If there are no relations (like in the case where you want one column to be returned), it adds the 
	 * 		selectors in the edgeMap
	 * @return
	 */
	public Map<String, Set<String>> generateEdgeMap() {
		Map<String, Set<String>> edgeMap = new Hashtable<String, Set<String>>();
		
		Hashtable<String, Hashtable<String, Vector>> rels = qs.relations;
		
		// add the relationships into the edge map
		if(!rels.isEmpty()) {
			Set<String> relKeys = rels.keySet();
			// looping through the start node of the relationship
			for(String startNode : relKeys) {
				Hashtable<String, Vector> comps = rels.get(startNode);
				//TODO: currently going to not care about the compKeys and assume everything 
				// 		is an inner join for simplicity
				Set<String> compKeys = comps.keySet();
				for(String comp : compKeys) {
					// this is the end node of the relationship
					Vector<String> endNodes = comps.get(comp);
					
					Set<String> joinSet = new HashSet<String>();
					for(String node : endNodes) {
						// need to get rid of "__"
						// TODO: should this just never be passed for gremlin? it has no meaning
						if(node.contains("__")) {
							joinSet.add(node.substring(node.indexOf("__")+2));
						} else {
							joinSet.add(node);
						}
					}
					
					if(edgeMap.containsKey(startNode)) {
						Set<String> currSet = edgeMap.get(startNode);
						currSet.addAll(joinSet);
						// need to get rid of "__"
						// TODO: should this just never be passed for gremlin? it has no meaning
						if(startNode.contains("__")) {
							edgeMap.put(startNode.substring(startNode.indexOf("__")+2), joinSet);
						} else {
							edgeMap.put(startNode, joinSet);
						}
					} else {
						// need to get rid of "__"
						// TODO: should this just never be passed for gremlin? it has no meaning
						if(startNode.contains("__")) {
							edgeMap.put(startNode.substring(startNode.indexOf("__")+2), joinSet);
						} else {
							edgeMap.put(startNode, joinSet);
						}
					}
				}
			}
		} else {
			// this occurs when there are no relationships defined...
			// example is when you are only going for one column of data
			// made this generic to loop through, but in reality, it should only return one
			// if returns more than one, the query will return nothing...
			List<String> selector = getSelector();
			for(String s : selector) {
				edgeMap.put(s, new HashSet<String>());
			}
		}
		
		return edgeMap;
	}
	
	/**
	 * Get the list of selectors from the QueryStruct
	 * Save it as class variable so we don't repeat logic twice in case
	 * @return
	 */
	private List<String> getSelector() {
		if(this.selector == null) {
			this.selector = new Vector<String>();
			for(String key : qs.selectors.keySet()) {
				Vector<String> val = qs.selectors.get(key);
				for(String select : val) {
					if(select.equals("PRIM_KEY_PLACEHOLDER")) {
						selector.add(key);
					} else {
						// need to get rid of "__"
						// TODO: should this just never be passed for gremlin? it has no meaning
						if(select.contains("__")) {
							selector.add(select.substring(select.indexOf("__")+2));
						} else {
							selector.add(select);
						}
					}
				}
			}
		}
		return this.selector;
	}
	
	public static void main(String[] args) {
		TestUtilityMethods.loadDIHelper();

		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\Movie_RDBMS.smss";
		RDBMSNativeEngine movie = new RDBMSNativeEngine();
		movie.setEngineName("Movie_RDBMS");
		movie.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty("Movie_RDBMS", movie);
		
		PKQLTransformation pkql = new PKQLTransformation();
		Map<String, Object> props = new HashMap<String, Object>();
		String pkqlCmd = "data.import ( api: Movie_RDBMS . query ( [ c: Title , c: Title__Movie_Budget, c:Title__Revenue_Domestic ] , ( [ c: Title , inner.join , c: Title__Movie_Budget ], [c: Title , inner.join , c: Title__Revenue_Domestic] ) ) ) ;";
		props.put(PKQLTransformation.EXPRESSION, pkqlCmd);
		pkql.setProperties(props);
		PKQLRunner runner = new PKQLRunner();
		pkql.setRunner(runner);
		List<ISEMOSSTransformation> list = new Vector<ISEMOSSTransformation>();
		list.add(pkql);
		
		Insight insight = new Insight(null, "TinkerFrame", "Grid");
		insight.processPostTransformation(list);
		insight.syncPkqlRunnerAndFrame(runner);
		
		Map resultHash = insight.getPKQLData(true);
		
		System.out.println(resultHash);
		
		TinkerFrame tf = (TinkerFrame) insight.getDataMaker();
		Iterator<Object[]> it = tf.iterator(false);
		
		System.out.println("<<<<<<<");
		System.out.println("<<<<<<<");
		System.out.println("<<<<<<<");
		System.out.println("<<<<<<<");
		LOGGER.info("First 10 iterator values");

		int counter = 0;
		while(it.hasNext() && counter < 10) {
			System.out.println(Arrays.toString(it.next()));
			counter++;
		}
		
		QueryStruct qs = new QueryStruct();
		Hashtable <String, Vector<String>> selectors = new Hashtable <String, Vector<String>>();
		Vector<String> v1 = new Vector<String>();
		v1.add("PRIM_KEY_PLACEHOLDER");
		v1.add("Title__Movie_Budget");
//		v1.add("Title__Revenue_Domestic");
		selectors.put("Title", v1);
		qs.selectors = selectors;
		
		Hashtable <String, Hashtable<String, Vector>> relations = new Hashtable<String, Hashtable<String, Vector>>();
		Vector<String> v2 = new Vector<String>();
		v2.add("Title__Movie_Budget");
//		v2.add("Title__Revenue_Domestic");
		Hashtable<String, Vector> h2 = new Hashtable<String, Vector>();
		h2.put("inner.join", v2);
		relations.put("Title", h2);
		qs.relations = relations;

		Hashtable <String, Hashtable<String, Vector>> filters = new Hashtable<String, Hashtable<String, Vector>>();
		Vector v3 = new Vector();
		v3.add("0");
		Hashtable<String, Vector> h3 = new Hashtable<String, Vector>();
		h3.put(">", v3);
		Vector v4 = new Vector();
		v4.add("5000000");
		h3.put("<", v4);
		filters.put("Title__Movie_Budget", h3);
		qs.andfilters = filters;
		
		
		System.out.println("<<<<<<<");
		System.out.println("<<<<<<<");
		System.out.println("<<<<<<<");
		System.out.println("<<<<<<<");

		GremlinInterpreter interp = new GremlinInterpreter(tf.g, ((TinkerMetaData) tf.metaData).g);
		interp.setQueryStruct(qs);
		Iterator gremlinIt = interp.composeIterator();
		
		LOGGER.info("Gremlin Interpretor query output");

		Object [] retObject = new Object[interp.selector.size()];
		while(gremlinIt.hasNext()) {
			Object data = gremlinIt.next();
			if(data instanceof Map) {
				for(int colIndex = 0;colIndex < interp.selector.size();colIndex++) {
					Map<String, Object> mapData = (Map<String, Object>)data; //cast to map
					retObject[colIndex] = ((Vertex)mapData.get(interp.selector.get(colIndex))).property(Constants.NAME).value();
				}
			} else {
				retObject[0] = ((Vertex)data).property(Constants.NAME).value();
			}
			
			System.out.println(Arrays.toString(retObject));
		}
	}

}