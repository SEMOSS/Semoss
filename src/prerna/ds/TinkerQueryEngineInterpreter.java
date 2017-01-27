package prerna.ds;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import prerna.engine.impl.tinker.TinkerEngine;
import prerna.rdf.query.builder.IQueryInterpreter;

public class TinkerQueryEngineInterpreter extends AbstractTinkerInterpreter implements IQueryInterpreter {

	private Vector<String> nodeSelector;
	private HashMap<String, String> propHash;
	// private Vector<String> propSet;

	public Vector<String> getNodeSelector() {
		return nodeSelector;
	}

	public void setNodeSelector(Vector<String> nodeSelector) {
		this.nodeSelector = nodeSelector;
	}

	public TinkerQueryEngineInterpreter(TinkerEngine tinkerEngine) {
		this.g = tinkerEngine.getGraph();
		this.gt = g.traversal().V();
	}

	@Override
	public void setQueryStruct(QueryStruct qs) {
		this.qs = qs;
	}

	/**
	 * ugh... other interpreters return a string... but that is super
	 * inefficient on gremlin
	 */
	@Override
	public String composeQuery() {
		return null;
	}

	/**
	 * gets the selectors and adds it to the traversal
	 */
	protected void addSelectors() {
		List<String> selector = this.nodeSelector;
		// cause gremlin interface is weird...
		// need to determine which method to use based on size of selectors
		if (selector.size() == 1) {
//			gt = gt.select(selector.get(0));
			gt = g.traversal().V();
			gt = gt.has(TinkerFrame.TINKER_TYPE, nodeSelector.get(0));
		} else if (selector.size() == 2) {
			gt = gt.select(selector.get(0), selector.get(1));
		} else if (selector.size() >= 3) {
			String[] selectorArr = new String[selector.size() - 2];
			for (int i = 2; i < selector.size(); i++) {
				selectorArr[i - 2] = selector.get(i);
			}
			gt = gt.select(selector.get(0), selector.get(1), selectorArr);
		}
	}

	/**
	 * screw returning a string.. i'm going to go ahead and return an iterator..
	 * 
	 * @return
	 */
	public Iterator composeIterator() {
		addFilters();
		getSelector();
		addJoins();
		addSelectors();
		return new TinkerIterator(gt, this.selector, propHash, nodeSelector, this.filters);
	}

	/**
	 * Get the list of selectors from the QueryStruct Save it as class variable
	 * so we don't repeat logic twice in case
	 * 
	 * @return
	 */
	protected List<String> getSelector() {
		if (this.selector == null) {
			this.selector = new Vector<String>();
			this.nodeSelector = new Vector<String>();
			this.propHash = new HashMap<String, String>();

			for (String key : qs.selectors.keySet()) {
				nodeSelector.add(key);
				Vector<String> val = qs.selectors.get(key);

				for (String select : val) {
					if (!select.equals("PRIM_KEY_PLACEHOLDER")) {
						propHash.put(select, key);
						selector.add(select);
					}
					else {
						selector.add(key);
					}
				}

			}
		}
		return this.selector;
	}

	protected void addJoins() {
		// process the specific joins wanted in the traversal
		// this utilizes the previously definted filters

		// might want to consider doing some optimization in how i choose
		// the first node, similar to what is done in gremlin builder
		// but this will be a TODO
		if (edgeHash == null || edgeHash.isEmpty()) {
			edgeHash = generateEdgeMap();
		}
		
		if (nodeSelector.size() > 1) {
			addNodeEdge(edgeHash);
		}

	}

	/**
	 * This is the bulk of the class Uses the edgeMap to figure out what things
	 * are connected
	 * 
	 * @param edgeMap
	 */
	public void addNodeEdge(Map<String, Set<String>> edgeMap) {
		if (edgeMap.isEmpty()) {
			return;
		}

		List<String> travelledEdges = new Vector<String>();
		List<GraphTraversal<Object, Vertex>> traversals = new Vector<GraphTraversal<Object, Vertex>>();

		String startUniqueName = edgeMap.keySet().iterator().next();
		// TODO remove if overwritten
		// this.edgeHash = this.qs.getReturnConnectionsHash();

		Vertex startNode = g.traversal().V().has(TinkerFrame.TINKER_TYPE, startUniqueName).next();

		// check node properties

		// TinkerFrame.TINKER_NAME changes while TinkerFrame.TINKER_VALUE stays
		// constant
		String nameType = startNode.property(TinkerFrame.TINKER_NAME).value() + "";
		// String valueType =
		// startNode.property(TinkerFrame.TINKER_VALUE).value()+"";

		// remove prim_key when making a heatMap
		// if(valueType.equals(TinkerFrame.PRIM_KEY)){
		// valueType = startNode.property(TinkerFrame.TINKER_NAME).value() + "";
		// }

		// gt = gt.has(TinkerFrame.TINKER_TYPE, valueType).as(nameType);
		// there is a boolean at the metamodel level if this type has any
		// filters

		if (this.filters.containsKey(startUniqueName)) {
			addFilterInPath(gt, startUniqueName, this.filters.get(startUniqueName));
		}

		// add the logic to traverse
		traversals = visitNode(startNode, travelledEdges, edgeMap, traversals);

		if (traversals.size() > 0) {
			GraphTraversal[] array = new GraphTraversal[traversals.size()];
			gt = gt.match(traversals.toArray(array));
		}

	}

	protected List<GraphTraversal<Object, Vertex>> visitNode(Vertex orig, List<String> travelledEdges,
			Map<String, Set<String>> edgeMap, List<GraphTraversal<Object, Vertex>> traversals) {
		// TinkerFrame.TINKER_NAME changes while TinkerFrame.TINKER_VALUE stays
		// constant
		String origName = orig.value(TinkerFrame.TINKER_TYPE);
		// String origValue = orig.value(TinkerFrame.TINKER_VALUE);

		// remove prim_key when making a heatMap
		// if(origValue.equals(TinkerFrame.PRIM_KEY)){
		// origValue = orig.property(TinkerFrame.TINKER_NAME).value() + "";
		// }

		Set<String> edgesToTraverse = edgeMap.get(origName);
		if (edgesToTraverse != null) {
			for (String edge : edgesToTraverse) {
				String edgeKey = origName + TinkerFrame.EDGE_LABEL_DELIMETER + edge;
				GraphTraversal<Vertex, Vertex> downstreamIt = g.traversal().V(orig);

				downstreamIt.out(edgeKey);

				// downstreamIt.outV();

				// for each downstream node of this meta node

				// TODO: this can be optimized, using the edgeMap do determine
				// the traversal instead of iterating and guessing
				// GraphTraversal<Vertex, Vertex> downstreamIt =
				// g.traversal().V().has(TinkerFrame.TINKER_TYPE).has(TinkerFrame.TINKER_ID,
				// orig.property(TinkerFrame.TINKER_ID).value()).out(TinkerFrame.TINKER_TYPE
				// + TinkerFrame.EDGE_LABEL_DELIMETER + edge);
				while (downstreamIt.hasNext()) {
					// for each downstream node of this meta node
					Vertex nodeV = downstreamIt.next();

					// TinkerFrame.TINKER_NAME changes while
					// TinkerFrame.TINKER_VALUE stays constant
					String nameNode = nodeV.property(TinkerFrame.TINKER_TYPE).value() + "";
					String valueNode = nodeV.property(TinkerFrame.TINKER_NAME).value() + "";

					// remove prim_key when making a heatMap
					if (valueNode.equals(TinkerFrame.PRIM_KEY)) {
						valueNode = nodeV.property(TinkerFrame.TINKER_NAME).value() + "";
					}

					// String edgeKey = origName +
					// TinkerFrame.EDGE_LABEL_DELIMETER + nameNode;

					if (!travelledEdges.contains(edgeKey) && edgesToTraverse.contains(nameNode)) {
						// LOGGER.info("travelling down to " + nameNode);

						GraphTraversal<Object, Vertex> twoStepT = __.as(origName).out(edgeKey);

						if (this.filters.containsKey(nameNode)) {
							addFilterInPath(twoStepT, nameNode, this.filters.get(nameNode));
						}

						twoStepT = twoStepT.as(nameNode);
						// LOGGER.info("twoStepT downstream : " + twoStepT);
						traversals.add(twoStepT);

						travelledEdges.add(edgeKey);
						// travel as far downstream as possible
						traversals = visitNode(nodeV, travelledEdges, edgeMap, traversals);
					}
				}
			}
		}

		// do the same thing for upstream
		// TODO: this can be optimized, using the edgeMap do determine the
		// traversal instead of iterating and guessing
		for (String s : edgeMap.keySet()) {
			Set<String> upEdges = edgeMap.get(s);
			if (upEdges.contains(origName)) {

				GraphTraversal<Vertex, Vertex> upstreamIt = g.traversal().V(orig);
				String edgeKey = s + TinkerFrame.EDGE_LABEL_DELIMETER + origName;

				upstreamIt.in(edgeKey);

				while (upstreamIt.hasNext()) {
					Vertex nodeV = upstreamIt.next();

					// TinkerFrame.TINKER_NAME changes while
					// TinkerFrame.TINKER_VALUE stays constant
					String nameNode = nodeV.property(TinkerFrame.TINKER_TYPE).value() + "";
					String valueNode = nodeV.property(TinkerFrame.TINKER_NAME).value() + "";

					// remove prim_key when making a heatMap
					if (valueNode.equals(TinkerFrame.PRIM_KEY)) {
						valueNode = nodeV.property(TinkerFrame.TINKER_NAME).value() + "";
					}

					edgeKey = nameNode + TinkerFrame.EDGE_LABEL_DELIMETER + origName;
					if (!travelledEdges.contains(edgeKey) && s.equals(nameNode)) {
						// LOGGER.info("travelling down to " + nameNode);

						GraphTraversal<Object, Vertex> twoStepT = __.as(origName).in(edgeKey)
								.has(TinkerFrame.TINKER_TYPE, nameNode);
						if (nodeV.keys().contains(TinkerFrame.TINKER_FILTER)) {
							Object filtered = nodeV.value(TinkerFrame.TINKER_FILTER);
							if ((Boolean) filtered == true) {
								twoStepT = twoStepT.not(
										__.in(TinkerFrame.TINKER_FILTER + TinkerFrame.EDGE_LABEL_DELIMETER + nameNode)
												.has(TinkerFrame.TINKER_TYPE, TinkerFrame.TINKER_FILTER));
							}
						}
						if (this.filters.containsKey(nameNode)) {
							addFilterInPath(twoStepT, nameNode, this.filters.get(nameNode));
						}

						twoStepT = twoStepT.as(nameNode);
						// LOGGER.info("twoStepT upstream : " + twoStepT);
						traversals.add(twoStepT);

						travelledEdges.add(edgeKey);
						// travel as far upstream as possible
						traversals = visitNode(nodeV, travelledEdges, edgeMap, traversals);
					}
				}

			}
		}

		return traversals;
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
			for (String s : selector) {
				if (!propHash.containsKey(s)) {
					edgeMap.put(s, new HashSet<String>());
				}
			}
		}
		
		return edgeMap;
	}
	@Override
	public void setPerformCount(int performCount) {
		// TODO Auto-generated method stub

	}

	@Override
	public int isPerformCount() {
		return QueryStruct.NO_COUNT;
	}

	@Override
	public void clear() {

	}

}
