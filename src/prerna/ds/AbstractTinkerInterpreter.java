package prerna.ds;

import java.util.HashSet;
import java.util.Hashtable;
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

public abstract class AbstractTinkerInterpreter {

	private static final Logger LOGGER = LogManager.getLogger(GremlinInterpreter.class.getName());

	protected Graph g;
	private Graph metaGraph;
	protected GraphTraversal gt;
	protected QueryStruct qs = null;
	Map<String, Set<String>> edgeHash;
	protected List<String> selector;
	protected Map<String, Map<String, List>> filters;

	/**
	 * Get the list of selectors from the QueryStruct Save it as class variable
	 * so we don't repeat logic twice in case
	 * 
	 * @return
	 */
	protected List<String> getSelector() {
		if (this.selector == null) {
			this.selector = new Vector<String>();
			for (String key : qs.selectors.keySet()) {
				List<String> val = qs.selectors.get(key);
				for (String select : val) {
					if (select.equals("PRIM_KEY_PLACEHOLDER")) {
						selector.add(key);
					} else {
						// need to get rid of "__"
						// TODO: should this just never be passed for gremlin?
						// it has no meaning
						if (select.contains("__")) {
							selector.add(select.substring(select.indexOf("__") + 2));
						} else {
							selector.add(select);
						}
					}
				}
			}
		}
		return this.selector;
	}

	/**
	 * gets the selectors and adds it to the traversal
	 */
	protected void addSelectors() {
		List<String> selector = getSelector(); // get the selectors
		// cause gremlin interface is weird...
		// need to determine which method to use based on size of selectors
		if (selector.size() == 1) {
			String select = selector.get(0);
			String selectNode = qs.selectors.keySet().iterator().next();
			//property
			if (!select.equals(selectNode)) {
				gt = gt.select(selector.get(0));
				 if (qs.getPerformCount() == 2 || qs.getLimit() > 0) {
				 gt = gt.dedup();
				 }

			}
			// node
			else {
				if (qs.relations.size() > 0) {
					gt = gt.select(select);
					gt = gt.dedup();
				} else {
					if(qs.andfilters != null && !qs.andfilters.isEmpty()) {
						gt = gt.select(select);
					}
					else {
					gt = gt.has(TinkerFrame.TINKER_TYPE, selector.get(0));
					}

				}

			}

		}

		else if (selector.size() == 2) {
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
	 * Need to clean the filter headers
	 */
	protected void addFilters() {
		if (this.filters == null) {
			this.filters = qs.andfilters;
		}

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
		addNodeEdge(edgeHash);

	}

	/**
	 * Generates the edgeMap to determine what to traverse based on the
	 * relations Assumes all the joins are inner.joins... at the moment, not
	 * sure how to/what it would mean to do something like a left join or right
	 * join since there is only one graph backing a insight If there are no
	 * relations (like in the case where you want one column to be returned), it
	 * adds the selectors in the edgeMap
	 * 
	 * @return
	 */
	public Map<String, Set<String>> generateEdgeMap() {
		Map<String, Set<String>> edgeMap = new Hashtable<String, Set<String>>();

		Map<String, Map<String, List>> rels = qs.relations;

		// add the relationships into the edge map
		if (!rels.isEmpty()) {
			Set<String> relKeys = rels.keySet();
			// looping through the start node of the relationship
			for (String startNode : relKeys) {
				Map<String, List> comps = rels.get(startNode);
				// TODO: currently going to not care about the compKeys and
				// assume everything
				// is an inner join for simplicity
				Set<String> compKeys = comps.keySet();
				for (String comp : compKeys) {
					// this is the end node of the relationship
					List<String> endNodes = comps.get(comp);

					Set<String> joinSet = new HashSet<String>();
					for (String node : endNodes) {
						// need to get rid of "__"
						// TODO: should this just never be passed for gremlin?
						// it has no meaning
						if (node.contains("__")) {
							joinSet.add(node.substring(node.indexOf("__") + 2));
						} else {
							joinSet.add(node);
						}
					}

					if (edgeMap.containsKey(startNode)) {
						Set<String> currSet = edgeMap.get(startNode);
						currSet.addAll(joinSet);
						// need to get rid of "__"
						// TODO: should this just never be passed for gremlin?
						// it has no meaning
						if (startNode.contains("__")) {
							edgeMap.put(startNode.substring(startNode.indexOf("__") + 2), joinSet);
						} else {
							edgeMap.put(startNode, joinSet);
						}
					} else {
						// need to get rid of "__"
						// TODO: should this just never be passed for gremlin?
						// it has no meaning
						if (startNode.contains("__")) {
							edgeMap.put(startNode.substring(startNode.indexOf("__") + 2), joinSet);
						} else {
							edgeMap.put(startNode, joinSet);
						}
					}
				}
			}
		} else {
			// this occurs when there are no relationships defined...
			// example is when you are only going for one column of data
			// made this generic to loop through, but in reality, it should only
			// return one
			// if returns more than one, the query will return nothing...
			List<String> selector = getSelector();
			for (String s : selector) {
				edgeMap.put(s, new HashSet<String>());
			}
		}

		return edgeMap;
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

		Vertex startNode = this.metaGraph.traversal().V().has(TinkerFrame.TINKER_NAME, startUniqueName).next();

		// TinkerFrame.TINKER_NAME changes while TinkerFrame.TINKER_VALUE stays
		// constant
		String nameType = startNode.property(TinkerFrame.TINKER_NAME).value() + "";
		String valueType = startNode.property(TinkerFrame.TINKER_VALUE).value() + "";

		// remove prim_key when making a heatMap
		if (valueType.equals(TinkerFrame.PRIM_KEY)) {
			valueType = startNode.property(TinkerFrame.TINKER_NAME).value() + "";
		}

		gt = gt.has(TinkerFrame.TINKER_TYPE, valueType).as(nameType);
		// there is a boolean at the metamodel level if this type has any
		// filters
		Object filtered = startNode.value(TinkerFrame.TINKER_FILTER);
		if ((Boolean) filtered == true) {
			// filtered edges have a type of filter
			gt = gt.not(__.in(TinkerFrame.TINKER_FILTER + TinkerFrame.EDGE_LABEL_DELIMETER + nameType)
					.has(TinkerFrame.TINKER_TYPE, TinkerFrame.TINKER_FILTER));
		}
		if (this.filters.containsKey(nameType)) {
			addFilterInPath(gt, nameType, this.filters.get(nameType));
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
		String origName = orig.value(TinkerFrame.TINKER_NAME);
		// String origValue = orig.value(TinkerFrame.TINKER_VALUE);

		// remove prim_key when making a heatMap
		// if(origValue.equals(TinkerFrame.PRIM_KEY)){
		// origValue = orig.property(TinkerFrame.TINKER_NAME).value() + "";
		// }

		Set<String> edgesToTraverse = edgeMap.get(origName);
		if (edgesToTraverse != null) {
			// for each downstream node of this meta node

			// TODO: this can be optimized, using the edgeMap do determine the
			// traversal instead of iterating and guessing
			GraphTraversal<Vertex, Vertex> downstreamIt = this.metaGraph.traversal().V()
					.has(TinkerFrame.TINKER_TYPE, TinkerMetaData.META)
					.has(TinkerFrame.TINKER_ID, orig.property(TinkerFrame.TINKER_ID).value())
					.out(TinkerMetaData.META + TinkerFrame.EDGE_LABEL_DELIMETER + TinkerMetaData.META);
			while (downstreamIt.hasNext()) {
				// for each downstream node of this meta node
				Vertex nodeV = downstreamIt.next();

				// TinkerFrame.TINKER_NAME changes while
				// TinkerFrame.TINKER_VALUE stays constant
				String nameNode = nodeV.property(TinkerFrame.TINKER_NAME).value() + "";
				String valueNode = nodeV.property(TinkerFrame.TINKER_VALUE).value() + "";

				// remove prim_key when making a heatMap
				if (valueNode.equals(TinkerFrame.PRIM_KEY)) {
					valueNode = nodeV.property(TinkerFrame.TINKER_NAME).value() + "";
				}

				String edgeKey = origName + TinkerFrame.EDGE_LABEL_DELIMETER + nameNode;

				if (!travelledEdges.contains(edgeKey) && edgesToTraverse.contains(nameNode)) {
					LOGGER.info("travelling down to " + nameNode);

					GraphTraversal<Object, Vertex> twoStepT = __.as(origName).out(edgeKey).has(TinkerFrame.TINKER_TYPE,
							valueNode);

					Object filtered = nodeV.value(TinkerFrame.TINKER_FILTER);
					if ((Boolean) filtered == true) {
						twoStepT = twoStepT
								.not(__.in(TinkerFrame.TINKER_FILTER + TinkerFrame.EDGE_LABEL_DELIMETER + nameNode)
										.has(TinkerFrame.TINKER_TYPE, TinkerFrame.TINKER_FILTER));
					}
					if (this.filters.containsKey(nameNode)) {
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
			// TODO: this can be optimized, using the edgeMap do determine the
			// traversal instead of iterating and guessing
			GraphTraversal<Vertex, Vertex> upstreamIt = this.metaGraph.traversal().V()
					.has(TinkerFrame.TINKER_TYPE, TinkerMetaData.META)
					.has(TinkerFrame.TINKER_ID, orig.property(TinkerFrame.TINKER_ID).value())
					.in(TinkerMetaData.META + TinkerFrame.EDGE_LABEL_DELIMETER + TinkerMetaData.META);
			while (upstreamIt.hasNext()) {
				Vertex nodeV = upstreamIt.next();

				// TinkerFrame.TINKER_NAME changes while
				// TinkerFrame.TINKER_VALUE stays constant
				String nameNode = nodeV.property(TinkerFrame.TINKER_NAME).value() + "";
				String valueNode = nodeV.property(TinkerFrame.TINKER_VALUE).value() + "";

				// remove prim_key when making a heatMap
				if (valueNode.equals(TinkerFrame.PRIM_KEY)) {
					valueNode = nodeV.property(TinkerFrame.TINKER_NAME).value() + "";
				}

				String edgeKey = nameNode + TinkerFrame.EDGE_LABEL_DELIMETER + origName;
				if (!travelledEdges.contains(edgeKey) && edgesToTraverse.contains(nameNode)) {
					LOGGER.info("travelling down to " + nameNode);

					GraphTraversal<Object, Vertex> twoStepT = __.as(origName).in(edgeKey).has(TinkerFrame.TINKER_TYPE,
							valueNode);

					Object filtered = nodeV.value(TinkerFrame.TINKER_FILTER);
					if ((Boolean) filtered == true) {
						twoStepT = twoStepT
								.not(__.in(TinkerFrame.TINKER_FILTER + TinkerFrame.EDGE_LABEL_DELIMETER + nameNode)
										.has(TinkerFrame.TINKER_TYPE, TinkerFrame.TINKER_FILTER));
					}
					if (this.filters.containsKey(nameNode)) {
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

	public void addFilterInPath(GraphTraversal<Object, Vertex> gt, String nameType, Map<String, List> map) {
		// TODO: right now, if its a math, assumption that vector only contains
		// one value
		for (String filterType : map.keySet()) {
			List filterVals = map.get(filterType);
			if (filterType.equals("=")) {
				// if(filterVals.get(0) instanceof Number) {
				// gt = gt.has(TinkerFrame.TINKER_NAME, P.eq(filterVals.get(0)
				// ));
				// } else {
				gt = gt.has(TinkerFrame.TINKER_NAME, P.within(filterVals.toArray()));
				// }
			} else if (filterType.equals("<")) {
				gt = gt.has(TinkerFrame.TINKER_NAME, P.lt(filterVals.get(0)));
			} else if (filterType.equals(">")) {
				gt = gt.has(TinkerFrame.TINKER_NAME, P.gt(filterVals.get(0)));
			} else if (filterType.equals("<=")) {
				gt = gt.has(TinkerFrame.TINKER_NAME, P.lte(filterVals.get(0)));
			} else if (filterType.equals(">=")) {
				gt = gt.has(TinkerFrame.TINKER_NAME, P.gte(filterVals.get(0)));
			} else if (filterType.equals("!=")) {
				// if(filterVals.get(0) instanceof Number) {
				// gt = gt.has(TinkerFrame.TINKER_NAME, P.neq(filterVals.get(0)
				// ));
				// } else {
				gt = gt.has(TinkerFrame.TINKER_NAME, P.without(filterVals.toArray()));
				// }
			}
		}
	}

}
