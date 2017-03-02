package prerna.ds;

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

import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.rdf.query.builder.IQueryInterpreter;

public class TinkerQueryInterpreter extends AbstractTinkerInterpreter implements IQueryInterpreter {

	private static final Logger LOGGER = LogManager.getLogger(AbstractEngine.class.getName());

	private HashMap<String, List<String>> propHash;

	public TinkerQueryInterpreter(TinkerEngine tinkerEngine) {
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
	 * screw returning a string.. i'm going to go ahead and return an iterator..
	 * 
	 * @return
	 */
	public Iterator composeIterator() {
		getSelector();
		addFilters();
		addJoins();
		addSelectors();
		addLimitOffset();
		return new TinkerIterator(gt, this.selector, qs);
	}

	/**
	 * gets the Limit/Offset and adds it to the traversal
	 */
	private void addLimitOffset() {
		Integer limit = qs.getLimit();
		Integer offset = qs.getOffset();
		if (limit > 0 && offset >= 0) {
			gt = gt.range(offset, offset + limit);
		} else if (limit > 0) {
			gt = gt.range(0, limit);
		} else {
		}
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
			this.propHash = new HashMap<String, List<String>>();
			for (String key : qs.selectors.keySet()) {
				List<String> val = qs.selectors.get(key);
				List<String> props = new Vector<String>();
				for (String select : val) {
					if (!select.equals("PRIM_KEY_PLACEHOLDER")) {
						selector.add(select);
						props.add(select);
					} else {
						selector.add(key);
					}
				}
				if (props.size() > 0) {
					propHash.put(key, props);
				}

			}
		}
		return this.selector;
	}

	/**
	 * Create the edge hash and start traversal
	 */
	protected void addJoins() {
		if (edgeHash == null || edgeHash.isEmpty()) {
			edgeHash = generateEdgeMap();
		}
		addNodeEdge();
	}

	/**
	 * This is the bulk of the class Uses the edgeMap to figure out what things
	 * are connected
	 * 
	 */
	public void addNodeEdge() {
		// no edges to traverse
		if (edgeHash.isEmpty()) {
			// initialize gt based on node or property
			if (selector.size() == 1) {
				gt = g.traversal().V();
				String select = selector.get(0);
				String selectNode = qs.selectors.keySet().iterator().next();
				// property
				GraphTraversal twoStepT = __.as(selectNode);

				// filterNode
				if (this.filters.containsKey(select)) {
					addFilterInPath2(twoStepT, select, this.filters.get(select));
				}
				// Get properties from startName node
				if (!selectNode.equals(select)) {
					List<GraphTraversal<Object, Object>> propTraversals = getProperties(twoStepT, selectNode);

					if (propTraversals.size() > 0) {
						GraphTraversal[] propArray = new GraphTraversal[propTraversals.size()];
						twoStepT = twoStepT.match(propTraversals.toArray(propArray)).select(selectNode);
					}
					gt = gt.match(twoStepT);
				} else {
					gt = gt.has(TinkerFrame.TINKER_TYPE, selectNode).as(selectNode);
					if (this.filters.containsKey(select)) {
						addFilterInPath2(gt, select, this.filters.get(select));
					}
				}
			}
			return;
		}

		// start traversal if edgeHash is not empty
		String startNode = edgeHash.keySet().iterator().next();
		gt = gt.has(TinkerFrame.TINKER_TYPE, startNode);

		if (this.filters.containsKey(startNode)) {
			addFilterInPath2(gt, startNode, this.filters.get(startNode));
		}

		List<String> travelledEdges = new Vector<String>();
		List<String> travelledNodeProperties = new Vector<String>();
		List<GraphTraversal<Object, Object>> traversals = new Vector<GraphTraversal<Object, Object>>();

		// add the logic to traverse
		traversals = visitNode(startNode, travelledEdges, travelledNodeProperties, traversals);
		if (traversals.size() > 0) {
			GraphTraversal[] array = new GraphTraversal[traversals.size()];
			gt = gt.match(traversals.toArray(array));

			// TODO get properties for final node

		} else {
			// get the traversal and store the necessary info
			GraphTraversal twoStepT = __.as(startNode);

			List<GraphTraversal<Object, Object>> propTraversals = getProperties(twoStepT, startNode);

			if (propTraversals.size() > 0) {
				GraphTraversal[] propArray = new GraphTraversal[propTraversals.size()];
				twoStepT = twoStepT.match(propTraversals.toArray(propArray)).select(startNode);
				gt = gt.match(twoStepT);

			}

		}

	}

	private List<GraphTraversal<Object, Object>> visitNode(String startName, List<String> travelledEdges,
			List<String> travelledNodeProps, List<GraphTraversal<Object, Object>> traversals) {
		// first see if there are downstream nodes
		if (edgeHash.containsKey(startName)) {
			Iterator<String> downstreamIt = edgeHash.get(startName).iterator();
			while (downstreamIt.hasNext()) {
				// for each downstream node of this node
				String downstreamNodeType = downstreamIt.next();

				String edgeKey = startName + TinkerFrame.EDGE_LABEL_DELIMETER + downstreamNodeType;
				if (!travelledEdges.contains(edgeKey)) {
					LOGGER.info("travelling from node = '" + startName + "' to node = '" + downstreamNodeType + "'");

					// get the traversal and store the necessary info
					GraphTraversal twoStepT = __.as(startName);

					// Get properties from startName node
					if (!travelledNodeProps.contains(startName)) {
						List<GraphTraversal<Object, Object>> propTraversals = getProperties(twoStepT, startName);

						if (propTraversals.size() > 0) {
							GraphTraversal[] propArray = new GraphTraversal[propTraversals.size()];
							twoStepT = twoStepT.match(propTraversals.toArray(propArray)).select(startName);
						}
					}

					twoStepT = twoStepT.out(edgeKey).has(TinkerFrame.TINKER_TYPE, downstreamNodeType)
							.as(downstreamNodeType);
					if (this.filters.containsKey(downstreamNodeType)) {
						addFilterInPath2(twoStepT, downstreamNodeType, this.filters.get(downstreamNodeType));
					}
					if (!travelledNodeProps.contains(downstreamNodeType)) {

						// get properties for the downstream node
						GraphTraversal downStepT = __.as(downstreamNodeType);

						// Get properties from downstream Node node
						List<GraphTraversal<Object, Object>> propTraversals = getProperties(downStepT,
								downstreamNodeType);

						if (propTraversals.size() > 0) {
							GraphTraversal[] propArray = new GraphTraversal[propTraversals.size()];
							twoStepT = twoStepT.match(propTraversals.toArray(propArray)).select(downstreamNodeType);
						}

						travelledNodeProps.add(downstreamNodeType);

					}

					traversals.add(twoStepT);
					travelledEdges.add(edgeKey);
					travelledNodeProps.add(startName);

					// recursively travel as far downstream as possible
					traversals = visitNode(downstreamNodeType, travelledEdges, travelledNodeProps, traversals);
				}
			}
		}

		// do the same thing for upstream
		// slightly more annoying to get upstream nodes...
		Set<String> upstreamNodes = getUpstreamNodes(startName, edgeHash);
		if (upstreamNodes != null && !upstreamNodes.isEmpty()) {
			Iterator<String> upstreamIt = upstreamNodes.iterator();
			while (upstreamIt.hasNext()) {
				String upstreamNodeType = upstreamIt.next();

				String edgeKey = upstreamNodeType + TinkerFrame.EDGE_LABEL_DELIMETER + startName;
				if (!travelledEdges.contains(edgeKey)) {
					LOGGER.info("travelling from node = '" + upstreamNodeType + "' to node = '" + startName + "'");

					// get the traversal and store the necessary info
					GraphTraversal twoStepT = __.as(startName);

					// Get properties from startName node
					if (!travelledNodeProps.contains(startName)) {
						List<GraphTraversal<Object, Object>> propTraversals = getProperties(twoStepT, startName);

						if (propTraversals.size() > 0) {
							GraphTraversal[] propArray = new GraphTraversal[propTraversals.size()];
							twoStepT = twoStepT.match(propTraversals.toArray(propArray)).select(startName);
						}
					}
					twoStepT = twoStepT.in(edgeKey).has(TinkerFrame.TINKER_TYPE, upstreamNodeType).as(upstreamNodeType);

					if (this.filters.containsKey(upstreamNodeType)) {
						addFilterInPath2(twoStepT, upstreamNodeType, this.filters.get(upstreamNodeType));
					}

					if (!travelledNodeProps.contains(upstreamNodeType)) {

						// get properties for the upstream node
						GraphTraversal upStepT = __.as(upstreamNodeType);

						
						List<GraphTraversal<Object, Object>> propTraversals = getProperties(upStepT,
								upstreamNodeType);

						if (propTraversals.size() > 0) {
							GraphTraversal[] propArray = new GraphTraversal[propTraversals.size()];
							twoStepT = twoStepT.match(propTraversals.toArray(propArray)).select(upstreamNodeType);
						}

						travelledNodeProps.add(upstreamNodeType);

					}

					traversals.add(twoStepT);
					travelledEdges.add(edgeKey);
					travelledNodeProps.add(startName);

					// recursively travel as far upstream as possible
					traversals = visitNode(upstreamNodeType, travelledEdges, travelledNodeProps, traversals);
				}
			}
		}

		return traversals;
	}

	// using filters to apply the queried properties to the nodes
	private List<GraphTraversal<Object, Object>> getProperties(GraphTraversal twoStepT, String startName) {
		List<GraphTraversal<Object, Object>> propTraversals = new Vector<GraphTraversal<Object, Object>>();
		List<String> propTraversalSelect = new Vector<String>();
		// check if filter is in the node
		if (this.filters.containsKey(startName)) {
			addFilterInPath2(twoStepT, startName, this.filters.get(startName));
		}

		// iterate through nodes using propHash

		Vector<String> propList = (Vector<String>) propHash.get(startName);
		if(propList != null)
		for (String property : propList) { // iterate through properties

			// define the match traversal
			GraphTraversal matchTraversal = __.as(startName);
			String qsProperty = startName + "__" + property;
			if (this.filters.containsKey(qsProperty)) {
				// we impose the filter on the node and then return the
				// property value
				Map<String, List> comparatorMap = this.filters.get(qsProperty);
				for (String comparison : comparatorMap.keySet()) {
					comparison = comparison.trim();
					List values = comparatorMap.get(comparison);
					if (comparison.equals("=")) {
						if (values.size() == 1) {
							matchTraversal = matchTraversal.has(property, P.eq(values.get(0))).values(property)
									.as(property);
						} else {
							matchTraversal = matchTraversal.has(property, P.within(values.toArray())).values(property)
									.as(property);
						}
					} else if (comparison.equals("!=")) {
						if (values.size() == 1) {
							matchTraversal = matchTraversal.has(property, P.neq(values.get(0))).values(property)
									.as(property);
						} else {
							matchTraversal = matchTraversal.has(property, P.without(values.toArray())).values(property)
									.as(property);
						}
					} else if (comparison.equals("<")) {
						matchTraversal = matchTraversal.has(property, P.lt(values.get(0))).values(property)
								.as(property);
					} else if (comparison.equals(">")) {
						matchTraversal = matchTraversal.has(property, P.gt(values.get(0))).values(property)
								.as(property);
					} else if (comparison.equals("<=")) {
						matchTraversal = matchTraversal.has(property, P.lte(values.get(0))).values(property)
								.as(property);
					} else if (comparison.equals(">=")) {
						matchTraversal = matchTraversal.has(property, P.gte(values.get(0))).values(property)
								.as(property);
					}
				}
			} else {
				matchTraversal = matchTraversal.values(property).as(property);
			}

			propTraversals.add(matchTraversal);
			propTraversalSelect.add(property);

		}

		return propTraversals;

	}

	/**
	 * Get the upstream nodes for a given downstream node
	 * 
	 * @param downstreamNodeToFind
	 * @param edgeHash
	 * @return
	 */
	private Set<String> getUpstreamNodes(String downstreamNodeToFind, Map<String, Set<String>> edgeHash) {
		Set<String> upstreamNodes = new HashSet<String>();
		for (String possibleUpstreamNode : edgeHash.keySet()) {
			Set<String> downstreamNodes = edgeHash.get(possibleUpstreamNode);
			if (downstreamNodes.contains(downstreamNodeToFind)) {
				// the node we want to find is listed as downstream
				upstreamNodes.add(possibleUpstreamNode);
			}
		}

		return upstreamNodes;
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
				if (propHash.containsKey(s)) {
					edgeMap.put(s, new HashSet<String>());
				}
			}
		}

		return edgeMap;
	}

	public void addFilterInPath2(GraphTraversal<Object, Object> gt, String nameType, Map<String, List> filterInfo) {
		// TODO: right now, if its a math, assumption that vector only contains
		// one value
		for (String filterType : filterInfo.keySet()) {
			List filterVals = filterInfo.get(filterType);
			if (filterType.equals("=")) {
				gt = gt.has(TinkerFrame.TINKER_NAME, P.within(filterVals.toArray()));
			} else if (filterType.equals("<")) {
				gt = gt.has(TinkerFrame.TINKER_NAME, P.lt(filterVals.get(0)));
			} else if (filterType.equals(">")) {
				gt = gt.has(TinkerFrame.TINKER_NAME, P.gt(filterVals.get(0)));
			} else if (filterType.equals("<=")) {
				gt = gt.has(TinkerFrame.TINKER_NAME, P.lte(filterVals.get(0)));
			} else if (filterType.equals(">=")) {
				gt = gt.has(TinkerFrame.TINKER_NAME, P.gte(filterVals.get(0)));
			} else if (filterType.equals("!=")) {
				gt = gt.has(TinkerFrame.TINKER_NAME, P.without(filterVals.toArray()));
			}
		}
	}

	@Override
	public void setPerformCount(int performCount) {

	}

	@Override
	public int isPerformCount() {
		return QueryStruct.NO_COUNT;
	}

	@Override
	public void clear() {

	}

}
