package prerna.query.interpreters;

import java.util.ArrayList;
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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;

import prerna.ds.TinkerFrame;
import prerna.sablecc2.om.Filter2;
import prerna.sablecc2.om.NounMetadata;

public class GremlinInterpreter2 extends AbstractQueryInterpreter {

	private static final Logger LOGGER = LogManager.getLogger(GremlinInterpreter2.class.getName());

	// reference to the graph which can execute gremlin
	private Graph g;
	// the gremlin traversal being created
	private GraphTraversal gt;
	// the list of variables being returned within the traversal
	private List<String> selectors;
	// the list of properties for a given vertex
	private Map<String, List<String>> propHash;
	// the alias that is assigned each vertex/property
	private Map<String, String> aliasMap;

	// for filtering
	private Map<String, Map<String, List<Object>>> filterColToValues;
	private Map<String, Map<String, List<String>>> filterColToCol;
	
	public GremlinInterpreter2(Graph g) {
		this.g = g;
		this.gt = g.traversal().V();
	}

	public Iterator composeIterator() {
		generateSelectors();
		processFilters();
		traverseRelations();
		return null;
	}

	/**
	 * Get the selectors from the QS
	 * Store both the list of selectors (which includes names of vertices and properties)
	 * Also maintain a list of vertex to list of properties
	 */
	private void generateSelectors() {
		if (this.selectors == null) {
			// generate the names for each component of the selector
			this.selectors = new Vector<String>();
			this.propHash = new HashMap<String, List<String>>();
			this.aliasMap = new HashMap<String, String>();

			// iterate through the selectors
			List<QueryStructSelector> selectorComps = qs.selectors;
			for (QueryStructSelector selectorComp : selectorComps) {
				String table = selectorComp.getTable();
				String column = selectorComp.getColumn();

				// are we trying to grab a vertex
				// or are we grabbing a property on the vertex
				if(column.equals(QueryStruct2.PRIM_KEY_PLACEHOLDER)) {
					// add the vertex
					this.selectors.add(table);
				} else {
					// add the property
					this.selectors.add(column);
					// also store the property in the prop hash
					List<String> properties = null;
					if(this.propHash.containsKey(table)) {
						properties = this.propHash.get(table);
					} else {
						properties = new ArrayList<String>();
						this.propHash.put(table, properties);
					}
					properties.add(column);
				}

				String alias = selectorComp.getAlias();
				if(alias != null) {
					//TODO: how do i best store this???
				}
			}
		}
	}

	/**
	 * We need to store the filters that are required
	 * I wish we could do this while iterating through
	 * But with the new filters, it is difficult to determine where within the iterator we should add them
	 * ... this issue doesn't arise for other querying languages...
	 */
	private void processFilters() {
		List<Filter2> filters = qs.filters.getFilters();
		for(Filter2 filter : filters) {
			Filter2.FILTER_TYPE filterType = Filter2.determineFilterType(filter);
			NounMetadata lComp = filter.getLComparison();
			NounMetadata rComp = filter.getRComparison();
			String comp = filter.getComparator();
			
			if(filterType == Filter2.FILTER_TYPE.COL_TO_VALUES) {
				// here, lcomp is the column and rComp is a set of values
				processFilterColToValues(lComp, rComp, comp);
			} else if(filterType == Filter2.FILTER_TYPE.VALUES_TO_COL) {
				// here, lcomp is the values and rComp is a the column
				// so same as above, but switch the order
				processFilterColToValues(rComp, lComp, comp);
			}
		}
	}

	/**
	 * Handle adding a column to set of values filter
	 * @param colComp
	 * @param valuesComp
	 * @param comparison
	 */
	private void processFilterColToValues(NounMetadata colComp, NounMetadata valuesComp, String comparison) {
		
	}
	
	private void traverseRelations() {
		Map<String, Set<String>> edgeMap = generateEdgeMap();
		if(edgeMap.isEmpty()) {
			// we have only a single selector
			// simple traversal
			String selector = this.selectors.get(0);
			List<String> props = this.propHash.get(selector);
			// but is this traversal, to get a vertex
			// or a property on the vertex
			if(props != null) {
				// it is for a property on a vertex
				GraphTraversal twoStepT = __.as(selector);

				// TODO: add logic for filtering
				// TODO: add logic for filtering
				// TODO: add logic for filtering

				List<GraphTraversal<Object, Object>> propTraversals = getProperties(twoStepT, selector);
				if (propTraversals.size() > 0) {
					GraphTraversal[] propArray = new GraphTraversal[propTraversals.size()];
					twoStepT = twoStepT.match(propTraversals.toArray(propArray));
				}
				gt = gt.match(twoStepT);
			} else {
				// it is just the vertex
				this.gt.has(TinkerFrame.TINKER_TYPE, selector).as(selector);
				
				// TODO: add logic for filtering
				// TODO: add logic for filtering
				// TODO: add logic for filtering
			}
		} else {
			// we need to go through the traversal
			addNodeEdge(edgeMap);
		}
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
				// TODO: currently going to not care about the type of join
				// assume everything is an inner join for simplicity
				Set<String> compKeys = comps.keySet();
				for (String comp : compKeys) {
					// this is the end node of the relationship
					List<String> endNodes = comps.get(comp);

					Set<String> joinSet = new HashSet<String>();
					for (String node : endNodes) {
						joinSet.add(node);
					}

					if (edgeMap.containsKey(startNode)) {
						Set<String> currSet = edgeMap.get(startNode);
						currSet.addAll(joinSet);
						edgeMap.put(startNode, joinSet);
					} else {
						// need to get rid of "__"
						edgeMap.put(startNode, joinSet);
					}
				}
			}
		}
		//		else {
		//			// this occurs when there are no relationships defined...
		//			// example is when you are only going for one column of data
		//		}

		return edgeMap;
	}

	/**
	 * This is the bulk of the class Uses the edgeMap to figure out what things
	 * are connected
	 */
	public void addNodeEdge(Map<String, Set<String>> edgeMap) {
		// start traversal if edgeHash is not empty
		String startNode = edgeMap.keySet().iterator().next();
		
		// TODO: come back to this to optimize the traversal
		// can do this by picking a "better" startNode
		this.gt = this.gt.has(TinkerFrame.TINKER_TYPE, startNode);

//		if (this.filters.containsKey(startNode)) {
//			addFilterInPath2(gt, startNode, this.filters.get(startNode));
//		}

		List<String> travelledEdges = new Vector<String>();
		List<String> travelledNodeProperties = new Vector<String>();
		List<GraphTraversal<Object, Object>> traversals = new Vector<GraphTraversal<Object, Object>>();

		// add the logic to traverse
		traversals = visitNode(startNode, edgeMap, travelledEdges, travelledNodeProperties, traversals);
		if (traversals.size() > 0) {
			GraphTraversal[] array = new GraphTraversal[traversals.size()];
			gt = gt.match(traversals.toArray(array));
		}
	}

	/**
	 * The main method to traversal the graph relationships
	 * @param startName
	 * @param edgeMap
	 * @param travelledEdges
	 * @param travelledNodeProps
	 * @param traversals
	 * @return
	 */
	private List<GraphTraversal<Object, Object>> visitNode(
			String startName,
			Map<String, Set<String>> edgeMap,
			List<String> travelledEdges,
			List<String> travelledNodeProps, 
			List<GraphTraversal<Object, Object>> traversals) 
	{
		// TODO: should automatically add any properties that are required for the passed in node
		// instead of only doing it is the node has an upstream/downstream
		
			
		// first see if there are downstream nodes
		if (edgeMap.containsKey(startName)) {
			Iterator<String> downstreamIt = edgeMap.get(startName).iterator();
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

					twoStepT = twoStepT.out(edgeKey).has(TinkerFrame.TINKER_TYPE, downstreamNodeType).as(downstreamNodeType);
//					if (this.filters.containsKey(downstreamNodeType)) {
//						addFilterInPath2(twoStepT, downstreamNodeType, this.filters.get(downstreamNodeType));
//					}
					if (!travelledNodeProps.contains(downstreamNodeType)) {

						// get properties for the downstream node
						GraphTraversal downStepT = __.as(downstreamNodeType);

						// Get properties from downstream Node node
						List<GraphTraversal<Object, Object>> propTraversals = getProperties(downStepT, downstreamNodeType);

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
					traversals = visitNode(downstreamNodeType, edgeMap, travelledEdges, travelledNodeProps, traversals);
				}
			}
		}

		// do the same thing for upstream
		// slightly more annoying to get upstream nodes...
		Set<String> upstreamNodes = getUpstreamNodes(startName, edgeMap);
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

//					if (this.filters.containsKey(upstreamNodeType)) {
//						addFilterInPath2(twoStepT, upstreamNodeType, this.filters.get(upstreamNodeType));
//					}

					if (!travelledNodeProps.contains(upstreamNodeType)) {

						// get properties for the upstream node
						GraphTraversal upStepT = __.as(upstreamNodeType);

						List<GraphTraversal<Object, Object>> propTraversals = getProperties(upStepT, upstreamNodeType);

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
					traversals = visitNode(upstreamNodeType, edgeMap, travelledEdges, travelledNodeProps, traversals);
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
//		if (this.filters.containsKey(startName)) {
//			addFilterInPath2(twoStepT, startName, this.filters.get(startName));
//		}

		// iterate through nodes using propHash

		Vector<String> propList = (Vector<String>) propHash.get(startName);
		if (propList != null)
			for (String property : propList) { // iterate through properties

				// define the match traversal
				GraphTraversal matchTraversal = __.as(startName);
				String qsProperty = startName + "__" + property;
//				if (this.filters.containsKey(qsProperty)) {
//					// we impose the filter on the node and then return the
//					// property value
//					Map<String, List> comparatorMap = this.filters.get(qsProperty);
//					for (String comparison : comparatorMap.keySet()) {
//						comparison = comparison.trim();
//						List values = comparatorMap.get(comparison);
//						if (comparison.equals("=")) {
//							if (values.size() == 1) {
//								matchTraversal = matchTraversal.has(property, P.eq(values.get(0))).values(property).as(property);
//							} else {
//								matchTraversal = matchTraversal.has(property, P.within(values.toArray())).values(property).as(property);
//							}
//						} else if (comparison.equals("!=")) {
//							if (values.size() == 1) {
//								matchTraversal = matchTraversal.has(property, P.neq(values.get(0))).values(property).as(property);
//							} else {
//								matchTraversal = matchTraversal.has(property, P.without(values.toArray())).values(property).as(property);
//							}
//						} else if (comparison.equals("<")) {
//							matchTraversal = matchTraversal.has(property, P.lt(values.get(0))).values(property).as(property);
//						} else if (comparison.equals(">")) {
//							matchTraversal = matchTraversal.has(property, P.gt(values.get(0))).values(property).as(property);
//						} else if (comparison.equals("<=")) {
//							matchTraversal = matchTraversal.has(property, P.lte(values.get(0))).values(property).as(property);
//						} else if (comparison.equals(">=")) {
//							matchTraversal = matchTraversal.has(property, P.gte(values.get(0))).values(property).as(property);
//						}
//					}
//				} else {
					matchTraversal = matchTraversal.values(property).as(property);
//				}

				propTraversals.add(matchTraversal);
				propTraversalSelect.add(property);

			}

		return propTraversals;
	}

	/**
	 * Get the upstream nodes for a given downstream node
	 * @param downstreamNodeToFind
	 * @param edgeMap
	 * @return
	 */
	private Set<String> getUpstreamNodes(String downstreamNodeToFind, Map<String, Set<String>> edgeMap) {
		Set<String> upstreamNodes = new HashSet<String>();
		for (String possibleUpstreamNode : edgeMap.keySet()) {
			Set<String> downstreamNodes = edgeMap.get(possibleUpstreamNode);
			if (downstreamNodes.contains(downstreamNodeToFind)) {
				// the node we want to find is listed as downstream
				upstreamNodes.add(possibleUpstreamNode);
			}
		}

		return upstreamNodes;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub

	}

	@Override
	public String composeQuery() {
		return null;
	}
}
