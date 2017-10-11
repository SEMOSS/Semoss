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

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;

import prerna.ds.TinkerFrame;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector.ORDER_BY_DIRECTION;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.QueryFilter;

public class GremlinInterpreter2 extends AbstractQueryInterpreter {

	// reference to the graph which can execute gremlin
	private Graph g;
	// the gremlin traversal being created
	private GraphTraversal gt;
	// the list of variables being returned within the traversal
	private List<String> selectors;
	// the list of properties for a given vertex
	private Map<String, List<String>> propHash;
	// store the unique name to the 

	public GremlinInterpreter2(Graph g) {
		this.g = g;
		this.gt = g.traversal().V();
	}

	public Iterator composeIterator() {
		generateSelectors();
		traverseRelations();
		setSelectors();
		addOrderBy();
		return this.gt;
	}

	private void setSelectors() {
		// note, we add all things in the alias map even if they are not returned
		// i.e. remember we can skip intermediary nodes
		List<String> allAliasSelectors = new Vector<String>();
		for(String nodeSelector : this.selectors) {
			allAliasSelectors.add(nodeSelector);
		}
		for(String conceptKey : this.propHash.keySet()) {
			List<String> props = this.propHash.get(conceptKey);
			for(String propertySelector : props) {
				allAliasSelectors.add(propertySelector);
			}
		}

		if(allAliasSelectors.size() == 1) {
			this.gt = this.gt.select(allAliasSelectors.get(0));
		} else if(allAliasSelectors.size() == 2) {
			this.gt = this.gt.select(allAliasSelectors.get(0), allAliasSelectors.get(1));
		} else {
			String[] otherSelectors = new String[allAliasSelectors.size() - 2];
			for(int i = 2; i < allAliasSelectors.size(); i++) {
				otherSelectors[i-2] = allAliasSelectors.get(i);
			}
			this.gt = this.gt.select(allAliasSelectors.get(0), allAliasSelectors.get(1), otherSelectors);
		}

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

			Set<QueryColumnSelector> selectorComps = new HashSet<QueryColumnSelector>();
			List<IQuerySelector> iSelectors = qs.getSelectors();
			for(IQuerySelector s : iSelectors) {
				selectorComps.addAll(s.getAllQueryColumns());
			}

			// iterate through the selectors
			for (QueryColumnSelector selectorComp : selectorComps) {
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
			}
		}
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

				// logic to filter
				List<QueryFilter> startNodeFilters = this.qs.getFilters().getAllQueryFiltersContainingColumn(selector);
				addFiltersToPath(twoStepT, startNodeFilters);

				List<GraphTraversal<Object, Object>> propTraversals = getProperties(twoStepT, selector);
				if (propTraversals.size() > 0) {
					GraphTraversal[] propArray = new GraphTraversal[propTraversals.size()];
					twoStepT = twoStepT.match(propTraversals.toArray(propArray));
				}
				gt = gt.match(twoStepT);
			} else {
				// it is just the vertex
				this.gt.has(TinkerFrame.TINKER_TYPE, selector).as(selector);
				// logic to filter
				List<QueryFilter> startNodeFilters = this.qs.getFilters().getAllQueryFiltersContainingColumn(selector);
				addFiltersToPath(this.gt, startNodeFilters);
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
		Map<String, Map<String, List>> rels = qs.getRelations();
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
						// we may be using joins and not outputting the values
						// so we will add a fake alias so we dont need to check if alias exists 
						// when we traverse the map
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
		this.gt = this.gt.has(TinkerFrame.TINKER_TYPE, startNode).as(startNode);
		List<QueryFilter> startNodeFilters = this.qs.getFilters().getAllQueryFiltersContainingColumn(startNode);
		addFiltersToPath(this.gt, startNodeFilters);

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
	 * Add the filter object to the current graph traversal
	 * @param filterVec
	 */
	private void addFiltersToPath(GraphTraversal traversalSegment, List<QueryFilter> filterVec) {
		for(QueryFilter filter : filterVec) {
			QueryFilter.FILTER_TYPE filterType = QueryFilter.determineFilterType(filter);
			NounMetadata lComp = filter.getLComparison();
			NounMetadata rComp = filter.getRComparison();
			String comp = filter.getComparator();

			if(filterType == QueryFilter.FILTER_TYPE.COL_TO_VALUES) {
				// here, lcomp is the column and rComp is a set of values
				processFilterColToValues(traversalSegment, lComp, rComp, comp);
			} else if(filterType == QueryFilter.FILTER_TYPE.VALUES_TO_COL) {
				// here, lcomp is the values and rComp is a the column
				// so same as above, but switch the order
				processFilterColToValues(traversalSegment, rComp, lComp, QueryFilter.getReverseNumericalComparator(comp));
			}
		}
	}

	/**
	 * Handle adding a column to set of values filter
	 * @param traversalSegment 
	 * @param colComp
	 * @param valuesComp
	 * @param comparison
	 */
	private void processFilterColToValues(GraphTraversal traversalSegment, NounMetadata colComp, NounMetadata valuesComp, String comparison) {
		Object filterObject = valuesComp.getValue();
		List<Object> filterValues = new Vector<Object>();
		// ughhh... this could be a list or an object
		// need to make this consistent!
		if(filterObject instanceof List) {
			filterValues.addAll(((List) filterObject));
		} else {
			filterValues.add(filterObject);
		}
		if (comparison.equals("==")) {
			traversalSegment = traversalSegment.has(TinkerFrame.TINKER_NAME, P.within(filterValues.toArray()));
		} else if (comparison.equals("<")) {
			traversalSegment = traversalSegment.has(TinkerFrame.TINKER_NAME, P.lt(filterValues.get(0)));
		} else if (comparison.equals(">")) {
			traversalSegment = traversalSegment.has(TinkerFrame.TINKER_NAME, P.gt(filterValues.get(0)));
		} else if (comparison.equals("<=")) {
			traversalSegment = traversalSegment.has(TinkerFrame.TINKER_NAME, P.lte(filterValues.get(0)));
		} else if (comparison.equals(">=")) {
			traversalSegment = traversalSegment.has(TinkerFrame.TINKER_NAME, P.gte(filterValues.get(0)));
		} else if (comparison.equals("!=")) {
			traversalSegment = traversalSegment.has(TinkerFrame.TINKER_NAME, P.without(filterValues.toArray()));
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
					logger.info("travelling from node = '" + startName + "' to node = '" + downstreamNodeType + "'");

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
					// add filters
					List<QueryFilter> nodeFilters = this.qs.getFilters().getAllQueryFiltersContainingColumn(downstreamNodeType);
					addFiltersToPath(twoStepT, nodeFilters);

					// add properties if present
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
					logger.info("travelling from node = '" + upstreamNodeType + "' to node = '" + startName + "'");

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

					// add filtering
					List<QueryFilter> nodeFilters = this.qs.getFilters().getAllQueryFiltersContainingColumn(upstreamNodeType);
					addFiltersToPath(twoStepT, nodeFilters);

					// add properties if present
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
		// iterate through nodes using propHash
		List<String> propList = (List<String>) propHash.get(startName);
		if (propList != null) {
			for (String property : propList) { // iterate through properties
				// define the match traversal
				GraphTraversal matchTraversal = __.as(startName);

				// logic to filter
				String qsProperty = startName + "__" + property;
				List<QueryFilter> propFilters = this.qs.getFilters().getAllQueryFiltersContainingColumn(qsProperty);
				addFiltersToPath(twoStepT, propFilters);

				// after we add the filter, grab the actual values to return from the traversal
				matchTraversal = matchTraversal.values(property).as(qsProperty);
				propTraversals.add(matchTraversal);
				propTraversalSelect.add(property);
			}
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

	private void addOrderBy() {
		List<QueryColumnOrderBySelector> orderBy = qs.getOrderBy();
		int numOrderBys = orderBy.size();
		for(int i = 0; i < numOrderBys; i++) {
			QueryColumnOrderBySelector orderSelector = orderBy.get(i);
			String tableName = orderSelector.getTable();
			String columnName = orderSelector.getColumn();
			ORDER_BY_DIRECTION sortDirection = orderSelector.getSortDir();
			//order by for vector
			if (columnName.contains("PRIM_KEY_PLACEHOLDER")) {
				if(sortDirection == ORDER_BY_DIRECTION.ASC) {
					gt = gt.select(tableName).order().by(TinkerFrame.TINKER_NAME, Order.incr).as(tableName);
				} else {
					gt = gt.select(tableName).order().by(TinkerFrame.TINKER_NAME, Order.decr).as(tableName);
				}
			}
			//order by for property
			else {
				String property = tableName + "__" + columnName;
				if(sortDirection == ORDER_BY_DIRECTION.ASC) {
					gt = gt.select(tableName).order().by(property, Order.incr).as(property);
				} else {
					gt = gt.select(tableName).order().by(property, Order.decr).as(property);
				}
			}
		}
	}

	@Override
	public String composeQuery() {
		return null;
	}
}