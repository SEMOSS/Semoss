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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.TinkerFrame;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector.ORDER_BY_DIRECTION;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GremlinInterpreter extends AbstractQueryInterpreter {

	// reference for getting actual names for loops within frames
	protected OwlTemporalEngineMeta meta;
	// all the filters being used
	protected GenRowFilters allFilters;
	
	// the gremlin traversal being created
	protected GraphTraversalSource g;
	protected GraphTraversal gt;
	// the list of variables being returned within the traversal
	protected List<String> selectors;
	// the list of properties for a given vertex
	protected Map<String, List<String>> propHash;
	// identify the name for the type of the name
	protected Map<String, String> typeMap;
	// identify the name for the vertex label
	protected Map<String, String> nameMap;
		
	public GremlinInterpreter(GraphTraversalSource g, Map<String, String> typeMap, Map<String, String> nameMap) {
		this.g = g;
		this.gt = g.V();
		this.typeMap = typeMap;
		this.nameMap = nameMap;
	}
	
	public GremlinInterpreter(GraphTraversalSource g, OwlTemporalEngineMeta meta) {
		this.g = g;
		this.gt = g.V();
		this.meta = meta;
	}

	public GraphTraversal composeIterator() {
		this.allFilters = this.qs.getCombinedFilters();
		generateSelectors();
		traverseRelations();
		addOrderBy();
		setSelectors();
		if(((SelectQueryStruct) this.qs).isDistinct()) {
			this.gt.dedup();
		}
		
		String query = this.gt.toString();
		if(query.length() > 500) {
			logger.debug("GREMLIN QUERY....  " + query.substring(0,  500) + "...");
		} else {
			logger.debug("GREMLIN QUERY....  " + query);
		}
		return this.gt;
	}

	protected void setSelectors() {
		// note, we add all things in the alias map even if they are not returned
		// i.e. remember we can skip intermediary nodes
		List<String> allAliasSelectors = new Vector<String>();
		for(String nodeSelector : this.selectors) {
			allAliasSelectors.add(nodeSelector);
		}
		for(String conceptKey : this.propHash.keySet()) {
			List<String> props = this.propHash.get(conceptKey);
			for(String propertySelector : props) {
				allAliasSelectors.add(conceptKey + "__" + propertySelector);
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
	protected void generateSelectors() {
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
				if(column.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
					// add the vertex
					this.selectors.add(table);
				} else {
					// add the property
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

	protected void traverseRelations() {
		Map<String, Set<String>> edgeMap = generateEdgeMap();
		if(edgeMap.isEmpty()) {
			// we have only a single selector
			// simple traversal
			String selector = null;
			if(this.selectors.isEmpty()) {
				selector = this.propHash.keySet().iterator().next();
			} else {
				selector = this.selectors.get(0);
			}
			List<String> props = this.propHash.get(selector);
			// but is this traversal, to get a vertex
			// or a property on the vertex
			if(props != null) {
//				this.gt.has(getNodeType(selector), getPhysicalNodeType(selector)).as(selector);

				// it is for a property on a vertex
				GraphTraversal twoStepT = __.as(selector);

				// logic to filter
				List<SimpleQueryFilter> startNodeFilters = this.allFilters.getAllSimpleQueryFiltersContainingColumn(selector);
				addFiltersToPath(twoStepT, startNodeFilters, getNodeName(selector));

				List<GraphTraversal<Object, Object>> propTraversals = getProperties(twoStepT, selector);
				if (propTraversals.size() > 0) {
					GraphTraversal[] propArray = new GraphTraversal[propTraversals.size()];
					twoStepT = twoStepT.match(propTraversals.toArray(propArray));
				}
				gt = gt.match(twoStepT);
			} else {
				// it is just the vertex
				this.gt.has(getNodeType(selector), getPhysicalNodeType(selector)).as(selector);
				// logic to filter
				List<SimpleQueryFilter> startNodeFilters = this.allFilters.getAllSimpleQueryFiltersContainingColumn(selector);
				addFiltersToPath(this.gt, startNodeFilters, getNodeName(selector));
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
		// add the relationships into the edge map
		Set<String[]> relations = qs.getRelations();
		for(String[] rel : relations) {
			String startNode = rel[0];
			String endNode = rel[2];
			
			Set<String> joinSet = null;
			if(edgeMap.containsKey(startNode)) {
				joinSet = edgeMap.get(startNode);
			} else {
				joinSet = new HashSet<String>();
				edgeMap.put(startNode, joinSet);
			}
			joinSet.add(endNode);
		}
//		Map<String, Map<String, List>> rels = qs.getRelations();
//		// add the relationships into the edge map
//		if (!rels.isEmpty()) {
//			Set<String> relKeys = rels.keySet();
//			// looping through the start node of the relationship
//			for (String startNode : relKeys) {
//				Map<String, List> comps = rels.get(startNode);
//				// TODO: currently going to not care about the type of join
//				// assume everything is an inner join for simplicity
//				Set<String> compKeys = comps.keySet();
//				for (String comp : compKeys) {
//					// this is the end node of the relationship
//					List<String> endNodes = comps.get(comp);
//
//					Set<String> joinSet = new HashSet<String>();
//					for (String node : endNodes) {
//						// we may be using joins and not outputting the values
//						// so we will add a fake alias so we dont need to check if alias exists 
//						// when we traverse the map
//						joinSet.add(node);
//					}
//
//					if (edgeMap.containsKey(startNode)) {
//						Set<String> currSet = edgeMap.get(startNode);
//						currSet.addAll(joinSet);
//					} else {
//						// need to get rid of "__"
//						edgeMap.put(startNode, joinSet);
//					}
//				}
//			}
//		}
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
		this.gt = this.gt.has(getNodeType(startNode), getPhysicalNodeType(startNode)).as(startNode);
		List<SimpleQueryFilter> startNodeFilters = this.allFilters.getAllSimpleQueryFiltersContainingColumn(startNode);
		addFiltersToPath(this.gt, startNodeFilters, getNodeName(startNode));

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
	protected void addFiltersToPath(GraphTraversal traversalSegment, List<SimpleQueryFilter> filterVec, String filterPropertyName) {
		for(SimpleQueryFilter filter : filterVec) {
			SimpleQueryFilter.FILTER_TYPE filterType = filter.getFilterType();
			NounMetadata lComp = filter.getLComparison();
			NounMetadata rComp = filter.getRComparison();
			String comp = filter.getComparator();

			if(filterType == SimpleQueryFilter.FILTER_TYPE.COL_TO_VALUES) {
				// here, lcomp is the column and rComp is a set of values
				processFilterColToValues(traversalSegment, lComp, rComp, comp, filterPropertyName);
			} else if(filterType == SimpleQueryFilter.FILTER_TYPE.VALUES_TO_COL) {
				// here, lcomp is the values and rComp is a the column
				// so same as above, but switch the order
				processFilterColToValues(traversalSegment, rComp, lComp, IQueryFilter.getReverseNumericalComparator(comp), filterPropertyName);
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
	protected void processFilterColToValues(GraphTraversal traversalSegment, NounMetadata colComp, NounMetadata valuesComp, String comparison, String filterPropertyName) {
		PixelDataType dataType = valuesComp.getNounType();
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
			int size = filterValues.size();
			if(size == 1) {
				traversalSegment = traversalSegment.has(filterPropertyName, P.eq(filterValues.get(0)));
			} else {
				if(dataType == PixelDataType.CONST_STRING) {
					traversalSegment = traversalSegment.has(filterPropertyName, P.within(filterValues));
				} else {
					GraphTraversal[] ors = new GraphTraversal[size];
					for(int i = 0; i < size; i++) {
						ors[i] = __.has(filterPropertyName, P.eq(filterValues.get(i)));
					}
					traversalSegment = traversalSegment.or(ors);
				}
			}
		} else if (comparison.equals("<")) {
			traversalSegment = traversalSegment.has(filterPropertyName, P.lt(filterValues.get(0)));
		} else if (comparison.equals(">")) {
			traversalSegment = traversalSegment.has(filterPropertyName, P.gt(filterValues.get(0)));
		} else if (comparison.equals("<=")) {
			traversalSegment = traversalSegment.has(filterPropertyName, P.lte(filterValues.get(0)));
		} else if (comparison.equals(">=")) {
			traversalSegment = traversalSegment.has(filterPropertyName, P.gte(filterValues.get(0)));
		} else if (comparison.equals("!=")) {
			int size = filterValues.size();
			if(size == 1) {
				traversalSegment = traversalSegment.has(filterPropertyName, P.neq(filterValues.get(0)));
			} else {
				if(dataType == PixelDataType.CONST_STRING) {
					traversalSegment = traversalSegment.has(filterPropertyName, P.without(filterValues));
				} else {
					GraphTraversal[] ors = new GraphTraversal[size];
					for(int i = 0; i < size; i++) {
						ors[i] = __.has(filterPropertyName, P.neq(filterValues.get(i)));
					}
					traversalSegment = traversalSegment.or(ors);
				}
			}
		} else if(comparison.equals("?like")) {
			int size = filterValues.size();
			if(size == 1) {
				traversalSegment = traversalSegment.has(filterPropertyName, GremlinRegexMatch.regex(filterValues.get(0)));
			} else {
				GraphTraversal[] ors = new GraphTraversal[size];
				for(int i = 0; i < size; i++) {
					ors[i] = __.has(filterPropertyName, GremlinRegexMatch.regex(filterValues.get(i)));
				}
				traversalSegment = traversalSegment.or(ors);
			}
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
	protected List<GraphTraversal<Object, Object>> visitNode(
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
					logger.debug("travelling from node = '" + startName + "' to node = '" + downstreamNodeType + "'");

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

					twoStepT = twoStepT.out(edgeKey).has(getNodeType(downstreamNodeType), getPhysicalNodeType(downstreamNodeType)).as(downstreamNodeType);
					// add filters
					List<SimpleQueryFilter> nodeFilters = this.allFilters.getAllSimpleQueryFiltersContainingColumn(downstreamNodeType);
					addFiltersToPath(twoStepT, nodeFilters, getNodeName(downstreamNodeType));

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
					logger.debug("travelling from node = '" + upstreamNodeType + "' to node = '" + startName + "'");

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
					twoStepT = twoStepT.in(edgeKey).has(getNodeType(upstreamNodeType), getPhysicalNodeType(upstreamNodeType)).as(upstreamNodeType);

					// add filtering
					List<SimpleQueryFilter> nodeFilters = this.allFilters.getAllSimpleQueryFiltersContainingColumn(upstreamNodeType);
					addFiltersToPath(twoStepT, nodeFilters, getNodeName(upstreamNodeType));

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
	protected List<GraphTraversal<Object, Object>> getProperties(GraphTraversal twoStepT, String startName) {
		List<GraphTraversal<Object, Object>> propTraversals = new Vector<GraphTraversal<Object, Object>>();
		List<String> propTraversalSelect = new Vector<String>();
		// iterate through nodes using propHash
		List<String> propList = (List<String>) propHash.get(startName);
		if (propList != null) {
			for (String property : propList) { // iterate through properties
				// define the match traversal
				GraphTraversal matchTraversal = __.as(startName);

				// filter the property that we have just retrieved
				// it will be stored in the filter list based on the alias being the property
				List<SimpleQueryFilter> propFilters = this.allFilters.getAllSimpleQueryFiltersContainingColumn(property);
				addFiltersToPath(matchTraversal, propFilters, property);
				
				// grab the value from the startNode
				String qsProperty = startName + "__" + property;
				// after we add the filter, grab the actual values to return from the traversal
				matchTraversal = matchTraversal.values(property).as(qsProperty);
				
				// add it to the list
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
	protected Set<String> getUpstreamNodes(String downstreamNodeToFind, Map<String, Set<String>> edgeMap) {
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

	protected void addOrderBy() {
		List<QueryColumnOrderBySelector> orderBy = ((SelectQueryStruct) this.qs).getOrderBy();
		int numOrderBys = orderBy.size();
		for(int i = 0; i < numOrderBys; i++) {
			QueryColumnOrderBySelector orderSelector = orderBy.get(i);
			String tableName = orderSelector.getTable();
			String columnName = orderSelector.getColumn();
			ORDER_BY_DIRECTION sortDirection = orderSelector.getSortDir();
			//order by for vector
			if (columnName.contains("PRIM_KEY_PLACEHOLDER")) {
				if(this.selectors.contains(tableName)) {
					if(sortDirection == ORDER_BY_DIRECTION.ASC) {
						gt = gt.select(tableName).order().by(getNodeName(tableName), Order.incr);
					} else {
						gt = gt.select(tableName).order().by(getNodeName(tableName), Order.decr);
					}
				}
			}
			//order by for property
			else {
				if(sortDirection == ORDER_BY_DIRECTION.ASC) {
					gt = gt.select(tableName).order().by(columnName, Order.incr);
				} else {
					gt = gt.select(tableName).order().by(columnName, Order.decr);
				}
			}
		}
	}
	
	//////////////////////////////////////////////////////////
	
	/*
	 * Getters for the type and name of the node
	 * Default will be what we create on upload of tinker / frames of tinker
	 * 
	 * If engine has defined a specific value, we will use that
	 */
	
	protected String getNodeType(String node) {
		if(this.typeMap != null) {
			if(this.typeMap.containsKey(node)) {
				return this.typeMap.get(node);
			}
		}
		return TinkerFrame.TINKER_TYPE;
	}
	
	protected String getNodeName(String node) {
		if(this.nameMap != null) {
			if(this.nameMap.containsKey(node)) {
				return this.nameMap.get(node);
			}
		}
		return TinkerFrame.TINKER_NAME;
	}
	
	/**
	 * Method to get the actual type of the node
	 * This is important for loops when we want the node to not be doubled
	 * But need a way to distinguish on the FE
	 * @param node
	 * @return
	 */
	protected String getPhysicalNodeType(String node) {
		if(this.meta == null) {
			return node;
		}
		return this.meta.getPhysicalName(node);
	}

	@Override
	public String composeQuery() {
		return null;
	}
	
	//////////////////////////////////////////////////////////
	
	/**
	 * Get the names of the nodes
	 * @return
	 */
	public Map<String, String> getNameMap() {
		return this.nameMap;
	}
	
	public void reset() {
		this.gt = g.V();
	}
	
	public GremlinInterpreter copy() {
		GremlinInterpreter interp = null;
		if(this.meta != null) {
			interp = new GremlinInterpreter(this.g, this.meta);
		} else {
			interp = new GremlinInterpreter(this.g, this.typeMap, this.nameMap);
		}
		interp.setQueryStruct(this.qs);
		return interp;
	}
	
}