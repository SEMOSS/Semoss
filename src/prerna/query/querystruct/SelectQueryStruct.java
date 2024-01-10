package prerna.query.querystruct;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.ds.nativeframe.NativeFrame;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.om.InsightPanel;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryCustomOrderBy;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;

public class SelectQueryStruct extends AbstractQueryStruct {
	
	protected boolean isDistinct = true;
	
	protected List<IQuerySort> orderByOperations = new Vector<>();
	protected List<IQuerySelector> groupBy = new Vector<>();

	// panel specific 
	protected transient List<IQuerySort> panelOrderByOperations = new Vector<>();
	
	protected long limit = -1;
	protected long offset = -1;
	
	// query is the complete query overhall
	public enum Query_Part {
		SELECT, FILTER, HAVING, SORT, GROUP, AGGREGATE, QUERY;
	} 
	
	protected transient Map queryPartHash = new HashMap();
	
	////////////////////////////////////////////////////
	///////////////////// ORDERING /////////////////////
	////////////////////////////////////////////////////	
	
	public void setOrderBy(List<IQuerySort> orderByOperations) {
		this.orderByOperations = orderByOperations;
	}
	
	public void addOrderBy(List<IQuerySort> orderByOperations) {
		for(IQuerySort orderBy : orderByOperations) {
			addOrderBy(orderBy);
		}
	}
	
	public void addOrderBy(String concept, String property, String sortDir) {
		if(property == null) {
			property = AbstractQueryStruct.PRIM_KEY_PLACEHOLDER; 
		}
		QueryColumnOrderBySelector selector = new QueryColumnOrderBySelector();
		selector.setTable(concept);
		selector.setColumn(property);
		selector.setSortDir(sortDir);
		this.orderByOperations.add(selector);
	}
	
	public void addOrderBy(String qsName) {
		QueryColumnOrderBySelector selector = new QueryColumnOrderBySelector(qsName);
		this.orderByOperations.add(selector);
	}
	
	public void addOrderBy(String qsName, String sortDir) {
		QueryColumnOrderBySelector selector = new QueryColumnOrderBySelector(qsName);
		selector.setSortDir(sortDir);
		this.orderByOperations.add(selector);
	}
	
	public void addOrderBy(IQuerySort selector) {
		this.orderByOperations.add(selector);
	}
	
	/**
	 * Returns order bys defined in the qs
	 * NOTE - USE COMBINED ORDER BY TO COMBINE PANEL ORDER BY ON VISUALIZATIONS
	 * @return
	 */
	public List<IQuerySort> getOrderBy() {
		return this.orderByOperations;
	}
	
	public List<IQuerySort> getCombinedOrderBy() {
		List<IQuerySort> combinedSorts = new ArrayList<>();
		combinedSorts.addAll(this.orderByOperations);
		combinedSorts.addAll(this.panelOrderByOperations);
		return combinedSorts;
	}
	
	public void setPanelOrderBy(List<IQuerySort> panelOrderByOperations) {
		this.panelOrderByOperations = panelOrderByOperations;
	}
	
	public List<IQuerySort> getPanelOrderBy() {
		return this.panelOrderByOperations;
	}
	
	// overriding methods for ordering with panels
	@Override
	public void addPanel(InsightPanel panel) {
		if(!this.panelList.contains(panel)) {
			this.panelList.add(panel);
			this.panelIdList.add(panel.getPanelId());
			// also add in the current panel state
			this.panelImplicitFilters.merge(panel.getPanelFilters());
			this.panelOrderByOperations.addAll(panel.getPanelOrderBys());
		}
	}
	
	@Override
	public void setPanelList(List<InsightPanel> panelList) {
		// this method is same as the super
		// but we are also doing the order by
		this.panelList = panelList;
		this.panelImplicitFilters.clear();
		this.panelOrderByOperations.clear();
		for(InsightPanel panel : panelList) {
			// also add in the current panel state
			this.panelImplicitFilters.merge(panel.getPanelFilters());
			this.panelOrderByOperations.addAll(panel.getPanelOrderBys());
		}
	}
	
	////////////////////////////////////////////////////
	///////////////////// GROUP BY /////////////////////
	////////////////////////////////////////////////////
	
	public void setGroupBy(List<IQuerySelector> groupBy) {
		this.groupBy = groupBy;
	}
	
	public void addGroupBy(IQuerySelector selector) {
		this.groupBy.add(selector);
	}
	
	public void addGroupBy(String concept, String property) {
		if(property == null) {
			property = AbstractQueryStruct.PRIM_KEY_PLACEHOLDER; 
		}
		QueryColumnSelector selector = new QueryColumnSelector();
		
		selector.setTable(concept);
		selector.setColumn(property);
		this.groupBy.add(selector);
	}
	
	public List<IQuerySelector> getGroupBy() {
		return this.groupBy;
	}
	
	////////////////////////////////////////////////////
	/////////////////////// OTHER //////////////////////
	////////////////////////////////////////////////////
	
	public void setLimit(long limit) {
		this.limit = limit;
	}
	
	public long getLimit() {
		return this.limit;
	}
	
	public void setOffSet(long offset) {
		this.offset = offset;
	}
	
	public long getOffset() {
		return this.offset;
	}
	
	public void setDistinct(boolean isDistinct) {
		this.isDistinct = isDistinct;
	}
	
	public boolean isDistinct() {
		return this.isDistinct;
	}
	
	/**
	 * Return is this column has an existing filter in the QS
	 * @param column
	 * @return
	 */
	public boolean hasFiltered(String column) {
		return this.explicitFilters.hasFilter(column);
	}
	
	/**
	 * Return if this column is part of the return
	 * @param qsName
	 * @return
	 */
	public boolean hasColumn(String qsName) {
		for(IQuerySelector selector : this.selectors) {
			if(selector.getQueryStructName().equals(qsName)) {
				return true;
			}
		}
		return false;
	}
	
	public IQuerySelector findSelectorFromAlias(String alias) {
		for(IQuerySelector selector : this.selectors) {
			if(selector.getAlias().equals(alias)) {
				return selector;
			}
		}
		return null;
	}
	
	/**
	 * Returns if no information has been set into the query struct
	 * @return
	 */
	public boolean isEmpty() {
		// if any of the main 3 objects within the QS have info, return false
		// even in the case that selectors are empty, if other info is set, the QS will still 
		// return false for this method		
		return (!this.selectors.isEmpty() || !this.relationsSet.isEmpty() || !this.explicitFilters.isEmpty()) ;
	}
	
	/**
	 * 
	 * @param incomingQS
	 * 
	 * This method is responsible for merging "incomingQS's" data with THIS querystruct
	 */
	public void merge(AbstractQueryStruct incomingQS) {
		super.merge(incomingQS);
		if(incomingQS instanceof SelectQueryStruct) {
			SelectQueryStruct selectQS = (SelectQueryStruct) incomingQS;
			mergeGroupBy(selectQS.groupBy);
			mergeOrderBy(selectQS.orderByOperations);
			if(selectQS.limit > -1) {
				setLimit(selectQS.limit);
			}
			
			if(selectQS.offset > -1) {
				setOffSet(selectQS.offset);
			}
		}
	}
	
	/**
	 * 
	 * @param groupBys
	 * Merge the group by selectors
	 */
	public void mergeGroupBy(List<IQuerySelector> groupBys) {
		for(IQuerySelector selector : groupBys) {
			if(!this.groupBy.contains(selector)) {
				this.groupBy.add(selector);
			}
		}
	}
	
	/**
	 * 
	 * @param orderBys
	 * Merge the order by selectors
	 */
	public void mergeOrderBy(List<IQuerySort> orderBys) {
		for(IQuerySort selector : orderBys) {
			if(!this.orderByOperations.contains(selector)) {
				this.orderByOperations.add(selector);
			}
		}
	}
	
	////////////////////////////////////////////////////
	////////////// For Task Meta Info //////////////////
	////////////////////////////////////////////////////

	//TODO: this only handles base case of columns and math on a single column
	public List<Map<String, Object>> getHeaderInfo() {
		// these are the only types
		// where we have metadata for additional types
		boolean frameQuery = qsType == QUERY_STRUCT_TYPE.FRAME;
		boolean engineQuery = qsType == QUERY_STRUCT_TYPE.ENGINE;
		
		List<Map<String, Object>> headerInfo = new Vector<Map<String, Object>>();
		for(IQuerySelector selector : this.selectors) {
			Map<String, Object> selectorMap = new HashMap<String, Object>();
			// get header information based on the type of column
			IQuerySelector.SELECTOR_TYPE selectorType = selector.getSelectorType();
			String alias = selector.getAlias();
			selectorMap.put("alias", alias);
			selectorMap.put("header", selector.getQueryStructName());
			if(selectorType == IQuerySelector.SELECTOR_TYPE.COLUMN) {
				selectorMap.put("derived", false);
				
				// right now, just adding in additional data type
				// we pull the type from the iterator which is 
				// the only place where we call this
				addDataType((QueryColumnSelector) selector, frameQuery, engineQuery, selectorMap);
				
			} else {
				// if we have a constant, we will have it be derived false
				// otherwise derived is true
				boolean derived = selectorType != IQuerySelector.SELECTOR_TYPE.CONSTANT;
				selectorMap.put("derived", derived);
				List<String> groupBy = new Vector<String>();
				for(IQuerySelector groupBySelector : this.groupBy) {
					String groupQs = groupBySelector.getQueryStructName();
					groupBy.add(groupQs);
				}
				selectorMap.put("groupBy", groupBy);
			}
			// if it is a math, then there must be a group by associated with it
			if(selectorType == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
				QueryFunctionSelector mathSelector = (QueryFunctionSelector) selector;
				selectorMap.put("header", alias);
				selectorMap.put("math", QueryFunctionHelper.getPrettyName(mathSelector.getFunction()));

				// add inner selector QS
				List<IQuerySelector> innerSelector = mathSelector.getInnerSelector();
				// TODO: STOP ASSUMING THIS IS 1 SELECTOR
				selectorMap.put("calculatedBy", innerSelector.get(0).getQueryStructName());

				// right now, just adding in additional data type
				// we pull the type from the iterator which is 
				// the only place where we call this
				if(innerSelector.get(0).getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
					addDataType((QueryColumnSelector) innerSelector.get(0), frameQuery, engineQuery, selectorMap);
				}
				
				List<String> groupBy = new Vector<>();
				for(IQuerySelector groupBySelector : this.groupBy) {
					String groupQs = groupBySelector.getQueryStructName();
					groupBy.add(groupQs);
				}
				selectorMap.put("groupBy", groupBy);
				selectorMap.put("derived", true);
			}
			headerInfo.add(selectorMap);
		}
		return headerInfo;
	}
	
	private void addDataType(QueryColumnSelector selector, boolean frameQuery, boolean engineQuery, Map<String, Object> selectorMap) {
		// add the additional data type if present
		String additionalDataType = null;
		if(frameQuery && this.frame != null) {
			String name = selector.getQueryStructName();
			additionalDataType = this.frame.getMetaData().getHeaderAdtlType(name);
			if(additionalDataType == null) {
				name = this.frame.getMetaData().getUniqueNameFromAlias(name);
				if(name != null) {
					additionalDataType = this.frame.getMetaData().getHeaderAdtlType(name);
				}
			}
		} else if(engineQuery && getEngine() != null) {
			String name = selector.getQueryStructName();
			String parent = null;
			if(name.contains("__")) {
				String[] split = name.split("__");
				parent = split[0];
				name = split[1];
			}
			additionalDataType = MasterDatabaseUtility.getAdditionalDataType(additionalDataType, name, parent);
		}
		
		if(additionalDataType != null) {
			selectorMap.put("additionalDataType", additionalDataType);
		} else {
			selectorMap.put("additionalDataType", "");
		}
	}
	
	public List<Map<String, Object>> getSortInfo() {
		List<Map<String, Object>> orderByInfo = new Vector<Map<String, Object>>();
		for(IQuerySort orderBy : this.orderByOperations) {
			if(orderBy.getQuerySortType() == IQuerySort.QUERY_SORT_TYPE.COLUMN) {
				QueryColumnOrderBySelector columnSort = (QueryColumnOrderBySelector) orderBy;
				Map<String, Object> selectorMap = new HashMap<String, Object>();
				selectorMap.put("alias", columnSort.getAlias());
				selectorMap.put("header", columnSort.getQueryStructName());
				selectorMap.put("derived", false);
				selectorMap.put("dir", columnSort.getSortDir().toString());
				orderByInfo.add(selectorMap);
			} else if(orderBy.getQuerySortType() == IQuerySort.QUERY_SORT_TYPE.CUSTOM) {
				QueryCustomOrderBy customSort = (QueryCustomOrderBy) orderBy;
				QueryColumnSelector columnSort = customSort.getColumnToSort();
				Map<String, Object> selectorMap = new HashMap<String, Object>();
				selectorMap.put("alias", columnSort.getAlias());
				selectorMap.put("header", columnSort.getQueryStructName());
				selectorMap.put("derived", false);
				selectorMap.put("customOrder", customSort.getCustomOrder());
				orderByInfo.add(selectorMap);
			}
		}
		return orderByInfo;
	}
	
	/**
	 * Gets a new QS with the base information moved over
	 * This is basically the qs type + enginename + csv/excel properties
	 * Note csv/excel qs overrides this method
	 * @return
	 */
	public SelectQueryStruct getNewBaseQueryStruct() {
		SelectQueryStruct newQs = new SelectQueryStruct();
		newQs.setQsType(this.qsType);
		if(this.qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.ENGINE) {
			newQs.setEngineId(this.engineId);
			newQs.setEngine(this.engine);
		} else if(this.qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.FRAME) {
			newQs.setFrame(this.frame);
		}
		newQs.setDistinct(this.isDistinct);
		newQs.setOverrideImplicit(this.overrideImplicit);
		newQs.setCustomFrom(this.customFrom);
		newQs.setCustomFromAliasName(this.customFromAliasName);
		return newQs;
	}
	
	/**
	 * Get an appropriate message
	 * @param qs
	 * @return
	 */
	public static String getExecutingQueryMessage(SelectQueryStruct qs) {
		String message = null;
		if(qs.getQsType() == QUERY_STRUCT_TYPE.RAW_FRAME_QUERY
				|| qs.getQsType() == QUERY_STRUCT_TYPE.FRAME) {
			if(qs.getFrame() instanceof NativeFrame) {
				message = "Executing query against the database and generating the result set";
			} else {
				message = "Executing query against the frame and generating the result set";
			}
		} else {
			message = "Executing query against the datasource and generating the result set";
		}
		
		return message;
	}
	
	
//	/**
//	 * Logic to process the relationship
//	 * This takes into consideration intermediary nodes that should not be added to the return hash
//	 * e.g. i have concepts a -> b -> c -> d but I only want to return a-> d
//	 * @param startNode				The startNode of the relationship
//	 * @param relMap				The relationships being observed for the startNode
//	 * @param edgeHash				The existing edge hash to determine what the current selectors are
//	 */
//	private void processRelationship(String startNode, Map<String, List> relMap, Map<String, Set<String>> edgeHash) {
//		// grab all the end nodes
//		// the edge hash doesn't care about what kind of join it is
//		Collection<List> endNodeValues = relMap.values();
//		for(List<String> endNodeList : endNodeValues) {
//			// iterate through all the end nodes
//			for(String endNode : endNodeList) {
//				// need to ignore the prim_key_value...
//				if(startNode.equals(endNode)) {
//					continue;
//				}
//				
//				// if the endNode already exists as a key in the edgeHash,
//				// then just connect it and we are done
//				if(edgeHash.containsKey(endNode)) {
//					edgeHash.get(startNode).add(endNode);
//				} else {
//					// maybe we are joining on a prop
//					// lets first test this out
//					if(endNode.contains("__")) {
//						String concept = endNode.substring(0, endNode.indexOf("__"));
//						if(edgeHash.containsKey(concept)) {
//							// we found the parent.. therefore we add it 
//							// just add parent to the startNode
//							edgeHash.get(startNode).add(concept);
//						} else {
//							// here we need to loop through and find the shortest path
//							// starting from this specific endNode to an endNode which is 
//							// a selector to be returned
//							// we use a recursive method determineShortestEndNodePath to fill in 
//							// the list newEndNodeList and then we add that to the edgeHash
//							List<String> newEndNodeList = new Vector<String>();
//							determineShortestEndNodePath(endNode, edgeHash, newEndNodeList);
//							for(String newEndNode : newEndNodeList) {
//								edgeHash.get(startNode).add(newEndNode);
//							}
//						}
//					} else {
//						// here we need to loop through and find the shortest path
//						// starting from this specific endNode to an endNode which is 
//						// a selector to be returned
//						// we use a recursive method determineShortestEndNodePath to fill in 
//						// the list newEndNodeList and then we add that to the edgeHash
//						List<String> newEndNodeList = new Vector<String>();
//						determineShortestEndNodePath(endNode, edgeHash, newEndNodeList);
//						for(String newEndNode : newEndNodeList) {
//							edgeHash.get(startNode).add(newEndNode);
//						}
//					}
//				}
//			}
//		}
//	}
	
//	/**
//	 * Recursive method to find the shortest path to all the nearest concepts that are being returned as selectors
//	 * @param endNode					The endNode that is node a selector which we are trying to find the shortest path to 
//	 * @param edgeHash					The edgeHash to find the current selectors
//	 * @param newEndNodeList			The list of endNodes that have been found using the logic to find the shortest 
//	 * 									path for connected nodes
//	 */
//	private void determineShortestEndNodePath(String endNode, Map<String, Set<String>> edgeHash, List<String> newEndNodeList) {
//		// this endNode is a node which is not a selector
//		// need to find the shortest path to nodes which this endNode is connected to which is also a selector
//
//		// first see if there is a connection for the endNode to traverse to
//		if(this.relations.containsKey(endNode)) {
//			// grab the join map
//			Map<String, List> joinMap = this.relations.get(endNode);
//			// we do not care at all about the type of join
//			// just go through and get the list of nodes which we care about
//			Collection<List> connections = joinMap.values();
//			for(List<String> endNodeList :  connections) {
//				for(String possibleNewEndNode : endNodeList) {
//					// if this connection is a selector (i.e. key in the edgeHash), then we need to add it to the newEndNodeList
//					if(edgeHash.containsKey(possibleNewEndNode)) {
//						newEndNodeList.add(possibleNewEndNode);
//					} else {
//						// maybe we are joining on a prop
//						// lets first test this out
//						if(possibleNewEndNode.contains("__")) {
//							String concept = possibleNewEndNode.substring(0, possibleNewEndNode.indexOf("__"));
//							if(edgeHash.containsKey(concept)) {
//								// we found the parent.. therefore we add it 
//								// append it to the list
//								newEndNodeList.add(concept);
//							} else {
//								// if possibleNewEndNode is in fact not a end node
//								// then we need to recursively go down the path and see if it has a possibleNewEndNode
//								determineShortestEndNodePath(possibleNewEndNode, edgeHash, newEndNodeList);
//							}
//						} else {
//							// if possibleNewEndNode is in fact not a end node
//							// then we need to recursively go down the path and see if it has a possibleNewEndNode
//							determineShortestEndNodePath(possibleNewEndNode, edgeHash, newEndNodeList);
//						}
//					}
//				}
//			}
//		}
//	}
	
	/**
	 * This uses the selector list and relations lists to determine how everything is connected
	 *
	 * Will return like this:
	 * Title --> [Title__Budget, Studio]
	 * Studio --> [StudioOwner]
	 * etc.
	 * 
	 * @return
	 */
	public Map<String, Set<String>> getReturnConnectionsHash() {
		// create the return edgeHash map
		Map<String, Set<String>> edgeHash = new HashMap<String, Set<String>>();
		
		/*
		 * 1) iterate through and add concepts and properties
		 * This step is very simple and doesn't require any special logic
		 * Just need to consider the case when PRIM_KEY_PLACEHOLDER is not present which means
		 * That the query return only returns the property and not the main concept
		 * 
		 * 2) iterate through and add the relationships
		 * This needs to take into consideration intermediary nodes
		 * e.g. i have concepts a -> b -> c -> d but I only want to return a-> d
		 * thus, the edge hash should only contain a -> d
		 */
		
		// 1) iterate through all the selectors
		for(IQuerySelector anySelector : this.selectors) {
			if(!(anySelector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN)) {
				continue;
			}
			QueryColumnSelector selector = (QueryColumnSelector) anySelector;
			String column = selector.getColumn();
			String table = selector.getTable();
			edgeHash.putIfAbsent(table, new HashSet<String>());
			
			boolean isPrimKey = AbstractQueryStruct.PRIM_KEY_PLACEHOLDER.equals(column);
			if(!isPrimKey) {
				edgeHash.put(table+"__"+column, new HashSet<String>()); //add the property as a key pointing to nothing
				edgeHash.get(table).add(table+"__"+column);  //add the property as a child of the table				
			}
		}
		
//		for(String selectorKey: this.selectors.keySet()) {
//			Vector<String> props = this.selectors.get(selectorKey);
//			Set<String> downNodeTypes = edgeHash.get(selectorKey);
//			// if the props doesn't contain a prim_key_placeholder... then it is actually just a property and not a concept
//			if(!props.contains(PRIM_KEY_PLACEHOLDER)) {
//				// just loop through and add all the properties by themselves
//				for(String prop : props){
//					edgeHash.put(selectorKey + "__" + prop, new HashSet<String>());
//				}
//			} else {
//				// a prim_key_placeholder was found
//				// thus, we need to add the concept to all of its properties
//				if(downNodeTypes == null){
//					downNodeTypes = new HashSet<String>();
//				}
//				edgeHash.put(selectorKey, downNodeTypes);
//				for(String prop : props){
//					// make sure we don't add a node to itself (e.g. Title__Title)
//					if(prop.equals(PRIM_KEY_PLACEHOLDER)) {
//						continue;
//					}
//					// mergeQSEdgeHash needs this to be the concept__property... plus need to keep it consistent with relations
//					downNodeTypes.add(selectorKey + "__" + prop); 
//				}
//			}
//		}

		// 2) need to determine and connect the appropriate connections based on the 
//		if(this.relations != null) {
//			// get the starting concept
//			for(String startNode : this.relations.keySet()) {
//				// the relMap contains the joinType pointing to a list of columns to be joined to
//				Map<String, List> relMap = this.relations.get(startNode);
//				// else, just doing a normal join
//				// if the edge hash has the start node as a selector
//				// then we need to see if we should connect it
//				// otherwise, check if it is a relationship based on a property
//				// if that also fails, do nothing
//				// this is because the logic for returning a -> d can be done when checking
//				// the endNode of the relationship
//				if(edgeHash.containsKey(startNode)) {
//					processRelationship(startNode, relMap, edgeHash);
//				} else {
//					if(startNode.contains("__")) {
//						String concept = startNode.substring(0, startNode.indexOf("__"));
//						if(edgeHash.containsKey(concept)) {
//							processRelationship(concept, relMap, edgeHash);
//						}
//					}
//				}
//			}
//		}

		return edgeHash;
	}
	
	public void setPart(Query_Part part, String overrider)
	{
		queryPartHash.put(part, overrider);
	}
	
	public Map getParts()
	{
		return queryPartHash;
	}
	
	public void clearParts()
	{
		queryPartHash.clear();
	}
	
	public void setPragmap(Map map)
	{
		this.pragmap = map;
	}
	
	public Map getPragmap()
	{
		return this.pragmap;
	}
	
	public void clearPragmap()
	{
		pragmap.clear();
	}

}
