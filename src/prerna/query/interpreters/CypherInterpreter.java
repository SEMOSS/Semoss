package prerna.query.interpreters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RSyntaxHelper;
import prerna.engine.api.IDatabaseEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.joins.BasicRelationship;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySelector.SELECTOR_TYPE;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector.ORDER_BY_DIRECTION;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class CypherInterpreter extends AbstractQueryInterpreter {

	// keep track of the selectors
	private StringBuilder selectorCriteria;

	// keep track of the filters
	private StringBuilder filterCriteria = new StringBuilder();

	// keep track of order bys
	private StringBuilder orderBys = new StringBuilder();

	// Start and End
	long start = 0;
	long end = 500;

	private Map<String, Map<String, String>> labelMap = null;
	private Map<String, String> labelAliasMap = null;
	private Map<String, SemossDataType> colDataTypes;
	ArrayList<String> headers = null;
	ArrayList<String> actHeaders = null;
	ArrayList<SemossDataType> types = null;

	// Query constants
	private static final String MATCH = "MATCH";
	private static final String WHERE = "WHERE";
	private static final String RETURN = "RETURN";
	private static final String DISTINCT = "DISTINCT";
	private static final String AS = "AS";
	private static final String ORDER_BY = "ORDER BY";
	private static final String OFFSET = "SKIP";
	private static final String LIMIT = "LIMIT";

	protected transient IDatabaseEngine engine;
	// identify the name for the type of the name
	protected Map<String, String> typeMap;
	// identify the name for the vertex label
	protected Map<String, String> nameMap;
	protected boolean useLabel = false;

	public CypherInterpreter(Map<String, String> typeMap, Map<String, String> nameMap) {
		this.typeMap = typeMap;
		this.nameMap = nameMap;
	}

	@Override
	public String composeQuery() {
		if (this.qs instanceof HardSelectQueryStruct) {
			return ((HardSelectQueryStruct) this.qs).getQuery();
		}
		engine = this.qs.getEngine();

		StringBuilder query = new StringBuilder();
		labelMap = new HashMap<>();
		labelAliasMap = new HashMap<>();

		if (this.colDataTypes == null) {
			this.colDataTypes = new Hashtable<>();
		}
		boolean isDistinct = ((SelectQueryStruct) this.qs).isDistinct();

		query.append(MATCH);
		// adds selectors into map, then loops through map and appends selectors
		appendSelectors(query);
		addFilters(qs.getCombinedFilters().getFilters(), this.filterCriteria, false, engine);

		if (this.filterCriteria.length() > 0) {
			query.append(" ").append(WHERE).append(" ").append(this.filterCriteria);
		}
		query.append(" ").append(RETURN);
		if (isDistinct) {
			query.append(" ").append(DISTINCT);
		}
		query.append(" ").append(this.selectorCriteria);
		addOrderBy();
		query.append(" ").append(this.orderBys);
		// Set start
		start = ((SelectQueryStruct) this.qs).getOffset();
		if (start > -1) {
			query.append(" ").append(OFFSET).append(" ").append(start);
		}
		// Set end
		end = ((SelectQueryStruct) this.qs).getLimit();
		if (end > 0) {
			query.append(" ").append(LIMIT).append(" ").append(end);
		}
		// return cypher query
		return query.toString();
	}

	//////////////////////////////////// start adding selectors ////////////////////////////////////

	public void appendSelectors(StringBuilder query) {
		Map<String, Set<String>> edgeMap = generateEdgeMap();
		selectorCriteria = new StringBuilder();
		List<String> selectorList = new ArrayList<>();
		Set<String> nodesDefined = new HashSet<>();
		// add joined nodes first
		List<String> joinValues = new ArrayList<>();
		addSelectors(selectorList);

		for (String startNode : edgeMap.keySet()) {
			// format start node
			String startNodeAlias = labelAliasMap.get(startNode);
			// if alias label name doesn't exist in labelAliasMap, create new
			// intermediateNode() and store
			if (startNodeAlias == null) {
				startNodeAlias = generateIntermediateNode(startNode);
			}
			String cq = queryNode(startNodeAlias, startNode);
			Set<String> visitNodes = edgeMap.get(startNode);
			for (String endNode : visitNodes) {
				String endNodeAlias = labelAliasMap.get(endNode);
				String queryVisit = queryNode(endNodeAlias, endNode);
				// traversal
				nodesDefined.add(startNode);
				nodesDefined.add(endNode);
				joinValues.add(cq + "-[]-" + queryVisit);
			}
		}

		for (String node : labelMap.keySet()) {
			// if node is not defined from joins add to query
			String intermediateNode = labelAliasMap.get(node);
			if (!nodesDefined.contains(node)) {
				String queryNode = queryNode(intermediateNode, node);
				joinValues.add(queryNode);
			}
		}

		// append joins to query
		for (int i = 0; i < joinValues.size(); i++) {
			query.append(joinValues.get(i));
			if (i < joinValues.size() - 1) {
				query.append(", ");
			}
		}

		// append Return/Select criteria
		for (int i = 0; i < selectorList.size(); i++) {
			selectorCriteria.append(selectorList.get(i));
			if (i < selectorList.size() - 1) {
				selectorCriteria.append(", ");
			}
		}
	}

	// loop through the label map and generate intermediate nodes for all added
	// selectors
	public String generateIntermediateNode(String labelName) {
		String intermediateNode = null;
		// Check if intermediate node already exists in map
		if (labelAliasMap.containsKey(labelName)) {
			intermediateNode = labelAliasMap.get(labelName);
		} else {
			intermediateNode = Utility.getRandomString(5);
			labelAliasMap.put(labelName, intermediateNode);
		}

		return intermediateNode;
	}

	// add selectors into a map
	public void addSelectors(List<String> selectorList) {
		List<IQuerySelector> selectors = qs.getSelectors();
		// group bys to sort selectors by
		List<IQuerySelector> groupByList = ((SelectQueryStruct) this.qs).getGroupBy();

		// if GROUP BY exists, sort selector criteria for implicit Cypher GROUP
		// BY
		if (!groupByList.isEmpty()) {
			List<String> qcsSelectors = new ArrayList<>();
			List<String> gbSelectors = new ArrayList<>();

			// convert to string for comparison/sorting
			for (IQuerySelector selector : selectors) {	
				String selectorString = selector.toString();
				qcsSelectors.add(selectorString);	
			}

			for (IQuerySelector groupBy : groupByList) {
				if(groupBy.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
					QueryColumnSelector queryColumnSelector = (QueryColumnSelector) groupBy;
				    String gbString = queryColumnSelector.toString();
				    gbSelectors.add(gbString);
				} else {
					String errorMessage = "Cannot group by non QueryColumnSelector type yet...";
					logger.error(errorMessage);
					throw new IllegalArgumentException(errorMessage);
				}
			}

			// sort
			for (int i = 0; i < gbSelectors.size(); i++) {
				if (qcsSelectors.equals(gbSelectors)) {
					break;
				}
				for (int j = 0; j < qcsSelectors.size(); j++) {
					int c = gbSelectors.get(i).compareTo(qcsSelectors.get(j));
					if (c == 0) {
						String valueToSwap = qcsSelectors.get(i);
						qcsSelectors.set(i, gbSelectors.get(i));
						qcsSelectors.set(j, valueToSwap);
					}
				}
			}

			// clear original unordered list
			selectors.clear();

			// convert back to Custom Object for interpretation
			for (String selector : qcsSelectors) {
				QueryColumnSelector typeSelector = new QueryColumnSelector(selector);
				selectors.add((IQuerySelector) typeSelector);
			}

		}

		for (int i = 0; i < selectors.size(); i++) {
			IQuerySelector selector = selectors.get(i);
			SELECTOR_TYPE selectorType = selector.getSelectorType();
			StringBuilder listCriteria = new StringBuilder();

			// depending on selector type parse return input different way
			if (selectorType == IQuerySelector.SELECTOR_TYPE.COLUMN) {
				String[] selectorInfoArray = null;
				String selectorInfo = processSelector(selector);
				if (selectorInfo != null) {
					selectorInfoArray = selectorInfo.split(",");
					String labelName = selectorInfoArray[0];
					String originalPropertyName = selectorInfoArray[1];
					String aliasPropertyName = selectorInfoArray[2];
					String intermediateNode = generateIntermediateNode(labelName);
					selectorList.add(listCriteria.append(intermediateNode).append(".").append(originalPropertyName)
							.append(" ").append(AS).append(" ").append(aliasPropertyName).toString());
				}
			} else if (selectorType == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
				// FUNCTION NAME:TABLENAME;PROPERTY NAME, PROPERTY NAME, ...
				String[] selectorInfoArray = null;
				String selectorInfo = processSelector(selector);
				if (selectorInfo != null) {
					selectorInfoArray = selectorInfo.split(",");
					String functionName = selectorInfoArray[0];
					String tableName = selectorInfoArray[1];
					String aliasPropertyName = selectorInfoArray[2];
					String propertyName = selectorInfoArray[3];
					String intermediateNode = generateIntermediateNode(tableName);

					// "functionName(intermediateNode.propertyName) AS
					// aliasPropertyName"
					selectorList.add(listCriteria.append(functionName).append("(").append(intermediateNode).append(".")
							.append(propertyName).append(")").append(" ").append(AS).append(" ")
							.append(aliasPropertyName).toString());
				}
			} else if (selectorType == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {

			}
		}
	}

	private String processSelector(IQuerySelector selector) {
		SELECTOR_TYPE selectorType = selector.getSelectorType();

		/*
		 * if(selectorType == IQuerySelector.SELECTOR_TYPE.CONSTANT) { return
		 * processConstantSelector((QueryConstantSelector) selector); } else
		 */
		if (selectorType == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			return processColumnSelector((QueryColumnSelector) selector);
		} else if (selectorType == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
			return processFunctionSelector((QueryFunctionSelector) selector);
		} else if (selectorType == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			return processArithmeticSelector((QueryArithmeticSelector) selector);
		}

		return null;
	}

	private String processColumnSelector(QueryColumnSelector selector) {
		StringBuilder propertyNames = new StringBuilder();
		String labelName = selector.getTable();
		String originalPropertyName = selector.getColumn();
		if (originalPropertyName.equals(AbstractQueryStruct.PRIM_KEY_PLACEHOLDER)) {
			// get name for node from name map
			originalPropertyName = getNodeName(labelName);
		}
		String aliasPropertyName = selector.getAlias();
		Map<String, String> propertyMap = new HashMap<>();

		// append to return value for other selector processors
		propertyNames.append(labelName).append(",").append(originalPropertyName).append(",").append(aliasPropertyName);

		// if property exists just add that property name and alias to the map
		if (labelMap.containsKey(labelName)) {
			Map<String, String> existingPropertyMap = labelMap.get(labelName);
			existingPropertyMap.put(originalPropertyName, aliasPropertyName);
			labelMap.put(labelName, existingPropertyMap);
		} else { // property doesn't exist create new
			propertyMap.put(originalPropertyName, aliasPropertyName);
			labelMap.put(labelName, propertyMap);
		}

		return propertyNames.toString();
	}

	private String processFunctionSelector(QueryFunctionSelector selector) {
		List<IQuerySelector> innerSelectors = selector.getInnerSelector();
		String function = selector.getFunction();
		StringBuilder expression = new StringBuilder();

		switch (function) {
		case (QueryFunctionHelper.COUNT):
			expression.append("COUNT;");
			break;
		case (QueryFunctionHelper.MAX):
			expression.append("MAX;");
			break;
		case (QueryFunctionHelper.MIN):
			expression.append("MIN;");
			break;
		case (QueryFunctionHelper.SUM):
			expression.append("SUM;");
			break;
		case (QueryFunctionHelper.AVERAGE_1):
		case (QueryFunctionHelper.AVERAGE_2):
			expression.append("AVG;");
			break;
		default:
			expression.append(function.toUpperCase()).append(";");
		}

		int size = innerSelectors.size();

		for (int i = 0; i < size; i++) {
			String[] propertyInfo = processSelector(innerSelectors.get(i)).split(",");
			String labelName = propertyInfo[0];
			String originalPropertyName = propertyInfo[1];
			String aliasPropertyName = propertyInfo[2];

			expression.append(labelName).append(";").append(aliasPropertyName).append(";");
			if (i == 0) {
				expression.append(originalPropertyName);
			} else {
				expression.append(",").append(originalPropertyName);
			}
		}
		return expression.toString();
	}

	private String processArithmeticSelector(QueryArithmeticSelector selector) {
		IQuerySelector leftSelector = selector.getLeftSelector();
		IQuerySelector rightSelector = selector.getRightSelector();
		String mathExpr = selector.getMathExpr();
		return "(" + processSelector(leftSelector) + " " + mathExpr + " " + processSelector(rightSelector) + ")";
	}
	//////////////////////////////////// end adding selectors
	//////////////////////////////////// ////////////////////////////////////

	//////////////////////////////////// start adding filters
	//////////////////////////////////// ////////////////////////////////////
	public void addFilters(List<IQueryFilter> filters, StringBuilder builder, boolean useAlias, IDatabaseEngine engine) {
		for (IQueryFilter filter : filters) {
			StringBuilder filterSyntax = processFilter(filter, useAlias, engine);
			if (filterSyntax != null) {
				if (builder.length() > 0) {
					builder.append(" AND ");
				}
				builder.append(filterSyntax.toString());
			}
		}
	}

	private StringBuilder processFilter(IQueryFilter filter, boolean useAlias, IDatabaseEngine engine) {
		IQueryFilter.QUERY_FILTER_TYPE filterType = filter.getQueryFilterType();
		if (filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return processSimpleQueryFilter((SimpleQueryFilter) filter, useAlias);
		} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return processAndQueryFilter((AndQueryFilter) filter, useAlias);
		} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return processOrQueryFilter((OrQueryFilter) filter, useAlias);
		}
		return null;
	}

	private StringBuilder processOrQueryFilter(OrQueryFilter filter, boolean useAlias) {
		StringBuilder filterBuilder = new StringBuilder();
		List<IQueryFilter> filterList = filter.getFilterList();
		int numAnds = filterList.size();
		for (int i = 0; i < numAnds; i++) {
			if (i == 0) {
				filterBuilder.append("(");
			} else {
				filterBuilder.append(" OR ");
			}
			filterBuilder.append(processFilter(filterList.get(i), useAlias, engine));
		}
		filterBuilder.append(")");
		return filterBuilder;
	}

	private StringBuilder processAndQueryFilter(AndQueryFilter filter, boolean useAlias) {
		StringBuilder filterBuilder = new StringBuilder();
		List<IQueryFilter> filterList = filter.getFilterList();
		int numAnds = filterList.size();
		for (int i = 0; i < numAnds; i++) {
			if (i == 0) {
				filterBuilder.append("(");
			} else {
				filterBuilder.append(" AND ");
			}
			filterBuilder.append(processFilter(filterList.get(i), useAlias, engine));
		}
		filterBuilder.append(")");
		return filterBuilder;
	}

	private StringBuilder processSimpleQueryFilter(SimpleQueryFilter filter, boolean useAlias) {
		NounMetadata leftComp = filter.getLComparison();
		NounMetadata rightComp = filter.getRComparison();
		String thisComparator = filter.getComparator();

		FILTER_TYPE fType = filter.getSimpleFilterType();
		if (fType == FILTER_TYPE.COL_TO_COL) {
			return addSelectorToSelectorFilter(leftComp, rightComp, thisComparator, useAlias);
		} else if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToValuesFilter(leftComp, rightComp, thisComparator, useAlias);
		} else if (fType == FILTER_TYPE.VALUES_TO_COL) {
			// same logic as above, just switch the order and reverse the
			// comparator if it is numeric
			return addSelectorToValuesFilter(rightComp, leftComp,
					IQueryFilter.getReverseNumericalComparator(thisComparator), useAlias);
		} else if (fType == FILTER_TYPE.VALUE_TO_VALUE) {
			// WHY WOULD YOU DO THIS!!!
			// VALUE_TO_VALUE: "Why're you yelling at me?"
		}
		return null;
	}

	private StringBuilder addSelectorToSelectorFilter(NounMetadata leftComp, NounMetadata rightComp,
			String thisComparator, boolean useAlias) {
		// get the left side
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		leftSelector.getSelectorType();
		QueryColumnSelector selector = (QueryColumnSelector) leftSelector;
		String actualLabelName = selector.getTable();
		String tempNode = labelAliasMap.get(actualLabelName);
		IQuerySelector rightSelector = (IQuerySelector) rightComp.getValue();

		/*
		 * Add the filter syntax here once we have the correct physical names
		 */
		String lPropertyNames = processSelector(leftSelector);
		String[] lExtractedPropertyNames = lPropertyNames.trim().split(",");
		String lSelector = lExtractedPropertyNames[1];

		String rPropertyNames = processSelector(rightSelector);
		String[] rExtractedPropertyNames = rPropertyNames.trim().split(",");
		String rSelector = rExtractedPropertyNames[1];

		StringBuilder filterBuilder = new StringBuilder();

		if (thisComparator.equals("!=") || thisComparator.equals("<>")) {
			filterBuilder.append("( !(").append(lSelector).append(" = ").append(rSelector)
					// account for NA
					.append(") | ( is.na(").append(lSelector).append(") & !is.na(").append(rSelector)
					.append(") ) | ( !is.na(").append(lSelector).append(") & is.na(").append(rSelector).append(")) )");
		} else if (thisComparator.equals("?like")) {
			// some operation
			filterBuilder.append("as.character(").append(lSelector).append(") =~ as.character(").append(rSelector)
					.append(")");
		} else {
			if (thisComparator.equals("==")) {
				filterBuilder.append("(").append(tempNode + ".").append(lSelector).append(" = ").append(rSelector)
						// account for NA
						.append(" | is.na(").append(lSelector).append(") & is.na(").append(rSelector).append(") )");
			} else {
				// other op
				filterBuilder.append(tempNode + ".").append(lSelector).append(" ").append(thisComparator).append(" ")
						.append(rSelector);
			}
		}

		return filterBuilder;
	}

	@SuppressWarnings({ "rawtypes", "incomplete-switch", "unchecked" })
	private StringBuilder addSelectorToValuesFilter(NounMetadata leftComp, NounMetadata rightComp,
			String thisComparator, boolean useAlias) {
		// get the left side
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		QueryColumnSelector selector = (QueryColumnSelector) leftSelector;
		String actualLabelName = selector.getTable();
		String tempNode = labelAliasMap.get(actualLabelName);

		StringBuilder filterBuilder = new StringBuilder();
		String lPropertyNames = processSelector(leftSelector);
		String[] lExtractedPropertyNames = lPropertyNames.trim().split(",");
		String leftSelectorExpression = lExtractedPropertyNames[1];

		String columnDataType = this.engine.getDataTypes(
				"http://semoss.org/ontologies/Relation/Contains/" + leftSelectorExpression + "/" + actualLabelName);
		SemossDataType columnType = SemossDataType.convertStringToDataType(columnDataType);

		// grab the objects we are setting up for the comparison
		List<Object> objects = new Vector<>();
		// ugh... this is gross
		if (rightComp.getValue() instanceof Collection) {
			objects.addAll((Collection) rightComp.getValue());
		} else {
			objects.add(rightComp.getValue());
		}

		// need to account for null inputs
		boolean addNullCheck = objects.contains(null);
		if (addNullCheck) {
			objects.remove(null);
		}

		if (!objects.isEmpty()) {
			boolean multi = false;
			StringBuilder myFilterFormatted = new StringBuilder();
			// format the objects based on the type of the column
			if (objects.size() > 1) {
				multi = true;
				myFilterFormatted.append("[");
				for (int i = 0; i < objects.size(); i++) {
					// if string
					switch (columnType) {
					case STRING:
						myFilterFormatted.append("\"").append(objects.get(i)).append("\"");
						break;
					case INT:
					case DOUBLE:
						myFilterFormatted.append(objects.get(i).toString());
						break;
					case BOOLEAN:
						myFilterFormatted.append(objects.get(i));
						break;
					}
					if (i < objects.size() - 1) {
						myFilterFormatted.append(", ");
					}
				}
				myFilterFormatted.append("]");
			} else {
				// dont bother doing this if we have a date since we cannot use
				// "in" with dates
				switch (columnType) {
				case STRING:
					myFilterFormatted.append("\"").append(objects.get(0)).append("\"");
					break;
				case INT:
				case DOUBLE:
					myFilterFormatted.append(objects.get(0).toString());
					break;
				case BOOLEAN:
					myFilterFormatted.append(objects.get(0));
					break;
				}
			}

			// account for bad input
			// example - filtering out empty + null when its a number...
			if (myFilterFormatted.length() == 0) {
				return filterBuilder;
			}

			if (multi) {
				// special processing for date types
				// TODO test cypher date types
				if (SemossDataType.DATE == columnType) {
					int size = objects.size();
					if (thisComparator.equals("==")) {
						filterBuilder.append("(");
						for (int i = 0; i < size; i++) {
							filterBuilder.append(tempNode + ".").append(leftSelectorExpression).append(" = ")
									.append(RSyntaxHelper.formatFilterValue(objects.get(i), SemossDataType.DATE, null));
							if ((i + 1) < size) {
								filterBuilder.append(" OR ");
							}
						}
						filterBuilder.append(")");
					} else if (thisComparator.equals("!=") | thisComparator.equals("<>")) {
						filterBuilder.append("(");
						for (int i = 0; i < size; i++) {
							filterBuilder.append(tempNode + ".").append(leftSelectorExpression).append(" != ")
									.append(RSyntaxHelper.formatFilterValue(objects.get(i), SemossDataType.DATE, null));
							if ((i + 1) < size) {
								filterBuilder.append(" AND ");
							}
						}
						filterBuilder.append(")");
					} else {
						// this will probably break...
						myFilterFormatted.append(RSyntaxHelper.formatFilterValue(objects.get(0), SemossDataType.DATE, null));
						filterBuilder.append(tempNode + ".").append(leftSelectorExpression).append(" ")
								.append(thisComparator).append(myFilterFormatted);
					}
				}
				// now all the other types
				else {
					if (thisComparator.equals("==")) {
						filterBuilder.append(tempNode + ".").append(leftSelectorExpression).append(" IN ")
								.append(myFilterFormatted);
					} else if (thisComparator.equals("!=") | thisComparator.equals("<>")) {
						filterBuilder.append(tempNode + ".").append("!(").append(leftSelectorExpression).append(" IN ")
								.append(myFilterFormatted).append(")");
					} else {
						// this will probably break...
						filterBuilder.append(tempNode + ".").append(leftSelectorExpression).append(" ")
								.append(thisComparator).append(myFilterFormatted);
					}
				}
			} else {
				if (thisComparator.equals("?like")) {
					if (SemossDataType.STRING == columnType) {
						// format value with wild cards to get like
						// functionality
						filterBuilder.append(tempNode + ".").append(leftSelectorExpression).append(" =~ ")
								.append("\".*").append(objects.get(0)).append(".*\"");
					} else {
						filterBuilder.append(tempNode + ".").append(leftSelectorExpression).append(" =~ ")
								.append(myFilterFormatted);
					}
				} else {
					if (thisComparator.equals("==")) {
						thisComparator = " = ";
					}
					filterBuilder.append(tempNode + ".").append(leftSelectorExpression).append(" ")
							.append(thisComparator).append(" ").append(myFilterFormatted);
				}
			}
		}

		return filterBuilder;
	}
	//////////////////////////////////// end adding filters
	//////////////////////////////////// ////////////////////////////////////

	//////////////////////////////////// start join
	//////////////////////////////////// ////////////////////////////////////
	public void appendJoin(StringBuilder query) {
		for (Map.Entry<String, Map<String, String>> mapElement : labelMap.entrySet()) {
			String intermediateNode = null;
			String currentLabelName = mapElement.getKey();

			// grab intermediate node from label
			for (Map.Entry<String, String> labelNames : labelAliasMap.entrySet()) {
				String key = labelNames.getKey();
				if (currentLabelName.equals(key)) {
					intermediateNode = labelNames.getValue();
					// query.append(" (" + intermediateNode + ":" +
					// mapElement.getKey() + ")");
					query.append(" ").append(queryNode(intermediateNode, mapElement.getKey()));
					// if more values exist in the labelAliasMap
					if (labelMap.entrySet().iterator().hasNext()) {
						query.append("-[]-");
					}
				}
			}
		}
	}

	public Map<String, Set<String>> generateEdgeMap() {
		Map<String, Set<String>> edgeMap = new Hashtable<>();
		// add the relationships into the edge map
		Set<IRelation> relations = qs.getRelations();
		for (IRelation relationship : relations) {
			if(relationship.getRelationType() == IRelation.RELATION_TYPE.BASIC) {
				BasicRelationship rel = (BasicRelationship) relationship;
				String startNode = rel.getFromConcept();
				String endNode = rel.getToConcept();
	
				Set<String> joinSet = null;
				if (edgeMap.containsKey(startNode)) {
					joinSet = edgeMap.get(startNode);
				} else {
					joinSet = new HashSet<>();
					edgeMap.put(startNode, joinSet);
				}
				joinSet.add(endNode);
			} else {
				logger.info("Cannot process relationship of type: " + relationship.getRelationType());
			}
		}

		return edgeMap;
	}
	
	//////////////////////////////////// end join ////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////

	//////////////////////////////////// start order by //////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	
	private void addOrderBy() {
		// grab the order by and get the corresponding display name for that
		// order by column
		List<IQuerySort> orderByList = ((SelectQueryStruct) this.qs).getCombinedOrderBy();
		if (orderByList == null || orderByList.isEmpty()) {
			return;
		}

		boolean initialized = false;
		StringBuilder builderOrdering = null;
		for (IQuerySort orderBy : orderByList) {
			if(orderBy.getQuerySortType() == IQuerySort.QUERY_SORT_TYPE.COLUMN) {
				QueryColumnOrderBySelector orderBySelector = (QueryColumnOrderBySelector) orderBy;
				String tableName = orderBySelector.getTable();
				String columnName = orderBySelector.getColumn();
				if (columnName.equals(AbstractQueryStruct.PRIM_KEY_PLACEHOLDER)) {
					// we are querying a node I need to get the property
					columnName = getNodeName(tableName);
				}
				ORDER_BY_DIRECTION orderByDir = orderBySelector.getSortDir();
	
				// grab temp table name
				String intermediateNode = labelAliasMap.get(tableName);
	
				if (initialized) {
					builderOrdering.append(", ").append(intermediateNode).append(".").append(columnName);
				} else {
					builderOrdering = new StringBuilder();
					builderOrdering.append(intermediateNode).append(".").append(columnName);
					initialized = true;
				}
	
				if (orderByDir == ORDER_BY_DIRECTION.DESC) {
					builderOrdering.append(" ").append(ORDER_BY_DIRECTION.DESC);
				}
			}
		}

		if (builderOrdering != null) {
			orderBys.append(ORDER_BY).append(" ").append(builderOrdering.toString());
		}
	}
	//////////////////////////////////// end order by
	//////////////////////////////////// ////////////////////////////////////

	/**
	 * Set this boolean to true if you want the interpreter to grab nodes using the
	 * label
	 * 
	 * @param useLabel
	 */
	public void setUseLabel(boolean useLabel) {
		this.useLabel = useLabel;
	}

	/**
	 * This is the method that will query the node based on how the engine has been
	 * defined to query the node
	 * 
	 * @param gt
	 * @param nodeType
	 * @return
	 */
	public String queryNode(String nodeAlias, String node) {
		StringBuilder sb = new StringBuilder();
		// grab the node by the label
		if (useLabel) {
			sb.append("(" + nodeAlias + ":" + node + ")");
		} else {
			// grab the node by a specific property
			sb.append("(" + nodeAlias + " {" + getNodeType(node) + ": '" + node + "'})");
		}
		return sb.toString();
	}

	//////////////////////////////////////////////////////////

	/*
	 * Getters for the type and name of the node
	 * 
	 * If engine has defined a specific value, we will use that
	 */

	protected String getNodeType(String node) {
		return this.typeMap != null && this.typeMap.containsKey(node) ? this.typeMap.get(node) : null;
	}

	protected String getNodeName(String node) {
		return this.nameMap != null && this.nameMap.containsKey(node) ? this.nameMap.get(node) : null;
	}
}
