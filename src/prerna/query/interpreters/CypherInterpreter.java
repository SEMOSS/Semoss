package prerna.query.interpreters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RSyntaxHelper;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySelector.SELECTOR_TYPE;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CypherInterpreter extends AbstractQueryInterpreter {

	// keep track of the selectors
	private StringBuilder selectorCriteria;

	// keep track of the filters
	private StringBuilder filterCriteria = new StringBuilder();

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
	private static final String AS = "AS";
	

	@Override
	public String composeQuery() {
		if (this.qs instanceof HardSelectQueryStruct) {
			return ((HardSelectQueryStruct) this.qs).getQuery();
		}
		
		StringBuilder query = new StringBuilder();
		labelMap = new HashMap<String, Map<String, String>>();
		labelAliasMap = new HashMap<String, String>();
		boolean joinIsActive = false;

		if(this.colDataTypes == null) {
			this.colDataTypes = new Hashtable<String, SemossDataType>();
		}

		query.append(MATCH);	
		// adds selectors to map for reference guidance
		addSelectors();
		// generate intermediate nodes for Labels
		generateIntermediateNodes();
		// loops through map and appends selectors
		appendSelectors(query, joinIsActive);
		addFilters(qs.getCombinedFilters().getFilters(), this.filterCriteria, false);

		if (this.filterCriteria.length() > 0) {
			query.append(WHERE + " " + this.filterCriteria);
		}
		query.append(" " + RETURN + " " + this.selectorCriteria);

		// return cypher query
		return query.toString();
	}

	//////////////////////////////////// start adding selectors /////////////////////////////////////

	public void appendSelectors(StringBuilder query, boolean join) {
		Map<String, Set<String>> edgeMap = generateEdgeMap();
		Set<String> nodesDefined = new HashSet<>();
		// add joined nodes first
		List<String> joinValues = new ArrayList<>();
		List<String> selectorList = new ArrayList<>();
		for(String startNode: edgeMap.keySet()) {
			// format start node
			String startNodeAlias = labelAliasMap.get(startNode);
			String cq = "(" + startNodeAlias + ":" + startNode + ")";
			Set<String> visitNodes = edgeMap.get(startNode);
			for(String endNode : visitNodes) {
				String endNodeAlias = labelAliasMap.get(endNode);
				String queryVisit = "(" + endNodeAlias + ":" + endNode + ")";
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
				String queryNode = "(" + intermediateNode + ": " + node + ")";
				joinValues.add(queryNode);
			}
			Map<String, String> nodeProps = labelMap.get(node);
			for (String property : nodeProps.keySet()) {
				String propertyAlias = nodeProps.get(property);
				selectorList.add(intermediateNode + "." + property + " " + AS + " " + propertyAlias +" ");
			}
		}
		
		// append joins to query
		for (int i= 0; i < joinValues.size(); i++) {
			query.append(joinValues.get(i));
			if (i < joinValues.size()-1) {
				query.append(", ");
			}
		}
		
		// append Return/Select criteria
		for (int i= 0; i < selectorList.size(); i++) {
			selectorCriteria.append(selectorList.get(i));
			if (i < selectorList.size()-1) {
				selectorCriteria.append(", ");
			}
		}
	}

	// loop through the label map and generate intermediate nodes for all added selectors
	public String generateIntermediateNodes() {
		for (Map.Entry<String, Map<String, String>> mapElement : labelMap.entrySet()) {
			// insert label and random character in map for tracking
			String intermediateNode = this.getRandomVariable();
			labelAliasMap.put(mapElement.getKey(), intermediateNode);
		}
		return null;
	}

	// add selectors into a map
	public void addSelectors() {
		this.selectorCriteria = new StringBuilder();
		List<IQuerySelector> selectors = qs.getSelectors();
		int size = selectors.size();
		for (int i = 0; i < size; i++) {
			IQuerySelector selector = selectors.get(i);
			String[] extractedPropertyNames = processSelector(selector);
			String labelName = extractedPropertyNames[0];
			String originalPropertyName = extractedPropertyNames[1];
			String aliasPropertyName = extractedPropertyNames[2];
			Map<String, String> propertyMap = new HashMap<String, String>();
			// TODO why is this here?
			if (originalPropertyName.length() > 0) {
				// if property exists just add that property name and alias to the map
				if (labelMap.containsKey(labelName)) {
					Map<String, String> existingPropertyMap = labelMap.get(labelName);
					existingPropertyMap.put(originalPropertyName, aliasPropertyName);
					labelMap.put(labelName, existingPropertyMap);
				} else { // property doesn't exist create new
					propertyMap.put(originalPropertyName, aliasPropertyName);
					labelMap.put(labelName, propertyMap);
				}
			}
		}
	}

	private String[] processSelector(IQuerySelector selector) {
		SELECTOR_TYPE selectorType = selector.getSelectorType();

		/*if(selectorType == IQuerySelector.SELECTOR_TYPE.CONSTANT) {
			return processConstantSelector((QueryConstantSelector) selector);
		} else*/ if(selectorType == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			return processColumnSelector((QueryColumnSelector) selector);
		} /*else if(selectorType == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
			return processFunctionSelector((QueryFunctionSelector) selector, tableName, includeTableName, useAlias);
			} else if(selectorType == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			return processArithmeticSelector((QueryArithmeticSelector) selector, tableName, includeTableName, useAlias);
			}*/

		return null;
	}

	private String[] processColumnSelector(QueryColumnSelector selector) {
		String label = selector.getTable();
		String columnName = selector.getColumn();
		String alias = selector.getAlias();

		return new String[] { label, columnName, alias };
	}

	private String processConstantSelector(QueryConstantSelector selector) {
		Object constant = selector.getConstant();
		if(constant instanceof Number) {
			return constant.toString();
		} else {
			return "\"" + constant + "\"";
		}
	}

	private String processFunctionSelector(QueryFunctionSelector selector, String tableName, boolean includeTableName, boolean useAlias) {
		List<IQuerySelector> innerSelectors = selector.getInnerSelector();
		String function = selector.getFunction();

		StringBuilder expression = new StringBuilder();
		expression.append(QueryFunctionHelper.convertFunctionToRSyntax(function));
		// we auto add some cleaning up for specific functions
		StringBuilder endExpr = new StringBuilder();
		if(function.equals(QueryFunctionHelper.GROUP_CONCAT)) {
			expression.append("(na.omit(");
			endExpr.append("), collapse = \", \")");
		} else if (function.equals(QueryFunctionHelper.UNIQUE_GROUP_CONCAT)) {
			expression.append("(unique((na.omit(");
			endExpr.append("))), collapse = \", \")");
		} else if(function.equals(QueryFunctionHelper.COUNT) || function.equals(QueryFunctionHelper.UNIQUE_COUNT) ) {
			expression.append("(na.omit(");
			endExpr.append("))");
		} else {
			// if we have a non-defined type of function
			// we need to account for additional params
			List<Object[]> additionalParams = selector.getAdditionalFunctionParams();
			for(int i = 0; i < additionalParams.size(); i++) {
				endExpr.append(",");
				Object[] param = additionalParams.get(i);
				String name = param[0].toString();
				if(!name.equals("noname")) {
					endExpr.append(name).append("=");
				}
				for(int j = 1; j < param.length; j++) {
					if(j > 1) {
						endExpr.append(",");
					}
					endExpr.append(param[j]);
				}
			}
			
			if(QueryFunctionHelper.determineTypeOfFunction(function).equals("NUMBER")) {
				if(selector.isDistinct()) {
					expression.append("(unique(as.numeric(na.omit(");
					endExpr.insert(0, ")))");
					endExpr.append(")");
				} else {
					expression.append("(as.numeric(na.omit(");
					endExpr.insert(0, "))");
					endExpr.append(")");
				}
			} else {
				expression.append("(");
				endExpr.append(")");
			}
		}
		
		int size = innerSelectors.size();
		for(int i = 0; i< size; i++) {
			if(i == 0) {
				expression.append(processSelector(innerSelectors.get(i)));
			} else {
				expression.append(",").append(processSelector(innerSelectors.get(i)));
			}
		}
		expression.append(endExpr);
		return expression.toString();
	}

	private String processArithmeticSelector(QueryArithmeticSelector selector, String tableName, boolean includeTableName, boolean useAlias) {
		IQuerySelector leftSelector = selector.getLeftSelector();
		IQuerySelector rightSelector = selector.getRightSelector();
		String mathExpr = selector.getMathExpr();
		return "(" + processSelector(leftSelector) + " " + mathExpr + " " + processSelector(rightSelector) + ")";
	}

	//////////////////////////////////// end adding selectors/////////////////////////////////////

	//////////////////////////////////// start adding filters/////////////////////////////////////

	public void addFilters(List<IQueryFilter> filters, StringBuilder builder, boolean useAlias) {
		for (IQueryFilter filter : filters) {
			StringBuilder filterSyntax = processFilter(filter, useAlias);
			if (filterSyntax != null) {
				if (builder.length() > 0) {
					builder.append(" & ");
				}
				builder.append(filterSyntax.toString());
			}
		}
	}

	private StringBuilder processFilter(IQueryFilter filter, boolean useAlias) {
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
				filterBuilder.append(" | ");
			}
			filterBuilder.append(processFilter(filterList.get(i), useAlias));
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
				filterBuilder.append(" & ");
			}
			filterBuilder.append(processFilter(filterList.get(i), useAlias));
		}
		filterBuilder.append(")");
		return filterBuilder;
	}

	private StringBuilder processSimpleQueryFilter(SimpleQueryFilter filter, boolean useAlias) {
		NounMetadata leftComp = filter.getLComparison();
		NounMetadata rightComp = filter.getRComparison();
		String thisComparator = filter.getComparator();

		FILTER_TYPE fType = filter.getFilterType();
		if (fType == FILTER_TYPE.COL_TO_COL) {
			return addSelectorToSelectorFilter(leftComp, rightComp, thisComparator, useAlias);
		} else if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToValuesFilter(leftComp, rightComp, thisComparator, useAlias);
		} else if (fType == FILTER_TYPE.VALUES_TO_COL) {
			// same logic as above, just switch the order and reverse the comparator if it
			// is numeric
			return addSelectorToValuesFilter(rightComp, leftComp,
					IQueryFilter.getReverseNumericalComparator(thisComparator), useAlias);
		} else if (fType == FILTER_TYPE.VALUE_TO_VALUE) {
			// WHY WOULD YOU DO THIS!!!
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
		String lSelector = processSelector(leftSelector)[1];
		String rSelector = processSelector(rightSelector)[1];
		StringBuilder filterBuilder = new StringBuilder();

		if (thisComparator.equals("!=") || thisComparator.equals("<>")) {
			filterBuilder.append("( !(").append(lSelector).append(" = ").append(rSelector)
					// account for NA
					.append(") | ( is.na(").append(lSelector).append(") & !is.na(").append(rSelector)
					.append(") ) | ( !is.na(").append(lSelector).append(") & is.na(").append(rSelector).append(")) )");
		} else if (thisComparator.equals("?like")) {
			// some operation
			filterBuilder.append("as.character(").append(lSelector).append(") %like% as.character(").append(rSelector)
					.append(")");
		} else {
			if (thisComparator.equals("==")) {
				filterBuilder.append("(").append(tempNode + ".").append(lSelector).append(" = ").append(rSelector)
						// account for NA
						.append(" | is.na(").append(lSelector).append(") & is.na(").append(rSelector).append(") )");
			} else {
				// other op
				filterBuilder.append(tempNode + ".").append(lSelector).append(" ").append(thisComparator).append(" ").append(rSelector);
			}
		}

		return filterBuilder;
	}

	private StringBuilder addSelectorToValuesFilter(NounMetadata leftComp, NounMetadata rightComp,
			String thisComparator, boolean useAlias) {
		// get the left side
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		QueryColumnSelector selector = (QueryColumnSelector) leftSelector;
		String actualLabelName = selector.getTable();
		String tempNode = labelAliasMap.get(actualLabelName);

		String leftSelectorExpression = processSelector(leftSelector)[1];
		SemossDataType leftDataType = SemossDataType.convertStringToDataType(leftSelector.getDataType());

		// if it is null, then we know we have a column
		// need to grab from metadata
		if (leftDataType == null) {
			leftDataType = this.colDataTypes.get(leftSelector.getQueryStructName());
		}

		// grab the objects we are setting up for the comparison
		List<Object> objects = new Vector<Object>();
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

		StringBuilder filterBuilder = new StringBuilder();

		// add the null check now
		if (addNullCheck) {
			// can only work if comparator is == or !=
			if (thisComparator.equals("==")) {
				filterBuilder.append("is.na(").append(leftSelectorExpression).append(") ");
			} else if (thisComparator.equals("!=") || thisComparator.equals("<>")) {
				filterBuilder.append("!is.na(").append(leftSelectorExpression).append(") ");
			}
		}

		// if there are other instances as well
		// also add that
		if (!objects.isEmpty()) {
			boolean multi = false;
			String myFilterFormatted = null;
			// format the objects based on the type of the column
			if (objects.size() > 1) {
				multi = true;
				myFilterFormatted = RSyntaxHelper.createRColVec(objects, leftDataType);
			} else {
				// dont bother doing this if we have a date
				// since we cannot use "in" with dates
				myFilterFormatted = RSyntaxHelper.formatFilterValue(objects.get(0), leftDataType);
			}

			// account for bad input
			// example - filtering out empty + null when its a number...
			if (myFilterFormatted.isEmpty()) {
				return filterBuilder;
			}

			if (addNullCheck) {
				// we added a null check above
				// we need to wrap
				filterBuilder.insert(0, "(");
				if (thisComparator.equals("!=") || thisComparator.equals("<>")) {
					filterBuilder.append("& ");
				} else {
					filterBuilder.append("| ");
				}
			}

			if (multi) {
				// special processing for date types
				if (SemossDataType.DATE == leftDataType) {
					int size = objects.size();
					if (thisComparator.equals("==")) {
						filterBuilder.append("(");
						for (int i = 0; i < size; i++) {
							filterBuilder.append(tempNode + ".").append(leftSelectorExpression).append(" = ")
									.append(RSyntaxHelper.formatFilterValue(objects.get(i), SemossDataType.DATE));
							if ((i + 1) < size) {
								filterBuilder.append(" | ");
							}
						}
						filterBuilder.append(")");
					} else if (thisComparator.equals("!=") | thisComparator.equals("<>")) {
						filterBuilder.append("(");
						for (int i = 0; i < size; i++) {
							filterBuilder.append(tempNode + ".").append(leftSelectorExpression).append(" != ")
									.append(RSyntaxHelper.formatFilterValue(objects.get(i), SemossDataType.DATE));
							if ((i + 1) < size) {
								filterBuilder.append(" & ");
							}
						}
						filterBuilder.append(")");
					} else {
						// this will probably break...
						myFilterFormatted = RSyntaxHelper.formatFilterValue(objects.get(0), SemossDataType.DATE);
						filterBuilder.append(tempNode + ".").append(leftSelectorExpression).append(" ").append(thisComparator)
								.append(myFilterFormatted);
					}
				}
				// now all the other types
				else {
					if (thisComparator.equals("==")) {
						filterBuilder.append(tempNode + ".").append(leftSelectorExpression).append(" ").append(" %in% ")
								.append(myFilterFormatted);
					} else if (thisComparator.equals("!=") | thisComparator.equals("<>")) {
						filterBuilder.append(tempNode + ".").append("!(").append(leftSelectorExpression).append(" ").append(" %in% ")
								.append(myFilterFormatted).append(")");
					} else {
						// this will probably break...
						filterBuilder.append(tempNode + ".").append(leftSelectorExpression).append(" ").append(thisComparator)
								.append(myFilterFormatted);
					}
				}
			} else {
				if (thisComparator.equals("?like")) {
					if (SemossDataType.STRING == leftDataType) {
						filterBuilder.append(tempNode + ".").append("tolower(").append(leftSelectorExpression).append(") %like% tolower(")
								.append(myFilterFormatted).append(")");
					} else {
						filterBuilder.append(tempNode + ".").append("tolower(as.character(").append(leftSelectorExpression)
								.append(")) %like% tolower(").append(myFilterFormatted).append(")");
					}
				} else {
					if (thisComparator.equals("==")) {
						thisComparator = " = ";
					}
					filterBuilder.append(tempNode + ".").append(leftSelectorExpression).append(" ").append(thisComparator).append(" ")
							.append(myFilterFormatted);
				}
			}
		}

		if (addNullCheck && !objects.isEmpty()) {
			// close due to wrapping
			filterBuilder.append(")");
		}

		return filterBuilder;
	}

	//////////////////////////////////// end adding filters /////////////////////////////////////

	////////////////////////////////////start join /////////////////////////////////////
	public void appendJoin(StringBuilder query) {
		// (o:Order)-[]->(p:Product)
		Map<String, Set<String>> edgeMap = generateEdgeMap();

		for (Map.Entry<String, Map<String, String>> mapElement : labelMap.entrySet()) {
			String intermediateNode = null;
			String currentLabelName = mapElement.getKey();

			// grab intermediate node from label
			for (Map.Entry<String, String> labelNames: labelAliasMap.entrySet()) {
				String key = labelNames.getKey();
				if (currentLabelName.equals(key)) {
					intermediateNode = labelNames.getValue();
					query.append(" (" + intermediateNode + ":" + mapElement.getKey() + ")");
					// if more values exist in the labelAliasMap
					if (labelMap.entrySet().iterator().hasNext()) {
						query.append("-[]-");
					}
				}
			}
		}
	}

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

		return edgeMap;
	}

	//////////////////////////////////// end join /////////////////////////////////////

	//////////////////////////////////// helper functions /////////////////////////////////////
	public String getRandomVariable() {
		int leftLimit = 97; // letter 'a'
		int rightLimit = 122; // letter 'z'
		int targetStringLength = 10;
		Random random = new Random();
		StringBuilder buffer = new StringBuilder(targetStringLength);
		for (int i = 0; i < targetStringLength; i++) {
			int randomLimitedInt = leftLimit + (int) (random.nextFloat() * (rightLimit - leftLimit + 1));
			buffer.append((char) randomLimitedInt);
		}

		return buffer.toString();
	}
}
