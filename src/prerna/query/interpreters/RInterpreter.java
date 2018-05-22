package prerna.query.interpreters;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RSyntaxHelper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector.ORDER_BY_DIRECTION;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class RInterpreter extends AbstractQueryInterpreter {

	private String dataTableName = null;
	private Map<String, SemossDataType> colDataTypes;

	//keep track of the selectors
	private StringBuilder selectorCriteria = new StringBuilder(); 
	// keep track of the filters
	private StringBuilder filterCriteria = new StringBuilder();
	private StringBuilder havingFilterCriteria = new StringBuilder();

	// keep track of group bys
	private StringBuilder groupBys = new StringBuilder();
	// keep track of order bys
	private StringBuilder orderBys = new StringBuilder();
	
	// need to keep track of selectors
	// to make sure the order by's are accurate
	private List<String> validHeaders = new Vector<String>();
	
	private List<String> headersToRemove = new Vector<String>();
	
	@Override
	public String composeQuery() {
		if(this.dataTableName == null) {
			throw new IllegalArgumentException("Please define the table name to use for the r data table query syntax to use");
		}
		if(this.colDataTypes == null) {
			this.colDataTypes = new Hashtable<String, SemossDataType>();
		}

		boolean isDistinct = qs.isDistinct();
		// note, that the join info in the QS has no meaning for a R frame as 
		// we cannot connect across data tables
		addFilters(qs.getCombinedFilters().getFilters(), this.dataTableName, this.filterCriteria, false);
		addSelector();
		addGroupBy();

		StringBuilder query = new StringBuilder();
		String tempVarName = "temp" + Utility.getRandomString(10);
		query.append(tempVarName + " <- ");
		
		if(isDistinct) {
			query.append("unique(");
		}
		query.append(this.dataTableName)
			.append("[ ")
			.append(this.filterCriteria.toString())
			.append(", ")
			.append(this.selectorCriteria.toString())
			.append(this.groupBys)
			.append("]");
		if(isDistinct) {
			query.append(")");
		}
		
		// add having filters 
		addFilters(qs.getHavingFilters().getFilters(), tempVarName, this.havingFilterCriteria, true);
		//append having filters
		String having = this.havingFilterCriteria.toString();
		if(!having.isEmpty()) {
			query.append("; ")
			.append(tempVarName)
			.append("<-")
			.append(tempVarName)
			.append("[ ")
			.append(having)
			.append(", ] ");
		}
		
		// get the order by values
		addOrderBy(tempVarName);
		// append order by at the end
		String order = this.orderBys.toString();
		if(!order.isEmpty()) {
			query.append(";").append(order).append("; ");
		}
		
		// we need to convert dates from being integer values
		// to output as dates
		boolean addedColToDateChange = false;
		for(String column : this.colDataTypes.keySet()) {
			SemossDataType thisColType = this.colDataTypes.get(column);
			if(SemossDataType.DATE == thisColType || SemossDataType.TIMESTAMP == thisColType) {
				if(column.contains("__")) {
					column = column.split("__")[1];
				}
				if(validHeaders.contains(column)) {
					addedColToDateChange = true;
					query.append(";")
						.append(tempVarName).append("$").append(column)
						.append("<- as.character(")
						.append(tempVarName).append("$").append(column)
						.append(")");
				}
			}
		}
		
		if(addedColToDateChange) {
			query.append(";").append(tempVarName).append(";");
		}

		if(query.length() > 500) {
			logger.info("R QUERY....  " + query.substring(0,  500) + "...");
		} else {
			logger.info("R QUERY....  " + query);
		}
		return query.toString();
	}
	
	//////////////////////////////////// start adding selectors /////////////////////////////////////

	private void addSelector() {
		StringBuilder selectorBuilder = new StringBuilder("{ ");
		StringBuilder outputNames = new StringBuilder(" ; list(");
		// need a way to remove the primary key selector
		List<IQuerySelector> selectors = qs.getSelectors();
		//selectors = removeFakeSelector(selectors);
		//iterate through to get properties of each selector
		int numSelectors = selectors.size();
		for(int i = 0; i < numSelectors; i++) {
			IQuerySelector selector = selectors.get(i);
			String alias = selector.getAlias();
			if(i >= 1) {
				selectorBuilder.append(" ; ");
				outputNames.append(" , ");
			}
			
			String tempName = "V" + i;
			selectorBuilder.append(tempName).append("=").append(processSelector(selector, this.dataTableName, false, false));
			outputNames.append(alias).append("=").append(tempName);
			
			// also keep track of headers
			// so we know what order by's are valid
			this.validHeaders.add(alias);
		}
		// append selectors + outputs to perform correct calculations + add correct alias
		this.selectorCriteria.append(selectorBuilder).append(outputNames).append(") }");
	}
	
	/**
	 * Method is used to generate the appropriate syntax for each type of selector
	 * Note, this returns everything without the alias (unless we have indicated to use alias) since this is called again from
	 * the base methods it calls to allow for complex math expressions
	 * @param selector
	 * @param tableName
	 * @param includeTableName
	 * @param useAlias
	 * @return
	 */
	private String processSelector(IQuerySelector selector, String tableName, boolean includeTableName, boolean useAlias) {
		if(useAlias) {
			if(includeTableName) {
				return tableName + "$" + selector.getAlias();
			}
			return selector.getAlias();
		}
		IQuerySelector.SELECTOR_TYPE selectorType = selector.getSelectorType();
		if(selectorType == IQuerySelector.SELECTOR_TYPE.CONSTANT) {
			return processConstantSelector((QueryConstantSelector) selector);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			return processColumnSelector((QueryColumnSelector) selector, tableName, includeTableName, useAlias);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
			return processFunctionSelector((QueryFunctionSelector) selector, tableName, includeTableName, useAlias);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			return processArithmeticSelector((QueryArithmeticSelector) selector, tableName, includeTableName, useAlias);
		}
		return null;
	}
	
	private String processConstantSelector(QueryConstantSelector selector) {
		Object constant = selector.getConstant();
		if(constant instanceof Number) {
			return constant.toString();
		} else {
			return "\"" + constant + "\"";
		}
	}

	private String processColumnSelector(QueryColumnSelector selector, String tableName, boolean includeTableName, boolean useAlias) {
		if(includeTableName) {
			if(useAlias) {
				return tableName + "$" + selector.getAlias();
			}
			return tableName + "$" + selector.getColumn();
		}
		if(useAlias) {
			return selector.getAlias();
		}
		return selector.getColumn();
	}
	
	private String processFunctionSelector(QueryFunctionSelector selector, String tableName, boolean includeTableName, boolean useAlias) {
		List<IQuerySelector> innerSelectors = selector.getInnerSelector();
		String function = selector.getFunction();

		StringBuilder expression = new StringBuilder();
		expression.append(QueryFunctionHelper.convertFunctionToRSyntax(function));
		// we auto add some cleaning up for specific functions
		String endExpr = "";
		if(function.equals(QueryFunctionHelper.GROUP_CONCAT)) {
			expression.append("(na.omit(");
			endExpr = "), collapse = \", \")";
		} else if (function.equals(QueryFunctionHelper.UNIQUE_GROUP_CONCAT)) {
			expression.append("(unique((na.omit(");
			endExpr = "))), collapse = \", \")";
		} else if(function.equals(QueryFunctionHelper.COUNT) || function.equals(QueryFunctionHelper.UNIQUE_COUNT) ) {
			expression.append("(na.omit(");
			endExpr = "))";
		} else if(QueryFunctionHelper.determineTypeOfFunction(function).equals("NUMBER")) {
			if(selector.isDistinct()) {
				expression.append("(unique(as.numeric(na.omit(");
				endExpr = "))))";
			} else {
				expression.append("(as.numeric(na.omit(");
				endExpr = ")))";
			}
		} else {
			expression.append("(");
			endExpr = ")";
		}
		
		int size = innerSelectors.size();
		for(int i = 0; i< size; i++) {
			if(i == 0) {
				expression.append(processSelector(innerSelectors.get(i), tableName, includeTableName, false));
			} else {
				expression.append(",").append(processSelector(innerSelectors.get(i), tableName, includeTableName, useAlias));
			}
		}
		expression.append(endExpr);
		return expression.toString();
	}
	
	private String processArithmeticSelector(QueryArithmeticSelector selector, String tableName, boolean includeTableName, boolean useAlias) {
		IQuerySelector leftSelector = selector.getLeftSelector();
		IQuerySelector rightSelector = selector.getRightSelector();
		String mathExpr = selector.getMathExpr();
		return "(" + processSelector(leftSelector, tableName, includeTableName, useAlias) + " " + mathExpr + " " + processSelector(rightSelector, tableName, includeTableName, useAlias) + ")";
	}
	
	//////////////////////////////////// end adding selectors /////////////////////////////////////

	//////////////////////////////////// start adding filters /////////////////////////////////////
	
	public void addFilters(List<IQueryFilter> filters, String tableName, StringBuilder builder, boolean useAlias) {
		for(IQueryFilter filter : filters) {
			StringBuilder filterSyntax = processFilter(filter, tableName, useAlias);
			if(filterSyntax != null) {
				if (builder.length() > 0) {
					builder.append(" & ");
				}
				builder.append(filterSyntax.toString());
			}
		}
	}
	
	private StringBuilder processFilter(IQueryFilter filter, String tableName, boolean useAlias) {
		IQueryFilter.QUERY_FILTER_TYPE filterType = filter.getQueryFilterType();
		if(filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return processSimpleQueryFilter((SimpleQueryFilter) filter, tableName, useAlias);
		} else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return processAndQueryFilter((AndQueryFilter) filter, tableName, useAlias);
		} else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return processOrQueryFilter((OrQueryFilter) filter, tableName, useAlias);
		}
		return null;
	}
	
	private StringBuilder processOrQueryFilter(OrQueryFilter filter, String tableName, boolean useAlias) {
		StringBuilder filterBuilder = new StringBuilder();
		List<IQueryFilter> filterList = filter.getFilterList();
		int numAnds = filterList.size();
		for(int i = 0; i < numAnds; i++) {
			if(i == 0) {
				filterBuilder.append("(");
			} else {
				filterBuilder.append(" | ");
			}
			filterBuilder.append(processFilter(filterList.get(i), tableName, useAlias));
		}
		filterBuilder.append(")");
		return filterBuilder;
	}

	private StringBuilder processAndQueryFilter(AndQueryFilter filter, String tableName, boolean useAlias) {
		StringBuilder filterBuilder = new StringBuilder();
		List<IQueryFilter> filterList = filter.getFilterList();
		int numAnds = filterList.size();
		for(int i = 0; i < numAnds; i++) {
			if(i == 0) {
				filterBuilder.append("(");
			} else {
				filterBuilder.append(" & ");
			}
			filterBuilder.append(processFilter(filterList.get(i), tableName, useAlias));
		}
		filterBuilder.append(")");
		return filterBuilder;
	}
	
	private StringBuilder processSimpleQueryFilter(SimpleQueryFilter filter, String tableName, boolean useAlias) {
		NounMetadata leftComp = filter.getLComparison();
		NounMetadata rightComp = filter.getRComparison();
		String thisComparator = filter.getComparator();
		
		FILTER_TYPE fType = filter.getFilterType();
		if(fType == FILTER_TYPE.COL_TO_COL) {
			return addSelectorToSelectorFilter(leftComp, rightComp, thisComparator, tableName, useAlias);
		} else if(fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToValuesFilter(leftComp, rightComp, thisComparator, tableName, useAlias);
		} else if(fType == FILTER_TYPE.VALUES_TO_COL) {
			// same logic as above, just switch the order and reverse the comparator if it is numeric
			return addSelectorToValuesFilter(rightComp, leftComp, IQueryFilter.getReverseNumericalComparator(thisComparator), tableName, useAlias);
		} else if(fType == FILTER_TYPE.VALUE_TO_VALUE) {
			// WHY WOULD YOU DO THIS!!!
		}
		return null;
	}
	
	/**
	 * Add filter for column to column
	 * @param leftComp
	 * @param rightComp
	 * @param thisComparator
	 */
	private StringBuilder addSelectorToSelectorFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator, String tableName, boolean useAlias) {
		// get the left side
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		IQuerySelector rightSelector = (IQuerySelector) rightComp.getValue();

		/*
		 * Add the filter syntax here once we have the correct physical names
		 */
		
		StringBuilder filterBuilder = new StringBuilder();
		if(thisComparator.equals("!=") || thisComparator.equals("<>")) {
			filterBuilder.append("!( ").append(processSelector(leftSelector, tableName, true, useAlias)).append(" ")
			.append(" == ").append(tableName).append("$").append(processSelector(rightSelector, tableName, true, useAlias)).append(")");
		} else if(thisComparator.equals("?like")) {
			// some operation
			filterBuilder.append("as.character(").append(processSelector(leftSelector, tableName, true, useAlias)).append(") %like% as.character(")
			.append(processSelector(rightSelector, tableName, true, useAlias)).append(")");
		} else {
			// some operation
			filterBuilder.append(processSelector(leftSelector, tableName, true, useAlias)).append(" ").append(thisComparator)
			.append(" ").append(processSelector(rightSelector, tableName, true, useAlias));
		}
		
		return filterBuilder;
	}
	
	private StringBuilder addSelectorToValuesFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator, String tableName, boolean useAlias) {
		// get the left side
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		String leftSelectorExpression = processSelector(leftSelector, tableName, true, useAlias);
		SemossDataType leftDataType = SemossDataType.convertStringToDataType(leftSelector.getDataType());
		
		// if it is null, then we know we have a column
		// need to grab from metadata
		if(leftDataType == null) {
			leftDataType = this.colDataTypes.get(leftSelector.getQueryStructName());
		}
		
		// grab the objects we are setting up for the comparison
		List<Object> objects = new Vector<Object>();
		// ugh... this is gross
		if(rightComp.getValue() instanceof List) {
			objects.addAll( (List) rightComp.getValue());
		} else {
			objects.add(rightComp.getValue());
		}
		
		// need to account for null inputs
		boolean addNullCheck = false;
		if(objects.contains(null)) {
			addNullCheck = true;
			objects.remove(null);
		}
		
		StringBuilder filterBuilder = null;
		// add the null check now
		if(addNullCheck) {
			// can only work if comparator is == or !=
			if(thisComparator.equals("==")) {
				filterBuilder = new StringBuilder();
				filterBuilder.append("is.na(").append(leftSelectorExpression).append(") ");
			} else if(thisComparator.equals("!=") || thisComparator.equals("<>")) {
				filterBuilder = new StringBuilder();
				filterBuilder.append("!is.na(").append(leftSelectorExpression).append(") ");
			}
		}
		
		// if there are other instances as well
		// also add that
		if(!objects.isEmpty()) {
			if(filterBuilder == null) {
				filterBuilder = new StringBuilder();
			} else {
				// we added a null check above
				filterBuilder.append("| ");
			}
		
			boolean multi = false;
			String myFilterFormatted = null;
			// format the objects based on the type of the column
			if(objects.size() > 1) {
				multi = true;
				myFilterFormatted = RSyntaxHelper.createRColVec(objects, leftDataType);
			} else {
				// dont bother doing this if we have a date
				// since we cannot use "in" with dates
				myFilterFormatted = RSyntaxHelper.formatFilterValue(objects.get(0), leftDataType);
			}
					
			if(multi) {
				// special processing for date types
				if(SemossDataType.DATE == leftDataType) {
					int size = objects.size();
					if(thisComparator.equals("==")) {
						filterBuilder.append("(");
						for (int i = 0; i < size; i++) {
							filterBuilder.append(leftSelectorExpression).append(" == ")
									.append(RSyntaxHelper.formatFilterValue(objects.get(i), SemossDataType.DATE));
							if ((i+1) < size) {
								filterBuilder.append(" | ");
							}
						}
						filterBuilder.append(")");
					} else if(thisComparator.equals("!=") | thisComparator.equals("<>")) {
						filterBuilder.append("(");
						for (int i = 0; i < size; i++) {
							filterBuilder.append(leftSelectorExpression).append(" != ")
									.append(RSyntaxHelper.formatFilterValue(objects.get(i), SemossDataType.DATE));
							if ((i+1) < size) {
								filterBuilder.append(" & ");
							}
						}
						filterBuilder.append(")");
					} else {
						// this will probably break...
						myFilterFormatted = RSyntaxHelper.formatFilterValue(objects.get(0), SemossDataType.DATE);
						filterBuilder.append(leftSelectorExpression).append(" ").append(thisComparator).append(myFilterFormatted);
					}
				} 
				// now all the other types
				else {
					if(thisComparator.equals("==")) {
						filterBuilder.append(leftSelectorExpression).append(" ").append(" %in% ").append(myFilterFormatted);
					} else if(thisComparator.equals("!=") | thisComparator.equals("<>")) {
						filterBuilder.append("!(").append(leftSelectorExpression).append(" ").append(" %in% ").append(myFilterFormatted).append(")");
					} else {
						// this will probably break...
						filterBuilder.append(leftSelectorExpression).append(" ").append(thisComparator).append(myFilterFormatted);
					}
				}
			} else {
				if(thisComparator.equals("?like")) {
					if(SemossDataType.STRING == leftDataType) {
						filterBuilder.append("tolower(").append(leftSelectorExpression).append(") %like% tolower(").append(myFilterFormatted).append(")");
					} else {
						filterBuilder.append("tolower(as.character(").append(leftSelectorExpression).append(")) %like% tolower(\"").append(myFilterFormatted).append("\")");
					}
				} else {
					filterBuilder.append(leftSelectorExpression).append(" ").append(thisComparator).append(" ").append(myFilterFormatted);
				}
			}
		}
		return filterBuilder;
	}
	
	//////////////////////////////////// end adding filters /////////////////////////////////////
	
	private void addGroupBy() {
		List<QueryColumnSelector> groups = qs.getGroupBy();
		if(groups == null || groups.isEmpty()) {
			return;
		}
		
		int numGroups = groups.size();
		this.groupBys.append(" , by = list(");
		for(int i = 0; i < numGroups; i++) {
			if(i >= 1) {
				groupBys.append(",");
			}
			groupBys.append(groups.get(i).getColumn());
		}
		this.groupBys.append(")");
 	}

	
	private void addOrderBy(String tempTableName) {
		//grab the order by and get the corresponding display name for that order by column
		List<QueryColumnOrderBySelector> orderBy = qs.getOrderBy();
		if (orderBy == null || orderBy.isEmpty()) {
			return;
		}
		
		boolean initialized = false;
		StringBuilder builderOrdering = null;
		for(QueryColumnOrderBySelector orderBySelector : orderBy) {
			String tableName = orderBySelector.getTable();
			String columnName = orderBySelector.getColumn();
			ORDER_BY_DIRECTION orderByDir = orderBySelector.getSortDir();
			
			String orderByName = null;
			if(columnName.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
				orderByName = tableName;
			} else {
				orderByName = columnName;
			}
			
			if(!this.validHeaders.contains(orderByName)) {
				// not a valid order by column based on what data is being 
				// return, so just continue
				continue;
			}
			
			if(initialized) {
				builderOrdering.append(",");
			} else {
				builderOrdering = new StringBuilder();
				initialized = true;
			}
			
			if(orderByDir == ORDER_BY_DIRECTION.DESC) {
				builderOrdering.append("-");
			}
			builderOrdering.append(tempTableName).append("$").append(orderByName);
		}
		
		if(builderOrdering != null) {
			orderBys.append(tempTableName).append(" <- ").append(tempTableName)
					.append("[order(").append(builderOrdering.toString()).append("),]");
		}
	}

	public void setDataTableName(String dataTableName) {
		this.dataTableName = dataTableName;
	}

	public void setColDataTypes(Map<String, SemossDataType> colDataTypes) {
		this.colDataTypes = colDataTypes;
	}

	public StringBuilder getFilterCriteria() {
		return this.filterCriteria;
	}
	
	public StringBuilder getHavingFilterCriteria() {
		return this.havingFilterCriteria;
	}
	
	public void addHeaderToRemove(String header)
	{
		headersToRemove.add(header);
	}

	public static void main(String[] args) {
		SelectQueryStruct qsTest = new SelectQueryStruct();
		qsTest.addSelector("Title", null);
		qsTest.addSelector("Other2", null);
		//qsTest.addSelector("Movie_Budget", null);

		//Vector filterData1 = new Vector<>();
		//filterData1.add("American Hustle");
		//filterData1.add("Captain Phillips");

		NounMetadata test1 = new NounMetadata("Title", PixelDataType.COLUMN);
		List<Object> values = new Vector<Object>();
		values.add(500);
		//values.add("string2");
		//values.add(2.3);
		NounMetadata test2 = new NounMetadata(values, PixelDataType.CONST_INT);
		NounMetadata test3 = new NounMetadata("Nominated", PixelDataType.CONST_STRING);

		SimpleQueryFilter filter1 = new SimpleQueryFilter(test1, "=", test3);
		qsTest.addExplicitFilter(filter1);

		//Vector filterData2 = new Vector<>();
		//filterData2.add(40000000);
		//qs.addFilter("Movie_Budget", ">", filterData2);

		RInterpreter rI = new RInterpreter();
		rI.setQueryStruct(qsTest);

		Map<String, SemossDataType> colDataTypes = new Hashtable<String, SemossDataType>();
		colDataTypes.put("Title", SemossDataType.STRING);
		colDataTypes.put("Other2", SemossDataType.STRING);

		rI.setColDataTypes(colDataTypes);

		String query = rI.composeQuery();
		System.out.println(query);
	}

}

