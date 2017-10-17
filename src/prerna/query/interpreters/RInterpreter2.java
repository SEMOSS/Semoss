package prerna.query.interpreters;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.IMetaData;
import prerna.ds.r.RSyntaxHelper;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryAggregationEnum;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector.ORDER_BY_DIRECTION;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryMathSelector;
import prerna.query.querystruct.selectors.QueryMultiColMathSelector;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.QueryFilter;
import prerna.sablecc2.om.QueryFilter.FILTER_TYPE;
import prerna.util.Utility;

public class RInterpreter2 extends AbstractQueryInterpreter {

	private String dataTableName = null;
	private Map<String, IMetaData.DATA_TYPES> colDataTypes;

	//keep track of the selectors
	private StringBuilder selectorCriteria = new StringBuilder(); 
	// keep track of the filters
	private StringBuilder filterCriteria = new StringBuilder();
	// keep track of group bys
	private StringBuilder groupBys = new StringBuilder();
	//keep track of order bys
	private StringBuilder orderBys = new StringBuilder();
	
	// need to keep track of selectors
	// to make sure the order by's are accurate
	private List<String> validHeaders = new Vector<String>();
	
	@Override
	public String composeQuery() {
		if(this.dataTableName == null) {
			throw new IllegalArgumentException("Please define the table name to use for the r data table query syntax to use");
		}
		if(this.colDataTypes == null) {
			this.colDataTypes = new Hashtable<String, IMetaData.DATA_TYPES>();
		}

		// note, that the join info in the QS has no meaning for a R frame as 
		// we cannot connect across data tables
		addFilters();
		addSelector();
		addGroupBy();

		StringBuilder query = new StringBuilder();
		String tempVarName = "temp" + Utility.getRandomString(10);
		query.append(tempVarName + " <- ");
		
		query.append("unique(")
			.append(this.dataTableName)
			.append("[ ")
			.append(this.filterCriteria.toString())
			.append(", ")
			.append(this.selectorCriteria.toString())
			.append(this.groupBys)
			.append("])");
		
		// get the order by values
		addOrderBy(tempVarName);
		
		// append order by at the end
		String order = this.orderBys.toString();
		if(!order.isEmpty()) {
			query.append(order).append("; ");
		}
		
		
		// we need to convert dates from being integer values
		// to output as dates
		boolean addedColToDateChange = false;
		for(String column : this.colDataTypes.keySet()) {
			if(IMetaData.DATA_TYPES.DATE == this.colDataTypes.get(column)) {
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
		List<IQuerySelector> selectors = qs.getSelectors();
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
			selectorBuilder.append(tempName).append("=").append(processSelector(selector));
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
	 * Note, this returns everything without the alias since this is called again from
	 * the base methods it calls to allow for complex math expressions
	 * @param selector
	 * @return
	 */
	private String processSelector(IQuerySelector selector) {
		IQuerySelector.SELECTOR_TYPE selectorType = selector.getSelectorType();
		if(selectorType == IQuerySelector.SELECTOR_TYPE.CONSTANT) {
			return processConstantSelector((QueryConstantSelector) selector);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			return processColumnSelector((QueryColumnSelector) selector);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.MATH) {
			return processMathSelector((QueryMathSelector) selector);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.MULTI_MATH) {
			return processMultiMathSelector((QueryMultiColMathSelector) selector);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			return processArithmeticSelector((QueryArithmeticSelector) selector);
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

	private String processColumnSelector(QueryColumnSelector selector) {
		return selector.getColumn();
	}
	
	private String processMathSelector(QueryMathSelector selector) {
		IQuerySelector innerSelector = selector.getInnerSelector();
		QueryAggregationEnum math = selector.getMath();
		if(math == QueryAggregationEnum.GROUP_CONCAT) {
			return math.getRSyntax() + "(na.omit(" + processSelector(innerSelector) + "), collapse = \", \")";
		} else if (math == QueryAggregationEnum.UNIQUE_GROUP_CONCAT) {
			return math.getRSyntax() + "(unique((na.omit(" + processSelector(innerSelector) + "))), collapse = \", \")";
		} else if(math == QueryAggregationEnum.COUNT || math == QueryAggregationEnum.UNIQUE_COUNT ) {
			return math.getRSyntax() + "(na.omit(" + processSelector(innerSelector) + "))";
		} else {
			return math.getRSyntax() + "(as.numeric(na.omit(" + processSelector(innerSelector) + ")))";
		}
	}
	
	private String processMultiMathSelector(QueryMultiColMathSelector selector) {
		List<IQuerySelector> innerSelectors = selector.getInnerSelector();
		QueryAggregationEnum math = selector.getMath();
		StringBuilder expression = new StringBuilder();
		expression.append(math.getRSyntax()).append("(");
		int size = innerSelectors.size();
		for(int i = 0; i< size; i++) {
			if(i == 0) {
				expression.append(processSelector(innerSelectors.get(i)));
			} else {
				expression.append(",").append(processSelector(innerSelectors.get(i)));
			}
		}
		expression.append(")");
		return expression.toString();
	}
	
	private String processArithmeticSelector(QueryArithmeticSelector selector) {
		IQuerySelector leftSelector = selector.getLeftSelector();
		IQuerySelector rightSelector = selector.getRightSelector();
		String mathExpr = selector.getMathExpr();
		return "(" + processSelector(leftSelector) + " " + mathExpr + " " + processSelector(rightSelector) + ")";
	}
	
	//////////////////////////////////// end adding selectors /////////////////////////////////////

	//////////////////////////////////// start adding filters /////////////////////////////////////

	private void addFilters() {
		List<QueryFilter> filters = qs.getFilters().getFilters();
		for(QueryFilter filter : filters) {
			if (filterCriteria.length() > 0) {
				filterCriteria.append(" & ");
			}
			NounMetadata leftComp = filter.getLComparison();
			NounMetadata rightComp = filter.getRComparison();
			String thisComparator = filter.getComparator();
			if(thisComparator.equals("<>")) {
				thisComparator = "!=";
			}
			
			FILTER_TYPE fType = QueryFilter.determineFilterType(filter);
			if(fType == FILTER_TYPE.COL_TO_COL) {
				addColToColFilter(leftComp, rightComp, thisComparator);
			} else if(fType == FILTER_TYPE.COL_TO_VALUES) {
				addColToValuesFilter(filter, leftComp, rightComp, thisComparator);
			} else if(fType == FILTER_TYPE.VALUES_TO_COL) {
				// same logic as above, just switch the order and reverse the comparator if it is numeric
				addColToValuesFilter(filter, rightComp, leftComp, QueryFilter.getReverseNumericalComparator(thisComparator));
			} else if(fType == FILTER_TYPE.VALUE_TO_VALUE) {
				// WHY WOULD YOU DO THIS!!!
//				addValueToValueFilter(rightComp, leftComp, thisComparator);
			}
		}
	}

	private void addColToColFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator) {
		String leftColumnSelector = leftComp.getValue().toString();
		String leftColumnName = leftColumnSelector.split("__")[1];
		
		String rightColumnSelector = rightComp.getValue().toString();
		String rightColumnName = rightColumnSelector.split("__")[1];

		if(thisComparator.equals("==")) {
			filterCriteria.append(this.dataTableName).append("$").append(leftColumnName).append(" ")
			.append(" == ").append(this.dataTableName).append("$").append(rightColumnName);
		} else if(thisComparator.equals("!=")) {
			filterCriteria.append("!( ").append(this.dataTableName).append("$").append(leftColumnName).append(" ")
			.append(" == ").append(this.dataTableName).append("$").append(rightColumnName).append(")");
		} else if(thisComparator.equals("?like")) {
			filterCriteria.append(this.dataTableName).append("$").append(leftColumnName).append(" ")
			.append(" %like% ").append(this.dataTableName).append("$").append(rightColumnName);
		} else {
			// these are some math operations
			filterCriteria.append(this.dataTableName).append("$").append(leftColumnName).append(" ")
			.append(thisComparator).append(" ").append(this.dataTableName).append("$").append(rightColumnName);
		}
	}
	
	private void addColToValuesFilter(QueryFilter filter, NounMetadata leftComp, NounMetadata rightComp, String thisComparator) {
		// grab the left column name
		String leftColumnSelector = leftComp.getValue().toString();
		String leftColumnName = leftColumnSelector.split("__")[1];
		
		// grab the objects we are setting up for the comparison
		List<Object> objects = new Vector<Object>();
		// ugh... this is gross
		if(rightComp.getValue() instanceof List) {
			objects.addAll( (List) rightComp.getValue());
		} else {
			objects.add(rightComp.getValue());
		}
		
		boolean multi = false;
		String myFilterFormatted = null;
		// format the objects based on the type of the column
		IMetaData.DATA_TYPES dataType = this.colDataTypes.get(this.dataTableName + "__" + leftColumnName);
		if(objects.size() > 1) {
			multi = true;
			myFilterFormatted = RSyntaxHelper.createRColVec(objects, dataType);
		} else if(IMetaData.DATA_TYPES.DATE != dataType) {
			// dont bother doing this if we have a date
			// since we cannot use "in" with dates
			myFilterFormatted = RSyntaxHelper.formatFilterValue(objects.get(0), dataType);
		}
		
		if(multi) {
			// special processing for date types
			if(IMetaData.DATA_TYPES.DATE == dataType) {
				int size = objects.size();
				if(thisComparator.equals("==")) {
					filterCriteria.append("(");
					for (int i = 0; i < size; i++) {
						filterCriteria.append(this.dataTableName).append("$").append(leftColumnName).append(" == ")
						.append(RSyntaxHelper.formatFilterValue(objects.get(i), IMetaData.DATA_TYPES.DATE));
						if ((i+1) < size) {
							filterCriteria.append(" | ");
						}
					}
					filterCriteria.append(")");
				} else if(thisComparator.equals("!=") | thisComparator.equals("<>")) {
					filterCriteria.append("(");
					for (int i = 0; i < size; i++) {
						filterCriteria.append(this.dataTableName).append("$").append(leftColumnName).append(" != ")
						.append(RSyntaxHelper.formatFilterValue(objects.get(i), IMetaData.DATA_TYPES.DATE));
						if ((i+1) < size) {
							filterCriteria.append(" & ");
						}
					}
					filterCriteria.append(")");
				} else {
					// this will probably break...
					myFilterFormatted = RSyntaxHelper.formatFilterValue(objects.get(0), IMetaData.DATA_TYPES.DATE);
					filterCriteria.append(this.dataTableName).append("$").append(leftColumnName).append(" ").append(thisComparator).append(myFilterFormatted);
				}
			} 
			// now all the other types
			else {
				if(thisComparator.equals("==")) {
					filterCriteria.append(this.dataTableName).append("$").append(leftColumnName).append(" ").append(" %in% ").append(myFilterFormatted);
				} else if(thisComparator.equals("!=") | thisComparator.equals("<>")) {
					filterCriteria.append("!(").append(this.dataTableName).append("$").append(leftColumnName).append(" ").append(" %in% ").append(myFilterFormatted).append(")");
				} else {
					// this will probably break...
					filterCriteria.append(this.dataTableName).append("$").append(leftColumnName).append(" ").append(thisComparator).append(myFilterFormatted);
				}
			}
		} else {
			if(thisComparator.equals("?like")) {
				filterCriteria.append(this.dataTableName).append("$").append(leftColumnName).append(" ").append(" %like% ").append(myFilterFormatted);
			} else {
				filterCriteria.append(this.dataTableName).append("$").append(leftColumnName).append(" ").append(thisComparator).append(" ").append(myFilterFormatted);
			}
		}
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
			if(columnName.equals(QueryStruct2.PRIM_KEY_PLACEHOLDER)) {
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
			orderBys.append("; ").append(tempTableName).append(" <- ").append(tempTableName)
					.append("[order(").append(builderOrdering.toString()).append("),]");
		}
	}

	public void setDataTableName(String dataTableName) {
		this.dataTableName = dataTableName;
	}

	public void setColDataTypes(Map<String, IMetaData.DATA_TYPES> colDataTypes) {
		this.colDataTypes = colDataTypes;
	}

	public StringBuilder getFilterCriteria() {
		return this.filterCriteria;
	}

	public static void main(String[] args) {
		QueryStruct2 qsTest = new QueryStruct2();
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

		QueryFilter filter1 = new QueryFilter(test1, "=", test3);
		qsTest.addFilter(filter1);

		//Vector filterData2 = new Vector<>();
		//filterData2.add(40000000);
		//qs.addFilter("Movie_Budget", ">", filterData2);

		RInterpreter2 rI = new RInterpreter2();
		rI.setQueryStruct(qsTest);

		Map<String, IMetaData.DATA_TYPES> colDataTypes = new Hashtable<String, IMetaData.DATA_TYPES>();
		colDataTypes.put("Title", IMetaData.DATA_TYPES.STRING);
		colDataTypes.put("Other2", IMetaData.DATA_TYPES.STRING);

		rI.setColDataTypes(colDataTypes);

		String query = rI.composeQuery();
		System.out.println(query);
	}

}

