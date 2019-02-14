package prerna.query.interpreters;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
		if(this.additionalTypes == null) {
			this.additionalTypes = new Hashtable<String, String>();
		}
		boolean isDistinct = ((SelectQueryStruct) this.qs).isDistinct();
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
		query.append(";");
		if(!order.isEmpty()) {
			query.append(order).append("; ");
		}
		
		// we need to convert dates from being integer values
		// to output as dates
		boolean addedColToDateChange = false;
		for (String column : this.additionalTypes.keySet()) {
			if (column.contains("__")) {
				column = column.split("__")[1];
			}
			if (validHeaders.contains(column) && this.additionalTypes.containsKey(this.dataTableName + "__" + column)) {
				addedColToDateChange = true;
				String javaFormat = this.additionalTypes.get(this.dataTableName + "__" + column);
				String[] rFormat = RSyntaxHelper.translateJavaRDateTimeFormat(javaFormat).split("\\|");
				query.append("if (is.Date(" + tempVarName + "$" + column + ") || is.POSIXct(" + tempVarName + "$"
						+ column + ")) {").append("options(digits.secs =" + rFormat[1] + ");")
						.append(tempVarName + "$" + column + " <- format(" + tempVarName + "$" + column + ", format='"
								+ rFormat[0] + "')");
				// handle potential leading zero and second/millisecond delimiter
				String rSubSyntax = getRSubSyntax(javaFormat, rFormat[0]);
				if (rSubSyntax.length() > 0) {
					query.append(" %>% " + rSubSyntax);
				}
				query.append("};");
			}
		}
		
		if(addedColToDateChange) {
			query.append(tempVarName).append(";");
		}

		if(query.length() > 500) {
			logger.debug("R QUERY....  " + query.substring(0,  500) + "...");
		} else {
			logger.debug("R QUERY....  " + query);
		}

		return query.toString();
	}
	
	///////////////////////// helper functions to parse additional datatypes /////////////////////////
	/**
	 * Remove leading zeroes if instructed by the java date/time format and/or 
	 * update delimiter separating seconds and milliseconds, if applicable
	 * @param jFormat
	 * @param rFormat
	 * @return
	 */
	private String getRSubSyntax(String jFormat, String rFormat) {
		StringBuilder sb = new StringBuilder();
		//regex that correspond to values that may potentially not need a leading zero
		String regex = "MdHm" ;
        String firstParam = rFormat;
        String secondParam = "";

        //remove anything that is wrapped in single quotes
        Pattern sqPattern = Pattern.compile("([\"'])(\\\\?.)*?\\1");
        jFormat = sqPattern.matcher(jFormat).replaceAll("");
        
        List<String> matchedRRegex = new ArrayList<String>();
        for (char ch: regex.toCharArray()) {
        	Pattern pattern = Pattern.compile(String.valueOf(ch));
            Matcher matcher = pattern.matcher(jFormat);
            int count = 0;
            while (matcher.find()){
            	count++;
            }
            if (count == 1) {
            	//if the character is matched once in the jFormat, then fetch corresponding r syntax 
            	String rRegex = (String.valueOf(ch).equals("M")) ? RSyntaxHelper.getValueJavaRDatTimeTranslationMap(ch + "1") : 
                	RSyntaxHelper.getValueJavaRDatTimeTranslationMap(String.valueOf(ch));
            	matchedRRegex.add(rRegex);
            }
        }

        if (matchedRRegex.size() > 0) {
        	//if leading zero is absent, then construct the R gsub syntax
        	int firstParamCurIndex = 0;
            int secondParamValue = 1;
            for (int i=0; i < rFormat.length(); i++){
                String c = Character.toString(rFormat.charAt(i));
                if (c.equals("%")) {
                    String substr = (rFormat.substring(i, i+2).equals("%O")) ? rFormat.substring(i, i+3) : rFormat.substring(i, i+2);
                    if (matchedRRegex.contains(substr)){
                        firstParam = firstParam.replaceAll(substr, "0?(.+)");
                        firstParamCurIndex += 6;
                    } else {
                        firstParam = firstParam.replaceAll(substr, "(.*)");
                        firstParamCurIndex += 4;
                    }
                    secondParam += "\\\\" + secondParamValue;
                    secondParamValue++;
                    //increment i appropriately
                    i = (substr.equals("%OS")) ? i+2 : i+1;
                } else {
                	firstParam = firstParam.substring(0, firstParamCurIndex) + "\\\\" + firstParam.substring(firstParamCurIndex);
                	firstParamCurIndex += 3;
                    secondParam += c;
                }
            }
            
            //if minutes is one where we have to handle leading zeros, then need to ensure "00" is left as such 
            if (matchedRRegex.contains("%M")) {
            	sb.append("sub('" + firstParam + "','" + secondParam + "', .) %>% gsub(':0:', ':00:', .)");
            } else {
            	sb.append("sub('" + firstParam + "','" + secondParam + "', .)");
            }
        }
        
        //check if delimiter between seconds and milliseconds needs to be addressed (replaced if not a period)
        int indexSeconds = jFormat.lastIndexOf("s");
        int indexMilliSeconds = jFormat.indexOf("S");
        if (indexSeconds < indexMilliSeconds) {
        	String delimiter = jFormat.substring(indexSeconds + 1,  indexMilliSeconds);
        	if (!delimiter.equals(".")){
        		if (sb.length() > 0) {
        			sb.append(" %>% ");
        		}
        		sb.append("gsub('.([^.]+)$', '\\\\2" + delimiter + "\\\\1', .)");
        	}
        }
		return sb.toString();
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
		
		String lSelector = processSelector(leftSelector, tableName, true, useAlias);
		String rSelector = processSelector(rightSelector, tableName, true, useAlias);
		
		StringBuilder filterBuilder = new StringBuilder();
		if(thisComparator.equals("!=") || thisComparator.equals("<>")) {
			filterBuilder.append("( !(").append(lSelector).append(" == ").append(rSelector)
			// account for NA
			.append(") | ( is.na(").append(lSelector).append(") & !is.na(").append(rSelector)
			.append(") ) | ( !is.na(").append(lSelector).append(") & is.na(").append(rSelector).append(")) )");
		} else if(thisComparator.equals("?like")) {
			// some operation
			filterBuilder.append("as.character(").append(lSelector)
			.append(") %like% as.character(").append(rSelector).append(")");
		} else {
			if(thisComparator.equals("==")) {
				filterBuilder.append("(").append(lSelector).append(" == ").append(rSelector)
				// account for NA
				.append(" | is.na(").append(lSelector).append(") & is.na(").append(rSelector).append(") )");
			} else {
				// other op
				filterBuilder.append(lSelector).append(" ").append(thisComparator)
				.append(" ").append(rSelector);
			}
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
		boolean addNullCheck = objects.contains(null);
		if(addNullCheck) {
			objects.remove(null);
		}
		
		StringBuilder filterBuilder = new StringBuilder();;
		// add the null check now
		if(addNullCheck) {
			// can only work if comparator is == or !=
			if(thisComparator.equals("==")) {
				filterBuilder.append("is.na(").append(leftSelectorExpression).append(") ");
			} else if(thisComparator.equals("!=") || thisComparator.equals("<>")) {
				filterBuilder.append("!is.na(").append(leftSelectorExpression).append(") ");
			}
		}
		
		// if there are other instances as well
		// also add that
		if(!objects.isEmpty()) {
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
			
			// account for bad input
			// example - filtering out empty + null when its a number...
			if(myFilterFormatted.isEmpty()) {
				return filterBuilder;
			}
			
			if(addNullCheck) {
				// we added a null check above
				// we need to wrap 
				filterBuilder.insert(0, "(");
				if(thisComparator.equals("!=") || thisComparator.equals("<>")) {
					filterBuilder.append("& ");
				} else {
					filterBuilder.append("| ");
				}
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
						filterBuilder.append("tolower(as.character(").append(leftSelectorExpression).append(")) %like% tolower(").append(myFilterFormatted).append(")");
					}
				} else {
					filterBuilder.append(leftSelectorExpression).append(" ").append(thisComparator).append(" ").append(myFilterFormatted);
				}
			}
		}
		
		if(addNullCheck && !objects.isEmpty()) {
			// close due to wrapping
			filterBuilder.append(")");
		}
		
		return filterBuilder;
	}
	
	//////////////////////////////////// end adding filters /////////////////////////////////////
	
	private void addGroupBy() {
		List<QueryColumnSelector> groups = ((SelectQueryStruct) this.qs).getGroupBy();
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
		List<QueryColumnOrderBySelector> orderBy = ((SelectQueryStruct) this.qs).getOrderBy();
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
//		SelectQueryStruct qsTest = new SelectQueryStruct();
//		qsTest.addSelector("Title", null);
//		qsTest.addSelector("Other2", null);
//		//qsTest.addSelector("Movie_Budget", null);
//
//		//Vector filterData1 = new Vector<>();
//		//filterData1.add("American Hustle");
//		//filterData1.add("Captain Phillips");
//
//		NounMetadata test1 = new NounMetadata("Title", PixelDataType.COLUMN);
//		List<Object> values = new Vector<Object>();
//		values.add(500);
//		//values.add("string2");
//		//values.add(2.3);
//		NounMetadata test2 = new NounMetadata(values, PixelDataType.CONST_INT);
//		NounMetadata test3 = new NounMetadata("Nominated", PixelDataType.CONST_STRING);
//
//		SimpleQueryFilter filter1 = new SimpleQueryFilter(test1, "=", test3);
//		qsTest.addExplicitFilter(filter1);
//
//		//Vector filterData2 = new Vector<>();
//		//filterData2.add(40000000);
//		//qs.addFilter("Movie_Budget", ">", filterData2);
//
//		RInterpreter rI = new RInterpreter();
//		rI.setQueryStruct(qsTest);
//
//		Map<String, SemossDataType> colDataTypes = new Hashtable<String, SemossDataType>();
//		colDataTypes.put("Title", SemossDataType.STRING);
//		colDataTypes.put("Other2", SemossDataType.STRING);
//
//		rI.setColDataTypes(colDataTypes);
//
//		String query = rI.composeQuery();
//		System.out.println(query);
		
//		String jFormat = "sss:SSS";
		String jFormat = "MM-dd'T'HH:mm:ss.SSS'Z'" ;
		//String jFormat = "M-d'T'HH:mm:ss" ;
		//String jFormat = "M-d'T'HH:mm" ;
		
		String rFormat = "%m-%dT%H:%M:%OSZ";
		
		StringBuilder sb = new StringBuilder();
		//regex that correspond to values that may potentially not need a leading zero
		String regex = "MdHm" ;
        String firstParam = rFormat;
        String secondParam = "";

        //remove anything that is wrapped in single quotes
        Pattern sqPattern = Pattern.compile("([\"'])(\\\\?.)*?\\1");
        jFormat = sqPattern.matcher(jFormat).replaceAll("");
        
        List<String> matchedRRegex = new ArrayList<String>();
        for (char ch: regex.toCharArray()) {
        	Pattern pattern = Pattern.compile(String.valueOf(ch));
            Matcher matcher = pattern.matcher(jFormat);
            int count = 0;
            while (matcher.find()){
            	count++;
            }
            if (count == 1) {
            	//if the character is matched once in the jFormat, then fetch corresponding r syntax 
            	String rRegex = (String.valueOf(ch).equals("M")) ? RSyntaxHelper.getValueJavaRDatTimeTranslationMap(ch + "1") : 
                	RSyntaxHelper.getValueJavaRDatTimeTranslationMap(String.valueOf(ch));
            	matchedRRegex.add(rRegex);
            }
        }

        if (matchedRRegex.size() > 0) {
        	//if leading zero is absent, then construct the R gsub syntax
            int secondParamValue = 1;
            for (int i=0; i < rFormat.length(); i++){
                String c = Character.toString(rFormat.charAt(i));
                if (c.equals("%")) {
                    String substr = (rFormat.substring(i, i+2).equals("%O")) ? rFormat.substring(i, i+3) : rFormat.substring(i, i+2);
                	System.out.println("INDEX: " + i + " ::: " + substr);
                    if (matchedRRegex.contains(substr)){
                        firstParam = firstParam.replaceAll(substr, "0?(.+)");
                    } else {
                        firstParam = firstParam.replaceAll(substr, "(.*)");
                    }
                    secondParam += "\\\\" + secondParamValue;
                    secondParamValue++;
                    //increment i appropriately
                    i = (substr.equals("%OS")) ? i+2 : i+1;
                } else {
                    secondParam += c;
                }
            }
            
            //if minutes is one where we have to handle leading zeros, then need to ensure "00" is left as such 
            if (matchedRRegex.contains("%M")) {
            	sb.append("('" + firstParam + "','" + secondParam + "', COLUMNNAME) %>% gsub(':0:', ':00:', .)");
            } else {
            	sb.append("('" + firstParam + "','" + secondParam + "', COLUMNNAME)");
            }
        }
        
        //get delimiter between seconds and milliseconds, if present, from the java format
        int indexSeconds = jFormat.lastIndexOf("s");
        int indexMilliSeconds = jFormat.indexOf("S");
        if (indexSeconds < indexMilliSeconds) {
        	String delimiter = jFormat.substring(indexSeconds + 1,  indexMilliSeconds);
        	if (!delimiter.equals(".")){
        		sb.append(" %>% gsub('.([^.]+)$', '\\\\2" + delimiter + "\\\\1', .)");
        	}
        }
        String x = sb.toString();
        
        System.out.println(sb.toString()==null);
        System.out.println(sb.length());
        

	}

}

