package prerna.query.interpreters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.ds.r.RSyntaxHelper;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySelector.SELECTOR_TYPE;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PandasInterpreter extends AbstractQueryInterpreter {

	private String dataTableName = null;
	private Map<String, SemossDataType> colDataTypes;
	
	private StringBuilder selectorCriteria;
	private StringBuilder filterCriteria;
	private StringBuilder groupCriteria = new StringBuilder("");
	private StringBuilder aggCriteria = new StringBuilder("");
	private StringBuilder renCriteria = new StringBuilder(".rename(columns={'mean':'Average', 'nunique':'UniqueCount', 'sum':'Sum', 'median':'Median', 'max':'Max', 'min':'Min', 'count':'Count'})");
	private StringBuilder orderBy = new StringBuilder("");
	private StringBuilder normalizer = new StringBuilder(".to_dict('split')['data']");
	
	private Map <String, StringBuilder>aggHash = null;
	private Map <String, StringBuilder>orderHash = null;
	
	private Map <String, String> functionMap = null;
	
	Map <String, Boolean> processedSelector = new HashMap<String, Boolean>();
	
	ArrayList <String> aggKeys = new ArrayList<String>();
	
	
	ArrayList <String> headers = null;
	
	// this is the headers being kept on the array list being generated
	ArrayList <String> actHeaders = null;
	
	int groupIndex = 0;
	
	ArrayList <SemossDataType> types= null;
	
	String start = "0";
	String end = "500";
	
	// need to keep the ordinality of the selectors and match that with the aliases
	ArrayList <String> groupColumns = null;
		
	boolean scalar = false;
	
	public void setDataTypeMap(Map<String, SemossDataType> dataTypeMap)
	{
		this.colDataTypes = dataTypeMap;
	}
	
	public boolean isScalar()
	{
		return scalar;
	}
	
	@Override
	public String composeQuery() {
		StringBuilder query = new StringBuilder();
		// there are a couple of things I need to do here
		// build the selectors separately
		// build the filters separately
		
		// if the filter is empty.. then I can go directly to iloc
		// normalize everything since we are not creating this everytime.. we might as well but we need it at different points
		headers = new ArrayList<String>();
		groupIndex = 0;
		actHeaders = new ArrayList<String>();
		types= new ArrayList<SemossDataType>();
		groupColumns = new ArrayList<String>();
		selectorCriteria = new StringBuilder("");
		groupCriteria = new StringBuilder("");
		aggCriteria = new StringBuilder("");
		renCriteria = new StringBuilder("");
		filterCriteria = new StringBuilder("");
		scalar = false;
		functionMap = new HashMap<String, String>();
		aggHash = new HashMap<String, StringBuilder>();
		aggKeys = new ArrayList<String>();
		orderHash = new HashMap<String, StringBuilder>();
		orderBy = new StringBuilder("");
		normalizer = new StringBuilder(".to_dict('split')");//['data']");
		
		
		long limit = 500;
		start = 0 + "";
		end = limit + "";

		if(qs.getOffset() > 0)
			start = qs.getOffset() + "";
		
		if(qs.getLimit() != 0)
			end = (Integer.parseInt(start) + qs.getLimit()) + "";
		
	
		// add the filters, it doesn't matter where you add it.
		// add the selectors - the act headers are no longer required since I look at the columns now to generate the remaining pieces
		//
		
		addFilters();
		addSelectors();
		processGroupSelectors();
		genAggString();
		processOrderBy();
		closeAll();
		System.out.println("C...");
		
		
		query.append(this.dataTableName)
			//.append(".iloc[")
			//.append(qs.getOffset() + ":" + (qs.getOffset() + qs.getLimit()) + "]")
			//.append(0 + ":" + 500+ "]")
			//.append(this)
			.append(this.filterCriteria)
			.append(this.selectorCriteria)
			.append(this.groupCriteria)
			.append(this.aggCriteria)
			.append(orderBy)
			.append(normalizer);
			//.append(this.renCriteria);
		
		return query.toString();
	}
	
	public void closeAll()
	{
		
		//t.agg({'Title': 'count'}).rename({'Title': 'count(Title)'}).reset_index().to_dict('split')['data']
		if(this.aggCriteria.toString().length() > 0)
		{
			if(qs.getGroupBy().size() != 0)
			{
				this.aggCriteria = aggCriteria.append("})").append(".iloc[" + start + ":" + end + "]").append(".reset_index()");
				this.renCriteria = renCriteria.append("}).reset_index()");
			}
			if(headers.size() == 1) // it is just getting one single data
			{
				this.aggCriteria = aggCriteria.append("}).reset_index()");
				normalizer = new StringBuilder(".to_dict('split')['data'][0][1]");
				scalar = true;
			}
		}
		
		// if there is agroup by.. this whole thing should be ignored pretty much
		if(this.selectorCriteria.toString().length() > 0 && qs.getGroupBy().size() == 0)
		{
			StringBuilder newSelectorCriteria = new StringBuilder(".iloc[" + start + ":" + end + "][[");
			this.selectorCriteria = newSelectorCriteria.append(this.selectorCriteria).append("]].drop_duplicates()");
		}
		else
			this.selectorCriteria = new StringBuilder("");
		
		if(groupCriteria.length() > 0)
			groupCriteria.append("])");
		
		if(filterCriteria.length() > 0)
		{
			filterCriteria = new StringBuilder("[").append(filterCriteria).append("]");
			
			// update the selector only if if there is no agg
			//if(selectorCriteria.length() == 0)
			//	selectorCriteria.append(".drop_duplicates()");
		
		}
		
		
			
		
	}
	
	private void processOrderBy()
	{
		List <QueryColumnOrderBySelector> qcos = qs.getOrderBy();
		boolean processed = false;
		StringBuilder thisOrderBy = new StringBuilder("");
		for(int orderIndex = 0;orderIndex < qcos.size();orderIndex++)
		{
			thisOrderBy = new StringBuilder(".sort_values(");
			String sort = null;
			String alias = qcos.get(orderIndex).getAlias();
			if(alias.length() == 0)
				alias = qcos.get(orderIndex).getTable();
			String sortDir = qcos.get(orderIndex).getSortDirString();
			if(sortDir.equalsIgnoreCase("ASC"))
				sort = "True";
			else
				sort = "False";
			
			StringBuilder orderByClause = null;
			
			if(orderHash.containsKey(alias))
				orderByClause = orderHash.get(alias);
			
			if(orderByClause != null)
			{
				thisOrderBy.append(orderByClause).append(", ascending=").append(sort).append(")");
				processed = true;
				orderBy.append(thisOrderBy);
			}
		}
		
		//if(!processed)
		//	orderBy = new StringBuilder("");
	}
	
	public void addFilters()
	{
		addFilters(qs.getCombinedFilters().getFilters(), this.dataTableName, this.filterCriteria, false);
	}

	// add all the selectors
	public void addSelectors() {
		this.selectorCriteria = new StringBuilder();
		List<IQuerySelector> selectors = qs.getSelectors();
		int size = selectors.size();
		for(int i = 0; i < size; i++) {
			IQuerySelector selector = selectors.get(i);
			String newHeader = null;
			newHeader = processSelector(selector, dataTableName, true, true);
			
			// if this is an aggregator, it needs to accomodated in a different place which is why the processAgg will return an empty string
			// if it is not, go ahead and add it
			// if a selector has been processed for something and there is a groupby only one of it would get added
			if (newHeader.length() > 0)
			{
				if(i == 0) {
					this.selectorCriteria.append(newHeader);
				} else {
					this.selectorCriteria.append(", ").append(newHeader);
				}
				StringBuilder builder = new StringBuilder("[" + newHeader + "]");
				newHeader = newHeader.replace("'", "");
				headers.add(selector.getAlias());
				orderHash.put(selector.getAlias(), builder);
				actHeaders.add(newHeader);
				types.add(colDataTypes.get(newHeader));
			}
		}
	}
	
	
	public String[] getHeaders()
	{
		String [] headerArray = new String[this.headers.size()];
		this.headers.toArray(headerArray);
		return headerArray;
	}

	public SemossDataType[] getTypes()
	{
		SemossDataType [] typeArray = new SemossDataType[this.headers.size()];
		this.types.toArray(typeArray);
		return typeArray;
	}

	private String processSelector(IQuerySelector selector, String tableName, boolean includeTableName, boolean useAlias) {
		SELECTOR_TYPE selectorType = selector.getSelectorType();
		
		if(selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			return processColumnSelector( (QueryColumnSelector) selector);
		}
		// constan is not touched yet
		else if(selectorType == IQuerySelector.SELECTOR_TYPE.CONSTANT) {
			return processConstantSelector((QueryConstantSelector) selector);
		}  else if(selectorType == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
			return processAggSelector((QueryFunctionSelector) selector); //, tableName, includeTableName, useAlias);
		}
		// arithmetic selector is not implemented
		else if(selectorType == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			return processArithmeticSelector((QueryArithmeticSelector) selector, tableName, includeTableName, useAlias);
		} else {
			return null;
		}
	}
	
	private String processColumnSelector(QueryColumnSelector selector) {
		String columnName = selector.getColumn();
		String alias = selector.getAlias();
		
		// just return the column name
		return "'" + alias + "'";
	}
	
	public void setDataTableName(String dataTableName) {
		this.dataTableName = dataTableName;
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
	
	private void processGroupSelectors()
	{
		List <QueryColumnSelector> groupSelectors = qs.getGroupBy();
		for(int sIndex = 0;sIndex < groupSelectors.size();sIndex++)
		{
			QueryColumnSelector qcs = groupSelectors.get(sIndex);
			processGroupSelector(qcs);
		}
	}
	
	private void processGroupSelector(QueryColumnSelector selector)
	{
		//if(!processedSelector.containsKey(selector.getAlias()))
		{
			//if(!aggHash.containsKey(selector.getTable()))
			{
				if(groupCriteria.length() == 0)
					groupCriteria.append(".groupby([");
				else
					groupCriteria.append(",");
			
				groupCriteria.append("'").append(selector.getTable()).append("'");
			}
			
			if(actHeaders.contains(selector.getTable()))
			{
				int index = actHeaders.indexOf(selector.getTable());
				actHeaders.remove(selector.getTable());
				//headers.remove(selector.getTable());
				actHeaders.add(groupIndex, headers.get(index));
				groupIndex++;
			}
			
			//headers.add(groupIndex, selector.getTable());
			
			// we dont know how many groups would it be.. so updating

			if(processedSelector.containsKey(selector.getTable()))
			{
				processedSelector.put(selector.getTable(), Boolean.TRUE);
				headers.add(selector.getTable());
			}
		}
	}
	
	
	public Map functionMap()
	{
		return this.functionMap;
	}
	
	
	
	private void genAggString()
	{
		aggCriteria = new StringBuilder("");
		renCriteria = new StringBuilder("");

		for(int cIndex = 0;cIndex < aggKeys.size();cIndex++)
		{
			String colKey = aggKeys.get(cIndex);
			if(cIndex != 0)
			{
				aggCriteria.append(",");
			}
			aggCriteria.append(aggHash.get(colKey)).append("]");
		}
		if(aggCriteria.length() > 0)
			aggCriteria = new StringBuilder(".agg({").append(aggCriteria);
	}
	
	private String processAggSelector(QueryFunctionSelector selector)
	{
		// if it is using a function.. usually it is an aggregation
		String function = selector.getFunction();
		String columnName = selector.getAllQueryColumns().get(0).getAlias();
		
		System.out.println("Column Name .. >>" + selector.getAllQueryColumns().get(0).getColumn() + "<<>>" + selector.getAllQueryColumns().get(0).getTable());
		
		// you need to get to the column selector and then get the alias
		String pandasFunction = QueryFunctionHelper.convertFunctionToPandasSyntax(function);
		StringBuilder aggBuilder = new StringBuilder("");
		
		// I also need to keep track of the alias here so I can use that in the sort later

		
		
		if(aggHash.containsKey(columnName))
		{
			aggBuilder = aggHash.get(columnName);
			aggBuilder.append(",");
		}
		else
			aggBuilder.append("'").append(columnName).append("':[");
		

		aggBuilder.append("'" + pandasFunction +"'");
		
		
		orderHash.put(selector.getAlias(), new StringBuilder("[('").append(columnName).append("','").append(pandasFunction).append("')]"));
		
		headers.add(selector.getAlias());
		aggHash.put(columnName, aggBuilder);
		
		aggKeys.add(columnName);
		
		// if it is a group concat.. dont add this to actual headers here.. since it will get added during group by
		if(function.equalsIgnoreCase(QueryFunctionHelper.UNIQUE_GROUP_CONCAT))
		{
			actHeaders.add(columnName);
			functionMap.put(pandasFunction+columnName, selector.getAlias());
		}
		else
		{
			actHeaders.add(selector.getAlias());
			// else it will get added by the way of group by clause
			
			functionMap.put(pandasFunction+columnName, selector.getAlias());
		}
			
		
		
		// I can avoid all of this by creating a dataframe and imputing.. but let us see how far we can inline this
		// I am going to assume that this is the same type as header for most operations
		SemossDataType curType = colDataTypes.get(columnName);
		
		// it can also depend on the operation but.. 
		if(curType == SemossDataType.STRING || curType == SemossDataType.BOOLEAN)
			types.add(SemossDataType.INT);
		else
			types.add(curType);
		

		return "";
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
		
		StringBuilder filterBuilder = new StringBuilder("(");;
		// add the null check now
		if(addNullCheck) {
			// can only work if comparator is == or !=
			if(thisComparator.equals("==")) {
				filterBuilder.append("(").append(dataTableName).append("[").append(leftSelectorExpression).append("]").append(" ").append(thisComparator).append(" 'null'").append(")");
				//filterBuilder.append("is.na(").append(leftSelectorExpression).append(") ");
			} else if(thisComparator.equals("!=") || thisComparator.equals("<>")) {
				filterBuilder.append("(").append(dataTableName).append("[").append(leftSelectorExpression).append("]").append(" ").append(thisComparator).append(" 'null'").append(")");
				//filterBuilder.append("!is.na(").append(leftSelectorExpression).append(") ");
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
				// need a similar one for pandas
				myFilterFormatted = PandasSyntaxHelper.createPandasColVec(objects, leftDataType);
			} else {
				// dont bother doing this if we have a date
				// since we cannot use "in" with dates
				myFilterFormatted = PandasSyntaxHelper.formatFilterValue(objects.get(0), leftDataType);
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
						filterBuilder.append(dataTableName).append("[").append(leftSelectorExpression).append("]").append(".isin").append(myFilterFormatted);
					} else if(thisComparator.equals("!=") | thisComparator.equals("<>")) {
						filterBuilder.append("~").append(dataTableName).append("[").append(leftSelectorExpression).append("]").append(".isin").append(myFilterFormatted);
					} else {
						// this will probably break...
						filterBuilder.append(leftSelectorExpression).append(" ").append(thisComparator).append(myFilterFormatted);
					}
				}
			}
			else {
				// need to see this a bit more when we get here
				// dont know how the other types of comparator are being sent here
				if(thisComparator.equals("?like")) {
					if(SemossDataType.STRING == leftDataType) {
						// t[t['Title'].str.upper().str.contains('ALA')]
						filterBuilder.append(dataTableName).append("[").append(leftSelectorExpression).append("].str.contains(").append(myFilterFormatted).append(",case=False)");
					} else {
						filterBuilder.append(dataTableName).append("[").append(leftSelectorExpression).append("].str.contains(").append(myFilterFormatted).append(",case=False)");
					}
				} else {
					filterBuilder.append(dataTableName).append("[").append(leftSelectorExpression).append("]").append(" ").append(thisComparator).append(" ").append(myFilterFormatted);
				}
			}
		}
		
		if(addNullCheck && !objects.isEmpty()) {
			// close due to wrapping
			filterBuilder.append(")");
		}
		
		return filterBuilder.append(")");
	}
	
	//////////////////////////////////// end adding filters /////////////////////////////////////
}
