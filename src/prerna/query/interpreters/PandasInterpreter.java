package prerna.query.interpreters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySelector.SELECTOR_TYPE;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector.ORDER_BY_DIRECTION;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PandasInterpreter extends AbstractQueryInterpreter {

	private String frameName = null;
	private String wrapperFrameName = null;
	
	private Map<String, SemossDataType> colDataTypes;
	
	private StringBuilder selectorCriteria;
	private StringBuilder filterCriteria;
	private StringBuilder groupCriteria = new StringBuilder("");
	private StringBuilder aggCriteria = new StringBuilder("");
	private StringBuilder aggCriteria2 = new StringBuilder("");
	private StringBuilder renCriteria = new StringBuilder(".rename(columns={'mean':'Average', 'nunique':'UniqueCount', 'sum':'Sum', 'median':'Median', 'max':'Max', 'min':'Min', 'count':'Count'})");
	private StringBuilder orderBy = new StringBuilder("");
	private StringBuilder orderBy2 = new StringBuilder("");
	private StringBuilder ascending = new StringBuilder("");
	private StringBuilder ascending2 = new StringBuilder("");
	private StringBuilder index2Drop = new StringBuilder("[");
	
	private StringBuilder normalizer = new StringBuilder(".to_dict('split')['data']");
	
	private Map <String, StringBuilder>aggHash = null;
	private Map <String, StringBuilder>aggHash2 = null;
	private Map <String, StringBuilder>orderHash = null;
	
	private Map <String, String> functionMap = null;
	
	Map <String, Boolean> processedSelector = new HashMap<String, Boolean>();
	
	ArrayList <String> aggKeys = new ArrayList<String>();
	
	
	ArrayList <String> headers = null;
	
	// this is the headers being kept on the array list being generated
	ArrayList <String> actHeaders = null;
	
	int groupIndex = 0;
	
	ArrayList <SemossDataType> types= null;
	
	long start = 0;
	long end = 500;
	
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
		aggHash2 = new HashMap<String, StringBuilder>();
		orderHash = new HashMap<String, StringBuilder>();
		orderBy = new StringBuilder("");
		normalizer = new StringBuilder(".to_dict('split')");//['data']");
		ascending = new StringBuilder("");
		
		long limit = 500;
		start = 0 ;
		end = limit;

		if(((SelectQueryStruct) this.qs).getOffset() > 0) {
			start = ((SelectQueryStruct) this.qs).getOffset();
		}
		if(((SelectQueryStruct) this.qs).getLimit() != 0) {
			end = (start + ((SelectQueryStruct) this.qs).getLimit());
		}
	
		// add the filters, it doesn't matter where you add it.
		// add the selectors - the act headers are no longer required since I look at the columns now to generate the remaining pieces
		//
		
		addFilters();
		addSelectors();
		processGroupSelectors();
		genAggString();
		processOrderBy();
		closeAll();
		System.out.println("");
		
		
		query.append(this.wrapperFrameName)
			//.append(".cache['data']")
			//.append(".iloc[")
			//.append(qs.getOffset() + ":" + (qs.getOffset() + qs.getLimit()) + "]")
			//.append(0 + ":" + 500+ "]")
			//.append(this)
			.append(this.filterCriteria)
			.append(this.selectorCriteria)
			.append(addDistinct(((SelectQueryStruct) this.qs).isDistinct()))
			.append(this.groupCriteria)
			//.append(this.aggCriteria2)
			.append(this.aggCriteria2)
			// TODO: need to be more elegant than this
			.append(scalar ? "" : orderBy)
			//.append(orderBy2)
			.append(normalizer);
			//.append(this.renCriteria);
		
		return query.toString();
	}
	
	private String addDistinct(boolean distinct) {
		if(distinct) {
			return ".drop_duplicates()";
		}
		return "";
	}

	public void closeAll()
	{
		boolean aggregate = false;
		
		//t.agg({'Title': 'count'}).rename({'Title': 'count(Title)'}).reset_index().to_dict('split')['data']
		if(this.aggCriteria.toString().length() > 0)
		{
			if(((SelectQueryStruct) this.qs).getGroupBy().size() != 0)
			{
				this.aggCriteria = aggCriteria.append("})").append(addLimitOffset(start, end)).append(".reset_index()");
				this.aggCriteria2 = aggCriteria2.append(")").append(addLimitOffset(start, end)).append(".reset_index()");
				this.renCriteria = renCriteria.append("}).reset_index()");
			}
			if(headers.size() == 1) // it is just getting one single data
			{
				this.aggCriteria = aggCriteria.append("}).reset_index()");
				this.aggCriteria2 = aggCriteria2.append(").reset_index()");
				normalizer = new StringBuilder(".to_dict('split')['data'][0][1]");
				aggCriteria2 = aggCriteria;
				scalar = true;
			}
			aggregate = true;
		}
		
		// if there is agroup by.. this whole thing should be ignored pretty much
		if(this.selectorCriteria.toString().length() > 0 && ((SelectQueryStruct) this.qs).getGroupBy().size() == 0)
		{
			StringBuilder newSelectorCriteria = new StringBuilder(addLimitOffset(start, end) + "[[");
			this.selectorCriteria = newSelectorCriteria.append(this.selectorCriteria).append("]]");
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
		if(orderBy.length() != 0)
			// combine it
			orderBy.append("],").append(ascending).append("])");
		if(orderBy2.length() != 0)
			// combine it
			orderBy2.append("],").append(ascending2).append("])");
		
		//if(aggregate) // swap the order
		//	orderBy = orderBy;
			
	}
	
	private void processOrderBy() {
		List <QueryColumnOrderBySelector> qcos = ((SelectQueryStruct) this.qs).getOrderBy();
		for(int orderIndex = 0; orderIndex < qcos.size(); orderIndex++) {
			String sort = null;
			String alias = qcos.get(orderIndex).getAlias();
			if(alias.length() == 0) {
				alias = qcos.get(orderIndex).getTable();
			}
			ORDER_BY_DIRECTION sortDir = qcos.get(orderIndex).getSortDir();
			if(sortDir == ORDER_BY_DIRECTION.ASC) {
				sort = "True";
			} else {
				sort = "False";
			}
			StringBuilder orderByClause = null;
			if(orderHash.containsKey(alias)) {
				orderByClause = orderHash.get(alias);
			}
			
			if(orderByClause != null) {
				// check if it is aggregate
				// at this point the alias does it
				//addOrder(orderByClause, sort);
				addOrder(new StringBuilder(alias), sort);
				
				// also add the other piece to test
				addOrder2(orderByClause, sort);
			}
		}
		
		//if(!processed)
		//	orderBy = new StringBuilder("");
	}
	
	private void addOrder(StringBuilder curOrder, String asc)
	{
		// I need to find out which are the pieces I need to drop
		if(orderBy.length() == 0)
		{
			orderBy = new StringBuilder(".sort_values([");
			ascending = new StringBuilder("ascending=[");
		}
		else
		{
			orderBy.append(",");
			ascending.append(",");
		}
		
		// add the ascending
		ascending.append(asc);
		
		// add the order by
		orderBy.append("'").append(curOrder).append("'");
		
	}

	private void addOrder2(StringBuilder curOrder, String asc)
	{
		// I need to find out which are the pieces I need to drop
		// get the ordinal value
		//int colIndex = headers.indexOf(curOrder);
		if(orderBy2.length() == 0)
		{
			orderBy2 = new StringBuilder(".sort_index(level=[");
			ascending2 = new StringBuilder("ascending=[");
		}
		else
		{
			orderBy2.append(",");
			ascending2.append(",");
		}
		
		// add the ascending
		ascending2.append(asc);
		
		// add the order by
		orderBy2.append(curOrder);
		
	}

	
	private String addLimitOffset(long start, long end) {
		StringBuilder sb = new StringBuilder();
		sb.append(".iloc[" + start + ":");
		if (end > 0) {
			sb.append(end);
		}
		sb.append("]");
		return sb.toString();
	}
	
	public void addFilters()
	{
		addFilters(qs.getCombinedFilters().getFilters(), this.wrapperFrameName, this.filterCriteria, false);
	}

	// add all the selectors
	public void addSelectors() {
		this.selectorCriteria = new StringBuilder();
		List<IQuerySelector> selectors = qs.getSelectors();
		int size = selectors.size();
		for(int i = 0; i < size; i++) {
			IQuerySelector selector = selectors.get(i);
			String newHeader = null;
			newHeader = processSelector(selector, wrapperFrameName, true, true);
			
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
				StringBuilder builder = new StringBuilder(newHeader);
				newHeader = newHeader.replace("'", "");
				headers.add(selector.getAlias());
				orderHash.put(selector.getAlias(), builder);
				actHeaders.add(newHeader);
				types.add(colDataTypes.get(this.frameName + "__" + newHeader));
				
				/*
				if(index2Drop.length() == 0)
					index2Drop.append("[").append(i);
				else
					index2Drop.append(",").append(i);
				*/
			}
			
		}
		
		//index2Drop.append("]");
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
	
	public void setDataTableName(String frameName, String wrapperFrameName) {
		this.frameName = frameName;
		this.wrapperFrameName = wrapperFrameName;
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
		List <QueryColumnSelector> groupSelectors = ((SelectQueryStruct) this.qs).getGroupBy();
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
	
	
	public Map<String, String> functionMap() {
		return this.functionMap;
	}
	
	
	
	private void genAggString()
	{
		aggCriteria = new StringBuilder("");

		for(int cIndex = 0;cIndex < aggKeys.size();cIndex++)
		{
			String colKey = aggKeys.get(cIndex);
			if(cIndex != 0)
			{
				aggCriteria.append(",");
				aggCriteria2.append(",");
			}
			// I need to replace this with aggHash2
			aggCriteria.append(aggHash.get(colKey)).append("]");
			//aggCriteria.append(aggHash.get(colKey));
			aggCriteria2.append(aggHash2.get(colKey));
		}
		if(aggCriteria.length() > 0)
		{
			aggCriteria = new StringBuilder(".agg({").append(aggCriteria);
			aggCriteria2 = new StringBuilder(".agg(").append(aggCriteria2);
		}
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
		StringBuilder aggBuilder2 = new StringBuilder("");
		
		// I also need to keep track of the alias here so I can use that in the sort later
		// I need to get the alias here
		String aggAlias = selector.getAlias();
		// format is
		// mv.drop_duplicates().groupby(['Genre']).agg(Mango = ('Studio','count')).iloc[0:2000]
		// mango is the name of the alias.. no quotes
		aggBuilder2.append(aggAlias).append("=('").append(columnName).append("' , '").append(pandasFunction).append("')");
		
		
		
		if(aggHash.containsKey(columnName))
		{
			aggBuilder = aggHash.get(columnName);
			aggBuilder.append(",");
		}
		else
			aggBuilder.append("'").append(columnName).append("':[");
		

		aggBuilder.append("'" + pandasFunction +"'");
		
		
		orderHash.put(selector.getAlias(), new StringBuilder("('").append(columnName).append("')"));
		
		headers.add(selector.getAlias());
		aggHash.put(columnName, aggBuilder);
		aggHash2.put(columnName, aggBuilder2);
		
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
		} else if(thisComparator.equals(SEARCH_COMPARATOR)) {
			// some operation
			filterBuilder.append("as.character(").append(lSelector)
			.append(") %like% as.character(").append(rSelector).append(")");
		} else if(thisComparator.equals(NOT_SEARCH_COMPARATOR)) {
			// some operation
			filterBuilder.append("!(as.character(").append(lSelector)
			.append(") %like% as.character(").append(rSelector).append("))");
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
		if(SemossDataType.isNotString(leftDataType) && !thisComparator.equals(SEARCH_COMPARATOR) && !thisComparator.equals(NOT_SEARCH_COMPARATOR) && objects.contains("")) {
			addNullCheck = true;
			objects.remove("");
		}
		
		StringBuilder filterBuilder = new StringBuilder("(");;
		// add the null check now
		if(addNullCheck) {
			// can only work if comparator is == or !=
			if(thisComparator.equals("==")) {
				filterBuilder.append("(").append(wrapperFrameName).append("[").append(leftSelectorExpression).append("]").append(".isna())");
			} else if(thisComparator.equals("!=") || thisComparator.equals("<>")) {
				filterBuilder.append("(~").append(wrapperFrameName).append("[").append(leftSelectorExpression).append("]").append(".isna())");
			}
		}
		
		// if there are other instances as well
		// also add that
		if(!objects.isEmpty()) {
			boolean multi = false;
			String myFilterFormatted = null;
			// format the objects based on the type of the column
			SemossDataType objectsDataType = leftDataType;
			boolean useStringForType = thisComparator.equals(SEARCH_COMPARATOR) || thisComparator.contentEquals(NOT_SEARCH_COMPARATOR);
			if(useStringForType) {
				objectsDataType = SemossDataType.STRING;
			}
			// format the objects based on the type of the column
			if(objects.size() > 1) {
				multi = true;
				// need a similar one for pandas
				myFilterFormatted = PandasSyntaxHelper.createPandasColVec(objects, objectsDataType);
			} else {
				// dont bother doing this if we have a date
				// since we cannot use "in" with dates
				myFilterFormatted = PandasSyntaxHelper.formatFilterValue(objects.get(0), objectsDataType);
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
				if(thisComparator.equals("==")) {
					filterBuilder.append(wrapperFrameName).append("[").append(leftSelectorExpression).append("]").append(".isin").append(myFilterFormatted);
				} else if(thisComparator.equals("!=") | thisComparator.equals("<>")) {
					filterBuilder.append("~").append(wrapperFrameName).append("[").append(leftSelectorExpression).append("]").append(".isin").append(myFilterFormatted);
				} else {
					// this will probably break...
					filterBuilder.append(leftSelectorExpression).append(" ").append(thisComparator).append(myFilterFormatted);
				}
			}
			else {
				// need to see this a bit more when we get here
				// dont know how the other types of comparator are being sent here
				if(thisComparator.equals(SEARCH_COMPARATOR)) {
					if(SemossDataType.STRING == leftDataType) {
						// t[t['Title'].str.upper().str.contains('ALA')]
						filterBuilder.append(wrapperFrameName).append("[").append(leftSelectorExpression).append("].str.contains(").append(myFilterFormatted).append(",case=False)");
					} else if(SemossDataType.DATE == leftDataType) {
						filterBuilder.append(wrapperFrameName).append("[").append(leftSelectorExpression).append("].dt.strftime('%Y-%m-%d').str.contains(").append(myFilterFormatted).append(",case=False)");
					} else if(SemossDataType.TIMESTAMP == leftDataType) {
						filterBuilder.append(wrapperFrameName).append("[").append(leftSelectorExpression).append("].dt.strftime('%Y-%m-%d %H:%M:%s').str.contains(").append(myFilterFormatted).append(",case=False)");
					} else if(SemossDataType.INT == leftDataType || SemossDataType.DOUBLE == leftDataType) {
						filterBuilder.append(wrapperFrameName).append("[").append(leftSelectorExpression).append("].astype('str').str.contains(").append(myFilterFormatted).append(",case=False)");
					} else {
						filterBuilder.append(wrapperFrameName).append("[").append(leftSelectorExpression).append("].str.contains(").append(myFilterFormatted).append(",case=False)");
					}
				} else if(thisComparator.equals(NOT_SEARCH_COMPARATOR)) {
					if(SemossDataType.STRING == leftDataType) {
						// t[t['Title'].str.upper().str.contains('ALA')]
						filterBuilder.append("~").append(wrapperFrameName).append("[").append(leftSelectorExpression).append("].str.contains(").append(myFilterFormatted).append(",case=False)");
					} else if(SemossDataType.DATE == leftDataType) {
						filterBuilder.append("~").append(wrapperFrameName).append("[").append(leftSelectorExpression).append("].dt.strftime('%Y-%m-%d').str.contains(").append(myFilterFormatted).append(",case=False)");
					} else if(SemossDataType.TIMESTAMP == leftDataType) {
						filterBuilder.append("~").append(wrapperFrameName).append("[").append(leftSelectorExpression).append("].dt.strftime('%Y-%m-%d %H:%M:%s').str.contains(").append(myFilterFormatted).append(",case=False)");
					} else if(SemossDataType.INT == leftDataType || SemossDataType.DOUBLE == leftDataType) {
						filterBuilder.append("~").append(wrapperFrameName).append("[").append(leftSelectorExpression).append("].astype('str').str.contains(").append(myFilterFormatted).append(",case=False)");
					} else {
						filterBuilder.append("~").append(wrapperFrameName).append("[").append(leftSelectorExpression).append("].str.contains(").append(myFilterFormatted).append(",case=False)");
					}
				} else {
					filterBuilder.append(wrapperFrameName).append("[").append(leftSelectorExpression).append("]").append(" ").append(thisComparator).append(" ").append(myFilterFormatted);
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
