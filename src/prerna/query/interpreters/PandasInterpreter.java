package prerna.query.interpreters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.ds.py.PyTranslator;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.BetweenQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySelector.SELECTOR_TYPE;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector.ORDER_BY_DIRECTION;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class PandasInterpreter extends AbstractQueryInterpreter {

	private String frameName = null;
	private String wrapperFrameName = null;
	private String swifter = "";
	private String exp = ""; // says if this feature is experimental
	
	private Map<String, SemossDataType> colDataTypes;
	
	private StringBuilder selectorCriteria;
	private StringBuilder filterCriteria;
	private StringBuilder havingCriteria;
	private StringBuilder groupCriteria = new StringBuilder("");
	private StringBuilder aggCriteria = new StringBuilder("");
	private StringBuilder aggCriteria2 = new StringBuilder("");
	private StringBuilder renCriteria = new StringBuilder(".rename(columns={'mean':'Average', 'nunique':'UniqueCount', 'sum':'Sum', 'median':'Median', 'max':'Max', 'min':'Min', 'count':'Count'})");
	private StringBuilder orderBy = new StringBuilder("");
	private StringBuilder orderBy2 = new StringBuilder("");
	private StringBuilder ascending = new StringBuilder("");
	private StringBuilder ascending2 = new StringBuilder("");
	private StringBuilder overrideQuery = null;
	
	private StringBuilder normalizer = new StringBuilder(".to_dict('split')['data']");
	
	
	private Map <String, StringBuilder>aggHash = null;
	private Map <String, StringBuilder>aggHash2 = null;
	private Map <String, StringBuilder>orderHash = null;

	static final String DEF_FILTER = "this.cache['data']__f";
	
	private Map <String, String> functionMap = null;
	
	Map <String, Boolean> processedSelector = new HashMap<>();
	
	ArrayList <String> aggKeys = new ArrayList<>();
	
	
	ArrayList <String> headers = null;
	
	// this is the headers being kept on the array list being generated
	ArrayList <String> actHeaders = null;
	
	int groupIndex = 0;
	
	ArrayList <SemossDataType> types= null;
	
	long start = 0;
	long end = 500;
	
	// need to keep the ordinality of the selectors and match that with the aliases
	ArrayList <String> groupColumns = null;
	PyTranslator pyt = null;
		
	boolean scalar = false;
	
	// experimental stuff trying the numpy groupies guy
	List groupColList = new ArrayList();
	Map aggColMap = new HashMap();
	List aggColList = new ArrayList();
	List functionList = new ArrayList();
	List orderList = new ArrayList();
	Map orderListMap = new HashMap(); // keeps track of what the items are called

	// cache of all the keys
	List keyCache = new ArrayList();
	
	public void setDataTypeMap(Map<String, SemossDataType> dataTypeMap)
	{
		this.colDataTypes = dataTypeMap;
	}
	
	public void setKeyCache(List keyCache)
	{
		this.keyCache = keyCache;
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
		// eventually move this to static
		if(DIHelper.getInstance().getCoreProp().containsKey("SWIFTER")) {
			swifter = DIHelper.getInstance().getCoreProp().get("SWIFTER")+"";
		} else {
			swifter = "";
		}
		// force swifter
		swifter = "";
				
		if(DIHelper.getInstance().getCoreProp().containsKey("EXP")) {
			exp = DIHelper.getInstance().getCoreProp().get("EXP")+"";
		} else {
			exp = "";
		}
		
		headers = new ArrayList<>();
		groupIndex = 0;
		actHeaders = new ArrayList<>();
		types= new ArrayList<>();
		groupColumns = new ArrayList<>();
		selectorCriteria = new StringBuilder("");
		groupCriteria = new StringBuilder("");
		aggCriteria = new StringBuilder("");
		renCriteria = new StringBuilder("");
		filterCriteria = new StringBuilder("");
		havingCriteria = new StringBuilder("");
		scalar = false;
		functionMap = new HashMap<>();
		aggHash = new HashMap<>();
		aggKeys = new ArrayList<>();
		aggHash2 = new HashMap<>();
		orderHash = new HashMap<>();
		orderBy = new StringBuilder("");
		normalizer = new StringBuilder(".to_dict('split')");//['data']"); // Ideally I should be able to put drop duplicates here
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
		
		// I need to account for close all here
		/*addFilters();
		addSelectors();
		processGroupSelectors();
		genAggString();
		processOrderBy();
		*/
		fillParts();
		// since I have accounted for various things do the close all here
		closeAll();
		
		// experimental part should come here
		
		StringBuilder cachedFrame = new StringBuilder(wrapperFrameName);
		// I need to account for when group is not there and filter is there
		// as well as when filter is not there and group is there
		// and when neither
		/*
		if(filterCriteria.length() > 0 && groupCriteria.length() == 0)
		{
			//createFilterOnlyCache(filterCriteria.toString(), this.wrapperFrameName + ".loc[" +  filterCriteria + "]");
			createFilterOnlyCache(filterCriteria.toString(), this.wrapperFrameName + filterCriteria );
			cachedFrame = new StringBuilder(frameName).append("w.cache[\"").append(filterCriteria).append("\"]");
		}
		
		else if(filterCriteria.length() > 0 && groupCriteria.length() > 0)
		{
			// create group here
			// but take into account the filter key
			// essentially I dont need to check for the stuff in the end
			String groupKey = filterCriteria + "__" + groupCriteria;
			String groupQuery = this.wrapperFrameName + filterCriteria + groupCriteria ;
			String filterQuery = this.wrapperFrameName + filterCriteria ;
			createGroupAndFilterCache(filterCriteria.toString(), groupKey, filterQuery, groupCriteria + "");
			cachedFrame = new StringBuilder(frameName).append("w.cache[\"").append(groupKey).append("\"]");
			groupCriteria = new StringBuilder("");
		}*/


		
		/*
		 
		 For some reason this is not yielding the result I thought it would. 
		 
		else if(groupCriteria.length() > 0 && filterCriteria.length() == 0)
		{
			// groupcache again separately
			// how is this useful ? given that we already have the other cache ?
			// we will come back to this
			String groupKey = groupCriteria.toString();
			String groupQuery = this.wrapperFrameName +  groupCriteria + "], sort=False)";
			createGroupOnlyCache(groupKey, groupQuery);
			cachedFrame = new StringBuilder(frameName).append("w.cache[\"").append(groupKey).append("\"]");
			groupCriteria = new StringBuilder("");
		}*/
		
		// see if replacements need to be done

		
		if(overrideQuery == null) {
			query.append(cachedFrame)
				//.append(".cache['data']")
				//.append(".iloc[")
				//.append(qs.getOffset() + ":" + (qs.getOffset() + qs.getLimit()) + "]")
				//.append(0 + ":" + 500+ "]")
				//.append(this)
				.append(this.filterCriteria)
				.append(this.selectorCriteria)
				.append(this.groupCriteria) //<-- trying disabling this now
				//.append(this.aggCriteria2)
				.append(this.aggCriteria2)
				// add the having clause
				.append(this.havingCriteria);
				// add distinct after we aggregate
				if(!scalar && aggCriteria2.toString().isEmpty()) {
					query.append(addDistinct(((SelectQueryStruct) this.qs).isDistinct()));
				}
				// TODO: need to be more elegant than this
				query.append(scalar ? "" : orderBy)
				.append(addLimitOffset(start, end))
				//.append(orderBy2)
				.append(normalizer);
				//.append(".fillna('')");
				// TODO: NEED TO DISTINCT THE LIST RETURNED
//				if(!scalar && !aggCriteria2.toString().isEmpty()) {
//					query.append(addDistinct(((SelectQueryStruct) this.qs).isDistinct()));
//				}
				//.append(this.renCriteria);
		} else {
			query = overrideQuery;
			if(actHeaders != null && actHeaders.size() > 0) {
				headers = actHeaders;
			}
		}
		
		// in the end if we want to wipe it we can
		//pyt.runScript("del " + frameName + "w.cache[\"" + filterCriteria + "\""]);
		
		//buildListMap();
		//if(overrideQuery != null)
		//	query = overrideQuery;
		return query.toString();
	}
	
	private void buildListMap() {
		// step1 - iterate through order list
		// for every order try to see if it is a groupby or is it a aggregate
		// based on either one build that list
		// as you build the aggregate also build the function list
		
		// this ONLY works when there is one groupby
		// this ONLY works when the groupby is ahead of calculated column.. although I will force it to the first one just now
		
		try {
			if(!groupColList.isEmpty()) {
				String filter = "''";
				if(filterCriteria.length() > 0)
					filter =  "\"" + composeFilterString() +"\"";
				filter = filter.replace("__f", "");
				filter = filter.replace(frameName, "this.cache['data']");
				StringBuilder gList = new StringBuilder("[");
				StringBuilder aggList = new StringBuilder("[");
				StringBuilder fList = new StringBuilder("[");
				String groupcol = (String)groupColList.get(0);
				for(int selectIndex = 0;selectIndex < orderList.size();selectIndex++) {
					String thisSelector = (String)orderList.get(selectIndex);
					if(groupColList.contains(thisSelector)) {
						// process it as group
						gList.append("'").append(thisSelector).append("'");
						composeGroupCacheString(thisSelector, true);
					} else if(aggColMap.containsKey(thisSelector)) {
						// process this as an aggregate
						String aggCol = (String)aggColMap.get(thisSelector); 
						String aggFunc = (String)aggColMap.get(thisSelector+"__f"); 
						aggList.append("'").append(aggCol).append("'");
						fList.append("'").append(aggFunc).append("'");
						composeAggCacheString(groupcol, aggCol, thisSelector, aggFunc, true);
					}
				}
				gList.append("]");
				aggList.append("]");
				fList.append("]");
				
				logger.info("index  >>" + gList);
				logger.info("agg  >>" + aggList);
				logger.info("Function  >>" + fList);
				
				// order map
				logger.info("Order Map" + orderListMap);
				
				StringBuilder orderString = new StringBuilder("[");
				String cacheName = frameName + "w.cache";
				
				for(int orderIndex = 0;orderIndex < orderList.size();orderIndex++) {
					String thisOrder = (String)orderList.get(orderIndex);
					if(orderIndex != 0)
						orderString.append(",");
					
					// pull the name of selector
					String orderSelector = (String)orderListMap.get(thisOrder);
					// if this was a group tag a list with it
					if(!aggColMap.containsKey(thisOrder)) {
						orderString.append("list(").append(cacheName).append("[\"").append(orderSelector).append("\"]").append(")");
					} else {
						orderString.append(cacheName).append("[\"").append(orderSelector).append("\"]");
					}
				}
				orderString.append("]");
				
				String script = frameName + "w.runGroupy(" + filter + ", " + gList + ", " + aggList + ", " + fList + ", '')";
				Object obj = pyt.runScript(script);

				// this will ultimately be the query
				logger.info("And the order string " + orderString);

				// try replacing the query
				this.overrideQuery = orderString;
				qs.getPragmap().put("format", "parquet");
			}
			else {
				// nothing to see please move on
			}
		} catch (Exception e) {
			logger.error("StackTrace: ", e);
		}
	}
	
	private void fillParts() {
		SelectQueryStruct sqs = (SelectQueryStruct) qs;
		Map partMap = sqs.getParts();

		if (partMap.containsKey(SelectQueryStruct.Query_Part.QUERY)) {
			overrideQuery = new StringBuilder(partMap.get(SelectQueryStruct.Query_Part.QUERY) + "");
		}
		if (partMap.containsKey(SelectQueryStruct.Query_Part.FILTER)) {
			filterCriteria = new StringBuilder(partMap.get(SelectQueryStruct.Query_Part.FILTER) + "");
		} else {
			addFilters();
		}
		if (partMap.containsKey(SelectQueryStruct.Query_Part.HAVING)) {
			havingCriteria = new StringBuilder(partMap.get(SelectQueryStruct.Query_Part.HAVING) + "");
		} else {
			addHavings();
		}
		// ideally this should not be but.. I need types
		if (partMap.containsKey(SelectQueryStruct.Query_Part.SELECT)) {
			selectorCriteria = new StringBuilder(partMap.get(SelectQueryStruct.Query_Part.SELECT) + "");
		} else {
			addSelectors();
		}
		
		// if(overrideQuery == null) {
			if (partMap.containsKey(SelectQueryStruct.Query_Part.SORT)) {
				orderBy = new StringBuilder(partMap.get(SelectQueryStruct.Query_Part.SORT) + "");
			} else {
				processOrderBy();
			}
			
			if (partMap.containsKey(SelectQueryStruct.Query_Part.AGGREGATE)) {
				aggCriteria2 = new StringBuilder(partMap.get(SelectQueryStruct.Query_Part.AGGREGATE) + "");
			} else {
				genAggString();
			}
			
			if (partMap.containsKey(SelectQueryStruct.Query_Part.GROUP)) {
				groupCriteria = new StringBuilder(partMap.get(SelectQueryStruct.Query_Part.GROUP) + "");
			} else {
				processGroupSelectors();
			}
//		}
	}
	
	
	private String addDistinct(boolean distinct) {
		if(distinct) {
			// try to find if there is more than 1 column
//			if(orderHash.size() > 1)
//				return "";
//			else if(orderHash.size() == 1 && aggHash.size() == 0)
				return ".drop_duplicates()";
		}
		return "";
	}
	
	private void closeFilters() {
		if(filterCriteria.length() > 0 ) {
			filterCriteria = new StringBuilder(".loc[").append(filterCriteria).append("]");
			// update the selector only if if there is no agg
			//if(selectorCriteria.length() == 0)
			//	selectorCriteria.append(".drop_duplicates()");
		}
	}
	
	public void closeAll() {
		boolean aggregate = false;
		SelectQueryStruct sqs = (SelectQueryStruct)qs;
		Map partMap = sqs.getParts();
		
		//t.agg({'Title': 'count'}).rename({'Title': 'count(Title)'}).reset_index().to_dict('split')['data']
		if(this.aggCriteria.toString().length() > 0 && !partMap.containsKey(SelectQueryStruct.Query_Part.AGGREGATE))
		{
			if(!((SelectQueryStruct) this.qs).getGroupBy().isEmpty()) {
				this.aggCriteria = aggCriteria.append("})").append(".reset_index()");
				this.aggCriteria2 = aggCriteria2.append(")").append(".reset_index()");
				this.renCriteria = renCriteria.append("}).reset_index()");
				if(headers.size() == 1) {
					aggCriteria2 = aggCriteria.append(".reset_index()");
				}
			} 
			// it is just getting one single data
			else if(headers.size() == 1) {
				this.aggCriteria = aggCriteria.append("}).reset_index()");
				this.aggCriteria2 = aggCriteria2.append(").reset_index()");
				normalizer = new StringBuilder(".to_dict('split')['data'][0][1]");
				aggCriteria2 = aggCriteria;
				scalar = true;
			}
			aggregate = true;
		}
		
		// if there is agroup by.. this whole thing should be ignored pretty much
		if(this.selectorCriteria.toString().length() > 0 && ((SelectQueryStruct) this.qs).getGroupBy().isEmpty() 
				&& !partMap.containsKey(SelectQueryStruct.Query_Part.SELECT)) {
			StringBuilder newSelectorCriteria = new StringBuilder("[[");
			this.selectorCriteria = newSelectorCriteria.append(this.selectorCriteria).append("]]");
		} else if(!partMap.containsKey(SelectQueryStruct.Query_Part.SELECT)) {
			this.selectorCriteria = new StringBuilder("");
		}
		
		if(groupCriteria.length() > 0 && !partMap.containsKey(SelectQueryStruct.Query_Part.GROUP)) {
			groupCriteria.append("], sort=False)");
		}
		/*
		else if(aggregate) // this is the case for greater than less than etc. this means it I need to force a group by - the assumption is there is only one
		{
			// and I need to close out aggcriteria 2 as well
			groupCriteria.append(".groupby('" + aggKeys.get(0) + "')");
			this.aggCriteria2 = aggCriteria2.append(")").append(addLimitOffset(start, end)).append(".reset_index()");
		}*/
		
		if(filterCriteria.length() > 0 && !partMap.containsKey(SelectQueryStruct.Query_Part.FILTER)) {
			filterCriteria = new StringBuilder(".loc[").append(filterCriteria).append("]");
			// update the selector only if if there is no agg
			//if(selectorCriteria.length() == 0)
			//	selectorCriteria.append(".drop_duplicates()");
		}
		// add the having criteria 
		if(havingCriteria.length() > 0 && !partMap.containsKey(SelectQueryStruct.Query_Part.HAVING)) {
			havingCriteria = new StringBuilder(".loc[").append(havingCriteria).append("]");
		}
		if(orderBy.length() != 0 && !partMap.containsKey(SelectQueryStruct.Query_Part.SORT)) {
			// combine it
			orderBy.append("],").append(ascending).append("])");
		}
		if(orderBy2.length() != 0 && !partMap.containsKey(SelectQueryStruct.Query_Part.SORT)) {
			// combine it
			orderBy2.append("],").append(ascending2).append("])");
		}
		//if(aggregate) // swap the order
		//	orderBy = orderBy;
	}
	
	private void processOrderBy() {
		List<IQuerySort> qcos = ((SelectQueryStruct) this.qs).getOrderBy();
		for(int orderIndex = 0; orderIndex < qcos.size(); orderIndex++) {
			IQuerySort sortOp = qcos.get(orderIndex);
			if(sortOp.getQuerySortType() == IQuerySort.QUERY_SORT_TYPE.COLUMN) {
				QueryColumnOrderBySelector orderBy = (QueryColumnOrderBySelector) sortOp;
				String sort = null;
				String alias = orderBy.getAlias();
				if(alias.length() == 0) {
					alias = orderBy.getTable();
				}
				ORDER_BY_DIRECTION sortDir = orderBy.getSortDir();
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
		}
		
		//if(!processed)
		//	orderBy = new StringBuilder("");
	}
	
	private void addOrder(StringBuilder curOrder, String asc) {
		// I need to find out which are the pieces I need to drop
		if(orderBy.length() == 0) {
			orderBy = new StringBuilder(".sort_values([");
			ascending = new StringBuilder("ascending=[");
		} else {
			orderBy.append(",");
			ascending.append(",");
		}
		
		// add the ascending
		ascending.append(asc);
		
		// add the order by
		orderBy.append("'").append(curOrder).append("'");
	}

	private void addOrder2(StringBuilder curOrder, String asc) {
		// I need to find out which are the pieces I need to drop
		// get the ordinal value
		//int colIndex = headers.indexOf(curOrder);
		if(orderBy2.length() == 0) {
			orderBy2 = new StringBuilder(".sort_index(level=[");
			ascending2 = new StringBuilder("ascending=[");
		} else {
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
	
	public void addFilters() {
		addFilters(qs.getCombinedFilters().getFilters(), this.wrapperFrameName, this.filterCriteria, false);
	}
	
	public void addHavings() {
		addFilters(qs.getHavingFilters().getFilters(), this.wrapperFrameName, this.havingCriteria, false);
	}

	// add all the selectors
	public void addSelectors() {
		this.selectorCriteria = new StringBuilder();
		List<IQuerySelector> selectors = qs.getSelectors();
		int size = selectors.size();
		// get the cue from DIHelper
//		int maxColumns = 100;
//		if(DIHelper.getInstance().getCoreProp().containsKey("MAX_COL_ON_GRID")) {
//			maxColumns = Integer.parseInt(DIHelper.getInstance().getCoreProp().getProperty("MAX_COL_ON_GRID"));
//		}
//		if(size > maxColumns) {
//			size = maxColumns;
//		}
		
		for(int i = 0; i < size; i++) {
			IQuerySelector selector = selectors.get(i);
			String newHeader = null;
			newHeader = processSelector(selector, wrapperFrameName, true, true);
			
			//EXPERIMENTAL
			orderList.add(selector.getAlias());
			
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
				if(selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
					types.add(colDataTypes.get(this.frameName + "__" + newHeader));
				}
				
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

	public String processSelector(IQuerySelector selector, String tableName, boolean includeTableName, boolean useAlias, boolean...useTable) {
		SELECTOR_TYPE selectorType = selector.getSelectorType();
		String tableNameForCol = null;
		
		if(useTable != null && useTable.length > 0 &&  useTable[0])
			tableNameForCol = tableName;
		
		if(selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			return processColumnSelector( (QueryColumnSelector) selector, tableNameForCol);
		}
		// constan is not touched yet
		else if(selectorType == IQuerySelector.SELECTOR_TYPE.CONSTANT) {
			return processConstantSelector((QueryConstantSelector) selector);
		}  
		else if(selectorType == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
			if(aggHash != null) // making a dichotomy for query function
				return processAggSelector((QueryFunctionSelector) selector);
			else
				return processFunctionSelector((QueryFunctionSelector)selector, tableName);
		}
		// arithmetic selector is not implemented
		else if(selectorType == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			return processArithmeticSelector((QueryArithmeticSelector) selector, tableName, includeTableName, useAlias, useTable);
		}
		else if(selectorType == IQuerySelector.SELECTOR_TYPE.IF_ELSE) {
			return processIfElseSelector((QueryIfSelector) selector, tableName, includeTableName, useAlias, useTable);
		} 
		else {
			return null;
		}
	}
	
	private String  processFunctionSelector(QueryFunctionSelector selector, String tableName)
	{
		//Sum(MovieBudget)
		StringBuffer retBuffer = new StringBuffer();
		// get the name of the column
		String functionName = selector.getFunction();
		List <IQuerySelector> paramSelectors = selector.getInnerSelector();
		// usually this is a single parameter, if it is more, I dont know what to do
		if(paramSelectors != null)
		{
			IQuerySelector curSelector = paramSelectors.get(0);
			if(curSelector instanceof QueryColumnSelector)
			{
				QueryColumnSelector cs = (QueryColumnSelector)curSelector;
				String columnName = cs.getAlias();
				functionName = QueryFunctionHelper.convertFunctionToPandasSyntax(functionName);
				retBuffer.append(tableName).append("['").append(columnName).append("'].").append(functionName).append("()");
			}
		}		
		return retBuffer.toString();
	}

	
	private String processIfElseSelector(QueryIfSelector selector,  String tableName, boolean includeTableName, boolean useAlias, boolean...useTable)
	{
		// get the condition first
		IQueryFilter condition = selector.getCondition();
		StringBuffer buf = new StringBuffer("np.where(");
		
		StringBuilder filterBuilder = new StringBuilder();

		filterBuilder = this.processFilter(condition, tableName, useAlias, useTable);

		// builder shoudl have what we need at this point
		buf.append(filterBuilder.toString());
		buf.append(",");
		
		// get the precedent
		IQuerySelector precedent = selector.getPrecedent();
		buf.append(processSelector(precedent, tableName, includeTableName, useAlias, useTable));

		IQuerySelector antecedent = selector.getAntecedent();
		if(antecedent != null)
		{
			buf.append(", ");
			buf.append(processSelector(antecedent, tableName, includeTableName, useAlias, useTable));
		}
		buf.append(")");
		
		return buf.toString();
	}

	
	private String processColumnSelector(QueryColumnSelector selector, String tableName) {
		String columnName = selector.getColumn();
		String alias = selector.getAlias();
		
		// just return the column name
		if(tableName != null)
			return new StringBuffer(tableName).append("['").append(alias).append("']") + "";
		else
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
	
	private void processGroupSelectors() {
		List <IQuerySelector> groupSelectors = ((SelectQueryStruct) this.qs).getGroupBy();
		
		QueryColumnSelector queryColumnSelector = null;
		for(int sIndex = 0;sIndex < groupSelectors.size();sIndex++) {
			IQuerySelector groupBySelector = groupSelectors.get(sIndex);
			if(groupBySelector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
				queryColumnSelector = (QueryColumnSelector) groupSelectors.get(sIndex);
				processGroupSelector(queryColumnSelector);
			} else {
				String errorMessage = "Cannot group by non QueryColumnSelector type yet...";
				logger.error(errorMessage);
				throw new IllegalArgumentException(errorMessage);
			}
			
			String colName = queryColumnSelector.getColumn();
			// EXPERIMENTAL BLOCK
			this.groupColList.add(colName);
		}
	}
	
	private void processGroupSelector(QueryColumnSelector selector) {
//		if(!processedSelector.containsKey(selector.getAlias())) {
//			if(!aggHash.containsKey(selector.getTable()))
			{
				if(groupCriteria.length() == 0)
					groupCriteria.append(".groupby([");
				else
					groupCriteria.append(",");
			
				groupCriteria.append("'").append(selector.getColumn()).append("'");
			}
			
			if(actHeaders.contains(selector.getColumn())) {
				int index = actHeaders.indexOf(selector.getColumn());
				actHeaders.remove(selector.getColumn());
				//headers.remove(selector.getTable());
				actHeaders.add(groupIndex, headers.get(index));
				groupIndex++;
			}
			
			//headers.add(groupIndex, selector.getTable());
			// we dont know how many groups would it be.. so updating
			if(processedSelector.containsKey(selector.getColumn())) {
				processedSelector.put(selector.getColumn(), Boolean.TRUE);
				headers.add(selector.getColumn());
			}
//		}
	}
	
	public Map<String, String> functionMap() {
		return this.functionMap;
	}
	
	private void genAggString()	{
		aggCriteria = new StringBuilder("");

		for(int cIndex = 0;cIndex < aggKeys.size();cIndex++) {
			String colKey = aggKeys.get(cIndex);
			// I need to replace this with aggHash2
			if(aggHash.containsKey(colKey)) {
				if(aggCriteria.length() != 0)
					aggCriteria.append(",");
				aggCriteria.append(aggHash.get(colKey)).append("]");
			}
			//aggCriteria.append(aggHash.get(colKey));
			if(aggHash2.containsKey(colKey)) {
				if(aggCriteria2.length() != 0)
					aggCriteria2.append(",");
				aggCriteria2.append(aggHash2.get(colKey));				
			}
		}
		
		if(aggCriteria.length() > 0) {
			aggCriteria = new StringBuilder(".agg({").append(aggCriteria);
			aggCriteria2 = new StringBuilder(".agg(").append(aggCriteria2);
		}
		// just a way to say the override was added by this guy and not coming from outside
		if(overrideQuery != null && overrideQuery.length() > 0 && aggHash2.size() > 0  && !((SelectQueryStruct)qs).getParts().containsKey(SelectQueryStruct.Query_Part.QUERY))
		{
			overrideQuery.append("]");
		}
	}
	
	private String processAggSelector(QueryFunctionSelector selector) {
		// if it is using a function.. usually it is an aggregation
		String function = selector.getFunction();
		String columnName = selector.getAllQueryColumns().get(0).getAlias();
		
		logger.info("Column Name .. >>" + selector.getAllQueryColumns().get(0).getColumn() + "<<>>" + selector.getAllQueryColumns().get(0).getTable());
		
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
		
		if(aggHash.containsKey(columnName)) {
			aggBuilder = aggHash.get(columnName);
			aggBuilder.append(",");
		} else {
			aggBuilder.append("'").append(columnName).append("':[");
		}
		aggBuilder.append("'" + pandasFunction +"'");
		
		orderHash.put(selector.getAlias(), new StringBuilder("('").append(columnName).append("')"));
		
		headers.add(selector.getAlias());
		aggHash.put(columnName, aggBuilder);
		// adding it through alias
		aggHash2.put(aggAlias, aggBuilder2);
		
		aggKeys.add(columnName);
		// also add the alias name
		if(!aggKeys.contains(aggAlias))
			aggKeys.add(aggAlias);
		
		// if it is a group concat.. dont add this to actual headers here.. since it will get added during group by
		if(function.equalsIgnoreCase(QueryFunctionHelper.UNIQUE_GROUP_CONCAT)) {
			actHeaders.add(columnName);
			functionMap.put(pandasFunction+columnName, selector.getAlias());
		} else {
			actHeaders.add(selector.getAlias());
			// else it will get added by the way of group by clause
			functionMap.put(pandasFunction+columnName, selector.getAlias());
		}
		
		// I can avoid all of this by creating a dataframe and imputing.. but let us see how far we can inline this
		// I am going to assume that this is the same type as header for most operations
		SemossDataType curType = colDataTypes.get(this.frameName + "__" +columnName);
		
		// it can also depend on the operation but.. 
		// I have no idea what I am doing here
		if(curType == SemossDataType.STRING || curType == SemossDataType.BOOLEAN) {
			types.add(SemossDataType.INT);
		} else {
			types.add(curType);
		}
		
		// if the groupby is empty then this is just simple min and max
		// need to revisit min and max
		// quick fix for min and max
		// I do need to honor the filter here
		if(((SelectQueryStruct) this.qs).getGroupBy().isEmpty() && (pandasFunction.contains("min") || pandasFunction.contains("max"))) {
			if(overrideQuery == null || overrideQuery.length() == 0) {
				overrideQuery = new StringBuilder("[");
			} else {
				overrideQuery.append(",");
			}
			overrideQuery.append(wrapperFrameName).append("['").append(columnName).append("'].").append(pandasFunction).append("()");
		}
		
		// EXPERIMENTAL BLOCK
		// I need to add this as well as the alias some place
		// I dont think I need the column name at all
		//aggCol_map.put(columnName, selector.getAlias());
		aggColMap.put(selector.getAlias(), columnName);
		aggColMap.put(selector.getAlias()+ "__f", pandasFunction);
		
		aggColList.add(columnName);
		// EXPERIMENTAL BLOCK
		
		return "";
	}

	
	
	private String processArithmeticSelector(QueryArithmeticSelector selector, String tableName, boolean includeTableName, boolean useAlias, boolean...useTable) {
		IQuerySelector leftSelector = selector.getLeftSelector();
		IQuerySelector rightSelector = selector.getRightSelector();
		String mathExpr = selector.getMathExpr();
		return "(" + processSelector(leftSelector, tableName, includeTableName, useAlias, useTable) + " " + mathExpr + " " + processSelector(rightSelector, tableName, includeTableName, useAlias, useTable) + ")";
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
	
	private StringBuilder processFilter(IQueryFilter filter, String tableName, boolean useAlias, boolean...useTable) {
		IQueryFilter.QUERY_FILTER_TYPE filterType = filter.getQueryFilterType();
		if(filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return processSimpleQueryFilter((SimpleQueryFilter) filter, tableName, useAlias, useTable);
		} else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return processAndQueryFilter((AndQueryFilter) filter, tableName, useAlias, useTable);
		} else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return processOrQueryFilter((OrQueryFilter) filter, tableName, useAlias, useTable);
		}else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.BETWEEN) {
			return processBetweenQueryFilter((BetweenQueryFilter) filter, tableName, useAlias, useTable);
		}
		return null;
	}
	
	private StringBuilder processOrQueryFilter(OrQueryFilter filter, String tableName, boolean useAlias, boolean...useTable) {
		StringBuilder filterBuilder = new StringBuilder();
		List<IQueryFilter> filterList = filter.getFilterList();
		int numAnds = filterList.size();
		for(int i = 0; i < numAnds; i++) {
			if(i == 0) {
				filterBuilder.append("(");
			} else {
				filterBuilder.append(" ) | ( ");
			}
			filterBuilder.append(processFilter(filterList.get(i), tableName, useAlias, useTable));
		}
		filterBuilder.append(")");
		return filterBuilder;
	}

	private StringBuilder processAndQueryFilter(AndQueryFilter filter, String tableName, boolean useAlias, boolean...useTable) {
		StringBuilder filterBuilder = new StringBuilder();
		List<IQueryFilter> filterList = filter.getFilterList();
		int numAnds = filterList.size();
		for(int i = 0; i < numAnds; i++) {
			if(i == 0) {
				filterBuilder.append("(");
			} else {
				filterBuilder.append(") & (");
			}
			filterBuilder.append(processFilter(filterList.get(i), tableName, useAlias, useTable));
		}
		filterBuilder.append(")");
		return filterBuilder;
	}
	
	private StringBuilder processBetweenQueryFilter(BetweenQueryFilter filter, String tableName, boolean useAlias, boolean...useTable)
	{
		StringBuilder retBuilder = new StringBuilder();
		String columnName = processSelector(filter.getColumn(), tableName, true, useAlias, true); 
		retBuilder.append("(");
		retBuilder.append(columnName);
		retBuilder.append("  >= ");
		retBuilder.append(filter.getStart());
		retBuilder.append(" ) & ( ");
		retBuilder.append(columnName);
		retBuilder.append("  <= ");
		retBuilder.append(filter.getEnd());
		retBuilder.append(")");
		return retBuilder;
	}

	
	private StringBuilder processSimpleQueryFilter(SimpleQueryFilter filter, String tableName, boolean useAlias, boolean...useTable) {
		NounMetadata leftComp = filter.getLComparison();
		NounMetadata rightComp = filter.getRComparison();
		String thisComparator = filter.getComparator();
		
		FILTER_TYPE fType = filter.getSimpleFilterType();
		if(fType == FILTER_TYPE.COL_TO_COL) {
			return addSelectorToSelectorFilter(leftComp, rightComp, thisComparator, tableName, useAlias, useTable);
		} else if(fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToValuesFilter2(leftComp, rightComp, thisComparator, tableName, useAlias, useTable);
		} else if(fType == FILTER_TYPE.VALUES_TO_COL) {
			// same logic as above, just switch the order and reverse the comparator if it is numeric
			return addSelectorToValuesFilter(rightComp, leftComp, IQueryFilter.getReverseNumericalComparator(thisComparator), tableName, useAlias, useTable);
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
	private StringBuilder addSelectorToSelectorFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator, String tableName, boolean useAlias, boolean...useTable) {
		// get the left side
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		IQuerySelector rightSelector = (IQuerySelector) rightComp.getValue();

		/*
		 * Add the filter syntax here once we have the correct physical names
		 */
		
		String lSelector = processSelector(leftSelector, tableName, true, useAlias, useTable);
		String rSelector = processSelector(rightSelector, tableName, true, useAlias, useTable);
		
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
	
	// tx2.loc[tx2['Sales Fact Number'].map(lambda x: x < 10000000) & tx2['Fiscal Year'].map(lambda x: x < 2011)].groupby('Fiscal Year').agg(Max_Sales_Fact= ('Sales Fact Number', 'max'))
	// this needs to be all map operation to start with
	
	private StringBuilder addSelectorToValuesFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator, String tableName, boolean useAlias, boolean...useTable) {
		// get the left side
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		String leftSelectorExpression = processSelector(leftSelector, tableName, true, useAlias, useTable);
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
		boolean addNullCheck = objects.remove(null);
		if(leftDataType != null && SemossDataType.isNotString(leftDataType)) {
			if(objects.remove("null")) {
				addNullCheck = true;
			}
			if(objects.remove("nan")) {
				addNullCheck = true;
			}
			if(thisComparator.equals("==") && objects.remove("")) {
				addNullCheck = true;
			}
		}
		if(!addNullCheck) {
			// are we searching for null?
			addNullCheck = IQueryInterpreter.getAllSearchComparators().contains(thisComparator) && objects.contains("null");
		}
		
		StringBuilder filterBuilder = new StringBuilder("(");;
		// add the null check now
		if(addNullCheck) {
			// can only work if comparator is == or !=
			if(thisComparator.equals("==")) {
				filterBuilder.append("(~").append(wrapperFrameName).append("[").append(leftSelectorExpression).append("]").append(".isna())");
			} else if(thisComparator.equals("!=") || thisComparator.equals("<>")) {
				filterBuilder.append("(~").append(wrapperFrameName).append("[").append(leftSelectorExpression).append("]").append(".isna())");
			}
		}
		SemossDataType objectsDataType = leftDataType;
		
		// if there are other instances as well
		// also add that
		if(!objects.isEmpty()) {
			boolean multi = false;
			String myFilterFormatted = null;
			// format the objects based on the type of the column
			boolean useStringForType = thisComparator.equals(SEARCH_COMPARATOR) || thisComparator.contentEquals(NOT_SEARCH_COMPARATOR);
			if(useStringForType) {
				objectsDataType = SemossDataType.STRING;
			}
			// format the objects based on the type of the column
			if(objects.size() > 1) {
				multi = true;
				// need a similar one for pandas
				myFilterFormatted = PandasSyntaxHelper.createPandasColVec(objects, objectsDataType);
			} else if(objectsDataType == SemossDataType.DATE){
				// dont bother doing this if we have a date
				// since we cannot use "in" with dates
				myFilterFormatted = PandasSyntaxHelper.formatFilterValue(objects.get(0), objectsDataType);
			}else
				myFilterFormatted = PandasSyntaxHelper.createPandasColVec(objects, objectsDataType);
				
			
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
					// this is where the like operator is coming through
					// this needs to be resolved
					filterBuilder.append(leftSelectorExpression).append(" ").append(thisComparator).append(myFilterFormatted);
				}
			}
			else {
				// need to see this a bit more when we get here
				// dont know how the other types of comparator are being sent here
				// this is going to be an interesting pattern
				// I need to replace the entire command
				if(thisComparator.equals(SEARCH_COMPARATOR)) {
					overrideQuery = new StringBuilder(frameName).append("w.").append("column_like('',").append(leftSelectorExpression).append(", '").append(objects.get(0)).append("')");
					this.headers.add(leftSelectorExpression);
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
				} else if(objectsDataType != SemossDataType.DATE){
					if(thisComparator.equals("==")) {
						filterBuilder.append(wrapperFrameName).append("[").append(leftSelectorExpression).append("]").append(".isin").append(myFilterFormatted);
					} else if(thisComparator.equals("!=") | thisComparator.equals("<>")) {
						filterBuilder.append("~").append(wrapperFrameName).append("[").append(leftSelectorExpression).append("]").append(".isin").append(myFilterFormatted);
					}
				}
				else{
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

	private StringBuilder addSelectorToValuesFilter2(NounMetadata leftComp, NounMetadata rightComp, String thisComparator, String tableName, boolean useAlias, boolean...useTable) 
	{
		//swifter = "";
		// get the left side
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		String leftSelectorExpression = processSelector(leftSelector, tableName, true, useAlias, useTable);
		
		// always index the column if it is string
		
		String gKey = leftSelectorExpression;
		
		SemossDataType leftDataType = SemossDataType.convertStringToDataType(leftSelector.getDataType());
		if(leftDataType == null) {
			String qsName = leftSelector.getQueryStructName();
			if (!qsName.contains("__")) {
				qsName = "__" + qsName;
			}
			String colDataTypesKey = this.frameName + qsName;
			leftDataType = this.colDataTypes.get(colDataTypesKey);
		}
		//if(leftDataType == SemossDataType.STRING)
		//	gKey = indexColumn(leftSelectorExpression);
			
		// grab the objects we are setting up for the comparison
		List<Object> objects = new Vector<Object>();
		// ugh... this is gross
		if(rightComp.getValue() instanceof List) {
			objects.addAll( (List) rightComp.getValue());
		} else {
			objects.add(rightComp.getValue());
		}
		
		// if the left data type is string
		// then use the is in operator
		// if not then pretty much for everything else use the apply like
		// tx2['Sales Fact Number'].map(lambda x: x < 10000000) 
		// with swifter it looks like this
		//  tx2.loc[tx2['Sales Fact Number'].swifter.apply(lambda x: x < 10000000) & tx2['Fiscal Year'].swifter.apply(lambda x: x < 2011)].groupby('Fiscal Year').agg(Max_Sales_Fact= ('Sales Fact Number', 'max'))
		
		// see if it is search if so.. move it to string call it a day
		// this needs to account for filter
		String finalFilter = filterCriteria + "";
		if(finalFilter != null && finalFilter.length() > 0)
			// add the frame name
			finalFilter = "this.cache['data']" + finalFilter;
		else
			finalFilter = "''";
			
		if(thisComparator.equals(SEARCH_COMPARATOR)) {
			overrideQuery = new StringBuilder(frameName).append("w.").append("column_like(").append(finalFilter).append(",").append(leftSelectorExpression).append(", '").append(objects.get(0)).append("')");
			this.headers.add(leftSelectorExpression);
			return new StringBuilder("");
		} else if(thisComparator.equals(NOT_SEARCH_COMPARATOR)) {
			overrideQuery = new StringBuilder(frameName).append("w.").append("column_not_like(").append(finalFilter).append(",").append(leftSelectorExpression).append(", '").append(objects.get(0)).append("')");
			this.headers.add(leftSelectorExpression);
			return new StringBuilder("");
		} else if(thisComparator.equals(BEGINS_COMPARATOR) || thisComparator.equals(ENDS_COMPARATOR)){
			String function = thisComparator.equals(BEGINS_COMPARATOR) ? "startswith" : "endswith";
			overrideQuery = new StringBuilder("{ 'data':"+tableName).append("[").append(tableName).append("[").append(leftSelectorExpression).append("].str.casefold().str.")
					.append(function).append("('").append(objects.get(0).toString().toLowerCase()).append("')].values.tolist(), 'columns': list("+tableName+ ".columns)}");
			this.headers.add(leftSelectorExpression);
			return new StringBuilder("");
		}
		
		// if it is null, then we know we have a column
		// need to account for null inputs
		boolean addNullCheck = objects.remove(null);
		if(leftDataType != null && SemossDataType.isNotString(leftDataType)) {
			if(objects.remove("null")) {
				addNullCheck = true;
			}
			if(objects.remove("nan")) {
				addNullCheck = true;
			}
			if(thisComparator.equals("==") && objects.remove("")) {
				addNullCheck = true;
			}
		}
		if(!addNullCheck) {
			// are we searching for null?
			addNullCheck = IQueryInterpreter.getAllSearchComparators().contains(thisComparator) && objects.contains("null");
		}
		
		StringBuilder filterBuilder = new StringBuilder("(");
		// add the null check now
		if(addNullCheck) {
			if(thisComparator.equals("==") || IQueryInterpreter.getPosSearchComparators().contains(thisComparator)) {
				filterBuilder.append(wrapperFrameName).append("[").append(leftSelectorExpression).append("]").append(".isna())");
			} else if(thisComparator.equals("!=") || thisComparator.equals("<>") || IQueryInterpreter.getNegSearchComparators().contains(thisComparator)) {
				filterBuilder.append("~").append(wrapperFrameName).append("[").append(leftSelectorExpression).append("]").append(".isna())");
			}
		} else {
			filterBuilder = null;
		}
		
		// need to grab from metadata
		if(leftDataType == null) {
			leftDataType = this.colDataTypes.get(leftSelector.getQueryStructName());
		}
		StringBuilder retBuilder = new StringBuilder();
		
		// python doesnt recognize <>
		if(leftSelectorExpression != null && leftSelectorExpression.contains("<>"))
			leftSelectorExpression = " != ";
		
		if(leftDataType == SemossDataType.STRING || leftDataType == SemossDataType.DATE || leftDataType == SemossDataType.TIMESTAMP)
		{
			// couple of things to check here
			// is this a search string
			String myFilterFormatted = PandasSyntaxHelper.createPandasColVec(objects, leftDataType);
			// adding the paranthesis for sanity purposes
			// i need to replace this with numpy
			//  tx.loc[np.flatnonzero(np.isin(bnpy, a2))][:200]
			// bnpy is the numpy representation of the column
			// I dont know if the extra jep call would kill me
			
			
			retBuilder.append("(");
			if(thisComparator.contains("!="))
				retBuilder.append("~");
			
			
			retBuilder.append(frameName).append("[").append(leftSelectorExpression).append("].isin").append(myFilterFormatted);
			retBuilder.append(") ");
			return retBuilder;
		}
		
		// ok so we we are done with string here
		// next is number
		// this can be > <
		// at this point it is all the same
		// that does it
		else
		{
			// add one filter for each of it
			for (int objIndex = 0;objIndex < objects.size();objIndex++)
			{
				if(retBuilder.length() > 0)
					retBuilder.append(" | ");
				retBuilder.append("(");
				retBuilder.append(frameName).append("[").append(leftSelectorExpression).append("]")
						  .append(swifter).append(".apply(lambda x : x ")
						  .append(thisComparator)
						  .append(objects.get(objIndex)).append(")");
				retBuilder.append(")  ");
			}
		}
		
		// if the null check was valid enable that too
		if(filterBuilder != null) {
			if(retBuilder.length() > 0) {
				retBuilder = new StringBuilder("(").append(filterBuilder).append(" & ").append(retBuilder);
			} else {
				retBuilder = filterBuilder;
			}
		}

		if(addNullCheck && !objects.isEmpty()) {
			// close due to wrapping
			retBuilder.append(") ");
		}
		
		return retBuilder;
	}

	public void setPyTranslator(PyTranslator pyt) {
		this.pyt = pyt;
	}
	
	// create the filter cache
	// this should also try group by
	// try the groupby first
	// if not try the filter
	
	// see if the groupby exists
	// if not see if the filter exists
	// if not create the filter
	// then create the group by
	// stepout
	
	private void createFilterOnlyCache(String filter, String query)
	{
		// if the filter is not existing then create the cache
		
		StringBuilder command = new StringBuilder("");
		command.append("if \"").append(filter).append("\" not in ").append(frameName).append("w.cache:");
		command.append("\n");
		String rand = "r" + Utility.getRandomString(6);
		command.append("\t").append(rand);
		command.append(" = ");
		command.append(query);
		command.append("\n");
		command.append("\t").append(frameName).append("w.cache[\"").append(filter).append("\"]");
		command.append(" = ");
		command.append(rand);
		// try to print a else command ? to see if there was use from it ?
		pyt.runEmptyPy(command.toString());
	}

	
	// see if the groupby exists
	// if not see if the filter exists
	// if not create the filter
	// then create the group by
	// stepout
	
	private void createGroupAndFilterCache(String filterKey, String groupKey, String filterQuery, String groupQuery)
	{
		// if the filter is not existing then create the cache
		
		StringBuilder command = new StringBuilder("");
		String rand = "r" + Utility.getRandomString(6);
		String rand2 = "r" + Utility.getRandomString(6);
		command.append("if \"").append(groupKey).append("\" not in ").append(frameName).append("w.cache:");
		command.append("\n");
			// see if the filter exists if not create
			command.append("\t").append("if \"").append(filterKey).append("\" not in ").append(frameName).append("w.cache:");
			command.append("\n");
				command.append("\t\t").append(rand);
				command.append(" = ");
				command.append(filterQuery);
				command.append("\n");
				command.append("\t\t").append(frameName).append("w.cache[\"").append(filterKey).append("\"]");
				command.append(" = ");
				command.append(rand);
			command.append("\n");
			command.append("\t").append(rand2);
			command.append("=");
			command.append(frameName).append("w.cache[\"").append(filterKey).append("\"]").append(groupQuery);
			command.append("\n");
			command.append("\t").append(frameName).append("w.cache[\"").append(groupKey).append("\"]");
			command.append("=");
			command.append(rand2);
			command.append("\n");
			command.append("\t").append("print('Created new cache')");
			command.append("\n");
		command.append("else:");
			command.append("\n");
			command.append("\t").append("print('Using Cache')");
			command.append("\n");
		// try to print a else command ? to see if there was use from it ?
		String output = pyt.runPyAndReturnOutput(command.toString());
		logger.info("Cache " + output);
	}

	private void createGroupOnlyCache(String groupKey, String groupQuery)
	{
		// if the filter is not existing then create the cache
		// essentially I can replace this with the idxColFilter
		StringBuilder command = new StringBuilder("");
		String rand2 = "r" + Utility.getRandomString(6);
		command.append("if \"").append(groupKey).append("\" not in ").append(frameName).append("w.cache:");
			command.append("\n");
			command.append("\t").append(rand2);
			command.append(" = ");
			command.append(groupQuery);
			command.append("\n");
			command.append("\t").append(frameName).append("w.cache[\"").append(groupKey).append("\"]");
			command.append(" = ");
			command.append(rand2);
			command.append("\n");
			command.append("\t").append("print('Created new Group Only cache')");
			command.append("\n");
		command.append("else:");
			command.append("\n");
			command.append("\t").append("print('Using Group Only Cache')");
			command.append("\n");
		// try to print a else command ? to see if there was use from it ?
		String output = pyt.runPyAndReturnOutput(command.toString());
		logger.info("Cache " + output);
	}
	
	private String indexColumn(String col)
	{
		String ffKey = "this.cache['data']" + "__f";
		String gKey = ffKey + "__" + col;
		if(keyCache.contains(gKey))
		{
			// categories and names
			String catCodes = gKey+"__cat__cat.code";
			String catNames = gKey+"__cat__cat.categories";
			
			//pyt.runScript(new String [] {frameName + "w.idxColFilter('', " + col + ")"});
			pyt.runScript(frameName + "w.idxColFilter('', " + col + ")");
			keyCache.add(gKey); // this is really the numpy key
			keyCache.add(catCodes);
			keyCache.add(catNames);
			
		}
		return gKey;
		
	}

	
	private String composeGroupCacheString(String col, boolean add)
	{
		/*
		col_index = ffKey + "__" + col
		col_index_u = col_index + "__u" # upper case
		col_key = ffKey + "__" + col + "__cat"
		col_catCodes = col_key + "__cat.code"
		col_catNames = col_key + "__cat.categories"
		gKey = ffKey + "__" + g + "__cat"
		col = g
		catCodes = gKey + "__cat.code"
		catNames = gKey + "__cat.categories"
		*/
		String ffKey = composeFilterString();
		String gKey = ffKey + "__" + col;
		String catCodes = gKey+"__cat__cat.code";
		String catNames = gKey+"__cat__cat.categories";
		if(add) {
			this.orderListMap.put(col, catNames);
		}
		return gKey;
		
	}
	
	private void composeAggCacheString(String col, String aggColName, String aggColAlias, String func, boolean add)
	{
		//outKey = gKey + "__" + a + "__" + f
		String ffKey = composeFilterString();
		String gKey = ffKey + "__" + col;
		String outKey = gKey + "__cat__" + aggColName + "__" + func;
		if(add) {
			this.orderListMap.put(aggColAlias, outKey);
		}
	}
	
	private String composeFilterString()
	{
		if(this.filterCriteria.length() > 0)
		{
			//String ffKey = frameName + filterCriteria +"__f"; //"w.cache['data']" +
			String ffKey = "this.cache['data']" + filterCriteria +"__f"; //"w.cache['data']" +
			return ffKey;
		}
		else
		{
			String ffKey = "this.cache['data']" + "__f";
			return ffKey;
		}
	}
	
	//////////////////////////////////// end adding filters /////////////////////////////////////
}
