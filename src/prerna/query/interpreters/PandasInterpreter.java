package prerna.query.interpreters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.ds.py.PyTranslator;
import prerna.engine.api.IRawSelectWrapper;
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
import prerna.reactor.IReactor;
import prerna.reactor.qs.SubQueryExpression;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class PandasInterpreter extends AbstractQueryInterpreter {

	private static final Logger classLogger = LogManager.getLogger(PandasInterpreter.class);

	private String frameName = null;
	private String wrapperFrameName = null;
	private String swifter = "";
	private String exp = ""; // says if this feature is experimental
	
	private Map<String, SemossDataType> colDataTypes;
	
	private StringBuilder selectorCriteria;
	private StringBuilder filterCriteria;
	private StringBuilder havingCriteria;
	private StringBuilder renameCriteria = new StringBuilder(""); // look into renCriteria below. I don't think it's necessary. 
	private StringBuilder groupCriteria = new StringBuilder("");
	private StringBuilder dateCriteria = new StringBuilder("");
	private StringBuilder arithmeticCriteria = new StringBuilder();
	private StringBuilder caseWhenCriteria = new StringBuilder();
	private StringBuilder aggCriteria = new StringBuilder("");
	private StringBuilder aggCriteria2 = new StringBuilder("");
	private StringBuilder renCriteria = new StringBuilder(".rename(columns={'mean':'Average', 'nunique':'UniqueCount', 'sum':'Sum', 'median':'Median', 'max':'Max', 'min':'Min', 'count':'Count'})");
	private StringBuilder orderBy = new StringBuilder("");
	private StringBuilder orderBy2 = new StringBuilder("");
	private StringBuilder ascending = new StringBuilder("");
	private StringBuilder ascending2 = new StringBuilder("");
	private StringBuilder overrideQuery = null;
	
	private StringBuilder normalizer = new StringBuilder(".to_dict('split')['data']");
	
	private boolean isHavingFilter;
	private List<StringBuilder> havingList = new ArrayList<StringBuilder>();
	
	private static final List<String> DATE_FUNCTION_LIST = new ArrayList<String>(5);
	static {
		DATE_FUNCTION_LIST.add(QueryFunctionHelper.YEAR);
		DATE_FUNCTION_LIST.add(QueryFunctionHelper.QUARTER);
		DATE_FUNCTION_LIST.add(QueryFunctionHelper.MONTH_NAME);
		DATE_FUNCTION_LIST.add(QueryFunctionHelper.WEEK);
		DATE_FUNCTION_LIST.add(QueryFunctionHelper.DAY_NAME);
	}
	
	private Map<String, StringBuilder> dateHash = null;
	private Map<String, StringBuilder> arithmeticHash = null;
	private Map<String, StringBuilder> caseWhenHash = null;
	private List<String> dateKeys = null;
	private List<String> arithmeticKeys = null;
	private List<String> caseWhenKeys = null;
	private int caseWhenCount;
	private List<StringBuilder> renameColList = null;
	
	private Map <String, StringBuilder>aggHash = null;
	private Map <String, StringBuilder>aggHash2 = null;
	private Map <String, StringBuilder>orderHash = null;
	private Map<String, SemossDataType> typesHash = null;
	
	// Experiment
	private List<String> caseWhenFunctionList = null;
	private boolean caseWhenFunction;
	

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
	
	// this is because we need to handle subquery
	private transient PandasFrame pandasFrame;
	
	public void setDataTypeMap(Map<String, SemossDataType> dataTypeMap) {
		this.colDataTypes = dataTypeMap;
		updateTypes();
	}
	
	private void updateTypes() {
		Map<String, SemossDataType> newTypesMap = new HashMap<>();
		for(String k : this.colDataTypes.keySet()) {
			String newK = null;
			if(k.contains("__")) {
				newK = k.split("__")[1];
			} else {
				newK = k;
			}

			newTypesMap.put(newK, this.colDataTypes.get(k));
		}
		this.colDataTypes = newTypesMap;
	}
	
	public void setKeyCache(List keyCache) {
		this.keyCache = keyCache;
	}
	
	public boolean isScalar() {
		return scalar;
	}
	
	public void setPandasFrame(PandasFrame pandasFrame) {
		this.pandasFrame = pandasFrame;
	}
	
	@Override
	public String composeQuery() {
		StringBuilder query = new StringBuilder();
		
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
		typesHash = new HashMap<>(); // EXPERIMENTING
		orderHash = new HashMap<>();
		orderBy = new StringBuilder("");
		normalizer = new StringBuilder(".to_dict('split')");//['data']"); // Ideally I should be able to put drop duplicates here
		ascending = new StringBuilder("");
		
		dateCriteria = new StringBuilder("");
		dateHash = new HashMap<>();
		dateKeys = new ArrayList<>();
		arithmeticCriteria = new StringBuilder("");
		arithmeticHash = new HashMap<>();
		arithmeticKeys = new ArrayList<>();
		caseWhenCriteria = new StringBuilder("");
		caseWhenHash = new HashMap<>();
		caseWhenFunctionList = new ArrayList<>();
		caseWhenKeys = new ArrayList<>();
		renameColList = new ArrayList<>();
		
		caseWhenCount = 0;
		
		long limit = 500;
		start = 0 ;
		end = limit;

		if(((SelectQueryStruct) this.qs).getOffset() > 0) {
			start = ((SelectQueryStruct) this.qs).getOffset();
		}
		if(((SelectQueryStruct) this.qs).getLimit() != 0) {
			end = (start + ((SelectQueryStruct) this.qs).getLimit());
		}
	
		fillParts();
		closeAll();
				
		StringBuilder cachedFrame = new StringBuilder(wrapperFrameName);
		
		if(overrideQuery == null) {
			query.append(cachedFrame)
				.append(dateCriteria)
				.append(arithmeticCriteria)
				.append(this.filterCriteria)
				.append(this.havingCriteria)
				.append(this.groupCriteria)
				.append(this.aggCriteria2)
				.append(caseWhenCriteria)
				.append(renameCriteria)
				.append(this.selectorCriteria);
			
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
		} else {
			query = overrideQuery;
			if(actHeaders != null && actHeaders.size() > 0) {
				headers = actHeaders;
			}
		}
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
		if (partMap.containsKey(SelectQueryStruct.Query_Part.SELECT)) {
			selectorCriteria = new StringBuilder(partMap.get(SelectQueryStruct.Query_Part.SELECT) + "");
		} else {
			addSelectors();
		}
		
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
		genIfElseString();
		genDateFunctionString();
		genArithmeticString();
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
		
		if(this.aggCriteria2.toString().length() > 0 && !partMap.containsKey(SelectQueryStruct.Query_Part.AGGREGATE))
		{
			if(!((SelectQueryStruct) this.qs).getGroupBy().isEmpty()) {
				this.aggCriteria = aggCriteria.append("})").append(".reset_index()");
				this.aggCriteria2 = aggCriteria2.append(")").append(".reset_index()");
				this.renCriteria = renCriteria.append("}).reset_index()");
			} else if (headers.size() > 1) {
				String tempCol = Utility.getRandomString(6);
				this.aggCriteria2 = new StringBuilder(".assign(" + tempCol + "=0).groupby('"+tempCol+"')").append(aggCriteria2)
						.append(").reset_index().drop('"+tempCol+"',axis=1)");
			} else if(headers.size() == 1) {
				this.aggCriteria = aggCriteria.append("}).reset_index()");
				this.aggCriteria2 = aggCriteria2.append(").reset_index()");
				normalizer = new StringBuilder(".to_dict('split')['data'][0][1]");
				aggCriteria2 = aggCriteria;
				scalar = true;
			}
			aggregate = true;
		}
		
		if (this.selectorCriteria.length() > 0 && !partMap.containsKey(SelectQueryStruct.Query_Part.SELECT) && !scalar) {
			StringBuilder tempSelectorBuilder = new StringBuilder("[[");
			this.selectorCriteria = tempSelectorBuilder.append(this.selectorCriteria).append("]]");
			
			if (!renameColList.isEmpty()) {
				renameCriteria.append(".rename(columns={");
				for (int i = 0; i < renameColList.size(); i++) {
					if (i == 0) {
						renameCriteria.append(renameColList.get(i));
					} else {
						renameCriteria.append(",").append(renameColList.get(i));
					}
				}
				renameCriteria.append("})");
			}
			
		} else if (!partMap.containsKey(SelectQueryStruct.Query_Part.SELECT)) {
			this.selectorCriteria.delete(0, selectorCriteria.length());
		}
		
		if (havingCriteria.length() > 0 && !partMap.containsKey(SelectQueryStruct.Query_Part.HAVING)) {
			StringBuilder tempGroupCriteria = new StringBuilder(groupCriteria);
			havingCriteria = tempGroupCriteria.append("]).filter(lambda x: ").append(havingCriteria).append(")");
		}
		
		if(groupCriteria.length() > 0 && !partMap.containsKey(SelectQueryStruct.Query_Part.GROUP)) {
			groupCriteria.append("], sort=False)");
		}
		if(filterCriteria.length() > 0 && !partMap.containsKey(SelectQueryStruct.Query_Part.FILTER)) {
			filterCriteria = new StringBuilder(".loc[").append(filterCriteria).append("]");
		}
		if(orderBy.length() != 0 && !partMap.containsKey(SelectQueryStruct.Query_Part.SORT)) {
			// combine it
			orderBy.append("],").append(ascending).append("])");
		}
		if(orderBy2.length() != 0 && !partMap.containsKey(SelectQueryStruct.Query_Part.SORT)) {
			// combine it
			orderBy2.append("],").append(ascending2).append("])");
		}
	}
	
	private void processOrderBy() {
		List<IQuerySort> qcos = ((SelectQueryStruct) this.qs).getCombinedOrderBy();
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
		addHavingFilters(qs.getHavingFilters().getFilters(), this.wrapperFrameName, this.havingCriteria, false);
	}

	/**
	 * Adds all the parameters passed through the SELECT statement. Keeps track of SELECTOR headers and types for
	 * sync with output in PandasFrame. 
	 */
	public void addSelectors() {
		this.selectorCriteria = new StringBuilder();
		List<IQuerySelector> selectors = qs.getSelectors();
		
		for(int i = 0; i < selectors.size(); i++) {
			IQuerySelector selector = selectors.get(i);
			SELECTOR_TYPE selectorType = selector.getSelectorType();
			String newHeader = processSelector(selector, wrapperFrameName, true, true);
			
			if (i == 0) {
				this.selectorCriteria.append(newHeader);
			} else {
				this.selectorCriteria.append(",").append(newHeader);
			}
			
			StringBuilder sb = new StringBuilder(newHeader);
			newHeader = newHeader.replace("'", "");
			headers.add(newHeader);
			orderHash.put(newHeader, sb);
			
			if (selectorType == IQuerySelector.SELECTOR_TYPE.FUNCTION 
					&& ((QueryFunctionSelector) selector).getFunction().equalsIgnoreCase(QueryFunctionHelper.UNIQUE_GROUP_CONCAT)) {
				actHeaders.add(((QueryFunctionSelector) selector).getAllQueryColumns().get(0).getAlias());
			} else {
				actHeaders.add(newHeader);
			}		
			if (selectorType == IQuerySelector.SELECTOR_TYPE.COLUMN) {
				SemossDataType curType = this.colDataTypes.get(((QueryColumnSelector) selector).getColumn());
				typesHash.put(newHeader, curType);
			}
		}
		for (String header : headers) {
			if (typesHash.containsKey(header))
				types.add(typesHash.get(header));
		}
	}
	
	public String[] getHeaders()
	{
		if(headers != null)
		{
			String [] headerArray = new String[this.headers.size()];
			this.headers.toArray(headerArray);
			return headerArray;
		}
		return null;
	}

	public SemossDataType[] getTypes()
	{
		if(headers !=null)
		{
			SemossDataType [] typeArray = new SemossDataType[this.headers.size()];
			this.types.toArray(typeArray);
			return typeArray;
		}
		return null;
	}

	/*
	 * Process the types of Selectors being passed through the SELECT statement. 
	 */
	public String processSelector(IQuerySelector selector, String tableName, boolean includeTableName, boolean useAlias, boolean...useTable) {
		SELECTOR_TYPE selectorType = selector.getSelectorType();
		String tableNameForCol = null;
		
		if(useTable != null && useTable.length > 0 &&  useTable[0])
			tableNameForCol = tableName;
		
		if(selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			return processColumnSelector( (QueryColumnSelector) selector, tableNameForCol);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.CONSTANT) {
			return processConstantSelector((QueryConstantSelector) selector);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
			return processFunctionSelector((QueryFunctionSelector)selector, tableName);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			return processArithmeticSelector((QueryArithmeticSelector) selector, tableName, includeTableName, useAlias, useTable);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.IF_ELSE) {
			return processIfElseSelector((QueryIfSelector) selector, tableName);
		} else {
			return null;
		}
	}
	
	/**
	 * Process function calls for pandas date methods to extract fields from datetime objects. 
	 * 
	 * @param selector
	 * @param tableName
	 */
	private String processDateFunctionSelector(QueryFunctionSelector selector, String tableName) {
		IQuerySelector innerSelector = selector.getInnerSelector().get(0);		
		StringBuilder sb = new StringBuilder();
		String functionName = selector.getFunction();
		String pandasFunction = QueryFunctionHelper.convertFunctionToPandasSyntax(functionName);
		String alias = selector.getAlias();
		String columnName = processSelector(innerSelector, tableName, true, true, true);
		
		dateKeys.add(alias);
		sb.append(alias).append("=").append(columnName).append(".apply(pd.to_datetime).").append(pandasFunction).append(".values");
		
		dateHash.put(alias, sb);
		typesHash.put(alias, SemossDataType.STRING);
		
		// Add to functionMap. I need an example of this working, for processAgg and processDAte
		functionMap.put(pandasFunction + columnName, selector.getAlias());
		
		return "'" + alias.toString() + "'";
 	}
	
	/**
	 * Combine any date methods previously called by SELECT variables. 
	 */
	private void genDateFunctionString() {		
		for (String key : dateKeys) {
			if (dateHash.containsKey(key)) {
				if (dateCriteria.length() != 0) {
					dateCriteria.append(",");
				}
				dateCriteria.append(dateHash.get(key));
			}
		} 
		
		if (dateCriteria.length() > 0) {
			dateCriteria = new StringBuilder(".assign(").append(dateCriteria).append(")");
		}
	}
	
	/**
	 * Processes QueryFunctionSelector. Currently handles aggregate and date field extraction methods. 
	 * 
	 * @param selector
	 * @param tableName
	 * @return
	 */
	private String processFunctionSelector(QueryFunctionSelector selector, String tableName) {
		if (DATE_FUNCTION_LIST.contains(selector.getFunction())) {
			return processDateFunctionSelector(selector, tableName);
		}
		else {
			return processAggSelector(selector);
		}
	}
	/**
	 * Process CASEH WHEN statement (SQL translation). Currently handles column and function selectors in the filter logic, and Column
	 * to values / values to columns. Does not yet handle column to column or arithmetic in the filter logic. 
	 * @param selector
	 * @param tableName
	 * @return
	 */
	private String processIfElseSelector(QueryIfSelector selector, String tableName) {
		IQueryFilter filter = selector.getCondition();
		IQuerySelector precedent = selector.getPrecedent();
		IQuerySelector antecedent = selector.getAntecedent();
		caseWhenFunction = false;
		
		String alias = selector.getAlias();
		if (alias.length() == 0) {
			alias = "CASE_WHEN_"+caseWhenCount;
			caseWhenCount += 1;
		}
		
		StringBuilder filterBuilder = new StringBuilder(processSelector(precedent, tableName, false, false, false));
		filterBuilder.append(" if(").append(processFilter(filter, tableName)).append(") else ");
		
		if (antecedent != null) {
			filterBuilder.append(processSelector(antecedent, tableName, false, false, false));
		} else {
			filterBuilder.append("np.nan");
		}
		if (caseWhenFunction) {
			caseWhenFunctionList.add(alias);
			caseWhenFunction = false;
		}
		
		caseWhenKeys.add(alias);
		caseWhenHash.put(alias, filterBuilder);
		
		typesHash.put(alias, SemossDataType.convertStringToDataType(selector.getDataType()));
		
		return "'" + alias + "'";
	}
	/**
	 * Creates the assign(...) method to create new column(s) from CASE_WHEN logic. 
	 */
	private void genIfElseString() {
		for (String key : caseWhenKeys) {
			//Map<String, String> tempHash = caseWhen.get(key);
			if (caseWhenHash.containsKey(key)) {
				if (caseWhenCriteria.length() > 0) {
					caseWhenCriteria.append(",");
				}
				caseWhenCriteria.append(key).append("=");
				
				if (caseWhenFunctionList.contains(key)) {
					caseWhenCriteria.append(this.wrapperFrameName).append(groupCriteria).append("])").append(aggCriteria2)
									.append(").apply(lambda x: ").append(caseWhenHash.get(key)).append(",axis=1).values");
				} else {
					caseWhenCriteria.append(this.frameName).append(".apply(lambda x: ").append(caseWhenHash.get(key)).append(",axis=1).values");
				}
			}
		}
		if (caseWhenCriteria.length() > 0) {
			caseWhenCriteria = new StringBuilder(".assign(").append(caseWhenCriteria).append(")");
		}
	}
	/**
	 * Process selectors that are normal columns. Accounts for columns that need to be renamed. 
	 * 
	 * @param selector
	 * @param tableName
	 * @return
	 */
	private String processColumnSelector(QueryColumnSelector selector, String tableName) {
		StringBuilder sb = new StringBuilder();
		String columnName = selector.getColumn();
		String alias = selector.getAlias();
		
		if (!columnName.equals(alias)) {
			sb.append("'" + columnName + "'").append(":").append("'" + alias + "'");
			renameColList.add(sb);
		}
		if(tableName != null)
			return new StringBuffer(tableName).append("['").append(alias).append("']") + "";
		else
			return "'" + alias + "'";
	}
	
	/**
	 * 
	 * @param frameName
	 * @param wrapperFrameName
	 * @param originalFrameName
	 */
	public void setDataTableName(String frameName, String wrapperFrameName) {
		this.frameName = frameName;
		this.wrapperFrameName = wrapperFrameName;
	}
	
	/**
	 * Process a constance selector. Different output is returned base off of passed in boolean inputs. 
	 * @param selector
	 * @return
	 */
	private String processConstantSelector(QueryConstantSelector selector) {
		Object constant = selector.getConstant();
		if(constant instanceof SubQueryExpression) {
			ITask innerTask = null;
			try {
				innerTask = ((SubQueryExpression) constant).generateQsTask();
				innerTask.setLogger(logger);
				if(innerTask.hasNext()) {
					Object value = innerTask.next().getValues()[0];
					if(value instanceof Number) {
						return value.toString();
					} else {
						return "\"" + constant + "\"";
					}
				}
			} catch(Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				if(innerTask != null) {
					try {
						innerTask.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
			
			// if this doesn't return anything...
			return "pd.NA";
		} else if(constant instanceof Number) {
			return constant.toString();
		} else {
			return "\"" + constant + "\"";
		}
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
	
	/**
	 * Generates the aggregate string to use in the pandas query. Takes into account the instances when 
	 * an aggregation is needed but no aggregation parameters are passed through (i.e when querying a column and 
	 * using GROUPBY & having. 
	 */
	private void genAggString()	{
		aggCriteria = new StringBuilder("");
		
		if (aggKeys.size() == 0 && !((SelectQueryStruct) this.qs).getGroupBy().isEmpty()) {
			if (havingList.size() == 0) {
				groupCriteria.delete(0, groupCriteria.length());
			}
			aggCriteria2.append(havingList.get(0));
		}
		
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
		
		if(aggCriteria.length() > 0 || aggCriteria2.length() > 0) {
			aggCriteria = new StringBuilder(".agg({").append(aggCriteria);
			aggCriteria2 = new StringBuilder(".agg(").append(aggCriteria2);
		}
		// just a way to say the override was added by this guy and not coming from outside
		if(overrideQuery != null && overrideQuery.length() > 0 && aggHash2.size() > 0  && !((SelectQueryStruct)qs).getParts().containsKey(SelectQueryStruct.Query_Part.QUERY))
		{
			overrideQuery.append("]");
		}
	}
	
	/** Processes the Selectors that use aggregation. 
	 * Need to be harmonized w/ addSelector. 	
	 * 
	 * @param selector
	 * @return
	 */
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
		
		//headers.add(selector.getAlias());
		aggHash.put(columnName, aggBuilder);
		// adding it through alias
		aggHash2.put(aggAlias, aggBuilder2);
		
		aggKeys.add(columnName);
		// also add the alias name
		if(!aggKeys.contains(aggAlias))
			aggKeys.add(aggAlias);
		
		// if it is a group concat.. dont add this to actual headers here.. since it will get added during group by
		functionMap.put(pandasFunction + columnName, selector.getAlias());
		
		// I can avoid all of this by creating a dataframe and imputing.. but let us see how far we can inline this
		// I am going to assume that this is the same type as header for most operations
		SemossDataType curType = this.colDataTypes.get(columnName);
		
		if(curType == SemossDataType.STRING || curType == SemossDataType.BOOLEAN) {
			//types.add(SemossDataType.INT);
			typesHash.put(aggAlias, SemossDataType.INT);
		} else if (curType == SemossDataType.INT && pandasFunction.equalsIgnoreCase("mean")) {
			//types.add(SemossDataType.DOUBLE);
			typesHash.put(aggAlias, SemossDataType.DOUBLE);
		} else {
			//types.add(curType);
			typesHash.put(aggAlias, curType);
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
		
		return "'" + aggAlias + "'";
	}
	/**
	 * Process arithmetic selector. Can handle column or Numeric input. Returns empty string - similar method build as 
	 * the function processing methods.
	 * @param selector
	 * @param tableName
	 * @param includeTableName
	 * @param useAlias
	 * @param useTable
	 * @return
	 */
	private String processArithmeticSelector(QueryArithmeticSelector selector, String tableName, boolean includeTableName, boolean useAlias, boolean...useTable) {
		IQuerySelector leftSelector = selector.getLeftSelector();
		IQuerySelector rightSelector = selector.getRightSelector();
		
		String mathExpr = selector.getMathExpr();
		String alias = selector.getAlias();
		String lColumnName = processSelector(leftSelector, tableName, includeTableName, useAlias, true);
		String rColumnName = processSelector(rightSelector, tableName, includeTableName, useAlias, true);
		
		StringBuilder sb = new StringBuilder();
		StringBuilder replace = new StringBuilder();
		
		SemossDataType leftDataType = SemossDataType.convertStringToDataType(leftSelector.getDataType());
		if (leftDataType == null) {
			if(leftSelector.getQueryStructName().contains("__")) {
				leftDataType = this.colDataTypes.get(leftSelector.getQueryStructName().split("__")[1]);
			} else {
				leftDataType = this.colDataTypes.get(leftSelector.getQueryStructName());
			}
		}
		SemossDataType rightDataType = SemossDataType.convertStringToDataType(rightSelector.getDataType());
		if (rightDataType == null) {
			if(rightSelector.getQueryStructName().contains("__")) {
				rightDataType = this.colDataTypes.get(rightSelector.getQueryStructName().split("__")[1]);
			} else {
				rightDataType = this.colDataTypes.get(rightSelector.getQueryStructName());
			}
			rightDataType = this.colDataTypes.get(rightSelector.getQueryStructName());
		}
		if (!(leftDataType == SemossDataType.INT || leftDataType == SemossDataType.DOUBLE) || 
				!(rightDataType == SemossDataType.INT || rightDataType == SemossDataType.DOUBLE)) {
			throw new IllegalArgumentException("Arithmetic selectors must be of type INT or DOUBLE.");
		}
		if (mathExpr.equals("/")) {
			replace.append(".replace([np.inf, -np.inf], np.nan)");
		}
		
		arithmeticKeys.add(alias);
		sb.append(alias).append("=(").append(lColumnName).append(mathExpr).append(rColumnName).append(")").append(replace).append(".values");
		arithmeticHash.put(alias, sb);
		
		if (leftDataType == SemossDataType.DOUBLE || rightDataType == SemossDataType.DOUBLE || mathExpr.equals("/")) {
			//types.add(SemossDataType.DOUBLE);
			typesHash.put(alias, SemossDataType.DOUBLE);
		} else {
			//types.add(SemossDataType.INT);
			typesHash.put(alias, SemossDataType.INT);
		}
		return "'" + alias + "'";
	}
	
	/**
	 * Create the assign(...) method to put together a new column based off of arithmetic. 
	 */
	private void genArithmeticString() {
		for (String key : arithmeticKeys) {
			if (arithmeticHash.containsKey(key)) {
				if (arithmeticCriteria.length() != 0) {
					arithmeticCriteria.append(",");
				}
				arithmeticCriteria.append(arithmeticHash.get(key));
			}
		}
		if (arithmeticCriteria.length() > 0) {
			arithmeticCriteria = new StringBuilder(".assign(").append(arithmeticCriteria).append(")");
		}
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
	
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////     Filters for python lambda functions /////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	/**
	 * Decode general filter into its simpler parts. Method is sued for selectors that use lambda functions for filtering, specifically
	 * those used in the IF_ELSE logic. 
	 * @param filter
	 * @param tableName
	 * @return
	 */
	private StringBuilder processFilter(IQueryFilter filter, String tableName) {
		IQueryFilter.QUERY_FILTER_TYPE filterType = filter.getQueryFilterType();
		if (filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return processSimpleQueryFilter((SimpleQueryFilter) filter, tableName);
		} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return processAndQueryFilter((AndQueryFilter) filter, tableName);
		} else if (filterType == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return processOrQueryFilter((OrQueryFilter) filter, tableName);
		}
		return null;
	}
	/**
	 * Process Simple query filters. Passes to methods that construct lambda functions for filtering. 
	 * @param filter
	 * @param tableName
	 * @return
	 */
	private StringBuilder processSimpleQueryFilter(SimpleQueryFilter filter, String tableName) {
		NounMetadata leftComp = filter.getLComparison();
		NounMetadata rightComp = filter.getRComparison();
		String thisComparator = filter.getComparator();
		FILTER_TYPE fType = filter.getSimpleFilterType();
		
		if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return createLambdaFilter(leftComp, rightComp, thisComparator, tableName);
		} else if (fType == FILTER_TYPE.VALUES_TO_COL) {
			return createLambdaFilter(rightComp, leftComp, IQueryFilter.getReverseNumericalComparator(thisComparator), tableName);
		} else if (fType == FILTER_TYPE.COL_TO_COL) {
			// TODO
		} else if(fType == FILTER_TYPE.COL_TO_QUERY) {
			return createSubqueryLambdaFilter(leftComp, rightComp, thisComparator, tableName);
		} else if(fType == FILTER_TYPE.QUERY_TO_COL) {
			return createSubqueryLambdaFilter(rightComp, leftComp, IQueryFilter.getReverseNumericalComparator(thisComparator), tableName);
		}
		return null;
	}
	
	/**
	 * Create lambda string for filtering. Used specifically in the IF_ELSE processor. 
	 * @param leftComp
	 * @param rightComp
	 * @param thisComparator
	 * @param tableName
	 * @return
	 */
	private StringBuilder createLambdaFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator, String tableName) {
		// Get the data object to be filtered on as well as data type
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		SemossDataType leftDataType = SemossDataType.convertStringToDataType(leftSelector.getDataType());
		
		String leftSelectorExpression = processSelector(leftSelector, tableName, true, false, false);
		
		if (leftDataType == null) {
			if(leftSelector.getQueryStructName().contains("__")) {
				leftDataType = this.colDataTypes.get(leftSelector.getQueryStructName().split("__")[1]);
			} else {
				leftDataType = this.colDataTypes.get(leftSelector.getQueryStructName());
			}
		}
		
		List<Object> objects = new ArrayList<>();
		if (rightComp.getValue() instanceof List) {
			objects.addAll((List) rightComp.getValue());
		} else {
			objects.add(rightComp.getValue());
		}
		
		StringBuilder retBuilder = new StringBuilder();
		
		if (PandasSyntaxHelper.OPERATOR_LIST.contains(thisComparator)) {
			for (int i = 0; i < objects.size(); i++) {
				if (retBuilder.length() > 0) {
					if (thisComparator.equals("!=")) {
						retBuilder.append(" and ");
					} else if (thisComparator.equals("==")) {
						retBuilder.append(" or ");
					} else {
						throw new IllegalArgumentException("Cannot pass multiple filter values when using oprator " + thisComparator);
					}
				}
				if (leftDataType == SemossDataType.INT || leftDataType == SemossDataType.DOUBLE) {
					retBuilder.append("(x[").append(leftSelectorExpression).append("]").append(thisComparator).append(objects.get(i)).append(")");
				} else if (leftDataType == SemossDataType.DATE) {
					retBuilder.append("(x[").append(leftSelectorExpression).append("].strftime('%Y-%m-%d')").append(thisComparator).append("'")
					  .append(objects.get(i)).append("')");
				} else if (leftDataType == SemossDataType.TIMESTAMP) {
					retBuilder.append("(x[").append(leftSelectorExpression).append("].strftime('%Y-%m-%d %H-%M:%s')").append(thisComparator).append("'")
					  .append(objects.get(i)).append("')");
				} else if (leftDataType == SemossDataType.STRING) {
					retBuilder.append("(x[").append(leftSelectorExpression).append("]").append(thisComparator).append("'")
							  .append(objects.get(i)).append("')");
				}
				else {
					throw new IllegalArgumentException("Unsupported data type " + leftDataType);
				}
			}
		} else if (thisComparator.equals(SEARCH_COMPARATOR) || thisComparator.equals(NOT_SEARCH_COMPARATOR)) {
			for (int i = 0; i < objects.size(); i++) {
				if (retBuilder.length() > 0) {
					if (thisComparator.equals(NOT_SEARCH_COMPARATOR)) {
						retBuilder.append(" and ");
					} else  {
						retBuilder.append(" or ");
					}
				}
				retBuilder.append("('").append(objects.get(i).toString().toLowerCase()).append("'");;
				if (thisComparator.equals(NOT_SEARCH_COMPARATOR)) {
					retBuilder.append(" not ");
				}
				if (leftDataType == SemossDataType.DATE) {
					retBuilder.append("in x[").append(leftSelectorExpression).append("].strftime('%Y-%m-%d')");
				} else if (leftDataType == SemossDataType.TIMESTAMP) {
					retBuilder.append("in x[").append(leftSelectorExpression).append("].strftime('%Y-%m-%d %H-%M:%s')");
				} else {
					retBuilder.append("in x[").append(leftSelectorExpression).append("].casefold()");
				}
				retBuilder.append(")");
			}
		} else if (thisComparator.equals(BEGINS_COMPARATOR) || thisComparator.equals(ENDS_COMPARATOR)) {
			String function = thisComparator.equals(BEGINS_COMPARATOR) ? "startswith":"endswith";
			for (int i = 0; i < objects.size(); i++) {
				if (retBuilder.length() > 0) {
					retBuilder.append(" or ");
				}
				if (leftDataType == SemossDataType.STRING) {
					retBuilder.append("(").append("x[").append(leftSelectorExpression).append("].casefold().")
					  .append(function).append("('").append(objects.get(i).toString().toLowerCase()).append("'))");
				} else if (leftDataType == SemossDataType.DATE) {
					retBuilder.append("(").append("x[").append(leftSelectorExpression).append("].strftime('%Y-%m-%d').")
					  .append(function).append("('").append(objects.get(i).toString().toLowerCase()).append("'))");
				} else if (leftDataType == SemossDataType.TIMESTAMP) {
					retBuilder.append("(").append("x[").append(leftSelectorExpression).append("].strftime('%Y-%m-%d %H-%M:%s').")
					  .append(function).append("('").append(objects.get(i).toString().toLowerCase()).append("'))");
				} else {
					throw new IllegalArgumentException("Unsupported data type " + leftDataType + " for filter operator +" + thisComparator);
				}
			}
		} else if (thisComparator.equals(NOT_BEGINS_COMPARATOR) || thisComparator.equals(NOT_ENDS_COMPARATOR)) {
			String function = thisComparator.equals(NOT_BEGINS_COMPARATOR) ? "startswith":"endswith";
			for (int i = 0; i < objects.size(); i++) {
				if (retBuilder.length() > 0) {
					retBuilder.append(" and ");
				}
				if (leftDataType == SemossDataType.STRING) {
					retBuilder.append("(").append("not x[").append(leftSelectorExpression).append("].casefold().")
					  .append(function).append("('").append(objects.get(i).toString().toLowerCase()).append("'))");
				} else if (leftDataType == SemossDataType.DATE) {
					retBuilder.append("(").append("not x[").append(leftSelectorExpression).append("].strftime('%Y-%m-%d').")
					  .append(function).append("('").append(objects.get(i).toString().toLowerCase()).append("'))");
				} else if (leftDataType == SemossDataType.TIMESTAMP) {
					retBuilder.append("(").append("not x[").append(leftSelectorExpression).append("].strftime('%Y-%m-%d %H-%M:%s').")
					  .append(function).append("('").append(objects.get(i).toString().toLowerCase()).append("'))");
				} else {
					throw new IllegalArgumentException("Unsupported data type " + leftDataType + " for filter operator " + thisComparator);
				}
			}
		} else {
			throw new IllegalArgumentException("Unsupported operator type used. ");
		}
		
		return retBuilder;
	}
	
	/**
	 * Flush the subquery to a list of values and add a normal filter
	 * @param leftComp
	 * @param rightComp
	 * @param thisComparator
	 * @param tableName
	 * @param useAlias
	 * @param captureColumns
	 * @return
	 */
	private StringBuilder createSubqueryLambdaFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator, String tableName) {
		// flush out the right side to a list of values
		SelectQueryStruct subQs = (SelectQueryStruct) rightComp.getValue();
		IRawSelectWrapper subQueryValues = this.pandasFrame.query(subQs);
		List<Object> values = new ArrayList<>();
		while(subQueryValues.hasNext()) {
			values.add(subQueryValues.next().getValues()[0]);
		}
		NounMetadata newRightComp = new NounMetadata(values, SemossDataType.convertToPixelDataType(subQueryValues.getTypes()[0]));
		
		return createLambdaFilter(leftComp, newRightComp, thisComparator, tableName);
	}
	
	/**
	 * process filter that uses lambda functions. 
	 * @param filter
	 * @param tableName
	 * @return
	 */
	private StringBuilder processOrQueryFilter(OrQueryFilter filter, String tableName) {
		StringBuilder sb = new StringBuilder();
		List<IQueryFilter> filterList = filter.getFilterList();
		for (int i = 0; i < filterList.size(); i++) {
			if (i == 0) {
				sb.append("(");
			} else {
				sb.append(") or (");
			}
			sb.append(processFilter(filter, tableName));
		}
		return sb.append(")");
	}
	/**
	 * process filter that uses lambda functions. 
	 * @param filter
	 * @param tableName
	 * @return
	 */
	private StringBuilder processAndQueryFilter(AndQueryFilter filter, String tableName) {
		StringBuilder sb = new StringBuilder();
		List<IQueryFilter> filterList = filter.getFilterList();
		for (int i = 0; i < filterList.size(); i++) {
			if (i == 0) {
				sb.append("(");
			} else {
				sb.append(") and (");
			}
			sb.append(processFilter(filter, tableName));
		}
		return sb.append(")");
	}
	
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////       end filters for python lambda functions /////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Process filters. Handles both SQL HAVING and WHERE syntax. For general filters that use .loc[...].
	 */
	private StringBuilder processFilter(IQueryFilter filter, String tableName, boolean useAlias, boolean...useTable) {
		IQueryFilter.QUERY_FILTER_TYPE filterType = filter.getQueryFilterType();
		if (filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE && isHavingFilter) {
			return processSimpleHavingFilter((SimpleQueryFilter) filter, tableName, useAlias, useTable);
		} else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE && !isHavingFilter) {
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
	
	/*
	 * Process filter statements including OR parameter. Handles both SQL WHERE and HAVING syntax.
	 */
	private StringBuilder processOrQueryFilter(OrQueryFilter filter, String tableName, boolean useAlias, boolean...useTable) {
		StringBuilder filterBuilder = new StringBuilder();
		List<IQueryFilter> filterList = filter.getFilterList();
		int numOrs = filterList.size();
		for(int i = 0; i < numOrs; i++) {
			if(i == 0) {
				filterBuilder.append("(");
			} else {
				if (isHavingFilter) {
					filterBuilder.append(") or (");
				} else {
					filterBuilder.append(" ) | ( ");
				}
			}
			filterBuilder.append(processFilter(filterList.get(i), tableName, useAlias, useTable));
		}
		filterBuilder.append(")");
		return filterBuilder;
	}
	
	/**
	 * Process filter statements including AND parameter. Handles logic comparable to SQL WHERE and HAVING clauses. 
	 */
	private StringBuilder processAndQueryFilter(AndQueryFilter filter, String tableName, boolean useAlias, boolean...useTable) {
		StringBuilder filterBuilder = new StringBuilder();
		List<IQueryFilter> filterList = filter.getFilterList();
		int numAnds = filterList.size();
		for(int i = 0; i < numAnds; i++) {
			if(i == 0) {
				filterBuilder.append("(");
			} else {
				if (isHavingFilter) {
					filterBuilder.append(") and (");
				} else {
					filterBuilder.append(") & (");			
				}
			}
			filterBuilder.append(processFilter(filterList.get(i), tableName, useAlias, useTable));
		}
		filterBuilder.append(")");
		return filterBuilder;
	}
	
	/**
	 * Process the BetweenQueryFilter content. Output is either attached to filterCriteria or havingCriteria, depending on 
	 * whether a function is being used as input. 
	 * 
	 * @param filter
	 * @param tableName
	 * @param useAlias
	 * @param useTable
	 * @return
	 */
	private StringBuilder processBetweenQueryFilter(BetweenQueryFilter filter, String tableName, boolean useAlias, boolean...useTable) {
		StringBuilder retBuilder = new StringBuilder("((");
		StringBuilder havingsAggBuilder = new StringBuilder();
		
		IQuerySelector selector = filter.getColumn();
		
		if (selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			SemossDataType selectorDataType = SemossDataType.convertStringToDataType(selector.getDataType());
			if (selectorDataType == null) {
				if(selector.getQueryStructName().contains("__")) {
					selectorDataType = this.colDataTypes.get(selector.getQueryStructName().split("__")[1]);
				} else {
					selectorDataType = this.colDataTypes.get(selector.getQueryStructName());
				}
			}
			
			String columnName = processSelector(selector, tableName, true, useAlias, true);
			if (selectorDataType == SemossDataType.INT || selectorDataType == SemossDataType.DOUBLE) {
				retBuilder.append(columnName).append(" >= ").append(filter.getStart()).append(") & (")
						  .append(columnName).append(" <= ").append(filter.getEnd()).append(")");
			} else if (selectorDataType == SemossDataType.DATE) {
				retBuilder.append(columnName).append(".apply(pd.to_datetime).dt.date >= pd.to_datetime('").append(filter.getStart()).append("').date()) & (")
						  .append(columnName).append(".apply(pd.to_datetime).dt.date <= pd.to_datetime('").append(filter.getEnd()).append("').date())");
			} else if (selectorDataType == SemossDataType.TIMESTAMP) {
				retBuilder.append(columnName).append(".apply(od.to_datetime) >= pd.to_datetime('").append(filter.getStart()).append("')) & (")
						  .append(columnName).append(".apply(pd.to_datetime) <= pd.to_datetime('").append(filter.getEnd()).append("'))");
			} else {
				throw new IllegalArgumentException("Invalid column input.");
			}
		} else if (selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
			String function = ((QueryFunctionSelector) selector).getFunction();
			String pandasFunction = QueryFunctionHelper.convertFunctionToPandasSyntax(function);
			String columnName = selector.getAllQueryColumns().get(0).getAlias();
			
			havingsAggBuilder.append(selector.getAlias()).append("=('").append(columnName).append("','")
							 .append(pandasFunction).append("')");
			havingList.add(havingsAggBuilder);
			
			retBuilder.append("x['").append(columnName).append("'].").append(pandasFunction).append("() >= ")
					  .append(filter.getStart()).append(") & (x['").append(columnName).append("'].").append(pandasFunction)
					  .append("() <= ").append(filter.getEnd()).append(")");
		}
		return retBuilder.append(")");
	}

	private StringBuilder processSimpleQueryFilter(SimpleQueryFilter filter, String tableName, boolean useAlias, boolean...useTable) {
		NounMetadata leftComp = filter.getLComparison();
		NounMetadata rightComp = filter.getRComparison();
		String thisComparator = filter.getComparator();
		
		FILTER_TYPE fType = filter.getSimpleFilterType();
		if(fType == FILTER_TYPE.COL_TO_COL) {
			return addSelectorToSelectorFilter(leftComp, rightComp, thisComparator, tableName, useAlias, useTable);
			// EXPERIMENT
			//return testAddSelectorToSelectorFilter(leftComp, rightComp, thisComparator, tableName, useAlias, useTable);
		} else if(fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToValuesFilter(leftComp, rightComp, thisComparator, tableName, useAlias, useTable);
		} else if(fType == FILTER_TYPE.VALUES_TO_COL) {
			return addSelectorToValuesFilter(rightComp, leftComp, IQueryFilter.getReverseNumericalComparator(thisComparator), tableName, useAlias, useTable);
		} else if(fType == FILTER_TYPE.COL_TO_QUERY) {
			return addSelectorToQueryFilter(leftComp, rightComp, thisComparator, tableName, useAlias, useTable);
		} else if(fType == FILTER_TYPE.QUERY_TO_COL) {
			return addSelectorToQueryFilter(rightComp, leftComp, IQueryFilter.getReverseNumericalComparator(thisComparator), tableName, useAlias, useTable);
		} else if(fType == FILTER_TYPE.COL_TO_LAMBDA) {
			return addSelectorToLambda(leftComp, rightComp, thisComparator, tableName, useAlias);
		} else if(fType == FILTER_TYPE.LAMBDA_TO_COL) {
			// same logic as above, just switch the order and reverse the comparator if it is numeric
			return addSelectorToLambda(rightComp, leftComp, IQueryFilter.getReverseNumericalComparator(thisComparator), tableName, useAlias);
		}
		else if(fType == FILTER_TYPE.VALUE_TO_VALUE) {
			// WHY WOULD YOU DO THIS!!!
		}
		return null;
	}
	
	/**
	 * 
	 * @param leftComp
	 * @param rightComp
	 * @param thisComparator
	 * @return
	 */
	private StringBuilder addSelectorToLambda(NounMetadata leftComp, NounMetadata rightComp, String thisComparator, String tableName, boolean useAlias) {
		// need to evaluate the lambda on the right
		IReactor reactor = (IReactor) rightComp.getValue();
		NounMetadata nounEvaluated = reactor.execute();

		Map<String, Object> mergeMetadata = reactor.mergeIntoQsMetadata();
		if(mergeMetadata.get(IReactor.MERGE_INTO_QS_FORMAT).equals(IReactor.MERGE_INTO_QS_FORMAT_SCALAR)) {
			return addSelectorToValuesFilter(leftComp, nounEvaluated, thisComparator, tableName, useAlias);
		}
		
		throw new IllegalArgumentException("Unknown qs format to merge");
	}
	
	private StringBuilder testAddSelectorToSelectorFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator, String tableName, boolean useAlias, boolean...useTable) {
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		IQuerySelector rightSelector = (IQuerySelector) rightComp.getValue();
		
		String lSelector = processSelector(leftSelector, tableName, true, useAlias, true);
		String rSelector = processSelector(rightSelector, tableName, true, useAlias, true);
		
		StringBuilder filterBuilder = new StringBuilder();
		
		if (thisComparator.equals("<>")) {
			thisComparator = "!=";
		}
		if (!PandasSyntaxHelper.OPERATOR_LIST.contains(thisComparator)) {
			throw new IllegalArgumentException("");
		}
		
		filterBuilder.append("(").append(lSelector).append(thisComparator).append(rSelector).append(")");
		
		return filterBuilder;
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

	
	/**
	 * Method to create the filter syntax for pandas frames. Handles incorrect operand input for the given filter type. 
	 * 
	 * @param leftComp
	 * @param rightComp
	 * @param thisComparator
	 * @param tableName
	 * @param useAlias
	 * @param useTable
	 * @return
	 */
	private StringBuilder addSelectorToValuesFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator, String tableName, 
			boolean useAlias, boolean...useTable) {
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		SemossDataType leftDataType = SemossDataType.convertStringToDataType(leftSelector.getDataType());
		
		String leftSelectorExpression = processSelector(leftSelector, tableName, true, useAlias, useTable);
		
		if (leftDataType == null) {
			if(leftSelector.getQueryStructName().contains("__")) {
				leftDataType = this.colDataTypes.get(leftSelector.getQueryStructName().split("__")[1]);
			} else {
				leftDataType = this.colDataTypes.get(leftSelector.getQueryStructName());
			}
		}
		
		List<Object> objects = new ArrayList<>();
		boolean multi = false;
		if (rightComp.getValue() instanceof List) {
			objects.addAll((List) rightComp.getValue());
			multi = true;
		} else {
			objects.add(rightComp.getValue());
		}
		
		boolean addNullCheck = objects.remove(null);
		if (leftDataType != null && SemossDataType.isNotString(leftDataType)) {
			if (objects.remove("null") || objects.remove("nan") || (thisComparator.equals("==") && objects.remove(""))) {
				addNullCheck = true;
			}
		}
		if (!addNullCheck) {
			// are we searching for null?
			addNullCheck = IQueryInterpreter.getAllSearchComparators().contains(thisComparator) && 
					(objects.contains("n") || objects.contains("nu") || objects.contains("nul") || objects.contains("null"));
		}
		
		StringBuilder filterBuilder = new StringBuilder("(");
		StringBuilder retBuilder = new StringBuilder();
		
		if (addNullCheck) {
			if (thisComparator.equals("==") || IQueryInterpreter.getPosSearchComparators().contains(thisComparator)) {
				filterBuilder.append(wrapperFrameName).append("[").append(leftSelectorExpression).append("].isna())");
			} else if (thisComparator.equals("!=") || thisComparator.equals("<>") || IQueryInterpreter.getPosSearchComparators().contains(thisComparator)) {
				filterBuilder.append("~").append(wrapperFrameName).append("[").append(leftSelectorExpression).append("].isna())");
			}
		} else {
			filterBuilder = null;
		}
		
		if (leftDataType == SemossDataType.STRING || leftDataType == SemossDataType.DATE || leftDataType == SemossDataType.TIMESTAMP) {
			String myFilterFormatted = PandasSyntaxHelper.createPandasColVec(objects, leftDataType);
			
			if (leftDataType == SemossDataType.STRING && (thisComparator.equals("==") || thisComparator.equals("!="))) {
				retBuilder.append("(");
				if (thisComparator.equals("!=")) {
					retBuilder.append("~");
				}
				retBuilder.append(frameName).append("[").append(leftSelectorExpression).append("].isin").append(myFilterFormatted).append(")");
			} else if (thisComparator.equals(SEARCH_COMPARATOR) || thisComparator.equals(NOT_SEARCH_COMPARATOR)) {
				for (int i = 0; i < objects.size(); i++) {
					if (retBuilder.length() > 0) {
						retBuilder.append(" | ");
					}
					retBuilder.append("(");
					if (thisComparator.equals(NOT_SEARCH_COMPARATOR)) {
						retBuilder.append("~");
					}
					if (leftDataType == SemossDataType.DATE) {
						retBuilder.append(wrapperFrameName).append("[").append(leftSelectorExpression).append("].apply(pd.to_datetime).dt.strftime('%Y-%m-%d').str.contains('")
								  .append(objects.get(i)).append("',case=False)");
					} else if (leftDataType == SemossDataType.TIMESTAMP) {
						retBuilder.append(wrapperFrameName).append("[").append(leftSelectorExpression).append("].apply(pd.to_dateime).dt.strftime('%Y-%m-%d %H:%M:%s').str.contains('")
								  .append(objects.get(i)).append("',case=False)");
					} else {
						retBuilder.append(wrapperFrameName).append("[").append(leftSelectorExpression).append("].str.contains('")
								  .append(objects.get(i)).append("',case=False)");
					}
					retBuilder.append(")");
				}
			} else if (thisComparator.equals(BEGINS_COMPARATOR) || thisComparator.equals(ENDS_COMPARATOR)) {
				String function = thisComparator.equals(BEGINS_COMPARATOR) ? "startswith" : "endswith";
				for (int i = 0; i < objects.size(); i++) {
					if (retBuilder.length() > 0) {
						retBuilder.append(" | ");
					}
					retBuilder.append("(").append(wrapperFrameName).append("[").append(leftSelectorExpression).append("].str.casefold().str.")
							  .append(function).append("('").append(objects.get(i).toString().toLowerCase()).append("'))");
				}
			} else if (thisComparator.equals(NOT_BEGINS_COMPARATOR) || thisComparator.equals(NOT_ENDS_COMPARATOR)) {
				String function = thisComparator.equals(NOT_BEGINS_COMPARATOR) ? "startswith" : "endswith";
				for (int i = 0; i < objects.size(); i++) {
					if (retBuilder.length() > 0) {
						retBuilder.append(" | ");
					}
					retBuilder.append("(~").append(wrapperFrameName).append("[").append(leftSelectorExpression).append("].str.casefold().str.")
							  .append(function).append("('").append(objects.get(i).toString().toLowerCase()).append("'))");
				}
			} else if (leftDataType != SemossDataType.STRING) {
				if (multi) {
					if (!(thisComparator.equals("==") || thisComparator.equals("!="))) {
						throw new IllegalArgumentException("Unsupported operand argument '" + thisComparator + "' for filtering by multiple values.");
					}
					retBuilder.append("(");
					if (thisComparator.equals("!=")) {
						retBuilder.append("~");
					}
					retBuilder.append(frameName).append("[").append(leftSelectorExpression).append("].apply(pd.to_datetime).isin").append(myFilterFormatted).append(")");
				} else {
					retBuilder.append("(").append(frameName).append("[").append(leftSelectorExpression).append("].apply(pd.to_datetime) ").append(thisComparator)
							  .append(" pd.to_datetime('").append(objects.get(0)).append("'))");
				}
			} else {
				throw new IllegalArgumentException("Unsupported operand argument '" + thisComparator + "' for type String.");
			}
		} else {
			if (!PandasSyntaxHelper.OPERATOR_LIST.contains(thisComparator)) {
				throw new IllegalArgumentException("Unsupported operand argument '" + thisComparator + "' for type Numeric.");
			}
			for (int i = 0; i < objects.size(); i++) {
				if (retBuilder.length() > 0) {
					retBuilder.append(" | ");
				}
				retBuilder.append("(").append(frameName).append("[").append(leftSelectorExpression).append("].apply(lambda x: x")
						  .append(thisComparator).append(objects.get(i)).append("))");
			}
		}
		
		if (filterBuilder != null) {
			if (retBuilder.length() > 0) {
				retBuilder = new StringBuilder("(").append(filterBuilder).append(" & ").append(retBuilder).append(")");
			}
		}
		return retBuilder;
	}
	
	/**
	 * Flush the subquery to a list of values and add a normal filter
	 * @param leftComp
	 * @param rightComp
	 * @param thisComparator
	 * @param tableName
	 * @param useAlias
	 * @param useTable
	 * @return
	 */
	private StringBuilder addSelectorToQueryFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator, String tableName, boolean useAlias, boolean...useTable) {
		// flush out the right side to a list of values
		SelectQueryStruct subQs = (SelectQueryStruct) rightComp.getValue();
		IRawSelectWrapper subQueryValues = this.pandasFrame.query(subQs);
		List<Object> values = new ArrayList<>();
		while(subQueryValues.hasNext()) {
			values.add(subQueryValues.next().getValues()[0]);
		}
		NounMetadata newRightComp = new NounMetadata(values, SemossDataType.convertToPixelDataType(subQueryValues.getTypes()[0]));
		
		return addSelectorToValuesFilter(leftComp, newRightComp, thisComparator, tableName, useAlias, useTable);
	}

	
	/** Passes the query containing the HAVING clause to be processed. Potentially could combine with other methods, doesn't
	 *  need to be as fleshed out as addFilters. 
	 * 
	 * @param filters
	 * @param tableName
	 * @param builder
	 * @param useAlias
	 */
	public void addHavingFilters(List<IQueryFilter> filters, String tableName, StringBuilder builder, boolean useAlias) {	
		if (filters.size() > 0) {
			isHavingFilter = true;
		}
		if (isHavingFilter && ((SelectQueryStruct) this.qs).getGroupBy().isEmpty()) {
			throw new IllegalArgumentException("Invalid query statement. A GroupBy(...) is required for filtering by functions.");
		}
		for (IQueryFilter filter : filters) {
			StringBuilder filterSyntax = processFilter(filter, tableName, useAlias);
			
			if (builder.length() > 0) {
				builder.append(" and ");
			}
			if (filterSyntax != null) {
				builder.append(filterSyntax.toString());
			}
		}
		isHavingFilter = false;
	} 
		
	/** additional processing of HAVING query. At the moment will only contain column - > value operation. Need to look at examples
	 *  of other operations to see if they're relevant. 
	 * 
	 * @param filter
	 * @param tableName
	 * @param useAlias
	 * @param useTable
	 * @return
	 */
	private StringBuilder processSimpleHavingFilter(SimpleQueryFilter filter, String tableName, boolean useAlias, 
			boolean...useTable) {
		NounMetadata leftComp = filter.getLComparison();
		NounMetadata rightComp = filter.getRComparison();
		String thisOperator = filter.getComparator();
		
		FILTER_TYPE fType = filter.getSimpleFilterType();
		if (fType == FILTER_TYPE.COL_TO_VALUES) {
			return createHavingFilter(leftComp, rightComp, thisOperator, tableName, useAlias, useTable);
		} else if (fType == FILTER_TYPE.VALUES_TO_COL) {
			return createHavingFilter(rightComp, leftComp, IQueryFilter.getReverseNumericalComparator(thisOperator), tableName, useAlias, useTable);
		}
		return null;
	}
	
	/** Creates the HAVING expression. StringBuilder is set up for (... operator ...) format. Populates a private
	 * List that is used to construct an aggregate if none is passed through the query. 
	 * 
	 * @param leftComp
	 * @param rightComp
	 * @param operator
	 * @param tableName
	 * @param useAlias
	 * @param useTable
	 * @return
	 */
	private StringBuilder createHavingFilter(NounMetadata leftComp, NounMetadata rightComp, String operator, String tableName, 
			boolean useAlias, boolean...useTable) {
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		QueryFunctionSelector selector = (QueryFunctionSelector) leftSelector;
		
		String function = selector.getFunction();
		String columnName = selector.getAllQueryColumns().get(0).getAlias();
		String pandasFunction = QueryFunctionHelper.convertFunctionToPandasSyntax(function);
		
		List<Object> values = new Vector<Object>();
		if (rightComp.getValue() instanceof List) {
			values.addAll((List) rightComp.getValue());
		} else {
			values.add(rightComp.getValue());
		}		
		
		StringBuilder havingsAggBuilder = new StringBuilder();
		StringBuilder retBuilder = new StringBuilder();
		
		havingsAggBuilder.append(selector.getAlias())
						 .append("=('")
						 .append(columnName)
						 .append("','")
						 .append(pandasFunction)
						 .append("')");
		havingList.add(havingsAggBuilder);
		
		for (int index = 0; index < values.size(); index++) {
			retBuilder.append("(x['")
					  .append(columnName)
					  .append("'].")
					  .append(pandasFunction)
					  .append("()")
					  .append(operator)
					  .append(values.get(index))
					  .append(")");
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
