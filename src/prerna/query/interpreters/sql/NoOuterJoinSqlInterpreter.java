package prerna.query.interpreters.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.Utility;
import prerna.util.sql.SQLQueryUtil;

public class NoOuterJoinSqlInterpreter extends SqlInterpreter {	
	
	protected Set<String> selectorOrderedList = new LinkedHashSet<String>();
//	protected List<String> groupBySelectors = new Vector<String>();
//	protected List<String> orderBySelectors = new Vector<String>();
	// keep list of selectors for tables
	protected Map<String, LinkedHashSet<String>> retTableToSelectors = new HashMap<String, LinkedHashSet<String>>();
	// keep list of filters for tables
	protected Map<String, List<String>> retTableToFilters = new HashMap<String, List<String>>();
	protected Map<String, List<String>> retTableToHavingFilters = new HashMap<String, List<String>>();
	// keep list of traversed tables
	protected Set<String> traversedTables = new HashSet<String>();
	protected List<String> jTypeList = new Vector<String>();
	
	// so we can extend this class
	// but also have things register if we need to account for the 
	// outer join syntax or not
	protected boolean outerJoinsRequested = false;
	
	public NoOuterJoinSqlInterpreter() {
		
	}

	public NoOuterJoinSqlInterpreter(IEngine engine) {
		this.engine = engine;
		queryUtil = SQLQueryUtil.initialize(((RDBMSNativeEngine) engine).getDbType());
	}
	
	public NoOuterJoinSqlInterpreter(ITableDataFrame frame) {
		this.frame = frame;
	}

	/**
	 * Main method to invoke to take the QueryStruct to compose the appropriate SQL query
	 */
	@Override
	public String composeQuery()
	{
		if(this.qs instanceof HardSelectQueryStruct) {
			return ((HardSelectQueryStruct)this.qs).getQuery();
		}
		/*
		 * Need to create the query... 
		 * This to consider:
		 * 1) the user is going to be using the conceptual names as defined by the OWL (if present
		 * and OWL is the improved version). This has a few consequences:
		 * 1.a) when a user enters a table name, we need to determine what the primary key is
		 * 		for that table
		 * 1.b) need to consider what tables are used within joins and which are not. this will
		 * 		determine when we add it to the from clause or if the table will be defined via 
		 * 		the join 
		 */
		
		//if there are no outer joins requested, then call the SQL interpreter instead
		
		this.outerJoinsRequested = qs.getRelations().stream().anyMatch(s -> s[1].equalsIgnoreCase("outer.join"));
		if(!this.outerJoinsRequested) {
			// just return the parent
			return super.composeQuery();
		}
		
		addJoins();
		addSelectors();
		addFilters();
		addHavingFilters();
		
		String derivedTableName = "";
		String jQueryStr = "";
		StringBuilder query = new StringBuilder();
		StringBuilder subquery = new StringBuilder();
		String distinct = "";
		if (this.qs.isDistinct()) {
			distinct = "DISTINCT ";
		}
		
		//need to traverse in the order of the requested joins
		for (int i = 0; i < jTypeList.size(); i++){
			//clear the subquery sb 
			subquery.setLength(0);
			//get the current join type
			String jType = jTypeList.get(i);
			int nextOuterJoinIndex = jTypeList.subList(i, jTypeList.size()).indexOf("outer join");
			if (!jType.equals("outer join") && i < nextOuterJoinIndex){
				//process joins thru to just before the next outer join or if no more outer joins then to the last join
				int endSubsetIndex = (nextOuterJoinIndex == -1) ? jTypeList.size()-1 : nextOuterJoinIndex-1;
				SqlJoinStructList subQJoinStructList = joinStructList.getSubsetJoinStructList(i, endSubsetIndex);
				String subQJSyntax = subQJoinStructList.getJoinSyntax(derivedTableName, traversedTables, retTableToSelectors);
				String[] subQSelectorsFilters = determineH2JoinSelectorsFilters(subQJSyntax, derivedTableName);
				subquery.append("SELECT ").append(distinct).append(subQSelectorsFilters[0]).append(" FROM ");
				if (jQueryStr.isEmpty()){
					subquery.append(subQJSyntax);
				} else {
					subquery.append(jQueryStr).append(subQJSyntax);
				}
				if (!subQSelectorsFilters[1].isEmpty()){
					//subquery filters to process
					subquery.append(" WHERE ").append(subQSelectorsFilters[1]);
				}
				//set the i == endSubsetIndex
				i = endSubsetIndex;
			} else if (jType.equals("outer join")){
				//process outer join individually
				String[] subQJSyntax = joinStructList.getOuterJoinSyntax(derivedTableName, traversedTables, retTableToSelectors, i);
				String[] subQSelectorsFilters = determineOuterJoinSelectorsFilters(subQJSyntax[0], derivedTableName);
				String filterClause = "";
				if (!subQSelectorsFilters[2].isEmpty()){
					//subquery filters to process
					filterClause = " WHERE " + subQSelectorsFilters[2];
				}
				subquery.append("(SELECT ").append(distinct).append(subQSelectorsFilters[0]).append(" FROM ").append(jQueryStr)
					.append(subQJSyntax[0]).append(filterClause).append(") UNION (SELECT ").append(distinct).append(subQSelectorsFilters[1])
					.append(" FROM ").append(jQueryStr).append(subQJSyntax[1]).append(filterClause).append(") ");
			}
			//assign the processed joins to a derived table
			derivedTableName = getDerivedTableName();
			subquery.insert(0, "(");
			subquery.append(") "). append(derivedTableName).append(" ");
			//set aside the sql statement processed up to this point to jQueryStr
			jQueryStr = subquery.toString();
		}
		
		//once all the joins have been processed, select for just the columns user requested for
		String finalDerivedTableName = derivedTableName;
		String requestedSelectors = selectorAliases.stream().map(a -> finalDerivedTableName + "." + a).collect(Collectors.joining(" , "));
		query.append("SELECT ").append(distinct).append(requestedSelectors).append(" FROM ").append(jQueryStr);
	
		
		// add remaining where clause filters (would only be value-to-value type filters if there the map isn't empty)
		if (!retTableToFilters.isEmpty()){
			String filtersStr = retTableToFilters.values().stream().flatMap(Collection::stream).collect(Collectors.joining(" AND "));
			query.append(" WHERE ").append(filtersStr);
		}
		
		// add group by
		query = appendGroupBy(query);
//		if (!groupBySelectors.isEmpty()){
//			String groupByStr = String.join(" , ", groupBySelectors);
//			query.append(" GROUP BY ").append(groupByStr.replaceAll("[^ ]*\\.", ""));
//		}
		
		// add having filters
		if (!retTableToHavingFilters.isEmpty()){
			String havingFiltersStr = retTableToHavingFilters.values().stream().flatMap(Collection::stream).collect(Collectors.joining(" AND "));
			query.append(" HAVING ").append(havingFiltersStr.replaceAll("[^ (]*\\.", ""));
		}
		
		// add order by
		query = appendOrderBy(query);
//		if (!orderBySelectors.isEmpty()){
//			String orderByStr = String.join(" , ", orderBySelectors);
//			query.append(" ORDER BY ").append(orderByStr.replaceAll("[^ ]*\\.", ""));
//		}
		
		long limit = qs.getLimit();
		long offset = qs.getOffset();
		
		query = this.queryUtil.addLimitOffsetToQuery(query, limit, offset);
		
		if(query.length() > 500) {
			logger.info("SQL QUERY....  " + query.substring(0,  500) + "...");
		} else {
			logger.info("SQL QUERY....  " + query);
		}
		
		return query.toString();
	}

	/////////////////////////////////// adding from ////////////////////////////////////////////////

	/**
	 * Adds the form statement for each table
	 * @param conceptualTableName			The name of the table
	 */
	protected void addFrom(String conceptualTableName, String alias) {
		// need to determine if we can have multiple froms or not
		// we don't want to add the from table multiple times as this is invalid in sql
		if(!tablesProcessed.containsKey(conceptualTableName)) {
			tablesProcessed.put(conceptualTableName, "true");
			
			// we want to use the physical table name
			String physicalTableName = getPhysicalTableNameFromConceptualName(conceptualTableName);
			
			froms.add(new String[]{physicalTableName, alias});
		}
	}

	////////////////////////////////////// end adding from ///////////////////////////////////////
	
	/////////////////////////////////// adding join ////////////////////////////////////////////////

	
	/**
	 * Adds the join to the relationHash which gets added to the query in composeQuery
	 * @param fromCol					The starting column, this can be just a table
	 * 									or table__column
	 * @param thisComparator			The comparator for the type of join
	 * @param toCol						The ending column, this can be just a table
	 * 									or table__column
	 */
	@Override
	protected void addJoin(String fromCol, String thisComparator, String toCol) {
		// get the parts of the join
		String[] relConProp = getRelationshipConceptProperties(fromCol, toCol);
		String targetTable = relConProp[0];
		String targetColumn = relConProp[1];
		String sourceTable = relConProp[2];
		String sourceColumn = relConProp[3];
		
		String compName = thisComparator.replace(".", " ");
		if (!fromCol.equals(sourceTable)) {
			if (compName.startsWith("left")){
				compName = compName.replace("left", "right");
			} else if (compName.startsWith("right")){
				compName = compName.replace("right", "left");
			}
		}
		jTypeList.add(compName);
		
		SqlJoinStruct jStruct = new SqlJoinStruct();
		jStruct.setJoinType(compName);
		// add source
		jStruct.setSourceTable(sourceTable);
		jStruct.setSourceTableAlias(sourceTable);
		jStruct.setSourceCol(sourceColumn);
		// add target
		jStruct.setTargetTable(targetTable);
		jStruct.setTargetTableAlias(targetTable);
		jStruct.setTargetCol(targetColumn);
		
		joinStructList.addJoin(jStruct);
		
		// need to add the join keys to the retTableToSelectors map
		for (int i=0; i < relConProp.length; i+=2){
			QueryColumnSelector selector = new QueryColumnSelector();
			selector.setTable(relConProp[i]);
			selector.setColumn(relConProp[i+1]);
			processColumnSelector(selector, false);
		}
	}
	
	////////////////////////////////////// end adding join ///////////////////////////////////////
	
	////////////////////////////////////////// adding filters ////////////////////////////////////////////
	
	public void addHavingFilters() {
		if(!this.outerJoinsRequested) {
			super.addHavingFilters();
			return;
		}
		List<IQueryFilter> filters = qs.getHavingFilters().getFilters();
		for(IQueryFilter filter : filters) {
			Set<String> filterColumnAliases = filter.getAllUsedColumns();
			//for having filters, the columns that the filters are applied on must 
			//be among the selectors the users requested 
			if (selectorAliases.containsAll(filterColumnAliases)) {
				StringBuilder filterSyntax = processFilter(filter);
				String filterKey = String.join("__", filter.getAllUsedTables());
				retTableToHavingFilters.putIfAbsent(filterKey, new Vector<String>());
				retTableToHavingFilters.get(filterKey).add(filterSyntax.toString());
			}
		}
	}
	
	public void addFilters() {
		if(!this.outerJoinsRequested) {
			super.addFilters();
			return;
		}
		List<IQueryFilter> filters = qs.getCombinedFilters().getFilters();
		for(IQueryFilter filter : filters) {
			StringBuilder filterSyntax = processFilter(filter);
			String filterKey = (!filter.getAllUsedTables().isEmpty()) ? String.join("__", filter.getAllUsedTables()) : "tempReference";
			retTableToFilters.putIfAbsent(filterKey, new Vector<String>());
			retTableToFilters.get(filterKey).add(filterSyntax.toString());
		}
	}
	
	////////////////////////////////////// end adding filters ////////////////////////////////////////////

	
	//////////////////////////////////////append order by  ////////////////////////////////////////////
	
//	protected void addOrderBy(){
//		List<QueryColumnOrderBySelector> orderBy = qs.getOrderBy();
//		for(QueryColumnOrderBySelector orderBySelector : orderBy) {
//			String tableConceptualName = orderBySelector.getTable();
//			String colAlias = orderBySelector.getAlias().toUpperCase();
//			ORDER_BY_DIRECTION orderByDir = orderBySelector.getSortDir();
//			
//			//if the groupBy selector is not among the user-requested selectors, then 
//			//cannot be used as a groupBy selector
//			if (selectorAliases.contains(colAlias)){
//				orderBySelectors.add(tableConceptualName + "." + colAlias);
//			} else {
//				continue;
//			}
//		}	
//	}
	
	//////////////////////////////////////end append order by////////////////////////////////////////////
	
	
	//////////////////////////////////////append group by  ////////////////////////////////////////////
	
//	protected void addGroupBy(){
//		List<QueryColumnSelector> groupBy = qs.getGroupBy();
//		for(QueryColumnSelector groupBySelector : groupBy) {
//			String tableConceptualName = groupBySelector.getTable();
//			String colAlias = groupBySelector.getAlias().toUpperCase();
//			
//			//if the groupBy selector is not among the user-requested selectors, then 
//			//cannot be used as a groupBy selector
//			if (selectorAliases.contains(colAlias)){
//				groupBySelectors.add(tableConceptualName + "." + colAlias);
//			} else {
//				continue;
//			}
//		}
//	}
	
	//////////////////////////////////////end append group by////////////////////////////////////////////
	
	/**
	 * The second
	 * @param selector
	 * @param isTrueColumn
	 * @return
	 */
	protected String processColumnSelector(QueryColumnSelector selector, boolean notEmbeddedColumn) {
		String table = selector.getTable();
		String colName = selector.getColumn();
		String tableAlias = selector.getTableAlias();
		if(tableAlias == null) {
			tableAlias = getPhysicalTableNameFromConceptualName(table);
		}
		// will be getting the physical column name
		String physicalColName = colName;
		// if engine is not null, get the info from the engine
		if(engine != null && !engine.isBasic()) {
			// if the colName is the primary key placeholder
			// we will go ahead and grab the primary key from the table
			if(colName.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)){
				physicalColName = getPrimKey4Table(table);
				// the display name is defaulted to the table name
			} else {
				// default assumption is the info being passed is the conceptual name
				// get the physical from the conceptual
				physicalColName = getPhysicalPropertyNameFromConceptualName(table, colName);
			}
		}
		
		//keep track of columns by their selector reference syntax
		String tableCol = tableAlias + "." + physicalColName;
		String colAlias = selector.getAlias().toUpperCase();
		//if the column is a foreign key column, then need to ensure that its alias is unique
		if (colAlias.endsWith("_FK")){
			colAlias = tableAlias.toUpperCase() + "$$" + colAlias ;
		}
		this.retTableToSelectors.putIfAbsent(tableAlias, new LinkedHashSet<String>());
		this.retTableToSelectors.get(tableAlias).add(tableCol + " AS " + "\"" + colAlias +"\"");
		//if the selector is one that user-requested, then update selectorAliases 
		if (notEmbeddedColumn) {
			// keep track of all the processed columns
			this.retTableToCols.putIfAbsent(table, new Vector<String>());
			this.retTableToCols.get(table).add(colName);
		}
		
		// need to perform this check 
		// if there are no joins
		// we need to have a from table
		if(this.joinStructList.isEmpty()) {
			addFrom(table, tableAlias);
		}
		
		return tableCol;
	}
	
	
	//////////////////////////////////// traversal assistance /////////////////////////////////////
	
	protected String[] determineOuterJoinSelectorsFilters(String subqueryJoinSyntax, String derivedTableName) {
		//string @ index 0 will be the left join syntax
		//string @ index 1 will be the right join syntax
		//string @ index 2 will be the filters, if available
		String[] outerJoinSelectors = new String[3];
		String[] joinKeys = new String[2];
		Set<String> leftTables = new HashSet<String>();
		Set<String> rightTables = new HashSet<String>();
		String leftTableSelector = null;
		String rightTableSelector = null;
		
		Pattern p = Pattern.compile("[^ ]*=[^ ]*+");
		Matcher m = p.matcher(subqueryJoinSyntax);
		while (m.find()){
			joinKeys = m.group(0).split("=");
			String lTableCol = joinKeys[0];
			String rTableCol = joinKeys[1];
			String lTable = lTableCol.split("\\.")[0];
			String rTable = rTableCol.split("\\.")[0];
			
			//if there had been prior joins before this outer join, it's possible that 
			//the left table is a derived table reference
			if (!lTable.startsWith("derivedTempTable")){
				leftTables.add(lTable);
				leftTableSelector = parseTableColAlias(this.retTableToSelectors.get(lTable).stream()
					.filter(v -> v.split(" AS")[0].equals(lTableCol))
					.collect(Collectors.joining("")));
			} else {
				leftTableSelector =
						parseTableColAlias(this.retTableToSelectors.entrySet().stream()
						.map(e -> e.getValue().stream().filter(v -> v.split("\"")[1].equals(lTableCol.split("\\.")[1]))
										.collect(Collectors.joining("")))
						.collect(Collectors.joining("")));
				
			}
			
			//right table won't be a derived table reference
			rightTables.add(rTable);
			rightTableSelector = parseTableColAlias(this.retTableToSelectors.get(rTable).stream()
				.filter(v -> v.split(" AS")[0].equals(rTableCol))
				.collect(Collectors.joining("")));
		}
		
		//IDENTIFY RELEVANT FILTERS
		String subQueryFilter = getSubqueryFilters(derivedTableName, Stream.of(leftTableSelector.split("\\.")[0]).collect(Collectors.toSet()), rightTables);
		outerJoinSelectors[2] = subQueryFilter;
		
		//IDENTIFY RELEVANT SELECTORS
		//set left selectors
		String leftSelector = getSubquerySelectors(derivedTableName, leftTables, rightTables);
		outerJoinSelectors[0] = leftSelector;
		//set right selectors
		String leftKeyAlias = leftTableSelector.split("\\.")[1];
		String escapedleftJoinKey = joinKeys[0].replaceAll("([\\.\\$])", "\\\\$1");
		String rightSelector = leftSelector.replaceAll("(?:^|)" + escapedleftJoinKey + "(?:$|\\s)[^,]*", 
				Matcher.quoteReplacement(joinKeys[1] + " AS \"" + leftKeyAlias + "\" "));
		outerJoinSelectors[1] = rightSelector;
		
		return outerJoinSelectors;
	}

	protected String[] determineH2JoinSelectorsFilters(String subqueryJoinSyntax, String derivedTableName){
		//string @ index 0 will be the selectors & string @ index 1 will be the filters
		String[] selectorsFilters = new String[2];
		Set<String> leftTables = new HashSet<String>();
		Set<String> rightTables = new HashSet<String>();
		
		Pattern p = Pattern.compile("[^ ]*=[^ ]*+");
		Matcher m = p.matcher(subqueryJoinSyntax);
		while (m.find()){
			String[] joinKeys = m.group(0).split("=");
			String lTableCol = joinKeys[0];
			String rTableCol = joinKeys[1];
			String lTable = lTableCol.split("\\.")[0];
			String rTable = rTableCol.split("\\.")[0];
			
			if (!lTable.startsWith("derivedTempTable")){
				leftTables.add(lTable);
			}
			if (!rTable.startsWith("derivedTempTable")){
				rightTables.add(rTable);
			}
		}
		
		//IDENTIFY RELEVANT FILTERS
		String subQueryFilter = getSubqueryFilters(derivedTableName, leftTables, rightTables);
		selectorsFilters[1] = subQueryFilter;
		//IDENTIFY RELEVANT SELECTORS
		String subQuerySelector = getSubquerySelectors(derivedTableName, leftTables, rightTables);
		selectorsFilters[0] = subQuerySelector;
		return selectorsFilters;
	}
	
	protected String getSubqueryFilters(String derivedTableName, Set<String> leftTables, Set<String> rightTables){
		StringBuilder subFilters = new StringBuilder();
		String filterStatement = "";
		Iterator it = this.retTableToFilters.entrySet().iterator();
		while (it.hasNext()){
			Map.Entry entry = (Map.Entry) it.next();
			List<String> keyList = new ArrayList(Arrays.asList(((String) entry.getKey()).split("__")));
			List<String> derivedReferenceReplace = new ArrayList<String>(keyList);

			keyList.removeAll(leftTables);
			keyList.removeAll(rightTables);
			keyList.removeAll(traversedTables);
			if (keyList.isEmpty()){
				if (!traversedTables.isEmpty()){
					derivedReferenceReplace.retainAll(traversedTables);
					String regex = "\\b" + String.join("\\\\b\\\\.|\\\\b", derivedReferenceReplace) + "\\.\\b";
			        List<String> modifiedFilters = ((List<String>) entry.getValue()).stream()
			        		.map(i -> i.replaceAll(regex, derivedTableName + ".")).collect(Collectors.toList());
			        filterStatement = String.join(" AND ", modifiedFilters);
				} else {
					filterStatement = String.join(" AND ", ((List<String>) entry.getValue()));
				}
				
		        if (subFilters.length() == 0){
		        	subFilters.append(filterStatement);
		        }else {
		        	subFilters.append(" AND ").append(filterStatement);
		        }
		        
		        it.remove();
			}
		}
		return subFilters.toString();
	}
	
	protected String getSubquerySelectors(String derivedTableName, Set<String> leftTables, Set<String> rightTables){
		Set<String> subquerySelectorList = new LinkedHashSet<String>();
		String subSelectors = "";
		
		//first, update the selectors already in selectorOrderSet with the derived table name
		subSelectors = String.join(" , ", selectorOrderedList);
		subSelectors = subSelectors.replaceAll("[^ ]*\\.", derivedTableName + ".");

		//second, find/add the new selectors to the selectorOrderedSet
		for (String leftTable : leftTables){
			if (!traversedTables.contains(leftTable)) {
				subquerySelectorList.addAll(this.retTableToSelectors.get(leftTable));
				Set<String> leftTableCols = parseTableColAlias(this.retTableToSelectors.get(leftTable));
				selectorOrderedList.addAll(leftTableCols);
				traversedTables.add(leftTable);
			}
		}
		for (String rightTable : rightTables){
			if (!traversedTables.contains(rightTable)) {
				subquerySelectorList.addAll(this.retTableToSelectors.get(rightTable));
				Set<String> rightTableCols = parseTableColAlias(this.retTableToSelectors.get(rightTable));
				selectorOrderedList.addAll(rightTableCols);
				traversedTables.add(rightTable);
			}
		}
		if (subSelectors.length() > 0) {
			subSelectors = subSelectors + " , " + String.join(" , ", subquerySelectorList);
		} else {
			subSelectors = String.join(" , ", subquerySelectorList);
		}
		return subSelectors;
	}
	
	protected Set<String> parseTableColAlias(Set<String> selectorList){
		Set<String> tableColAlias = new LinkedHashSet<String>();
		for (String s : selectorList){
			tableColAlias.add(parseTableColAlias(s));
		}
		return tableColAlias;
	}
	
	protected String parseTableColAlias(String selector){
		if (!selector.isEmpty() && selector.contains("AS")){
			return selector.split("\\.")[0] + "." + selector.split("\"")[1];
		} else {
			//otherwise selector is an empty string
			return selector;
		}
	}
	
	////////////////////////////////// end traversal assistance ///////////////////////////////////
	
	/////////////////////////////// other utility methods /////////////////////////////////////////
	
	protected String getDerivedTableName(){
		return "derivedTempTable" + Utility.getRandomString(4);
	}
	
	////////////////////////////////////////// end other utility methods ///////////////////////////////////////////
	
	
	///////////////////////////////////////// test method /////////////////////////////////////////////////
	
	public static void main(String[] args) {
//		// load in the engine
//		TestUtilityMethods.loadDIHelper();
//
//		//TODO: put in correct path for your database
//		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\Movie_RDBMS.smss";
//		RDBMSNativeEngine coreEngine = new RDBMSNativeEngine();
//		coreEngine.setEngineId("Movie_RDBMS");
//		coreEngine.openDB(engineProp);
//		DIHelper.getInstance().setLocalProperty("Movie_RDBMS", coreEngine);
		
//		String str = "curTabl.col as \"alias\", curTabl.col as \"alias\", curTabl.col as \"alias\", curTabl.col as \"alias\", curTabl.col as \"alias\"";
//		Pattern p = Pattern.compile("[^ ]*\\.");
//		Matcher m = p.matcher(str);
//		while (m.find()){
//			System.out.println(m.group(0));
//		}
//		String str1 = str.replaceAll("[^ ]*\\.", "tempTABLE.");
//		System.out.println(str1);
		
	}

	///////////////////////////////////////// end test methods //////////////////////////////////////////////
	
}