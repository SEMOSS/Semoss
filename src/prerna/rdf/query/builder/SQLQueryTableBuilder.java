/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.rdf.query.builder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import com.google.gson.Gson;
import com.google.gson.internal.StringMap;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.util.SEMOSSQuery;
import prerna.rdf.query.util.SQLConstants;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.SQLQueryUtil;

public class SQLQueryTableBuilder extends AbstractQueryBuilder{
	static final Logger logger = LogManager.getLogger(SQLQueryTableBuilder.class.getName());
	IEngine engine = null;
	
	SEMOSSQuery semossQuery = new SEMOSSQuery();
	private static Hashtable <String,String> aliases = new Hashtable<String,String>();
	Hashtable <String, String> tableProcessed = new Hashtable<String, String>();
	Hashtable <String, String> columnProcessed = new Hashtable<String,String>();
	List<String> totalVarList = new ArrayList<String>();
	List<Hashtable<String,String>> nodeV = new ArrayList<Hashtable<String,String>>();
	List<Hashtable<String,String>> predV = new ArrayList<Hashtable<String,String>>();
	List<Map<String,String>> nodePropV = new ArrayList<Map<String,String>>();
	String variableSequence = "";
	List<String> returnVarOrder;
	
	int limit = -1;
	int limitFilter = 100;
	private boolean useOuterJoins = false;
	private SQLQueryUtil queryUtil;
	
	Hashtable <String,ArrayList<String>> tableHash = new Hashtable <String,ArrayList<String>>(); // contains the processed node name / table name and the properties
	String joins = "";
	String leftOuterJoins = "";
	String rightOuterJoins = "";
	String selectors = "";
	List<String> joinsArr = new ArrayList<String>();
	List<String> leftJoinsArr = new ArrayList<String>();
	List<String> rightJoinsArr = new ArrayList<String>();
	String nullSelectors = "";
	String froms = "";
	String filters = "";
	HashMap<String,String> searchFilter = new HashMap();
	HashMap<String,Boolean> clearFilter = new HashMap();

	String queryBindAndNoLimit = "";
	private static final String SQL_SELECTOR_BIND = "{selectorBind}";
	private static final String SQL_FILTER_BIND = "{filterBind}";
	private boolean useDistinct = true;
	private String groupBy = "";
	String parameters = "";

	public SQLQueryTableBuilder(IEngine engine)
	{
		this.engine = engine;
		SQLQueryUtil.DB_TYPE dbType = SQLQueryUtil.DB_TYPE.H2_DB;
		String dbTypeString = engine.getProperty(Constants.RDBMS_TYPE);
		if (dbTypeString != null) {
			dbType = (SQLQueryUtil.DB_TYPE.valueOf(dbTypeString));
		}
		
		queryUtil = SQLQueryUtil.initialize(dbType);
	}
	
	public void setLimit(int limit)	{
		this.limit = limit;
	}
	@Override
	public void buildQuery() 
	{
		String useOuterJoinsStr = engine.getProperty(Constants.USE_OUTER_JOINS);
		
		if(useOuterJoinsStr!=null && (useOuterJoinsStr.equalsIgnoreCase("TRUE") || useOuterJoinsStr.equalsIgnoreCase("YES")))
			useOuterJoins = true;
		
		semossQuery.setQueryType(SQLConstants.SELECT);
		semossQuery.setDisctinct(useDistinct);
		parsePath();
		// we are assuming properties are passed in now based on user selection
//		parsePropertiesFromPath(); 
		configureQuery();	
		makeQuery();
	}
	
	public void makeQuery(){
	
		String tempQueryFilter = filters + " AND " + SQL_FILTER_BIND;
		//used by search filter logic off of the explorer table (enter a filter value and hit the server)
		if(searchFilter.size()>0){
			tempQueryFilter = SQL_FILTER_BIND;
		}
		
		if(!useOuterJoins){
			query = queryUtil.getDialectInnerJoinQuery(useDistinct, selectors, froms, joins, filters, parameters, limit, groupBy);
			queryBindAndNoLimit = queryUtil.getDialectInnerJoinQuery(useDistinct, SQL_SELECTOR_BIND, froms, joins, tempQueryFilter, parameters, -1, groupBy);
		} else {
			query = queryUtil.getDialectFullOuterJoinQuery(useDistinct,selectors,rightJoinsArr,leftJoinsArr,joinsArr,filters, limit, groupBy);
			queryBindAndNoLimit = queryUtil.getDialectFullOuterJoinQuery(useDistinct,SQL_SELECTOR_BIND,rightJoinsArr,leftJoinsArr,joinsArr,tempQueryFilter, -1, groupBy);
		}
	}
	
	@Override
	public String getQuery() {
		return query;
	}
	
	
	protected void parsePath(){
		Hashtable<String, List> parsedPath = QueryBuilderHelper.parsePath(this.builderData, engine);
		totalVarList = parsedPath.get(QueryBuilderHelper.totalVarListKey);
		nodeV = parsedPath.get(QueryBuilderHelper.nodeVKey);
		predV = parsedPath.get(QueryBuilderHelper.predVKey);
	}
	
	@Override
	public void setBuilderData(QueryBuilderData queryBuilderData) {
//		QueryBuilderHelper.cleanBuilderData(engine, queryBuilderData);
		this.builderData = queryBuilderData;
		this.returnVarOrder = queryBuilderData.getReturnVars();
		this.semossQuery.setReturnVarOrder(this.returnVarOrder);
		this.nodePropV = queryBuilderData.getNodeProps();
	}

	protected void configureQuery()
	{
		// I need to pull each predicate
		// from the predicate identify what are the tables the user is querying
		// use alias for each one of these 
		// for each table I need to also get the column names in this table
		// for which I need to execute a query to get it
		// and then jam it completely in
		
		assimilateNodes();
		assimilateProperties();
		createSelectors();
		
		for(int predIndex = 0;predIndex < predV.size();predIndex++)
		{
			// get the predicate
			Hashtable <String,String> predInfoHash = predV.get(predIndex);
			// get the predicate out of it
			String predicate = predInfoHash.get("Pred");
			// split it in the dot
			// this is of the format
			// from_tablename.columnname.to_tablename.columnname
			predicate = Utility.getInstanceName(predicate);
			String [] items = predicate.split("\\.");
			// this will yield 4 strings
			String fromTable = items[0];
			String fromColumn = items[1];
			String toTable = items[2];
			String toColumn = items[3];
			
			if(!tableProcessed.containsKey(fromTable)) {
				tableProcessed.put(fromTable, fromTable);
			}
			if(!tableProcessed.containsKey(toTable)) {
				tableProcessed.put(toTable, toTable);
			}
			
			String join = getAlias(fromTable) + "." + fromColumn + "=" + getAlias(toTable) + "." + toColumn;
			
			joinsArr.add(join);
			if(joins.length() > 0)
				joins = joins + " AND " + join;
			else 
				joins = join;
		}		
		
		// finalize the filters
		filterData();
	}
	
	private void filterData()
	{
		Map<String, List<Object>> filterResults = this.builderData.getFilterData();
		List<List<String>> relTriples = this.builderData.getRelTriples();
		
		/**
		 * {TITLE=[http://semoss.org/ontologies/concept/TITLE/127_Hours, http://semoss.org/ontologies/concept/TITLE/12_Years_a_Slave, http://semoss.org/ontologies/concept/TITLE/16_Blocks, http://semoss.org/ontologies/concept/TITLE/17_Again]}
		 * {TITLE=[http://semoss.org/ontologies/concept/TITLE/American Hustle]}
		 * 
		 */
		
		if(filterResults != null)
		{
		
			Iterator<String> keys = filterResults.keySet().iterator();
			while(keys.hasNext()) // Process one column at a time
			{
				String currentFilters = "";
				String logName = keys.next(); // This gets me the Logical Name
//				String columnValue = engine.getTransformedNodeName(Constants.DISPLAY_URI + logName, false);
				String columnValue = "http://semoss.org/ontologies/Concept/" + logName;


				String tableValue = Utility.getInstanceName(columnValue);
				String simpleColumnValue = Utility.getPrimaryKeyFromURI(columnValue);
				
		        if(columnValue.contains("http://semoss.org/ontologies/Relation/Contains")){// this is such a mess... if this is a physical property name, we need to query the owl to figure out its table name :(
		        	String xmlQuery = "SELECT ?concept WHERE { ?concept rdfs:subClassOf <http://semoss.org/ontologies/Concept>. ?concept <http://www.w3.org/2002/07/owl#DatatypeProperty> <"+columnValue+">}";
		        	TupleQueryResult ret = (TupleQueryResult) engine.execOntoSelectQuery(xmlQuery);
					String conceptURI = null;
		        	try {
						if(ret.hasNext()){
							BindingSet row = ret.next();
							conceptURI = row.getBinding("concept").getValue().toString();
						}
					} catch (QueryEvaluationException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		        	simpleColumnValue = Utility.getInstanceName(columnValue);
		        	tableValue = Utility.getClassName(conceptURI);
		        }
				
				//we should skip adding a column to the filter if the column is not part of the selector
//				if(!columnProcessed.containsKey(columnValue.toUpperCase()) || !columnProcessed.containsKey(Utility.getPrimaryKeyFromURI(columnValue).toUpperCase())){
				if(!columnProcessed.containsKey(logName)) { // Should always keep the log name aside once we process a col
					continue;
				}
				
				String alias = getAlias(tableValue);
				// get the list
				List<Object> filterValues = (List<Object>)filterResults.get(Utility.getInstanceName(logName));
				
				//transform the column value
				columnValue = alias + "." + simpleColumnValue;
				
				for(int filterIndex = 0;filterIndex < filterValues.size();filterIndex++)
				{
					//if the filter value is blank we dont want to try to use utility.getinstance, it'll return a null value and cause an error
					
					String instance = Utility.getInstanceName(filterValues.get(filterIndex) + "");
					if(instance == null){
						instance = "";
					}
					instance = instance.replaceAll("'", "''");

					if(filterIndex%1000==0){
						if(filterIndex == 0){
							currentFilters += " ( ";
						} else {
							currentFilters += " ) OR ";
						}
						currentFilters += columnValue + " IN ( ";
					} else {
						currentFilters += " , " ;
					}
					
					SQLQueryUtil.DB_TYPE type = ((RDBMSNativeEngine) this.engine).getDbType();
					if(type.equals(SQLQueryUtil.DB_TYPE.H2_DB) || type.equals(SQLQueryUtil.DB_TYPE.SQL_Server)) {
						//H2 requires WHERE clauses to use single apostrophe - we are cleaning out single apostrophes on upload so this works
						currentFilters +=  "'" + instance + "'" ;
					} else {
						//Other RDBMS types take in quotation marks in WHERE clauses so we don't need to use apostrophes or scrub the data of them
						currentFilters +=  "\"" + instance + "\"" ;
					}
				}
				if(currentFilters.length() > 0){
					currentFilters+=")";// close your last in clause
					if(filters.length() > 0)// if we already have a filter on a column, add the AND clause before appending our new filter
						filters+= " AND ";
					filters += currentFilters + " ) ";
				}
			}

		}
		semossQuery.setSQLFilter(filters);
	}
	
	private void assimilateNodes()
	{
		if(nodeV != null) {
			for(int listIndex = 0;listIndex < nodeV.size();listIndex++) {
				Hashtable <String,String> node = nodeV.get(listIndex);
				String logicalNodeName = (String) node.get("varKey");
				
				ArrayList <String> propList = new ArrayList<String>();
				if(tableHash.containsKey(logicalNodeName)) {
					propList = tableHash.get(logicalNodeName);
				}
				propList.add(logicalNodeName);
				tableHash.put(logicalNodeName, propList);
				addToVariableSequence(logicalNodeName);
			}
		}
	}
	
	private void addToVariableSequence(String asName)
	{
		if(variableSequence.length() > 0)
			variableSequence = variableSequence + ";" + asName;
		else
			variableSequence = asName;
	}
	
	private void assimilateProperties()
	{
		// the list has three properties
		// nodePropV is the list
		/**
		 * This is of this format
		 * SelectedNodeProps=[{SubjectVar=Title, uriKey=http://semoss.org/ontologies/Relation/Contains/MovieBudget, varKey=TITLELOGICALNAME}]
		 */
		if(nodePropV != null)
		{
			for(int listIndex = 0;listIndex < nodePropV.size();listIndex++)
			{
				String logicalTableName = (String) nodePropV.get(listIndex).get("SubjectVar");
				String logicalPropName = (String) nodePropV.get(listIndex).get("varKey");
//				prop = Utility.getInstanceName(prop); //TODO: need to make sure FE always passes this back as URI, different from comment above
				ArrayList <String> propList = new ArrayList<String>();
				if(tableHash.containsKey(logicalTableName))
					propList = tableHash.get(logicalTableName);
				propList.add(logicalPropName);
				tableHash.put(logicalTableName, propList);
				addToVariableSequence(logicalPropName);
			}
		}
	}
	
	private void createSelectors()
	{
		String singleSelector = "";
		String nullSingleSelector = "";
		//ArrayList <String> tColumns = getColumnsFromTable(tableName);
		// instead get this from what they sent
	
		String [] columns = variableSequence.split(";");
		// order the columns based on what was in JSON
		List<String> orderedCols = SEMOSSQuery.orderVars(Arrays.asList(columns), returnVarOrder);
		orderedCols.toArray(columns);
		
		for(int colIndex = 0;colIndex < columns.length;colIndex++)
		{
			String tableName = null;
			String varURI = null;
			String asName = columns[colIndex];
//			varURI = this.engine.getTransformedNodeName(Constants.DISPLAY_URI + asName, false);
			varURI = "http://semoss.org/ontologies/Concept/" + asName;

			if(!varURI.startsWith(Constants.CONCEPT_URI)) { //Means we are working with a property nows
				for(Map<String, String> s : this.nodePropV) {
					if(asName.equals(s.get("varKey"))) {
//						tableName = Utility.getInstanceName(this.engine.getTransformedNodeName(Constants.DISPLAY_URI + s.get("SubjectVar"), false));
						tableName = s.get("SubjectVar");
						varURI = Constants.CONCEPT_URI + Utility.getInstanceName(varURI) + "/" + tableName;
						break;
					}
				}
			}
			
			String colName = Utility.getPrimaryKeyFromURI(varURI);
			tableName = Utility.getInstanceName(varURI);
			String alias = getAlias(tableName);
			
			if(!columnProcessed.containsKey(asName))
			{
				// the as query needs to reflect how I am sending eventually on the var headers
				singleSelector = alias + "." + colName + " AS " + asName;
				nullSingleSelector = " NULL AS " + asName;
				
				if(selectors.length() == 0) {
					selectors = singleSelector;
					nullSelectors = nullSingleSelector;
				} else {
					selectors = selectors + " , " + singleSelector;
					nullSelectors += " , " + nullSingleSelector;
				}
				
				semossQuery.addSingleReturnVariable(singleSelector);
				columnProcessed.put(asName, asName);					
			}
			addFrom(tableName, alias);
		}
	}
	
	private void addFrom(String tableName, String alias)
	{
		if(!tableProcessed.containsKey(tableName))
		{
			tableProcessed.put(tableName,"true");
			String fromText =  tableName + "  " + alias;
			if(froms.length() > 0){
				froms = froms + " , " + fromText;
				rightJoinsArr.add(queryUtil.getDialectOuterJoinRight(fromText));
				leftJoinsArr.add(queryUtil.getDialectOuterJoinLeft(fromText));
			} else {
				froms = fromText;
				rightJoinsArr.add(fromText);
				leftJoinsArr.add(fromText);
			}
		}
	}
	
	public static String getTableNameByAlias(String alias){
		alias = alias.toUpperCase();
		String returnTable = "";
		Enumeration<String> allTables = aliases.keys();
		while(allTables.hasMoreElements()){
			String currentTable = allTables.nextElement();
			String currentTableAlias = aliases.get(currentTable);
			if(currentTableAlias.equals(alias)){
				returnTable = currentTable;
				break;
			}
		}
		return returnTable;
	}
	
	public static String getAlias(String tableName)
	{
		String response = null;
		if(aliases.containsKey(tableName))
			response = aliases.get(tableName);
		else
		{
			boolean aliasComplete = false;
			int count = 0;
			String tryAlias = "";
			while(!aliasComplete)
			{
				if(tryAlias.length()>0){
					tryAlias+="_";//prevent an error where you may create an alias that is a reserved word (ie, we did this with "as")
				}
				tryAlias = (tryAlias + tableName.charAt(count)).toUpperCase();
				aliasComplete = !aliases.containsValue(tryAlias);
				count++;
			}
			response = tryAlias;
			aliases.put(tableName, tryAlias);
		}
		return response;
	}
	
	public ArrayList<Map<String,String>> getHeaderArray(){
		
		ArrayList<Map<String,String>> retArray = new ArrayList<Map<String,String>>();
		retArray.addAll(nodeV);
		retArray.addAll(nodePropV);
		
		Hashtable <String, Map<String,String>> sequencer = new Hashtable <String, Map<String,String>>();
		// add the filter queries
		// each list looks like this
		// Prop looks like this SelectedNodeProps=[{SubjectVar=Title, uriKey=TITLE, varKey=Title__TITLE}
		// node looks like a prop except it has no varKey I am told
		// i need a way to align this back to what I am spitting out upfront

		//we need to encode the equals signs, for when json sends the qry back over
		//tempjoins = tempjoins.replaceAll("=", "%3D"); //&#61;
		
		for(Map<String, String> headerHash : retArray){
			String logicalPropName = headerHash.get(QueryBuilderHelper.varKey); // title__rottenTomatoes
			String key = logicalPropName;
			// the var key needs to match the capitalization
			headerHash.put(QueryBuilderHelper.varKey, logicalPropName);
			String propPhysicalURI = (String) headerHash.get("uriKey"); // Get the prop physical URI
			String tableName = Utility.getInstanceName(propPhysicalURI);
			String colName = Utility.getPrimaryKeyFromURI(propPhysicalURI);
			
			String filterQuery = "";
			// need to modify the filter if there is an underscore to skip only to the last one
//			if(varName.contains("__"))
//			{
//				tableName = varName.substring(0,varName.indexOf("__"));
//				colName = varName.substring(varName.indexOf("__") + 2);
//			}
			String tableAlias = getAlias(tableName);
			String singleSelector = tableAlias + "." + colName + " AS " + logicalPropName;
			String columnForFilter = tableAlias + "." + colName ;
			String tempSearchFilter = "";
			boolean clearFilterResults = false;
			if(searchFilter.size()>0 && searchFilter.containsKey(logicalPropName)){
				tempSearchFilter = searchFilter.get(logicalPropName).replaceAll("%", "%25");//encode percent..
			}
			if(clearFilter.size()>0 && clearFilter.containsKey(logicalPropName)){
				if(clearFilter.get(logicalPropName)){
					tempSearchFilter = "";//overwrite the searchfilter logic (if it was set..), both shouldnt be coming in anyway.
					clearFilterResults = true;
				}
			}
			
			if(filters.length() > 0 && !clearFilterResults){ 
				//filter that query down even further, make sure you are showing the distinct values for that query.
				String filterAddition = columnForFilter + " IS NOT NULL ";//dont start with "AND" since we already appended it when adding the SQL_FILTER_BIND to the query
				if(tempSearchFilter.length() > 0 && !clearFilterResults) 
					filterAddition += " AND " + tempSearchFilter;
				
				filterQuery = queryBindAndNoLimit.replace(SQL_SELECTOR_BIND, singleSelector);
				filterQuery = filterQuery.replace(SQL_FILTER_BIND, filterAddition);
				filterQuery = filterQuery.replaceAll("=", "%3D");

			} else {
				filterQuery = "SELECT DISTINCT " + singleSelector + " FROM " + tableName + " " + tableAlias ;
				if(tempSearchFilter.length() > 0) 
					filterQuery += " WHERE " + tempSearchFilter; 
			}
			filterQuery += " ORDER BY 1 LIMIT " +  limitFilter;
			//System.out.println("DEBUG " + filterQuery);
			headerHash.put(QueryBuilderHelper.queryKey, filterQuery);
			sequencer.put(key, headerHash);
		}
		
		retArray = new ArrayList<Map<String,String>>();
		String [] vars = variableSequence.split(";");
		for(int varIndex = 0;varIndex < vars.length;varIndex++)
			retArray.add(sequencer.get(vars[varIndex]));
		
		return retArray;
	}
	
	private ArrayList<String> getColumnsFromTable(String table)
	{
		String query = queryUtil.getDialectAllColumns(table) ; //"SHOW COLUMNS from "
		ISelectWrapper sWrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		ArrayList <String> columns = new ArrayList<String>();
		while(sWrapper.hasNext())
		{
			ISelectStatement stmt = sWrapper.next();
			String colName = stmt.getVar(queryUtil.getAllColumnsResultColumnName())+"";
			columns.add(colName);
		}
		return columns;
	}
	
	@Override
	public SEMOSSQuery getSEMOSSQuery(){
		return this.semossQuery;
	}
	// adds the table
	public void addTable(String tableName, List<String> properties, List<String> properties2)
	{
		
		// first add the selectors
		String selectString = "";
		String tableAlias = getAlias(tableName);
		for(int propIndex = 0;propIndex < properties.size();propIndex++)
		{
			String propName = properties.get(propIndex);
			String asName = properties2.get(propIndex);
			String asString = tableAlias + "." + propName + "  AS " + asName;

			if(selectString.length() == 0)
				selectString = asString;
			else
				selectString = selectString + ", " + asString;
		}
		if(selectors.length() == 0)
			selectors = selectString;
		else if(selectString.length() != 0)
			selectors = selectors + ", " + selectString;
		
		// now add the from
		addFrom(tableName, tableAlias);	
	}
	
	// add a relationship
	public void addRelation(String relationName, String uri, String operator, boolean addToSelect)
	{
		// we can pick up the relationship from here
		//String relation = Utility.getInstanceName(relationName);
		String [] items = relationName.split("\\.");
		// this will yield 4 strings
		String fromTable = items[0];
		String fromColumn = items[1];
		String toTable = items[2];
		String toColumn = items[3];
		
		String fromAlias = getAlias(fromTable);
		String toAlias = getAlias(toTable);
		
		addFrom(fromTable, fromAlias);
		addFrom(toTable, toAlias);
		
		String join = fromAlias + "." + fromColumn + "=" + toAlias + "." + toColumn;
		
		if(joins.length() == 0)
			joins = join;
		else
			joins = joins + " " + operator + " " + join;
		
		if(addToSelect) // add this to selector
		{
			String qualifiedRelationName = uri + "/";
			addSelector(null, "concat('" + qualifiedRelationName + "'," + fromAlias + "." + fromColumn + ",':'," + toAlias + "." + toTable + ")");
		}

	}
	
	// add a filter
	public void addStringFilter(String tableName, String propertyAsName, Vector <String> instances)
	{
		String tableAlias = getAlias(tableName);
		String colAlias = tableAlias + "." + propertyAsName;
		String allFilters = "" ;
		for(int instanceIndex = 0;instanceIndex < instances.size();instanceIndex++)
		{
			String filterString = colAlias + " = '" + instances.get(instanceIndex) + "' ";  
			if(allFilters.length() == 0)
				allFilters = "(" + filterString;
			else
				allFilters = allFilters + " OR " + filterString;	
		}
		allFilters = allFilters + ")";
		
		if(filters.length() == 0)
			filters = allFilters;
		else
			filters = filters + " AND " + allFilters;
		
	}
	
	// add from
	public void addSelector(String table, String colName)
	{
		// the table can be null
		if(table != null) // this is a derived data
		{
			String tableAlias = getAlias(table);
			colName = tableAlias + "." + colName;
		}
		if(selectors.length() == 0)
			selectors = colName;
		else
			selectors = selectors + " , " + colName;
	}
	
	public List<Hashtable<String, String>> getNodeV(){
		return this.nodeV;
	}
	
	public List<Hashtable<String, String>> getPredV(){
		return this.predV;
	}
	
	public List<Map<String, String>> getNodePropV(){
		return this.nodePropV;
	}
	
}
