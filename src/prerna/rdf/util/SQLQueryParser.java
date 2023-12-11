/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.rdf.util;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;

public class SQLQueryParser extends AbstractQueryParser {

	public static final String conceptUri = "http://semoss.org/ontologies/Concept/";
	public static final String propertyUri = "http://semoss.org/ontologies/Relation/Contains/";
	
	private HashMap<String,String[]> tripleMappings = new HashMap<String,String[]>(); 
	private Hashtable <String, String> whereClauseVars = new Hashtable<String, String>();

	
	public SQLQueryParser(){
		super();
	}
	
	public SQLQueryParser(String query){
		super(query);
	}
	
//	public static void main(String[] args) throws Exception {
//		basicParseTest();
//	}
//	
	
	/**
	 * parse the query into pieces
	 */
	@Override
	public void parseQuery(){
		
		Statement statement;
		try {
			statement = CCJSqlParserUtil.parse(query);

			//only support parsing select at this time, we don't need update and insert parsing in the forseeable future
			if(statement instanceof Select){
				//the joins in sql are important because they help us figure out the triples, processAllTableJoins 
				parseTablesAndAlias(statement); //generate the props/and partially generate the joins.
				parseAllPropertiesAndVarsFromQuery(statement); //generate the return variables list and properties list as well other part of the joins
				
			} else {
				System.err.println("An error occurred, the sql statement you are trying to parse is not parseable " + query);
			}
		}  catch (JSQLParserException e1) {
			e1.printStackTrace();
		}
	}
	
	/**
	 * Get the tables and their aliases from the select statement
	 * generate a partial list of joins (these would be the explicit inner/outer joins)
	 * @param statement
	 * @throws JSQLParserException
	 */
	private void parseTablesAndAlias(Statement statement) throws JSQLParserException {
		HashMap<Column,Column> joinColumnsMap = new HashMap<Column, Column>();
		CCJSqlParserManager parserManager = new CCJSqlParserManager();
		Select selectStatement = (Select) statement;

		statement = (Select) parserManager.parse(new StringReader(query));
		
        List<PlainSelect> plainSelectList = getPlainSelectList(selectStatement);
		if(plainSelectList!=null && plainSelectList.size()>0){
			for(PlainSelect ps: plainSelectList){
				//get the first table in the from clause 
				FromItem initialTable = ps.getFromItem();
                if(initialTable instanceof Table) {
					setTableAndAlias((Table) initialTable);

					List<Join> psJoins = ps.getJoins();
					if(psJoins != null) {
						for(Join psJoin: psJoins){
							//System.out.println("join INFO " + psJoin.toString());
							Expression exp = psJoin.getOnExpression();
							if(exp != null) {
								//TODO: what if it is an AndExpression or OrExpression etc.
								//TODO: do we need to take these into consideration???
								if(exp instanceof EqualsTo) {
									EqualsTo joinExp = (EqualsTo) exp;
									//left part of the join
									Expression leftExpression = joinExp.getLeftExpression();
									Column leftJoinColumn = (Column) leftExpression;
									//right part of the join
									Expression rightExpression = joinExp.getRightExpression();
									Column rightJoinColumn = (Column) rightExpression;
									
									// since this is a map full of Column objects, you may get duplicates being added 
									// (if you are doing the SEMOSS outerjoin syntax, then you'll have a union
									// of two tables with the same joins, just left join vs right join
									// we'll filter the duplicates out later when we create tripleMappings.
									joinColumnsMap.put(leftJoinColumn, rightJoinColumn);
								}
							}
							//so get the table name from the join
							FromItem psJoinTable = psJoin.getRightItem();
							if(psJoinTable!=null){
								Table joinsTable = (Table) psJoinTable;
								setTableAndAlias(joinsTable);
							}
						}
					}
				}
			}
		}
			
		processAllTableJoins(joinColumnsMap);
	}
	

	/**
	 * The joinColumnsMap will contain the <left, right> columns of the join.  We will take the alias 
	 * that comes in and determine the table name linked to that alias, then store the name of the table 
	 * and the join column as a map within the columnMappings.  This way we can figure out the triples
	 * 
	 * T.Title = N.Title_FK
	 * Figure out the table name for T - Title
	 * Figure out the table name for N - Nominated
	 * 
	 * leftColumnMap will be a map of Title Title
	 * 
	 * @param joinColumnsMap
	 */
	private void processAllTableJoins(HashMap<Column,Column> joinColumnsMap){
		String joinColumnTable = ""; //may be the table name or alias, will use this as if its an alias to look up the table name
		String joinColumnTableName = "";
		String joinColumnName = "";
				
		//now I have my tables and their aliases, I can be sure I am grabbing the right table name from the join, and build the join/triple properly
		for(Column leftColumn: joinColumnsMap.keySet()){
			String[] relationships = new String[4];
			//process the left Column info first!
			//get the column name and column's table alias from the join information
			joinColumnTable = leftColumn.getTable().getName();
			joinColumnName = leftColumn.getColumnName();
			
			//look up the table name from the aliasTableMap
			joinColumnTableName = aliasTableMap.get(joinColumnTable);
			
			//now add the column and the table name to a map
			relationships[0] = joinColumnTableName;
			relationships[1] = joinColumnName;
			
			//now process the right column info
			Column rightColumn = joinColumnsMap.get(leftColumn);
			
			//get the column name and column's table alias from the join information
			joinColumnTable = rightColumn.getTable().getName();
			joinColumnName = rightColumn.getColumnName();
			
			//look up the table name from the aliasTableMap
			joinColumnTableName = aliasTableMap.get(joinColumnTable);
			
			//now add the column and the table name to a map
			relationships[2] = joinColumnTableName;
			relationships[3] = joinColumnName;
			
			//now add the left and right join info to the map
			//this is where the duplicates resolve
			String relTriple = "http://semoss.org/ontologies/Relation/" + Arrays.toString(relationships).replace(",",".").replace("[","").replace("]","").replaceAll("\\s+","");
			tripleMappings.put(relTriple,relationships);
			
		}	
	}
	
	private void parseAllPropertiesAndVarsFromQuery(Statement statement) throws JSQLParserException {
		CCJSqlParserManager parserManager = new CCJSqlParserManager();

		Select selectStatement = (Select) statement;

		statement = (Select) parserManager.parse(new StringReader(query));
		
        List<PlainSelect> plainSelectList = getPlainSelectList(selectStatement);
		if(plainSelectList!=null && plainSelectList.size()>0){
			for(PlainSelect ps: plainSelectList){
				if(ps.getWhere()!=null){
					//System.out.println("Your where clause: "  + ps.getWhere().toString());
					Expression whereClause = ps.getWhere();
					getIndividualWhereClauseValues(props, whereClause); //gets the joins here too
				}
			}
		}
		parseReturnVariables(statement);
	}
	
	public Hashtable<String, Hashtable<String, String>> getReturnVarsFromQuery(String query) {
		CCJSqlParserManager parserManager = new CCJSqlParserManager();
		try {
			Select selectStatement = (Select) parserManager.parse(new StringReader(query));
			parseTablesAndAlias(selectStatement);
			if(!aliasTableMap.isEmpty()) {
				parseReturnVariables(selectStatement);
			}
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
		
		return getReturnVariables();
	}
	
	
	/**
	 * Take in the Table object and parse out the table name and alias
	 * @param addTable
	 */
	private void setTableAndAlias(Table addTable){
		Alias tableAlias = addTable.getAlias();
		String tableName = addTable.getName();
		String tableAliasText = tableName; //so if there is no table alias, use the table name by default
		if(tableAlias!=null){ 
			tableAliasText = tableAlias.getName();
			aliasTableMap.put(tableAliasText, tableName); //the alias is the key because you can join the same table several times in a query
		}

		types.put(tableName, conceptUri + tableName);
	}
	
	private void parseReturnVariables(Statement statement) throws JSQLParserException {
		CCJSqlParserManager parserManager = new CCJSqlParserManager();

		Select selectStatement = (Select) statement;
		statement = (Select) parserManager.parse(new StringReader(query));
		List<PlainSelect> plainSelectList =  getPlainSelectList(selectStatement); 
		if(plainSelectList!=null && plainSelectList.size()>0){
			for(PlainSelect ps: plainSelectList){
				List<SelectItem> selectList = ps.getSelectItems();
				for(int i =0; i<selectList.size(); i++) {
					SelectItem selectedItem = selectList.get(i);
					SelectExpressionItem se = (SelectExpressionItem) selectedItem;
					Alias alias = se.getAlias();
					String expressionAlias = ""; 
					if(alias!=null){
						expressionAlias = alias.getName(); //heres the alias for the select clause expression you are working with, unused at the moment
					} else {
						expressionAlias = se.toString();
					}
					Expression expression = se.getExpression();
					String expressionValue = expression.toString();
					returnVariables.add(expressionValue); 
					if(expression instanceof Function){
						hasColumnAggregatorFunction = true;
					}
					if(expression instanceof Column){
						Column returnColumn = (Column) expression;
						String tableAliasName = returnColumn.getTable().getName();
						//String fullyQualifiedName = returnColumn.getFullyQualifiedName();
						String columnName = returnColumn.getColumnName();
						String tableName = aliasTableMap.get(tableAliasName);
						//only add the property if the column is not the same as the table name.
						addToVariablesMap(typePropVariables, tableName, expressionAlias, columnName);
						addToVariablesMap(typeReturnVariables, tableName, expressionAlias, columnName);
					}
					//expression returned MAY contain the table name/table alias prior to the expression value.  Keeping this for now
					
					//to do, actually save it as expression, table alias, so you can look up the table if you need to 
					//(don't want to do table  because your table alias could be referring to a subquery)
				}
			}
		}
	}
	
	/**
	 * generate a partial list of joins (these would be the implicit inner/outer joins)
	 * also maintain the where clause variables (unused at this time)
	 * @param props
	 * @param exp
	 */
	private void getIndividualWhereClauseValues(Hashtable<String,String> props, Expression exp){
		if(exp instanceof Parenthesis){
			Expression x = ((Parenthesis) exp).getExpression();
			getIndividualWhereClauseValues(props, x);
			return;
		}
		BinaryExpression individualExpressions = (BinaryExpression) exp;
		HashMap<Column,Column> joinColumnsMap = new HashMap<Column, Column>();
		while(individualExpressions.getLeftExpression()!=null){
			Expression leftExpression = individualExpressions.getLeftExpression();
			Expression rightExpression = individualExpressions.getRightExpression();
			if((leftExpression !=null && rightExpression != null) &&  (leftExpression instanceof Column && rightExpression instanceof Column)){
				// if both left and right expressions are columns, special logic! You will add these to the joins map, 
				// later you will compare this against the owl to figure out if this is a legit join
				
				//update the properties objects first
				Column rightJoinColumn = (Column) rightExpression;
				setWhereClauseDetails(rightJoinColumn);
				Column leftJoinColumn = (Column) leftExpression;
				setWhereClauseDetails(leftJoinColumn);
				
				//generate joins list!
				joinColumnsMap.put(leftJoinColumn, rightJoinColumn);
				
				if(individualExpressions instanceof EqualsTo){ 
					break;
				}
				
			} else {
				if(rightExpression != null){
					// if you needed the right hand value assignment/bind, you can get it here. 
					// for now we are setting the variable rightHandAssignmentValue but not using it
					// use individualExpressions.getRightExpression().getRightExpression, then check cast as necessary to get values.
					String rightHandAssignmentValue = "";
					if(rightExpression instanceof DateValue){
						DateValue bindValue = (DateValue) rightExpression;
						rightHandAssignmentValue = bindValue.getValue().toString();
					} else if (rightExpression instanceof DoubleValue){
						DoubleValue bindValue = (DoubleValue) rightExpression;
						rightHandAssignmentValue = Double.toString(bindValue.getValue());
					} else if (rightExpression instanceof LongValue){
						LongValue bindValue = (LongValue) rightExpression;
						rightHandAssignmentValue = bindValue.getStringValue();
					} else if (rightExpression instanceof NullValue){
						rightHandAssignmentValue = "null";
					} else if (rightExpression instanceof StringValue){
						StringValue bindValue = (StringValue) rightExpression;
						rightHandAssignmentValue = bindValue.getValue();
					} else if (rightExpression instanceof TimeValue){
						TimeValue bindValue = (TimeValue) rightExpression;
						rightHandAssignmentValue = bindValue.getValue().toString();
					} else if (rightExpression instanceof Column){
						Column columnDetail = (Column)rightExpression;
						setWhereClauseDetails(columnDetail);
					} else if (rightExpression instanceof IsNullExpression){
						Expression expNull = ((IsNullExpression) rightExpression).getLeftExpression();
						Column columnDetail = (Column)expNull;
						setWhereClauseDetails(columnDetail);
						rightHandAssignmentValue = "null";
					} else {
						getIndividualWhereClauseValues(props, individualExpressions.getRightExpression());
					}
					
				}
				
				if(individualExpressions instanceof EqualsTo){
					Column columnDetail = (Column)leftExpression;
					setWhereClauseDetails(columnDetail);
					break;
				}
			}
			individualExpressions = (BinaryExpression) individualExpressions.getLeftExpression();
			
		}
		processAllTableJoins(joinColumnsMap);

	}
	
	/**
	 * 
	 * @param columnDetail
	 */
	private void setWhereClauseDetails(Column columnDetail){
		String columnName = columnDetail.getColumnName();
		Table tableName = columnDetail.getTable();
		String tableFullName = tableName.getName();
		Alias tableAlias = tableName.getAlias();
		String tableAliasText = tableFullName;
		if(tableAlias!=null){
			tableAliasText = tableAlias.getName();
		}
		whereClauseVars.put(tableAliasText + "__" + columnName, propertyUri + columnName);
	}
	
	private List<PlainSelect> getPlainSelectList(Select selectStatement){
		List<PlainSelect> plainSelectList = new ArrayList<PlainSelect>();
		//so basically if you have a more complex query 
		try {
			SetOperationList setList = (SetOperationList) selectStatement.getSelectBody();
			List<SelectBody> allSelectors = setList.getSelects();
			for(SelectBody s : allSelectors) {
				if(s instanceof PlainSelect) {
					plainSelectList.add((PlainSelect) s);
				}
			}
		} catch (Exception e){
			//System.out.println("more simple query");
			PlainSelect plainSel = (PlainSelect) selectStatement.getSelectBody();
			plainSelectList.add(plainSel);
		}
		return plainSelectList;
	}

	@Override
	public List<String[]> getTriplesData() {

		for(String key: tripleMappings.keySet()){
			String[] triple = new String[3];
			String[] mapping = tripleMappings.get(key);
			String nodeFrom = conceptUri + mapping[0];
			String nodeFromProperty = mapping[1];
			String nodeTo = conceptUri + mapping[2];
			String nodeToProperty = mapping[3];
			
			String relTriple = key;
			
			triple[0] = nodeFrom;
			triple[1] = relTriple;
			triple[2] = nodeTo;
			
			triplesData.add(triple);
		}
		return triplesData;
	}
	
	
	
	
	
	/////////////////////////tester methods/////////////////////////
	
	private static void basicParseTest(){
		//yes most of these queries are not real, but they are good tests to work with.
		String sql = "SELECT 1 as r,2,a,b FROM MY_TABLE1 MT, yourtable YT, andanothertable AAT where YT.x=1 and YT.r = MT.F and MT.T is not null";
		
		String subquery = "select x.col1, y.col2 from (select col1, col3 from newtable) x, anothertable y";

		String anotherAdvSql = "SELECT  DISTINCT N.Nominated AS MOOVIENOMINATED , count(N.Nominated) AS MOOVIENOMINATED2  "
				+ "FROM  Title T LEFT JOIN Nominated N ON T.Title=N.Title_FK GROUP BY N.Nominated UNION SELECT  "
				+ "DISTINCT N.Nominated AS MOOVIENOMINATED , count(N.Nominated) AS MOOVIENOMINATED2  FROM  Title T "
				+ "RIGHT JOIN Nominated N ON T.Title=N.Title_FK GROUP BY N.Nominated";
		//sql = anotherAdvSql;
		
		String anotherJoin = "select n.nominated, t.title from title t, nominated n where n.title_fk = t.title";
		//sql = anotherAdvSql; // sql = anotherJoin;
		String aggregateFunc = "SELECT  DISTINCT N.Nominated AS MOOVIENOMINATED , count(N.Nominated) AS MOOVIENOMINATED2  "
				+ "FROM  Title T LEFT JOIN Nominated N ON T.Title=N.Title_FK GROUP BY N.Nominated UNION "
				+ "SELECT  DISTINCT N.Nominated AS MOOVIENOMINATED , count(N.Nominated) AS MOOVIENOMINATED2 "
				+ " FROM  Title T RIGHT JOIN Nominated N ON T.Title=N.Title_FK GROUP BY N.Nominated";
		String sqlOrs= "SELECT  DISTINCT T.TITLE AS TITLE , T.MOVIEBUDGET AS TITLE__MOVIEBUDGET , N.NOMINATED AS NOMINATED  "
				+ "FROM  Title T LEFT JOIN Nominated N ON T.Title=N.Title_FK WHERE  ( T.TITLE = '127_Hours' "
				+ "OR T.TITLE = '12_Years_a_Slave' OR T.TITLE = '16_Blocks' OR T.TITLE = '17_Again' "
				+ "OR T.TITLE = '200_Cigarettes' OR T.TITLE = '47_Ronin' OR T.TITLE = '50-50' ) ";
				/*
				+ " UNION SELECT  DISTINCT T.TITLE AS TITLE , T.MOVIEBUDGET AS TITLE__MOVIEBUDGET , "
				+ "N.NOMINATED AS NOMINATED  FROM  Title T RIGHT JOIN Nominated N ON T.Title=N.Title_FK WHERE"
				+ "  ( T.TITLE = '127_Hours' OR T.TITLE = '12_Years_a_Slave' OR T.TITLE = '16_Blocks' OR T.TITLE = "
				+ "'17_Again' OR T.TITLE = '200_Cigarettes' OR T.TITLE = '47_Ronin' OR T.TITLE = '50-50' ) ";*/
		
		String oneMoreTest = "SELECT  DISTINCT T.TITLE AS TITLE , T.MOVIEBUDGET AS TITLE__MOVIEBUDGET , N.NOMINATED AS NOMINATED  FROM  Title T LEFT JOIN Nominated N ON T.Title=N.Title_FK WHERE  ( T.TITLE = '127_Hours' OR T.TITLE = '12_Years_a_Slave' OR T.TITLE = '16_Blocks' OR T.TITLE = '17_Again' OR T.TITLE = '200_Cigarettes' OR T.TITLE = '47_Ronin' OR T.TITLE = '50-50' )  UNION SELECT  DISTINCT T.TITLE AS TITLE , T.MOVIEBUDGET AS TITLE__MOVIEBUDGET , N.NOMINATED AS NOMINATED  FROM  Title T RIGHT JOIN Nominated N ON T.Title=N.Title_FK WHERE  ( T.TITLE = '127_Hours' OR T.TITLE = '12_Years_a_Slave' OR T.TITLE = '16_Blocks' OR T.TITLE = '17_Again' OR T.TITLE = '200_Cigarettes' OR T.TITLE = '47_Ronin' OR T.TITLE = '50-50' )";
		
		sql = oneMoreTest;
		
		
		
		AbstractQueryParser qryParse = new SQLQueryParser(sql);
		qryParse.setQuery(sql);
		qryParse.parseQuery();
		///////////////////////////
		Hashtable<String, String> nodes = qryParse.getNodesFromQuery();
		for(String key: nodes.keySet()){
			System.out.println("Node : " + key);
			
		}

		Hashtable<String,Hashtable<String,String>> propsVariables = qryParse.getPropertiesFromQuery();

		for(String key: propsVariables.keySet()){
			System.out.println("Iterate through props table : " + key);
			Hashtable<String, String> variablesInTable = (Hashtable<String,String>) propsVariables.get(key);
			for(String singleVariable: variablesInTable.keySet()){
				System.out.println("props table : " + key + " column alias " +singleVariable + " column name " + variablesInTable.get(singleVariable) );
			}
		}
		
		List<String[]> triplesArr =  qryParse.getTriplesData();
		for(String[] eachItem: triplesArr){
			System.out.println("each Triple : " + Arrays.toString(eachItem));
		}
		
		Hashtable<String,Hashtable<String, String>> returnVariables = qryParse.getReturnVariables();

		for(String key: returnVariables.keySet()){
			System.out.println("Iterate through returns table : " + key);
			Hashtable<String, String> variablesInTable = (Hashtable<String,String>) returnVariables.get(key);
			for(String singleVariable: variablesInTable.keySet()){
				System.out.println("returns table : " + key + " column alias " +singleVariable + " column name " + variablesInTable.get(singleVariable) );
			}
		}
		
		boolean aggregate = qryParse.hasAggregateFunction();
		System.out.println("is this an aggregate query " + aggregate);
	}
	
}