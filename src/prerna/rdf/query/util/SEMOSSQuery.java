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
package prerna.rdf.query.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

import prerna.util.Utility;
import prerna.util.sql.SQLQueryUtil;

public class SEMOSSQuery {
	private static final String main = "Main";
	private List<SPARQLTriple> triples = new ArrayList<SPARQLTriple>();
	private List<TriplePart> retVars = new ArrayList<TriplePart>();
	private List<String> returnVariables = new ArrayList<String>(); //string equivalent list
	private List<List<String>> returnTripleArray = new ArrayList<List<String>>();
	private Hashtable<TriplePart, ISPARQLReturnModifier> retModifyPhrase = new Hashtable<TriplePart,ISPARQLReturnModifier>();
	private Hashtable<String, SPARQLPatternClause> clauseHash = new Hashtable<String, SPARQLPatternClause>();
	private SPARQLGroupBy groupBy = null;
	private SPARQLOrderBy orderBy = null;
	private SPARQLBindings bindings = null;
	private Integer limit = null;
	private Integer offset = null;
	private String customQueryStructure ="";
	private String queryType;
	private boolean distinct = false;
	private String query;
	private String queryID;
	private String SQLFilter = "";
	private SQLQueryUtil queryUtil = null;
	private boolean useOuterJoins = false;
	private List<String> returnVarOrder;

	public SEMOSSQuery()
	{
	}
	
	public SEMOSSQuery(String queryID)
	{
		this.queryID = queryID;
	}
	
	public void setQueryID (String queryID)
	{
		this.queryID = queryID;
	}
	
	public String getQueryID()
	{
		return this.queryID;
	}
	
	public void createQuery()
	{
		query = queryType + " ";
		if(distinct)
			query= query+ "DISTINCT ";
		
		String retVarString = "";
		if(queryType.equals(SPARQLConstants.SELECT))
			retVarString = createReturnVariableString();
		else if(queryType.equals(SPARQLConstants.CONSTRUCT))
			retVarString = createConstructReturnTriples();
		
		String wherePatternString = createPatternClauses();
		String postWhereString = createPostPatternString();
		query=query+retVarString + wherePatternString +postWhereString;
	}
	
	public void createSQLQuery(String selectorsString, String tableString, String joinString, String groupBy,
			ArrayList<String> joinsArr, ArrayList<String> leftJoinsArr, ArrayList<String> rightJoinsArr){
		int nolimit = -1;
		String joins = "";
		if(selectorsString.length() == 0){
			returnVariables = orderVars(returnVariables, returnVarOrder);
			for(String returnVar: returnVariables){
				if(selectorsString.length()>0){
					returnVar = ", "+returnVar;
				}
				selectorsString+= returnVar;
			}
		}
		
		if(joinString.length() > 0 && SQLFilter.length() > 0)
			joins += joinString + " AND " + SQLFilter;
		else if(SQLFilter.length() > 0)
			joins = SQLFilter;
		else if(joinString.length() > 0)
			joins = joinString;
		
		if(!useOuterJoins){
			query = queryUtil.getDialectInnerJoinQuery(distinct,selectorsString, tableString, joins, SQLFilter, "", nolimit, groupBy);//query = "SELECT DISTINCT " + selectors + "  FROM  " + froms + joins + " LIMIT " + limit ;
		} else {
			query = queryUtil.getDialectFullOuterJoinQuery(distinct, selectorsString,rightJoinsArr, leftJoinsArr, joinsArr, SQLFilter, nolimit, groupBy);
		}
	}
	
	public String getCountQuery(int maxCount)
	{
		String countQuery = "SELECT (COUNT(*) AS ?entity) WHERE { SELECT * ";
//		String retVarString = createCountReturnVariableString();
		String wherePatternString = createPatternClauses();
		String postWhereString = createPostPatternString();
		countQuery=countQuery + wherePatternString +postWhereString + "}"; //need extra bracket for nested query
		return countQuery;
	}
	
	public String getQueryPattern(){
		String wherePatternString = createPatternClauses();
		String postWhereString = createPostPatternString();
		return wherePatternString + postWhereString;
	}
	
	public void addSingleReturnVariable(TriplePart var) 
	{
		if(var.getType().equals(TriplePart.VARIABLE))
		{
			retVars.add(var);
		}
		else
		{
			throw new IllegalArgumentException("Cannot add non-variable parts to the return var string");
		}
	}
	
	public void addSingleReturnVariable(TriplePart var, ISPARQLReturnModifier mod) 
	{
		if(var.getType().equals(TriplePart.VARIABLE))
		{
			retVars.add(var);
			retModifyPhrase.put(var,  mod);
		}
		else
		{
			throw new IllegalArgumentException("Cannot add non-variable parts to the return var string");
		}
	}
	public void addSingleReturnVariable(String var){
		returnVariables.add(var);
	}
	
	public String createPatternClauses()
	{
		String wherePatternString;
		if(customQueryStructure.equals(""))
		{
			Enumeration clauseKeys = clauseHash.keys();
			wherePatternString= "";
			while (clauseKeys.hasMoreElements())
			{
				String key = (String) clauseKeys.nextElement();
				SPARQLPatternClause clause = (SPARQLPatternClause) clauseHash.get(key);
				wherePatternString = wherePatternString + clause.getClauseString();
			}
			wherePatternString = "WHERE { " + wherePatternString + " } ";
		}
		else
		{
			Enumeration clauseKeys = clauseHash.keys();
			wherePatternString= "WHERE { " + customQueryStructure + " } ";
			while (clauseKeys.hasMoreElements())
			{
				String key = (String) clauseKeys.nextElement();
				SPARQLPatternClause clause = (SPARQLPatternClause) clauseHash.get(key);
				wherePatternString = wherePatternString.replaceAll(key, clause.getClauseString());
			}
		}
		return wherePatternString;
	}
	
	public String createReturnVariableString(){
		String retVarString = "";
		retVars = orderVars(retVars, returnVarOrder);
		for (int varIdx=0; varIdx<retVars.size();varIdx++)
		{
			//space out the variables
			if(retModifyPhrase.containsKey(retVars.get(varIdx)))
			{
				ISPARQLReturnModifier mod= retModifyPhrase.get(retVars.get(varIdx));
				retVarString = retVarString + createRetStringWithMod(retVars.get(varIdx), mod) + " ";
			}
			else
			{
			retVarString = retVarString + SPARQLQueryHelper.createComponentString(retVars.get(varIdx))+" ";
			}
		}
		return retVarString;
	}
	
	public String createConstructReturnTriples() {
		String constructTriples = "{";
		
		final int subjId = 0;
		final int predId = 1;
		final int objId = 2;
		
		for(int i = 0; i < returnTripleArray.size(); i++){
			List<String> thisTripleArray = returnTripleArray.get(i);
			String subjectURI = thisTripleArray.get(subjId);
			String subjectName = Utility.getInstanceName(subjectURI);
			
			String objectURI = thisTripleArray.get(objId);
			String objectName = Utility.getInstanceName(objectURI);
			
			String predURI = thisTripleArray.get(predId);
			String predName = subjectName + "_" +Utility.getInstanceName(predURI) + "_" + objectName;
			
			constructTriples = constructTriples + "?" + subjectName + " ?" + predName + " ?" + objectName + ". ";
		}
		
		constructTriples = constructTriples + "} ";
		
		return constructTriples;
	}
	
	public void removeReturnVariables(){
		retVars.clear();
	}
	
	public String createCountReturnVariableString()
	{
		String retVarString = "(COUNT(CONCAT(" ;
		for (int varIdx=0; varIdx<retVars.size();varIdx++)
		{
			if(varIdx != 0){
				retVarString = retVarString + ", ";
			}
			String varVal = retVars.get(varIdx).getValue() + "";
			retVarString = retVarString + "STR(?" + varVal + ")";
			
		}
		retVarString = retVarString + ")) AS ?entity)";
//		String retVarString = "(COUNT(*) AS ?entity)";
		return retVarString;
	}
	
	public void addAllVarToOrderBy()
	{
		orderBy = new SPARQLOrderBy(retVars);
	}
	
	public String createPostPatternString()
	{
		String postWhereString = "";
		if(orderBy != null)
		{
			postWhereString = postWhereString + " " + orderBy.getString();
		}
		if(limit != null)
		{
			postWhereString = postWhereString + " " + "LIMIT " + limit;
		}
		if(offset != null)
		{
			postWhereString = postWhereString + " " + "OFFSET " + offset;
		}
		if(groupBy != null)
		{
			postWhereString = postWhereString + " " + groupBy.getString();
		}
		if(bindings != null)
		{
			postWhereString = postWhereString + " " + bindings.getBindingString();
		}
		return postWhereString;
	}
	
	public void addTriple(TriplePart subject, TriplePart predicate, TriplePart object)
	{
		SPARQLPatternClause clause;
		if(clauseHash.containsKey(main))
		{
			clause = clauseHash.get(main);
		}
		else
		{
			clause = new SPARQLPatternClause();
		}
		SPARQLTriple newTriple = new SPARQLTriple(subject, predicate, object);
		if(!this.hasTriple(newTriple))
			clause.addTriple(newTriple);
		
		clauseHash.put(main,  clause);
	}
	
	public void addTriple(TriplePart subject, TriplePart predicate, TriplePart object, String clauseName)
	{
		SPARQLPatternClause clause;
		if(clauseHash.containsKey(clauseName))
		{
			clause = clauseHash.get(clauseName);
		}
		else
		{
			clause = new SPARQLPatternClause();
		}
		SPARQLTriple newTriple = new SPARQLTriple(subject, predicate, object);
		if(!this.hasTriple(newTriple))
			clause.addTriple(newTriple);
		
		clauseHash.put(clauseName,  clause);
	}
	
	public boolean hasTriple(SPARQLTriple triple)
	{
		boolean retBoolean = false;
		
		for(String clauseKey : clauseHash.keySet())
		{
			SPARQLPatternClause clause = clauseHash.get(clauseKey);
			if(clause.hasTriple(triple))
			{
				retBoolean = true;
				break;
			}
		}
		return retBoolean;
	}
	
	public void addBind(TriplePart bindSubject, TriplePart bindObject)
	{
		SPARQLPatternClause clause;
		if(clauseHash.containsKey(main))
		{
			clause = clauseHash.get(main);
		}
		else
		{
			clause = new SPARQLPatternClause();
		}
		SPARQLBind newBind = new SPARQLBind(bindSubject, bindObject);
		clause.addBind(newBind);
		clauseHash.put(main, clause);
	}
	
	public void addBind(TriplePart bindSubject, TriplePart bindObject, String clauseName)
	{
		SPARQLPatternClause clause;
		if(clauseHash.containsKey(clauseName))
		{
			clause = clauseHash.get(clauseName);
		}
		else
		{
			clause = new SPARQLPatternClause();
		}
		SPARQLBind newBind = new SPARQLBind(bindSubject, bindObject);
		clause.addBind(newBind);
		clauseHash.put(clauseName,  clause);
	}
	
	public void addParameter(String param, String paramType, TriplePart paramPart) {
		SPARQLPatternClause clause;
		if(clauseHash.contains(main)){
			clause = clauseHash.get(main);
		}
		else {
			clause = new SPARQLPatternClause();
		}
		
		SEMOSSParameter newParameter = new SEMOSSParameter(param, paramType, paramPart);
		clause.addParameter(newParameter);
		clauseHash.put(main, clause);
		
	}
	
	public void addParameter(String param, String paramType, TriplePart paramPart, String clauseName) {
		SPARQLPatternClause clause;
		if(clauseHash.containsKey(clauseName))
		{
			clause = clauseHash.get(clauseName);
		}
		else
		{
			clause = new SPARQLPatternClause();
		}
		SEMOSSParameter newParam = new SEMOSSParameter(param, paramType, paramPart);
		clause.addParameter(newParam);
		clauseHash.put(clauseName,  clause);
	}
	
	public void addRegexFilter(TriplePart var, List<TriplePart> filterData, boolean isValueString, boolean or, boolean isCaseSensitive)
	{
		List<ISPARQLFilterInput> addToFilter = new ArrayList<ISPARQLFilterInput>();
		for(TriplePart bindVar : filterData)
		{
			SPARQLRegex regex = new SPARQLRegex(var, bindVar, isValueString, isCaseSensitive);
			addToFilter.add(regex);
		}
		addFilter(addToFilter, or);
	}
	
	public void addRegexFilter(TriplePart var, List<TriplePart> filterData, boolean isValueString, boolean or,  String clauseName, boolean isCaseSensitive)
	{
		List<ISPARQLFilterInput> addToFilter = new ArrayList<ISPARQLFilterInput>();
		for(TriplePart bindVar : filterData)
		{
			SPARQLRegex regex = new SPARQLRegex(var, bindVar, isValueString, isCaseSensitive);
			addToFilter.add(regex);
		}
		addFilter(addToFilter, or, clauseName);
	}
	
	public void addURIFilter(TriplePart var, List<TriplePart> filterData, boolean or) {
		List<ISPARQLFilterInput> addToFilter = new ArrayList<ISPARQLFilterInput>();
		for(TriplePart bindVar : filterData)
		{
			SPARQLFilterURIParam uriFilter = new SPARQLFilterURIParam(var, bindVar);
			addToFilter.add(uriFilter);
		}
		addFilter(addToFilter, or);
	}
	
	public void addFilter(List<ISPARQLFilterInput> filterData, boolean or)
	{
		SPARQLPatternClause clause;
		if(clauseHash.containsKey(main))
		{
			clause = clauseHash.get(main);
		}
		else
		{
			clause = new SPARQLPatternClause();
		}
		SPARQLFilter newFilter = new SPARQLFilter(filterData, or);
		clause.addFilter(newFilter);
		clauseHash.put(main, clause);
	}
	
	public void addFilter(List<ISPARQLFilterInput> filterData, boolean or, String clauseName)
	{
		SPARQLPatternClause clause;
		if(clauseHash.containsKey(clauseName))
		{
			clause = clauseHash.get(clauseName);
		}
		else
		{
			clause = new SPARQLPatternClause();
		}
		SPARQLFilter newFilter = new SPARQLFilter(filterData, or);
		clause.addFilter(newFilter);
		clauseHash.put(clauseName,  clause);
	}
	
	public String createRetStringWithMod(TriplePart var, ISPARQLReturnModifier mod)
	{
		String retString = "("+mod.getModifierAsString()+ " AS " + SPARQLQueryHelper.createComponentString(var)+ ")";
		return retString;
	}
	
	public String getQuery()
	{
		return query;
	}
	
	public String getQueryType()
	{
		return queryType;
	}
	
	public void setQueryType(String queryType)
	{
		if( !(queryType.equals(SPARQLConstants.SELECT)) && !(queryType.equals(SPARQLConstants.CONSTRUCT)))
		{
			throw new IllegalArgumentException("SELECT or CONSTRUCT Queries only");
		}
		this.queryType = queryType;
	}
	
	public List<SPARQLTriple> getTriples()
	{
		return triples;
	}
	
	public List<TriplePart> getRetVars()
	{
		return retVars;
	}
	
	public void setReturnTripleArray(List<List<String>> tripleArray) {
		this.returnTripleArray = tripleArray;
	}
	
	public List<List<String>> getTripleArray() {
		return returnTripleArray;
	}
	
	public void setCustomQueryStructure(String structure)
	{
		this.customQueryStructure= structure;
	}
	
	public void setDisctinct(boolean distinct)
	{
		this.distinct=distinct;
	}
	
	public void setGroupBy(SPARQLGroupBy groupBy)
	{
		this.groupBy = groupBy;
	}
	
	public SPARQLGroupBy getGroupBy()
	{
		return this.groupBy;
	}
	
	public void setBindings(SPARQLBindings bindings)
	{
		this.bindings = bindings;
	}

	public void setLimit(Integer limit)
	{
		this.limit = limit;
	}

	public Integer getLimit()
	{
		return this.limit;
	}

	public void setOffset(Integer offset)
	{
		this.offset = offset;
	}
	public void setSQLFilter(String filter){
		this.SQLFilter = filter;
	}
	public SQLQueryUtil getQueryUtil() {
		return queryUtil;
	}
	public void setQueryUtil(SQLQueryUtil queryUtil) {
		this.queryUtil = queryUtil;
	}
	public boolean isUseOuterJoins() {
		return useOuterJoins;
	}
	public void setUseOuterJoins(boolean useOuterJoins) {
		this.useOuterJoins = useOuterJoins;
	}
	public static List orderVars(List vars, List<String> returnVarOrder){
		if(returnVarOrder == null){
			return vars;
		}
		Object[] orderedVars = new Object[vars.size()];
		List<Integer> used = new ArrayList<Integer>();
		for(int or = 0; or < returnVarOrder.size(); or++){
			String orderedV  = returnVarOrder.get(or);
			if(orderedV != null){
				for(int x = 0; x < vars.size(); x ++ ){
					Object var = vars.get(x);
					String varName = var + "";
					if(var instanceof TriplePart){
						varName = ((TriplePart) var).getValue() + "";
					}
					System.out.println("does " + varName + " end with  " + orderedV);
					if(varName.toUpperCase().endsWith(orderedV.toUpperCase())){
						System.out.println("yes");
						System.out.println("adding in position " + or);
						orderedVars[or] = var;
						used.add(x);
						break;
					}
				}
			}
		}
		int masterIdx = 0;
		for(int i = 0; i < vars.size(); i++){
			if(!used.contains(i)){
				while(orderedVars[masterIdx] != null){
					masterIdx++;
				}
				System.out.println("adding in position " + i + " return var  " + vars.get(i));
				orderedVars[masterIdx] = vars.get(i);
				masterIdx++;
			}
		}
		
		
		System.err.println("ORDERED SPARQL RETURN VARS::: " +orderedVars);
		return Arrays.asList(orderedVars);
	}

	public void setReturnVarOrder(List<String> strings) {
		this.returnVarOrder = strings;
		
	}
	
}
