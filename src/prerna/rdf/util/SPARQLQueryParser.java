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

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import prerna.util.Utility;

public class SPARQLQueryParser extends AbstractQueryParser {
	
	private final double GOLDEN_RATIO = 0.618;
	private Hashtable<String, Double> finalHash;
	private Hashtable<String, Integer> countHash;
	private Hashtable<String, String> variableMapping;
	private List<StatementPattern> patterns;

	public SPARQLQueryParser(){
		super();
	}
	
	public SPARQLQueryParser(String query){
		super(query);
	}
	
//	public static void main(String[] args) throws Exception {
//		basicParseTest();
//	}
	
	@Override
	public void parseQuery(){
		
		variableMapping = new Hashtable<String, String>();
		
		final String regex = "\\(\\?([^(\\s|\\)|,)]*)|\\?([^(\\s|\\)|,)]*)\\)";
		final Pattern pattern = Pattern.compile(regex);
		Matcher matcher = null;
		if(query.toLowerCase().contains("select")){
			matcher = pattern.matcher(query.substring(query.indexOf("SELECT"), query.indexOf("WHERE")));
		}
		else {
			matcher = pattern.matcher(query.substring(query.indexOf("CONSTRUCT"), query.indexOf("WHERE")));
		}
		while(matcher.find()) {
			String orig = matcher.group();
			orig = processString(orig);
			String ret = "";
			if(matcher.find()) {
				ret = matcher.group(2);
				ret = processString(ret);
			}
			variableMapping.put(orig, ret);
		}
		
		countHash = new Hashtable<String, Integer>();
		try {
			SPARQLParser parser = new SPARQLParser();
			ParsedQuery parsedQuery = parser.parseQuery(query, null);
			

			StatementCollector collector = new StatementCollector();
			parsedQuery.getTupleExpr().visit(collector);

			FunctionCallCollector aggregateFunctionsCollector = new FunctionCallCollector();
			parsedQuery.getTupleExpr().visit(aggregateFunctionsCollector);
			if(aggregateFunctionsCollector.getValue() !=null){
				hasColumnAggregatorFunction = true;
			}
			
			returnVariables = parsedQuery.getTupleExpr().getBindingNames(); 
			
			patterns = collector.getPatterns();
			
			getURIList(); // populates finalHash, types, and props
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// calculate weights for all return variables
		finalHash = new Hashtable<String, Double>();
		int max = 0;
		for(String key : countHash.keySet()) {
			int c = countHash.get(key);
			if(c > max) {
				max = c;
			}
		}
		
		
		for(String key : countHash.keySet()) {
			double weight = GOLDEN_RATIO * countHash.get(key) / max;
			if(types.containsValue(key)) {
				List<String> possibleVariableNames = getKeyFromVal(key, types);
				for(String variableName : possibleVariableNames) {
					if(returnVariables.contains(variableName) || variableMapping.containsKey(variableName)) {
						weight += 1;
						break;
					}
				}
			} else if(props.containsValue(key)) {
				List<String> possibleVariableNames = getKeyFromVal(key, props);
				for(String variableName : possibleVariableNames) {
					if(returnVariables.contains(variableName) || variableMapping.containsKey(variableName)) {
						weight += 1;
						break;
					}
				}
			}
			finalHash.put(key, weight);
		}
		
		//System.out.println(finalHash);
	}
	
	private List<String> getKeyFromVal(String val, Hashtable<String, String> map) {
		List<String> retList = new ArrayList<String>();
		for(String key : map.keySet()) {
			if(map.get(key).equals(val)) {
				retList.add(key);
			}
		}
		return retList;
	}

	private void getURIList() {

		Hashtable <String, Integer> dataHash = new Hashtable<String, Integer>();
		types = new Hashtable<String, String>();
		props = new Hashtable<String, String>();

		//run through the types first, you need to do this first so that you can get all of the types their variables,
		//then when you get the properties you can use the mapping you create between variables and types to properly map the variables to their respective type
		for(int patIndex = 0;patIndex < patterns.size();patIndex++)
		{
			StatementPattern thisPattern = patterns.get(patIndex);

			Var subjectVar = thisPattern.getSubjectVar(); //cant use this, its the alias.
			Var objectVar = thisPattern.getObjectVar();
			Var predicateVar = thisPattern.getPredicateVar();//cant use this, its the alias.
			
			dataHash = recordVar(subjectVar, dataHash);
			dataHash = recordVar(objectVar, dataHash);
			
			if(predicateVar.isConstant() && (predicateVar.getValue()+"").equalsIgnoreCase("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
				types.put(Utility.getInstanceName(objectVar.getValue().toString()) + "", objectVar.getValue() + "");//types.put(subjectVar.getName() + "", objectVar.getValue() + "");
				aliasTableMap.put(subjectVar.getName(), Utility.getInstanceName(objectVar.getValue().toString()));
			}
		}
		
		//now that you have their types and their aliases, run through the properties and generate the triples list	
		for(int patIndex = 0;patIndex < patterns.size();patIndex++)
		{
			StatementPattern thisPattern = patterns.get(patIndex);

			Var subjectVar = thisPattern.getSubjectVar(); //cant use this, its the alias.
			Var objectVar = thisPattern.getObjectVar();
			Var predicateVar = thisPattern.getPredicateVar();//cant use this, its the alias.
			
			if(predicateVar.isConstant() && (predicateVar.getValue()+"").contains("ontologies/Relation/Contains/")) {
				String propPlainText = Utility.getInstanceName(predicateVar.getValue().toString());
				props.put(propPlainText  + "", predicateVar.getValue() + "");//props.put(objectVar.getName() + "", predicateVar.getValue() + "");
				String nodeType = aliasTableMap.get(subjectVar.getName());
				addToVariablesMap(typePropVariables, nodeType, objectVar.getName(), predicateVar.getValue().toString());
				if(returnVariables.contains(objectVar.getName())){
					addToVariablesMap(typeReturnVariables, nodeType, objectVar.getName(), predicateVar.getValue().toString());
				}
			} else if(predicateVar.isConstant() && (predicateVar.getValue()+"").contains("ontologies/Relation")) {
				//must a triple!
				String[] triple = new String[3];
				triple[0] = types.get(aliasTableMap.get(subjectVar.getName()));
				triple[1] = predicateVar.getValue().toString();
				triple[2] = types.get(aliasTableMap.get(objectVar.getName()));
				triplesData.add(triple);
			}
		}

		// synchronize it
		Enumeration<String> keys = dataHash.keys();
		while(keys.hasMoreElements())
		{
			String key = "" + keys.nextElement();
			if(key.contains(":")) // namespaced let it go
			{
				Integer typeProxyCount = dataHash.get(key);
				if(countHash.containsKey(key)) {
					typeProxyCount = typeProxyCount + countHash.get(key);
				}
				countHash.put(key, typeProxyCount);
			}else
			{
				String typeName = types.get(key);
				Integer typeProxyCount = dataHash.get(key);
				if(typeName != null) {
					if(countHash.containsKey(typeName)) {
						typeProxyCount = typeProxyCount + countHash.get(typeName);
					}
					countHash.put(typeName, typeProxyCount);
				} else {
					String propName = props.get(key);
					Integer propProxyCount = dataHash.get(key);
					if(propName != null) {
						if(countHash.containsKey(propName)) {
							propProxyCount = propProxyCount + countHash.get(typeName);
						}
						countHash.put(propName, propProxyCount);
					}
				}
			}
		}
	}

	private Hashtable <String, Integer> recordVar(Var var, Hashtable <String, Integer> inputHash) {
		if(var.hasValue()) {
			Integer count = inputHash.get(var.getValue()+"");
			if(count == null) {
				count = 0;
			}
			count++;
			inputHash.put(var.getValue()+"", count);
		} else {
			Integer count = inputHash.get(var.getName()+"");
			if(count == null) {
				count = 0;
			}
			count++;
			inputHash.put(var.getName()+"", count);
		}
		return inputHash;
	}
	
	private String processString(String s) {
		return s.replaceAll("\\?", "").replaceAll("\\)", "").replaceAll("\\(", "");
	}

	public List<StatementPattern> getPatterns() {
		return patterns;
	}

	private void setPatterns(List<StatementPattern> patterns) {
		this.patterns = patterns;
	}

	private void setReturnVariables(Set<String> returnVariables) {
		this.returnVariables = returnVariables;
	}

	@Override
	public List<String[]> getTriplesData() {
		return triplesData;
	}
	

	
	////tester methods
	private static void basicParseTest(){
		
		String query = "SELECT DISTINCT ?Director (AVG(?Title__MovieBudget) AS ?x) (SUM(?Title__MovieBudget) AS ?y) WHERE { BIND(<@Studio-http://semoss.org/ontologies/Concept/Studio@> AS ?Studio) {?Title &lt;http://www.w3.org/1999/02/22-rdf-syntax-ns#type&gt; &lt;http://semoss.org/ontologies/Concept/Title&gt;} {?Director &lt;http://www.w3.org/1999/02/22-rdf-syntax-ns#type&gt; &lt;http://semoss.org/ontologies/Concept/Director&gt;} {?Studio &lt;http://www.w3.org/1999/02/22-rdf-syntax-ns#type&gt; &lt;http://semoss.org/ontologies/Concept/Studio&gt;} {?Title &lt;http://semoss.org/ontologies/Relation/DirectedBy&gt; ?Director} {?Title &lt;http://semoss.org/ontologies/Relation/DirectedAt&gt; ?Studio} {?Title &lt;http://semoss.org/ontologies/Relation/Contains/MovieBudget&gt; ?Title__MovieBudget} {?Title &lt;http://semoss.org/ontologies/Relation/Contains/Revenue-International&gt; ?Title__Revenue_International} {?Title &lt;http://semoss.org/ontologies/Relation/Contains/Revenue-Domestic&gt; ?Title__Revenue_Domestic} {?Title &lt;http://semoss.org/ontologies/Relation/Contains/RottenTomatoes-Audience&gt; ?Title__RottenTomatoes_Audience} {?Title &lt;http://semoss.org/ontologies/Relation/Contains/RottenTomatoes-Critics&gt; ?Title__RottenTomatoes_Critics}  }  GROUP BY ?Director";	
		//query = "SELECT DISTINCT ?Title ?Nominated ?Genre ?Title__RevenueInternational ?Title__MovieBudget WHERE { {?Title &lt;http://www.w3.org/1999/02/22-rdf-syntax-ns#type&gt; &lt;http://semoss.org/ontologies/Concept/Title&gt;} {?Nominated &lt;http://www.w3.org/1999/02/22-rdf-syntax-ns#type&gt; &lt;http://semoss.org/ontologies/Concept/Nominated&gt;} {?Genre &lt;http://www.w3.org/1999/02/22-rdf-syntax-ns#type&gt; &lt;http://semoss.org/ontologies/Concept/Genre&gt;} {?Title &lt;http://semoss.org/ontologies/Relation/Was&gt; ?Nominated} {?Title &lt;http://semoss.org/ontologies/Relation/BelongsTo&gt; ?Genre} {?Title &lt;http://semoss.org/ontologies/Relation/Contains/Revenue-International&gt; ?Title__RevenueInternational} {?Title &lt;http://semoss.org/ontologies/Relation/Contains/MovieBudget&gt; ?Title__MovieBudget}  }";
		//query = "SELECT DISTINCT ?Title ?Title__RevenueDomestic WHERE { {?Title &lt;http://www.w3.org/1999/02/22-rdf-syntax-ns#type&gt; &lt;http://semoss.org/ontologies/Concept/Title&gt;} {?Studio &lt;http://www.w3.org/1999/02/22-rdf-syntax-ns#type&gt; &lt;http://semoss.org/ontologies/Concept/Studio&gt;} {?Title &lt;http://semoss.org/ontologies/Relation/Title_Studio&gt; ?Studio} {?Title &lt;http://semoss.org/ontologies/Relation/Contains/Revenue-Domestic&gt; ?Title__RevenueDomestic} {?Title &lt;http://semoss.org/ontologies/Relation/Contains/Revenue-International&gt; ?Title__RevenueInternational}  }";
		query = query.replace("&lt;", "<");
		query = query.replace("&gt;", ">");
				
		SPARQLQueryParser parse = new SPARQLQueryParser(query);
		parse.parseQuery(); // parse the query into grammar
		
		Hashtable <String, Hashtable<String,String>> returnVariables1 = parse.getReturnVariables();
		Hashtable <String, String> types1 = parse.getNodesFromQuery();
		Hashtable <String, Hashtable<String,String>> props1 = parse.getPropertiesFromQuery();
		List<String[]> mytrips = parse.getTriplesData();
		boolean hasAggregate = parse.hasAggregateFunction();
		System.out.println("Aggregate function " + hasAggregate);
	}
}