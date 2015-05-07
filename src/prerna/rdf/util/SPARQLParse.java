/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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

import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.repository.sail.SailTupleQuery;

public class SPARQLParse {

	private List<StatementPattern> patterns;
	private Set<String> returnVariables;
	private Hashtable<String, Integer> countHash;
	private Hashtable<String, String> variableMapping;
	private Hashtable <String, String> types;
	private Hashtable <String, String> props;
	
	private final double GOLDEN_RATIO = 0.618;
	private Hashtable<String, Double> finalHash;
	
	public static void main(String[] args) throws Exception {
		String query = "SELECT DISTINCT ?Director (AVG(?Title__MovieBudget) AS ?x) WHERE { BIND(<@Studio-http://semoss.org/ontologies/Concept/Studio@> AS ?Studio) {?Title &lt;http://www.w3.org/1999/02/22-rdf-syntax-ns#type&gt; &lt;http://semoss.org/ontologies/Concept/Title&gt;} {?Director &lt;http://www.w3.org/1999/02/22-rdf-syntax-ns#type&gt; &lt;http://semoss.org/ontologies/Concept/Director&gt;} {?Studio &lt;http://www.w3.org/1999/02/22-rdf-syntax-ns#type&gt; &lt;http://semoss.org/ontologies/Concept/Studio&gt;} {?Title &lt;http://semoss.org/ontologies/Relation/DirectedBy&gt; ?Director} {?Title &lt;http://semoss.org/ontologies/Relation/DirectedAt&gt; ?Studio} {?Title &lt;http://semoss.org/ontologies/Relation/Contains/MovieBudget&gt; ?Title__MovieBudget} {?Title &lt;http://semoss.org/ontologies/Relation/Contains/Revenue-International&gt; ?Title__Revenue_International} {?Title &lt;http://semoss.org/ontologies/Relation/Contains/Revenue-Domestic&gt; ?Title__Revenue_Domestic} {?Title &lt;http://semoss.org/ontologies/Relation/Contains/RottenTomatoes-Audience&gt; ?Title__RottenTomatoes_Audience} {?Title &lt;http://semoss.org/ontologies/Relation/Contains/RottenTomatoes-Critics&gt; ?Title__RottenTomatoes_Critics}  }  GROUP BY ?Director";	
		query = query.replace("&lt;", "<");
		query = query.replace("&gt;", ">");
		
		SPARQLParse parse = new SPARQLParse();
		parse.parseIt(query); // parse the query into grammar
	}

	private void parseIt(String query) {
		variableMapping = new Hashtable<String, String>();
		
		final String regex = "\\(\\?([^(\\s|\\)|,)]*)|\\?([^(\\s|\\)|,)]*)\\)";
		final Pattern pattern = Pattern.compile(regex);
		final Matcher matcher = pattern.matcher(query.substring(query.indexOf("SELECT"), query.indexOf("WHERE")));
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
		
		System.out.println(finalHash);
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

		for(int patIndex = 0;patIndex < patterns.size();patIndex++)
		{
			StatementPattern thisPattern = patterns.get(patIndex);

			Var subjectVar = thisPattern.getSubjectVar();
			Var objectVar = thisPattern.getObjectVar();
			Var predicateVar = thisPattern.getPredicateVar();
			
			dataHash = recordVar(subjectVar, dataHash);
			dataHash = recordVar(objectVar, dataHash);
			
			if(predicateVar.isConstant() && (predicateVar.getValue()+"").equalsIgnoreCase("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
				types.put(subjectVar.getName() + "", objectVar.getValue() + "");
			} else if(predicateVar.isConstant() && (predicateVar.getValue()+"").contains("ontologies/Relation/Contains/")) {
				props.put(objectVar.getName() + "", predicateVar.getValue() + "");
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

	public void setPatterns(List<StatementPattern> patterns) {
		this.patterns = patterns;
	}

	public Set<String> getReturnVariables() {
		return returnVariables;
	}

	public void setReturnVariables(Set<String> returnVariables) {
		this.returnVariables = returnVariables;
	}

}



class MyTupleQuery extends SailTupleQuery {
	public MyTupleQuery(ParsedTupleQuery query, SailRepositoryConnection sc) {
		super(query, sc);

	}
}

class MyExtension extends Extension
{
	@Override
	public Set<String> getBindingNames()
	{
		System.out.println("Going to crash");
		return super.getBindingNames();
	}
}
