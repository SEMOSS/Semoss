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
package prerna.poi.main.insights;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import javax.swing.JComboBox;
import javax.swing.JList;

import prerna.algorithm.learning.similarity.GenerateEntropyDensity;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.QuestionAdministrator;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * AutoInsightGenerator creates insights that are automatically added to a DB on upload
 * @author ksmart
 *
 */
public class AutoInsightGenerator implements InsightRuleConstants{

	private AbstractEngine engine;
	private ArrayList<String> concepts;
	private String concept;
	
	private String[] names;
	private ArrayList<Object []> list;
	
	private String[] classArr;
	private String[] colTypesArr;
	private double[] entropyArr;
	
	private InsightRule currRule;
	private List<String> params;
	private Hashtable<String, Set<Integer>> possibleParamAssignmentsHash = new Hashtable<String, Set<Integer>>();
	
	private QuestionAdministrator qa;
	private String perspective;
	private Integer order;
	
	private final String CONCEPTS_QUERY = "SELECT DISTINCT ?Concept WHERE {{?Concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>}}";
	private final String PROPERTIES_QUERY = "SELECT DISTINCT ?Prop WHERE {{?Concept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/@CONCEPT@>}{?Prop <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>}{?Concept ?Prop ?PropVal }}";
	private final String EMPTY_QUERY = "SELECT DISTINCT @RETVARS@ WHERE {{?@CONCEPT@ <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/@CONCEPT@>} @TRIPLES@} @ENDSTRING@";
	private final String PROPERTY_TRIPLE= "{?@CONCEPT@ <http://semoss.org/ontologies/Relation/Contains/@PROP@> ?@PROPCLEAN@}";
	
	public AutoInsightGenerator(AbstractEngine engine) {
		this.engine = engine;
	}
	
	public void execute() {
		
		qa = new QuestionAdministrator(engine);
		
		InsightTemplateProcessor templateProc = new InsightTemplateProcessor();
		List<InsightRule> rulesList = templateProc.runGenerateInsights();

		Vector<String> perspectiveList = engine.getPerspectives();

		//TODO how to filter the list of concepts?
		concepts = getQueryResultsAsList(CONCEPTS_QUERY);
		concepts.remove("Concept");
		
		int i;
		int numConcepts = concepts.size();
		for(i=0; i<numConcepts; i++) {
			concept = concepts.get(i);
			perspective = concept+"-Perspective";
			order = 1;
			if(perspectiveList.contains(perspective))
				order = engine.getInsights(perspective).size() + 1;
			Boolean tableCreated = createTable(concept);
			if(tableCreated) {
				calculateEntropiesAndTypes();
				
				for(InsightRule rule : rulesList) {			
					currRule = rule;

					Hashtable<String, Hashtable<String,Object>> paramConstraintHash = currRule.getConstraints();
					Hashtable<String, String> paramClassHash = currRule.getVariableTypeHash();
	
					Boolean allParamsMet = calculatePossibleParamAssignments(paramConstraintHash,paramClassHash);
					
					if(allParamsMet) {
						params = new ArrayList<String>(paramClassHash.keySet());
						addAllPossibleInsights(new ArrayList<Integer>());
					}
				}
			}	
		}

		//save the xmlFile with the changes
		String xmlFile = "db/" + engine.getEngineName() + "/" + engine.getEngineName() + "_Questions.XML";
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		qa.createQuestionXMLFile(xmlFile, baseFolder);

		//reload the perspectives for the db so that new perspectives/questions are visible
		reloadDB();
	}
	
	private Boolean createTable(String concept) {
		//query to get the properties related to this concept
		//construct a query to pull the properties for each instance of concept
		//create the table
		String propQuery = PROPERTIES_QUERY.replaceAll("@CONCEPT@", concept);
		
		ArrayList<String> properties = getQueryResultsAsList(propQuery);
		if(properties.isEmpty())
			return false;
		
		int numVariables = properties.size() + 1;
		names = new String[numVariables];
		names[0] = concept;
		for(int i=1; i<numVariables; i++) {
			names[i] = properties.get(i-1);
		}
		
		classArr = new String[numVariables];
		classArr[0] = CONCEPT_VALUE;
		for(int i=1; i<numVariables; i++)
			classArr[i] = PROPERTY_VALUE;
		
		String retVarString = "?"+concept;
		String triplesString = "";
		for(String prop : properties) {
			String propClean = prop.replaceAll("-","_");
			retVarString += " ?" + propClean;
			triplesString += PROPERTY_TRIPLE.replaceAll("@CONCEPT@",concept).replaceAll("@PROP@", prop).replaceAll("@PROPCLEAN@", propClean);
		}

		String tableQuery = buildQuery(retVarString,triplesString,"");
		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, tableQuery);
		String[] wrapNames = wrapper.getVariables();
			
		list = new ArrayList<Object[]>();
		while(wrapper.hasNext())
		{
			ISelectStatement sjss = wrapper.next();
			
			Object [] values = new Object[wrapNames.length];
			for(int colIndex = 0;colIndex < wrapNames.length;colIndex++)
			{
				values[colIndex] = sjss.getVar(wrapNames[colIndex]);
			}
			list.add(values);
		}
		return true;
	}
	
	private ArrayList<String> getQueryResultsAsList(String query) {
		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		// get the bindings from it
		String[] names = wrapper.getVariables();
		ArrayList<String> properties = new ArrayList<String>();
		
		// now get the bindings and generate the data
		while(wrapper.hasNext())
		{
			ISelectStatement sjss = wrapper.next();
			properties.add(sjss.getVar(names[0]).toString());
		}
		
		return properties;
	}
	
	
	private void calculateEntropiesAndTypes() {//given table
		GenerateEntropyDensity edGen = new GenerateEntropyDensity(list, true);
		colTypesArr = edGen.getColumnTypes();
		entropyArr = edGen.generateEntropy();
	}
	
	//checks to see if a given column in the table meets a given set of requirement
	private Boolean meetsParamRequirements(int colIndex, Hashtable<String,Object> requirementHash, String paramClass) {

		if(!paramClass.equals(classArr[colIndex]))
			return false;
		
		//classes are the same and no constraints so return true
		if(requirementHash == null) {
			return true;
		}

		Object paramType = requirementHash.get(DATA_TYPE);
		if(paramType != null && !paramType.toString().equals(colTypesArr[colIndex]))
			return false;
		
		Object entropyMax = requirementHash.get(ENTROPY_DENSITY_MAX);
		if(entropyMax != null && entropyArr[colIndex] > Double.parseDouble(entropyMax.toString()))
			return false;

		Object entropyMin = requirementHash.get(ENTROPY_DENSITY_MIN);
		if(entropyMin!=null && entropyArr[colIndex] <  Double.parseDouble(entropyMin.toString()))
			return false;
		
		return true;

	}

	
	private Boolean calculatePossibleParamAssignments(Hashtable<String, Hashtable<String,Object>> ruleParamHash, Hashtable<String, String> paramClassHash) {
		//creates a mapping of the possible assignments for each parameter
		//returns false if there is a param that cannot be met
		possibleParamAssignmentsHash = new Hashtable<String, Set<Integer>>();
		for(String param : paramClassHash.keySet()) {
			
			String paramClass = paramClassHash.get(param);
			Hashtable<String,Object> paramConstraintHash = ruleParamHash.get(param);
			
			Set<Integer> possibleAssignments = new HashSet<Integer>();
			
			for(int j=0;j<names.length;j++) {
				if(meetsParamRequirements(j,paramConstraintHash,paramClass)) {
					possibleAssignments.add(j);
				}
			}
			
			if(possibleAssignments.size() == 0)
				return false;
			
			possibleParamAssignmentsHash.put(param,possibleAssignments);
		}
		return true;
	}
	
	private void addAllPossibleInsights(List<Integer> assignments) {
		
		if(assignments.size() == params.size()) {
			createInsight(assignments);
		}else {
			
			String currParam = params.get(assignments.size());
			Set<Integer> possibleAssignments = possibleParamAssignmentsHash.get(currParam);
			
			Iterator<Integer> itr = possibleAssignments.iterator();
			
			while(itr.hasNext()) {
				Integer possibleAssignment = itr.next();
				
				if(!assignments.contains(possibleAssignment)) {
					assignments.add(possibleAssignment);
					addAllPossibleInsights(assignments);
					assignments.remove(possibleAssignment);
				}
			}
		}
		
	}
	
	private void createInsight(List<Integer> assignments) {
		
		//process through all the params
		//ignore the concept
		//if property
		//then add to retvar and add to triples string

		Hashtable<String, Hashtable<String,Object>> ruleParamHash = currRule.getConstraints();
		Hashtable<String, String> paramClassHash = currRule.getVariableTypeHash();
		
		int i;
		int paramSize = params.size();
		String retVarString = "";
		String triplesString = "";
		String endString;
		
		Boolean hasAggregation = currRule.isHasAggregation();
		
		if(hasAggregation) {
			endString = "GROUP BY ";
		}else {
			endString = "";
		}
		
		for(i=0; i<paramSize; i++) {
			String param = params.get(i);
			String classType = paramClassHash.get(param);
			String aggregationType = "";
			if(ruleParamHash.containsKey(param) && ruleParamHash.get(param).containsKey(AGGREGATION))
				aggregationType = ruleParamHash.get(param).get(AGGREGATION).toString();
			
			if(classType.equals(PROPERTY_VALUE)) {
				String prop = names[assignments.get(i)];
				String propClean = prop.replaceAll("-","_");

				//if no aggregation for insight -> basic return string
				//if aggregation for insight but not this param -> basic return string and end string
				//if aggregation for this insight and this param -> complicated return string
				if(!hasAggregation)
					retVarString += " ?"+propClean;
				else if(aggregationType.length() == 0) {
					retVarString = " ?"+propClean + retVarString;
					endString += " ?"+propClean;
				}else {
					retVarString += " (" + aggregationType.toString() + "(?" + propClean + ") AS ?" + propClean + "_" + aggregationType.toString() + ")";
				}
				
				triplesString += PROPERTY_TRIPLE.replaceAll("@CONCEPT@",concept).replaceAll("@PROP@", prop).replaceAll("@PROPCLEAN@", propClean);
				
			}if(classType.equals(CONCEPT_VALUE)) {
				
				if(!hasAggregation)
					retVarString = " ?"+concept + retVarString;
				else if(aggregationType.length() == 0) {
					retVarString = " ?"+concept + retVarString;
					endString += " ?"+concept;
				}else {
					retVarString = " (" + aggregationType.toString() + "(?" + concept + ") AS ?" + concept + "_" + aggregationType.toString() + ")" + retVarString;
				}
			}
		}
		
		String sparql = buildQuery(retVarString, triplesString, endString);
				
		String question = currRule.getQuestion();
		for(i=0; i<paramSize; i++) {
			question = question.replaceAll("\\$"+ params.get(i), names[assignments.get(i)]);
		}
		
		String layout = currRule.getOutput();
		
		String questionKey = qa.createQuestionKey(perspective);
		qa.cleanAddQuestion(perspective, questionKey, order.toString(), question, sparql, layout, null, null, null, null);
		order++;
	}
	
	private void reloadDB() {
		// selects the db in repolist so the questions refresh with the changes
		JList list = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		List selectedList = list.getSelectedValuesList();
		String selectedValue = selectedList.get(selectedList.size() - 1).toString();

		// don't need to refresh if selected db is not the db you're modifying.
		// when you click to it it will refresh anyway.
		if (engine.getEngineName().equals(selectedValue)) {
			Vector<String> perspectives = engine.getPerspectives();
			Collections.sort(perspectives);

			JComboBox<String> box = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.PERSPECTIVE_SELECTOR);
			box.removeAllItems();

			for (int itemIndex = 0; itemIndex < perspectives.size(); itemIndex++) {
				box.addItem(perspectives.get(itemIndex).toString());
			}
		}
	}
	
	private String buildQuery(String retVarString, String triplesString, String endString) {
		String ret = EMPTY_QUERY.replaceAll("@CONCEPT@", concept).replaceAll("@RETVARS@", retVarString).replaceAll("@TRIPLES@",triplesString).replaceAll("@ENDSTRING@",endString);
		return ret;
	}
}
