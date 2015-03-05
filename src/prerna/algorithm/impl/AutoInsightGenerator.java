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
package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import javax.swing.JComboBox;
import javax.swing.JList;

import prerna.algorithm.learning.similarity.GenerateEntropyDensity;
import prerna.poi.main.insights.InsightRule;
import prerna.poi.main.insights.InsightRuleConstants;
import prerna.poi.main.insights.InsightTemplateProcessor;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.QuestionAdministrator;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.PlaySheetEnum;

/**
 * AutoInsightGenerator creates insights that are automatically added to a DB on upload
 * @author ksmart
 *
 */
public class AutoInsightGenerator implements InsightRuleConstants{

	private AbstractEngine engine;
	private String[] concepts;
	
	private String[] names;
	private ArrayList<Object []> list;
	
	private String[] classArr;
	private String[] colTypesArr;
	private double[] entropyArr;
	
	private QuestionAdministrator qa;
	private String perspective;
	private Integer order;
	
	private final String PROPERTIES_QUERY = "SELECT DISTINCT ?Prop WHERE {{?Concept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/@CONCEPT@>}{?Prop <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>}{?Concept ?Prop ?PropVal }}";
	private final String PROPERTY_TABLE_QUERY = "SELECT DISTINCT ?@CONCEPT@ @RETVARS@ WHERE {{?@CONCEPT@ <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/@CONCEPT@>} @TRIPLES@}";
	private final String PROPERTY_TRIPLE= "{?@CONCEPT@ <http://semoss.org/ontologies/Relation/Contains/@PROP@> ?@PROPCLEAN@}";
	
	public AutoInsightGenerator(AbstractEngine engine) {
		this.engine = engine;
	}
	
	public void execute() {
		
		qa = new QuestionAdministrator(engine);
		
		InsightTemplateProcessor templateProc = new InsightTemplateProcessor();
		List<InsightRule> rulesList = templateProc.runGenerateInsights();

		Vector<String> perspectiveList = engine.getPerspectives();
//		TODO for all concepts instead of just title
//		int i;
//		int numConcepts = concepts.length;
//		for(i=0; i<numConcepts; i++) {
//			String concept = concepts[i];

			String concept = "Title";
			perspective = concept+"-Perspective";
			order = 1;
			if(perspectiveList.contains(perspective))
				order = engine.getInsights(perspective).size() + 1;
			createTable(concept);
			calculateEntropiesAndTypes();
			
			for(InsightRule rule : rulesList) {
				//Boolean hasAggregation = rule.isHasAggregation();//TODO account for aggregation
				
				Hashtable<String, Hashtable<String,Object>> ruleParamHash = rule.getConstraints();
				
				for(String param : ruleParamHash.keySet()) {
					Hashtable<String,Object> paramConstraintHash = ruleParamHash.get(param);

					for(int j=1;j<names.length;j++) {
						if(meetsParamRequirements(j,paramConstraintHash)) {
							
							String prop = names[j];
							String propClean = prop.replaceAll("-","_");
							
							String retVarString = " ?" + propClean + " ";
							String triplesString = PROPERTY_TRIPLE.replaceAll("@CONCEPT@",concept).replaceAll("@PROP@", prop).replaceAll("@PROPCLEAN@", propClean);
	
							String sparql = PROPERTY_TABLE_QUERY.replaceAll("@CONCEPT@", concept).replaceAll("@RETVARS@", retVarString).replaceAll("@TRIPLES@",triplesString);
							
							String question = rule.getQuestion();
							question = question.replaceAll("\\$"+"Concept", concept);
							question = question.replaceAll("\\$" + "Property", prop);
							
							String layout = rule.getOutput();
							
							addInsights(order, question, sparql, layout);
	
						}
					}
				}
			}
			
		//}
		String xmlFile = "db/" + engine.getEngineName() + "/" + engine.getEngineName() + "_Questions.XML";
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

		qa.createQuestionXMLFile(xmlFile, baseFolder);
		// Refresh the questions by selecting the db again and
		// populating all of the perspectives/questions based on new
		// xmlfile/s
//		String currentDBSelected = (String) questionDBSelector.getSelectedItem();
//		questionDBSelector.setSelectedItem(currentDBSelected);

		// reload the db with modified questions
		reloadDB();
		
		
	}
	
	private void createTable(String concept) {
		//query to get the properties related to this concept
		//construct a query to pull the properties for each instance of concept
		//create the table
		String propQuery = PROPERTIES_QUERY.replaceAll("@CONCEPT@", concept);
		
		ArrayList<String> properties = getQueryResultsAsList(propQuery);
		
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
		
		String retVarString = "";
		String triplesString = "";
		for(String prop : properties) {
			String propClean = prop.replaceAll("-","_");
			retVarString += " ?" + propClean + " ";
			triplesString += PROPERTY_TRIPLE.replaceAll("@CONCEPT@",concept).replaceAll("@PROP@", prop).replaceAll("@PROPCLEAN@", propClean);
		}

		String tableQuery = PROPERTY_TABLE_QUERY.replaceAll("@CONCEPT@", concept).replaceAll("@RETVARS@", retVarString).replaceAll("@TRIPLES@",triplesString);
		
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
	private Boolean meetsParamRequirements(int colIndex, Hashtable<String,Object> requirementHash) {

		Object paramClass = requirementHash.get(CLASS);
		if(paramClass!=null && !paramClass.toString().equals(classArr[colIndex]))
			return false;

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
	
	private void addInsights(Integer order, String question, String sparql, String layout) {
		String questionKey = qa.createQuestionKey(perspective);
		qa.cleanAddQuestion(perspective, questionKey, order.toString(), question, sparql, layout, null, null, null, null);
		this.order++;
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
}
