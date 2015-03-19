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
package prerna.rdf.engine.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.sail.SailException;

import prerna.nameserver.MasterDBHelper;
import prerna.om.Insight;
import prerna.om.SEMOSSParam;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.google.gson.internal.StringMap;
import com.ibm.icu.util.StringTokenizer;

public class QuestionAdministrator {

	private static final Logger logger = Logger.getLogger(QuestionAdministrator.class.getName());

	private AbstractEngine engine;
	private BigDataEngine masterDb;

	private RDFFileSesameEngine insightBaseXML;
	private String selectedEngine = null;

	protected static final String semossURI = "http://semoss.org/ontologies/";
	protected static final String conceptBaseURI = semossURI + Constants.DEFAULT_NODE_CLASS;
	protected static final String relationBaseURI = semossURI + Constants.DEFAULT_RELATION_CLASS;
	protected static final String engineBaseURI = semossURI + Constants.DEFAULT_NODE_CLASS + "/Engine";
	protected static final String perspectiveBaseURI = semossURI + Constants.DEFAULT_NODE_CLASS + "/Perspective";
	protected static final String insightBaseURI = semossURI + Constants.DEFAULT_NODE_CLASS + "/Insight";
	protected static final String paramBaseURI = semossURI + Constants.DEFAULT_NODE_CLASS + "/Param";
	protected final static String resourceURI = "http://www.w3.org/2000/01/rdf-schema#Resource";

	protected static final String enginePerspectiveBaseURI = semossURI + Constants.DEFAULT_RELATION_CLASS + "/Engine:Perspective";
	protected static final String perspectiveInsightBaseURI = semossURI + Constants.DEFAULT_RELATION_CLASS + "/Perspective:Insight";
	protected static final String engineInsightBaseURI = semossURI + Constants.DEFAULT_RELATION_CLASS + "/Engine:Insight";
	protected static final String containsBaseURI = semossURI + Constants.DEFAULT_RELATION_CLASS + "/Contains";
	protected static final String labelBaseURI = containsBaseURI + "/Label";
	protected static final String orderBaseURI = containsBaseURI + "/Order";
	protected static final String idLabelBaseURI = containsBaseURI + "/IDLabel";
	protected static final String layoutBaseURI = containsBaseURI + "/Layout";
	protected static final String sparqlBaseURI = containsBaseURI + "/SPARQL";
	protected static final String TIME_STAMP_URI = containsBaseURI + "/TimeStamp";
	// protected static final String tagBaseURI = containsBaseURI + "/Tag";
	protected static final String descriptionBaseURI = containsBaseURI + "/Description";

	private String engineURI2;

	public QuestionAdministrator(AbstractEngine engine) {
		this.engine = engine;
		this.selectedEngine = engine.getEngineName();
		insightBaseXML =  engine.getInsightBaseXML();
		engineURI2 = engineBaseURI + "/" + selectedEngine;

		masterDb = (BigDataEngine) DIHelper.getInstance().getLocalProp(Constants.LOCAL_MASTER_DB_NAME);
	}

	public RDFFileSesameEngine getInsightBaseXML() {
		return insightBaseXML;
	}

	public void setEngineURI2(String engineURI2) {
		this.engineURI2 = engineURI2;
	}

	public void revertQuestionXML(){
		engine.createInsightBase();
		String insights = this.engine.getProperty(Constants.INSIGHTS);
		engine.createBaseRelationXMLEngine(insights);

		try {
			masterDb.sc.rollback();
		} catch (SailException e) {
			e.printStackTrace();
		}
	}

	public void createQuestionXMLFile() {
		String insights = this.engine.getProperty(Constants.INSIGHTS);
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		createQuestionXMLFile(insights, baseFolder);
	}

	public void createQuestionXMLFile(String questionXMLFile, String baseFolder) {
		// remove previous time stamp
		removeTimeStampFromEngine();
		
		// get new current time
		Date currentTime = Utility.getCurrentTime();
		insightBaseXML.addStatement(engineURI2, TIME_STAMP_URI, currentTime, false);
		engine.createQuestionXMLFile(questionXMLFile, baseFolder);

		// add new current time to master db for engine
		masterDb.addStatement(engineURI2, TIME_STAMP_URI, currentTime, false);
		masterDb.infer();
		masterDb.commit();
	}
	
	public void removeTimeStampFromEngine() {
		String previousTimeStampQuery = "SELECT DISTINCT ?Engine ?Time Where { {?Engine <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine>} {?Engine <http://semoss.org/ontologies/Relation/Contains/TimeStamp> ?Time}}";
		
		ISelectWrapper sjsw = Utility.processQuery(insightBaseXML, previousTimeStampQuery);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String engine = sjss.getRawVar(names[0]).toString();
			Date engineDate = (Date) sjss.getVar(names[1]);
			insightBaseXML.removeStatement(engine, TIME_STAMP_URI, engineDate, false);
		}
	}
	
	private void add2XML(RDFFileSesameEngine xml, ArrayList<Object[]> masterList){
		for(Object[] triple : masterList){
			String sub = (String) triple[0];
			String pred = (String) triple[1];
			String obj = (String) triple[2];
			Boolean concept = (Boolean) triple[3];
			xml.addStatement(sub, pred, obj, concept);
			
			MasterDBHelper.addInsightStatementToMasterDBs(masterDb, selectedEngine, sub, pred, obj, concept);
		}
	}

	private void removeFromXML(RDFFileSesameEngine xml, ArrayList<Object[]> masterList){
		for(Object[] triple : masterList){
			String sub = (String) triple[0];
			String pred = (String) triple[1];
			String obj = (String) triple[2];
			Boolean concept = (Boolean) triple[3];
			xml.removeStatement(sub, pred, obj, concept);

			MasterDBHelper.removeInsightStatementToMasterDBs(masterDb, selectedEngine, sub, pred, obj, concept);
		}
	}

	/**
	 * Adds a perspective.
	 * @param perspectivePred 		String for relationship between engine and perspective full URI
	 * @param perspectiveURI		String for the perspective instance full URI
	 * @param perspective			String for the perspective instance
	 */
	private void addPerspective(String perspectivePred, String perspectiveURI, String perspective) {
		logger.info("Adding perspective " + perspective + "      " + perspectivePred + "     " + perspectiveURI);
		ArrayList<Object[]> masterList = getEngine2PerspectiveTriples(perspectivePred, perspectiveURI, perspective);
		add2XML(insightBaseXML, masterList);
	}

	private void removePerspective(String perspectivePred, String perspectiveURI, String perspective) {
		logger.info("Removing perspective " + perspective + "      " + perspectivePred + "     " + perspectiveURI);
		ArrayList<Object[]> masterList = getEngine2PerspectiveTriples(perspectivePred, perspectiveURI, perspective);
		removeFromXML(insightBaseXML, masterList);
	}

	private void addQuestionID(String ePred, String qURI, String perspectiveURI, String perspective, String qPred, String qsKey) {

		// add the question to the engine; if perspective change, the qURI will
		// need to change as well
		logger.info("Adding question " + qURI + "      " + perspective + "     " + qsKey);
		ArrayList<Object[]> masterList = getQuestionTriples(ePred, qURI, perspectiveURI, perspective, qPred, qsKey);
		add2XML(insightBaseXML, masterList);
	}

	private void removeQuestionID(String ePred, String qURI, String perspectiveURI, String perspective, String qPred, String qsKey) 
	{
		logger.info("Removing question " + qURI + "      " + perspective + "     " + qsKey);
		ArrayList<Object[]> masterList = getQuestionTriples(ePred, qURI, perspectiveURI, perspective, qPred, qsKey);
		removeFromXML(insightBaseXML, masterList);
	}

	private void addQuestionProperties(String qURI, String qsOrder, String question, String qsKey, String sparql, String layoutName, String description) {
		logger.info("Add question properties:: order " + qURI + "      " + qsOrder + "     question  " + question);
		ArrayList<Object[]> masterList = getQuestionPropTriples(qURI, qsOrder, question, qsKey, sparql, layoutName, description);
		add2XML(insightBaseXML, masterList);
	}

	private void removeQuestionProperties(String qURI, String qsOrder, String question, String qsKey, String sparql, String layoutName, String description) {
		logger.info("Removing question properties order " + qURI + "      " + qsOrder + "     Descr  " + question);
		ArrayList<Object[]> masterList = getQuestionPropTriples(qURI, qsOrder, question, qsKey, sparql, layoutName, description);
		removeFromXML(insightBaseXML, masterList);
	}

	private void addQuestionParam(Enumeration<String> paramKeys, String perspective, String qsKey, String qURI, HashMap<String, String> parameterProperties) {
		logger.info("Add question params " + qURI + "      " + paramKeys.toString()  + "     properties  " + parameterProperties.toString());
		ArrayList<Object[]> masterList = getParamTriples(paramKeys, perspective, qsKey, qURI, parameterProperties);
		add2XML(insightBaseXML, masterList);
	}

	private void removeQuestionParam(Enumeration<String> paramKeys, String perspective, String qsKey, String qURI, HashMap<String, String> parameterProperties) {
		logger.info("Remove question params " + qURI + "      " + paramKeys.toString()  + "     properties  " + parameterProperties.toString());
		ArrayList<Object[]> masterList = getParamTriples(paramKeys, perspective, qsKey, qURI, parameterProperties);
		removeFromXML(insightBaseXML, masterList);
	}

	/**
	 * Fills parameterProperties with dependencies, queries, and options. The
	 * keys and values will be of one of the 3 following options:
	 * "SysP1_Data_QUERY" to "SELECT DISTINCT ...."
	 * "SysP1_NumberOfClusters_OPTION" to "1;2;3;4;5" "GQ6_Instance_DEPEND" to
	 * "Concept"
	 * 
	 * @param parameterProperties
	 * @param parameterDependList
	 * @param parameterQueryList
	 * @param questionKey
	 */
	private void populateParamProps( HashMap<String, String> parameterProperties, Vector<String> parameterDependList, Vector<String> parameterQueryList, Vector<String> parameterOptionList, String questionKey) {
		// add dependencies to the hashmap
		if (parameterDependList != null && parameterDependList.size() > 0
				&& !parameterDependList.get(0).equals("None")) {
			for (int i = 0; i < parameterDependList.size(); i++) {
				String[] tmpParamDepend = parameterDependList.get(i).split(
						"_-_");
				parameterProperties.put(questionKey + "_" + tmpParamDepend[0],
						tmpParamDepend[1]);
			}
		}

		// add param queries to the hashmap
		if (parameterQueryList != null && parameterQueryList.size() > 0) {
			for (int i = 0; i < parameterQueryList.size(); i++) {
				String[] tmpParamQuery = parameterQueryList.get(i).split("_-_");
				parameterProperties.put(questionKey + "_" + tmpParamQuery[0],
						tmpParamQuery[1]);
			}
		}

		// add param options to the hashmap
		if (parameterOptionList != null && parameterOptionList.size() > 0) {
			for (int i = 0; i < parameterOptionList.size(); i++) {
				String[] tmpParamOption = parameterOptionList.get(i).split(
						"_-_");
				parameterProperties.put(questionKey + "_" + tmpParamOption[0],
						tmpParamOption[1]);
			}
		}
	}

	public String createQuestionKey(String perspective) {
		String questionKey = null;
		boolean existingPerspective = false;
		boolean existingAutoGenQuestionKey = false;

		Vector<Object> questionsV = engine.getInsights(perspective);

		if(questionsV!=null && !questionsV.isEmpty()) {
			existingPerspective = true;
			for (int j = 0; j < questionsV.size(); j++) {
				String question = questionsV.get(j)+"";
				Insight in = engine.getInsight2(question).get(0);

				String questionID = in.getId();
				String[] questionIDArray = questionID.split(":");
				String currentQuestionKey = questionIDArray[2];

				// checks if there has been any auto-generated question
				// key
				if (currentQuestionKey.contains(perspective)) {
					existingAutoGenQuestionKey = true;
					break;
				}
			}
		}

		// auto generate a questionKey based on existing similar
		// question key
		if (existingPerspective) {
			if (existingAutoGenQuestionKey) {
				// run through all of the questions with auto-generated
				// questionKey and determine what the current largest
				// questionKey is
				// assigns the next value for the new questionKey
				int largestQuestionKeyValue = 0;
				for (int i = 0; i < questionsV.size(); i++) {
					String question = questionsV.get(i)+"";

					Insight in = engine.getInsight2(question).get(0);

					String questionID = in.getId();
					String[] questionIDArray = questionID.split(":");
					String currentQuestionKey = questionIDArray[2];
					int currentQuestionKeyValue = 0;
					if (questionIDArray[2].contains(perspective)) {
						currentQuestionKeyValue = Integer.parseInt(currentQuestionKey.replace(perspective + "_", ""));
					}

					// the following will make largestQuestionKeyValue
					// equal to the last auto-generated questionkeyvalue
					if (currentQuestionKeyValue > largestQuestionKeyValue) {
						largestQuestionKeyValue = currentQuestionKeyValue;
					}
				}
				largestQuestionKeyValue += 1;
				questionKey = perspective + "_" + largestQuestionKeyValue;
			} else {
				questionKey = perspective + "_" + "1";
			}
		} else {
			questionKey = perspective + "_" + "1";
		}
		logger.info("New question key created: " + questionKey);
		return questionKey;
	}

	/**
	 * This method adds a question and then reorder the perspective as necessary
	 * questionKey will be created if it is null
	 */
	public void cleanAddQuestion(String perspective, String questionKey, String questionOrder, String question, String sparql,
			String layout, String questionDescription, Vector<String> parameterDependList,
			Vector<String> parameterQueryList, Vector<String> parameterOptionList) {

		// add question at the bottom of the list
		if(questionOrder == null) {
			if(engine.getInsights(perspective) != null) {
				questionOrder = engine.getInsights(perspective).size() + 1 + "";
			} else {
				questionOrder = "1";
			}
		}
		
		logger.info("CLEAN add of question with the following information: perspective=" + perspective + "; questionKey=" + questionKey + "; questionOrder="+ questionOrder+ "; questionLabel="+ question+ "; sparql="+ sparql+ "; layout="+ layout+ "; questionDescription="+ questionDescription+ "; parameterDependList="+ parameterDependList+ "; parameterQueryList="+ parameterQueryList+ "; parameterOptionList=" + parameterOptionList);

		//get current order of insights in this perspective
		String perspectiveURI = getPerspectiveURI(perspective);
		Vector<String> currentQuestionOrderVector = engine.getOrderedInsightsURI(perspectiveURI);
		if(currentQuestionOrderVector == null ) currentQuestionOrderVector = new Vector<String>();
		System.out.println("questions currenlty ordered as " + currentQuestionOrderVector);

		//now add the question
		String questionURI = addQuestion( perspective, questionKey, questionOrder, question, sparql, layout, questionDescription, parameterDependList, parameterQueryList, parameterOptionList);

		// else get the new order of the insights and reorder
		Vector<String> newQuestionOrderVector = new Vector<String>();
		newQuestionOrderVector.addAll(currentQuestionOrderVector);
		int order = Integer.parseInt(questionOrder);
		newQuestionOrderVector.add(order-1, questionURI);
		System.out.println("new order of questions : " + newQuestionOrderVector);

		// now reorder
		reorderPerspective2(currentQuestionOrderVector, newQuestionOrderVector);
	}

	/**
	 * This method simply adds a question to the question xml
	 * questionKey will be created if it is null
	 */
	public String addQuestion(String perspective, String questionKey, String questionOrder, String question, String sparql,
			String layout, String questionDescription, Vector<String> parameterDependList,
			Vector<String> parameterQueryList, Vector<String> parameterOptionList) {

		logger.info("Adding question with the following information: perspective=" + perspective + "; questionKey=" + questionKey + "; questionOrder="+ questionOrder+ "; questionLabel="+ question+ "; sparql="+ sparql+ "; layout="+ layout+ "; questionDescription="+ questionDescription+ "; parameterDependList="+ parameterDependList+ "; parameterQueryList="+ parameterQueryList+ "; parameterOptionList=" + parameterOptionList);

		HashMap<String, String> parameterProperties = new HashMap<String, String>();

		// take in the parameter properties and store in a hashmap; will be used
		// when adding param properties (dependencies and param queries)
		populateParamProps(parameterProperties, parameterDependList, parameterQueryList, parameterOptionList, questionKey);

		// create the perspective uris
		String perspectiveURI = getPerspectiveURI(perspective);
		String perspectivePred = getPerspectivePred(perspective);

		String qsKey = questionKey;

		if (qsKey == null) {
			qsKey = createQuestionKey(perspective);
		}

		// insight uri
		String qURI = getQURI(perspective, qsKey);

		// perspective to insight relationship
		String qPred = getQPred(perspective, qsKey);
		// engine to insight relationship
		String ePred = getEPred(perspective, qsKey);

		Hashtable paramHash = Utility.getParams(sparql);

		Enumeration<String> paramKeys = paramHash.keys();

		// add the question to the engine
		addPerspective(perspectivePred, perspectiveURI, perspective);
		addQuestionID(ePred, qURI, perspectiveURI, perspective, qPred, qsKey);

		// add all question properties
		addQuestionProperties(qURI, questionOrder, question, qsKey, sparql, layout, questionDescription);

		addQuestionParam(paramKeys, perspective, qsKey, qURI, parameterProperties);
		return qURI;
	}
	
	public void modifyQuestion(String perspective, String questionKey,
			String questionOrder, String question, String sparql,
			String layout, String questionDescription,
			Vector<String> parameterDependList,
			Vector<String> parameterQueryList,
			Vector<String> parameterOptionList, String currentPerspective,
			String currentQuestionKey, String currentQuestionOrder,
			String currentQuestion, String currentSparql, String currentLayout,
			String currentQuestionDescription,
			Vector<String> currentParameterDependListVector,
			Vector<String> currentParameterQueryListVector,
			Vector<String> currentParameterOptionListVector,
			String currentNumberofQuestions) {

		HashMap<String, String> currentParameterProperties = new HashMap<String, String>();

		currentParameterProperties.clear();

		String qsKey = questionKey;

		if (qsKey == null) {
			qsKey = createQuestionKey(perspective);
		}

		// need to keep track of questions from the perspectives getting added to and deleted
		// if they are the same, that would be great
		//get current order of insights in the perspective getting the delete
		String deletedPerspectiveURI = getPerspectiveURI(currentPerspective);
		Vector<String> currentDelQuestionOrderVector = engine.getOrderedInsightsURI(deletedPerspectiveURI);
		System.out.println("questions currenlty ordered in deleted as " + currentDelQuestionOrderVector);

		Vector<String> currentAddQuestionOrderVector = null;
		String addPerspectiveURI = null;
		if(!perspective.equals(currentPerspective)){
			addPerspectiveURI = getPerspectiveURI(perspective);
			currentAddQuestionOrderVector = engine.getOrderedInsightsURI(addPerspectiveURI);
			System.out.println("questions currenlty ordered in add as " + currentAddQuestionOrderVector);
		}

		deleteQuestion(currentPerspective, currentQuestionKey,
				currentQuestionOrder, currentQuestion, currentSparql,
				currentLayout, currentQuestionDescription,
				currentParameterDependListVector,
				currentParameterQueryListVector,
				currentParameterOptionListVector);
		addQuestion(perspective, questionKey, questionOrder, question, sparql,
				layout, questionDescription, parameterDependList,
				parameterQueryList, parameterOptionList);

		//now reorder
		Vector<String> newDelQuestionOrderVector = engine.getOrderedInsightsURI(deletedPerspectiveURI);
		reorderPerspective2(currentDelQuestionOrderVector, newDelQuestionOrderVector);
		if(currentAddQuestionOrderVector!=null){
			Vector<String> newAddQuestionOrderVector = engine.getOrderedInsightsURI(addPerspectiveURI);
			reorderPerspective2(currentAddQuestionOrderVector, newAddQuestionOrderVector);
		}

	}

	public void deleteQuestions(String perspective, Vector<String> questionTitles)
	{
		//get current order of insights in this perspective
		String perspectiveURI = getPerspectiveURI(perspective);
		Vector<String> currentQuestionOrderVector = engine.getOrderedInsightsURI(perspectiveURI);
		System.out.println("questions currenlty ordered as " + currentQuestionOrderVector);

		Hashtable<String, Boolean> results = new Hashtable<String,Boolean>();
		//now delete the questions
		for(String questionTitle : questionTitles){
			//first have to get the question
			Insight insight = engine.getInsight2(questionTitle).get(0);
			String questionID = insight.getId();
			String[] questionIDArray = questionID.split(":");
			String questionKey = questionIDArray[2];
			String questionOrder = insight.getOrder();
			String question = questionTitle;
			String sparql = insight.getSparql();
			String layout = insight.getOutput();
			String questionDescription = insight.getDescription();
			Hashtable<String, Vector<String>> paramHash = getParameterVectors(insight.getURI());
			Vector<String> parameterQueryVector = paramHash.get("parameterQueryVector");
			Vector<String> dependVector = paramHash.get("dependVector");
			Vector<String> optionVector = paramHash.get("optionVector");

			//delete it
			deleteQuestion( perspective, questionKey, questionOrder, question, sparql, layout, questionDescription, dependVector, parameterQueryVector, optionVector);
		}

		Vector<String> newQuestionOrderVector = engine.getOrderedInsightsURI(perspectiveURI);
		// if there are no questions left
		// delete the perspective.
		if (newQuestionOrderVector == null || newQuestionOrderVector.isEmpty()) {
			String perspectivePred = getPerspectivePred(perspective);
			removePerspective(perspectivePred, perspectiveURI, perspective);
		}

		// else get the new order of the insights and reorder
		else
		{
			System.out.println("new order of questions : " + newQuestionOrderVector);

			// now reorder
			reorderPerspective2(currentQuestionOrderVector, newQuestionOrderVector);
		}

	}

	/**This method should be used when simply deleting one question. It is clean in that it will reorder the perspective if needed or delete the perspective if it is now empty
	 * 
	 * @param perspective Perspective of question getting deleted
	 * @param questionKey key of question getting deleted (e.g. TAP_Core_Data:System-Perspective:SysP2)
	 * @param questionOrder order of question getting deleted
	 * @param question question title of question getting deleted
	 * @param sparql query of question getting deleted
	 * @param layout layout or playsheet of question getting deleted
	 * @param questionDescription description if exists otherwise null
	 * @param parameterDependList 
	 * @param parameterQueryList
	 * @param parameterOptionList
	 * @return
	 */
	public Boolean cleanDeleteQuestion(String perspective, String questionKey,
			String questionOrder, String question, String sparql,
			String layout, String questionDescription,
			Vector<String> parameterDependList,
			Vector<String> parameterQueryList,
			Vector<String> parameterOptionList) {

		logger.info("Deleting question: perspective=" + perspective
				+ "; questionKey=" + questionKey + "; questionOrder="
				+ questionOrder + "; questionLabel=" + question + "; sparql="
				+ sparql + "; layout=" + layout + "; questionDescription="
				+ questionDescription + "; parameterDependList="
				+ parameterDependList + "; parameterQueryList="
				+ parameterQueryList + "; parameterOptionList="
				+ parameterOptionList);

		//get current order of insights in this perspective
		String perspectiveURI = getPerspectiveURI(perspective);
		Vector<String> currentQuestionOrderVector = engine.getOrderedInsightsURI(perspectiveURI);
		System.out.println("questions currenlty ordered as " + currentQuestionOrderVector);

		//now delete the question
		String questionURI = deleteQuestion( perspective, questionKey, questionOrder, question, sparql, layout, questionDescription, parameterDependList, parameterQueryList, parameterOptionList);

		// if there are no questions left
		// delete the perspective.
		if (currentQuestionOrderVector.size() == 1) {
			String perspectivePred = getPerspectivePred(perspective);
			removePerspective(perspectivePred, perspectiveURI, perspective);
		}

		// else get the new order of the insights and reorder
		else
		{
			Vector<String> newQuestionOrderVector = new Vector<String>();//
			newQuestionOrderVector.addAll(currentQuestionOrderVector);
			int order = Integer.parseInt(questionOrder);
			newQuestionOrderVector.remove(order-1);
			System.out.println("new order of questions : " + newQuestionOrderVector);

			// now reorder
			reorderPerspective2(currentQuestionOrderVector, newQuestionOrderVector);
		}


		return true;
	}

	public String deleteQuestion(String perspective, String questionKey,
			String questionOrder, String question, String sparql,
			String layout, String questionDescription,
			Vector<String> parameterDependList,
			Vector<String> parameterQueryList,
			Vector<String> parameterOptionList) {

		logger.info("Deleting question: perspective=" + perspective
				+ "; questionKey=" + questionKey + "; questionOrder="
				+ questionOrder + "; questionLabel=" + question + "; sparql="
				+ sparql + "; layout=" + layout + "; questionDescription="
				+ questionDescription + "; parameterDependList="
				+ parameterDependList + "; parameterQueryList="
				+ parameterQueryList + "; parameterOptionList="
				+ parameterOptionList);

		HashMap<String, String> parameterProperties = new HashMap<String, String>();

		// populates parameterProperties based on on paramProp values passed in
		populateParamProps(parameterProperties, parameterDependList,
				parameterQueryList, parameterOptionList, questionKey);

		// create the perspective uris
		String perspectiveURI = getPerspectiveURI(perspective);

		// add the question
		Hashtable paramHash = Utility.getParams(sparql);

		// insight uri
		String qURI = getQURI(perspective, questionKey);
		// perspective to insight relationship
		String qPred = getQPred(perspective, questionKey);
		// engine to insight relationship
		String ePred = getEPred(perspective, questionKey);

		removeQuestionID(ePred, qURI, perspectiveURI, perspective, qPred,
				questionKey);

		// add all question properties
		removeQuestionProperties(qURI, questionOrder, question, questionKey, sparql, layout, questionDescription);


		Enumeration<String> paramKeys = paramHash.keys();

		removeQuestionParam(paramKeys, perspective, questionKey, qURI,
				parameterProperties);

		return qURI;
	}

	public Boolean deleteAllFromPersp(String perspective) {
		String pURI = getPerspectiveURI(perspective);
		deleteAllFromPerspective(pURI);
		return true;
	}

	private String getPerspectiveURI(String perspective) {
		String pURI = "";//
		Vector<String> pURIs = engine.getPerspectivesURI();
		if(pURIs != null){
			for (String p : pURIs) {
				String pURIinstance = Utility.getInstanceName(p);
				if (pURIinstance.equals(engine.getEngineName() + ":" + perspective)) {
					pURI = p;
					break;
				}
			}
		}
		//if the perspective triple has already been deleted or doesn't return for some reason, try concatenating
		if(pURI.isEmpty()) {
			pURI = perspectiveBaseURI + "/" + selectedEngine + ":"
					+ perspective;
		}

		return pURI;
	}

	public void deleteAllFromPerspective(String perspectiveURI) {
		logger.info("Deleting all questions from perspective with this URI: "
				+ perspectiveURI);

		Vector questionsVector = engine
				.getInsightsURI(perspectiveURI);
		ArrayList<String> questionList2 = new ArrayList<String>();
		String perspective = perspectiveURI.substring(perspectiveURI
				.lastIndexOf(":") + 1);
		String perspectivePred = getPerspectivePred(perspective);

		for (Object question : questionsVector) {
			questionList2.add((String) question);
		}

		for (String question2 : questionList2) {
			Insight in = engine.getInsight2URI(question2)
					.get(0);

			System.out.println("Removing question " + question2);

			String questionOrder = in.getOrder();
			String question = in.getLabel();
			String questionID = in.getId();
			String[] questionIDArray = questionID.split(":");
			String questionKey = questionIDArray[2];
			String sparql = in.getSparql();
			String layoutValue = in.getOutput();
			String questionDescription = in.getDescription();
			Hashtable<String, Vector<String>> paramHash = getParameterVectors(question2);
			Vector<String> parameterQueryVector = paramHash.get("parameterQueryVector");
			Vector<String> dependVector = paramHash.get("dependVector");
			Vector<String> optionVector = paramHash.get("optionVector");

			deleteQuestion(perspective, questionKey, questionOrder, question,
					sparql, layoutValue, questionDescription, dependVector,
					parameterQueryVector, optionVector);
		}
		// delete perspective from the engine
		removePerspective(perspectivePred, perspectiveURI, perspective);
	}

	public Hashtable<String, Vector<String>> getParameterVectors(String questionURI){
		Vector<String> parameterQueryVector = new Vector<String>();
		Vector<String> dependVector = new Vector<String>();
		Vector<String> optionVector = new Vector<String>();

		Vector<SEMOSSParam> paramInfoVector = engine.getParamsURI(questionURI);
		if (!paramInfoVector.isEmpty()) {
			for (int i = 0; i < paramInfoVector.size(); i++) {
				SEMOSSParam param = paramInfoVector.get(i);
				String name = param.getName();
				String query = param.getQuery();
				if (query != null && !query.equals(DIHelper.getInstance().getProperty("TYPE" + "_" + Constants.QUERY))) 
				{
					parameterQueryVector.add(name + "_QUERY_-_" + query);
				}

				Vector<String> dependVars = param.getDependVars();
				if (dependVars != null && !dependVars.isEmpty() && !dependVars.get(0).equals("None")) {
					for (int j = 0; j < paramInfoVector.get(i).getDependVars().size(); j++) 
					{
						dependVector.add(name + "_DEPEND_-_" + dependVars.get(j));
					}
				}

				Vector<String> optionsVec = param.getOptions();
				if (optionsVec != null && !optionsVec.isEmpty()) 
				{
					Vector<String> options = paramInfoVector.get(i).getOptions();
					String optionsConcat = "";
					for (int j = 0; j < options.size(); j++) 
					{
						optionsConcat += options.get(j);
						if (j != options.size() - 1) 
						{
							optionsConcat += ";";
						}
					}
					optionVector.add(paramInfoVector.get(i).getType() + "_OPTION_-_" + optionsConcat);
				}
			}
		}
		Hashtable<String, Vector<String>> resHash = new Hashtable<String,Vector<String>>();
		resHash.put("parameterQueryVector", parameterQueryVector);
		resHash.put("dependVector", dependVector);
		resHash.put("optionVector", optionVector);
		return resHash;
	}

	public void reorderPerspective(String perspective, Vector<Hashtable<String, Object>> orderedInsights) {
		Vector<String> idVector = new Vector<String>();
		for (int i = 0; i < orderedInsights.size(); i++) {
			Hashtable orderedInsightHash = orderedInsights.get(i);
			String qID = ((StringMap) orderedInsightHash.get("propHash"))
					.get("id") + "";
			idVector.add(i, qID);
		}
		reorderPerspective2(perspective, idVector);
	}

	public void reorderPerspective2(String perspective, Vector<String> orderedInsightIDs) {
		String perspectiveURI = getPerspectiveURI(perspective);
		logger.info("Reordering all questions from perspective with this URI: "
				+ perspectiveURI);
		Vector<String> questionsVector = engine
				.getOrderedInsightsURI(perspectiveURI);
		System.out.println("questions currenlty ordered as " + questionsVector);

		reorderPerspective2(questionsVector, orderedInsightIDs);
	}

	public void reorderPerspective2(Vector<String> currentOrderOfInsightsIDs, Vector<String> newOrderOfInsightsIDs) {

		//		// determine difference in order between orderedInsights and
		//		// questionsVector so that we only update the questions that need
		//		// updating
		//		Hashtable<String, Integer> needsReorder = new Hashtable<String, Integer>();
		//		for (int i = 0; i < newOrderOfInsightsIDs.size(); i++) {
		//			String qID = newOrderOfInsightsIDs.get(i);
		//			if ((currentOrderOfInsightsIDs != null && i >= currentOrderOfInsightsIDs.size()) || (currentOrderOfInsightsIDs != null && 
		//					!Utility.getInstanceName(qID).equals(Utility.getInstanceName(currentOrderOfInsightsIDs.get(i))))) {
		//				needsReorder.put(qID, i+1);
		//			}
		//		}

		// now reorder!
		//		Iterator<String> it = needsReorder.keySet().iterator();
		//		while (it.hasNext()) {
		//			String key = it.next();
		//			String qURI = key;
		//			if(!qURI.contains("http://semoss.org/ontologies/Concept/Insight/"))
		//				qURI = "http://semoss.org/ontologies/Concept/Insight/" + qURI;
		//			
		//			Insight in = ((AbstractEngine) engine).getInsight2URI(qURI).get(0);
		//
		//			System.out.println("Removing order question " + qURI);
		//			String questionOrder = in.getOrder();
		//			String newOrder = needsReorder.get(key) + "";
		//			reorderQuestion(qURI, newOrder, questionOrder);
		//
		//		}
		for (String key : newOrderOfInsightsIDs) {
			String qURI = key;
			if(!qURI.contains("http://semoss.org/ontologies/Concept/Insight/"))
				qURI = "http://semoss.org/ontologies/Concept/Insight/" + qURI;

			Insight in = engine.getInsight2URI(qURI).get(0);

			System.out.println("Removing order question " + qURI);
			String questionOrder = in.getOrder();
			String newOrder = newOrderOfInsightsIDs.indexOf(key)+1 + "";
			reorderQuestion(qURI, newOrder, questionOrder);

		}
	}

	private void reorderQuestion(String questionURI, String newQuestionOrder, String oldQuestionOrder){
		if(!newQuestionOrder.equals(oldQuestionOrder)){
			if (oldQuestionOrder != null) {
				System.out.println("Removing order " + oldQuestionOrder
						+ " from question " + questionURI);
				insightBaseXML.removeStatement(questionURI, orderBaseURI,
						oldQuestionOrder, false);
			}

			System.out.println("Changing order for " + questionURI + " from " + oldQuestionOrder + " to question "
					+ newQuestionOrder );
			insightBaseXML.addStatement(questionURI, orderBaseURI, newQuestionOrder, false);
		}
		else 
			System.out.println("the order has not been updtaed for " + questionURI);

	}

	private String getQPred(String perspective, String qsKey) {
		String qPred = perspectiveInsightBaseURI + "/" + selectedEngine + ":"
				+ perspective + Constants.RELATION_URI_CONCATENATOR
				+ selectedEngine + ":" + perspective + ":" + qsKey;

		return qPred;
	}

	private String getPerspectivePred(String perspective) {
		String perspectivePred = enginePerspectiveBaseURI + "/"
				+ selectedEngine + Constants.RELATION_URI_CONCATENATOR
				+ selectedEngine + ":" + perspective;

		return perspectivePred;
	}

	private String getEPred(String perspective, String qsKey) {
		String ePred = engineInsightBaseURI + "/" + selectedEngine
				+ Constants.RELATION_URI_CONCATENATOR + selectedEngine + ":"
				+ perspective + ":" + qsKey;
		return ePred;
	}

	private String getQURI(String perspective, String qsKey) {
		String qURI = insightBaseURI + "/" + selectedEngine + ":" + perspective
				+ ":" + qsKey;
		return qURI;
	}

	private String getqsParamKey(String paramBaseURI, String perspective, String qsKey, String paramKey)
	{
		String qsParamKey = paramBaseURI + "/" + selectedEngine + ":"
				+ perspective + ":" + qsKey + ":" + paramKey;
		return qsParamKey;
	}

	private ArrayList<Object[]> getEngine2PerspectiveTriples(String perspectivePred, String perspectiveURI, String perspective){
		ArrayList<Object[]> masterList = new ArrayList<Object[]>();

		//define the perspective
		masterList.add(new Object[] {perspectiveURI, RDF.TYPE.stringValue(), perspectiveBaseURI, true});
		masterList.add(new Object[] {perspectiveURI, RDFS.LABEL.stringValue(), selectedEngine + ":" + perspective, false});
		masterList.add(new Object[] {perspectiveURI, labelBaseURI, perspective, false});

		//define the predicate
		masterList.add(new Object[] {perspectivePred, RDFS.SUBPROPERTYOF.stringValue(), enginePerspectiveBaseURI, true});
		masterList.add(new Object[] {perspectivePred, RDFS.LABEL.stringValue(), selectedEngine + Constants.RELATION_URI_CONCATENATOR + selectedEngine + ":" + perspective, false});

		//define the relationship
		masterList.add(new Object[] {engineURI2, perspectivePred, perspectiveURI, true});

		// remove the resource and concept uris
		masterList.add(new Object[] {perspectiveURI, RDF.TYPE.stringValue(), conceptBaseURI, true});
		masterList.add(new Object[] {perspectiveURI, RDF.TYPE.stringValue(), resourceURI, true});

		return masterList;
	}

	private ArrayList<Object[]> getQuestionTriples(String ePred, String qURI, String perspectiveURI, String perspective, String qPred, String qsKey) 
	{
		ArrayList<Object[]> masterList = new ArrayList<Object[]>();
		// remove the question to the engine; if perspective change, the qURI
		// will need to change as well
		masterList.add(new Object[] {qURI, RDF.TYPE.stringValue(), insightBaseURI, true});
		masterList.add(new Object[] {qURI, RDFS.LABEL.stringValue(), selectedEngine + ":" + perspective + ":" + qsKey, false});

		// remove the engine to the question triples
		masterList.add(new Object[] {ePred, RDFS.SUBPROPERTYOF.stringValue(), engineInsightBaseURI, true});
		masterList.add(new Object[] {ePred, RDFS.LABEL.stringValue(), selectedEngine + Constants.RELATION_URI_CONCATENATOR + selectedEngine + ":" + perspective + ":" + qsKey, false});
		masterList.add(new Object[] {engineURI2, ePred, qURI, true});

		// remove question to perspective
		masterList.add(new Object[] {qPred, RDFS.SUBPROPERTYOF.stringValue(), perspectiveInsightBaseURI, true});
		masterList.add(new Object[] {qPred, RDFS.SUBPROPERTYOF.stringValue(), qPred, true});
		masterList.add(new Object[] {qPred, RDF.TYPE.stringValue(), resourceURI, true});
		masterList.add(new Object[] {qPred, RDF.TYPE.stringValue(), Constants.DEFAULT_PROPERTY_URI, true});
		masterList.add(new Object[] {qPred, RDFS.LABEL.stringValue(), selectedEngine + ":" + perspective + Constants.RELATION_URI_CONCATENATOR + selectedEngine + ":" + perspective + ":" + qsKey, false});

		masterList.add(new Object[] {perspectiveURI, relationBaseURI, qURI, true});
		masterList.add(new Object[] {perspectiveURI, perspectiveInsightBaseURI, qURI, true});
		masterList.add(new Object[] {perspectiveURI, qPred, qURI, true});

		masterList.add(new Object[] {qPred, RDFS.SUBPROPERTYOF.stringValue(), relationBaseURI, true});
		masterList.add(new Object[] {qURI, RDF.TYPE.stringValue(), conceptBaseURI, true});
		masterList.add(new Object[] {qURI, RDF.TYPE.stringValue(), resourceURI, true});

		return masterList;
	}

	private ArrayList<Object[]> getQuestionPropTriples(String qURI, String qsOrder, String question, String qsKey, String sparql, String layoutName, String description) 
	{
		ArrayList<Object[]> masterList = new ArrayList<Object[]>();
		// TODO might need to add a relationship between a perspective and the
		// label associated with the insight that perspective is related too,
		// but i think this should just be through queries.
		masterList.add(new Object[] {qURI, labelBaseURI, question, false});
		masterList.add(new Object[] {qURI, idLabelBaseURI, qsKey, false});
		masterList.add(new Object[] {qURI, sparqlBaseURI, sparql, false});
		masterList.add(new Object[] {qURI, layoutBaseURI, layoutName, false});
		if (qsOrder != null) {
			masterList.add(new Object[] {qURI, orderBaseURI, qsOrder, false});
		}
		if(description!=null){
			masterList.add(new Object[] {qURI, descriptionBaseURI, description, false});
		}
		return masterList;
	}

	private ArrayList<Object[]> getParamTriples(Enumeration<String> paramKeys, String perspective, String qsKey, String qURI, HashMap<String, String> parameterProperties) 
	{
		ArrayList<Object[]> masterList = new ArrayList<Object[]>();
		while (paramKeys.hasMoreElements()) {
			String param = paramKeys.nextElement();
			String paramKey = param.substring(0, param.indexOf("-"));
			String type = param.substring(param.indexOf("-") + 1);

			String qsParamKey = getqsParamKey(paramBaseURI, perspective, qsKey, paramKey);

			// get parameter to the insight relationship
			masterList.add(new Object[] {qURI, "INSIGHT:PARAM", qsParamKey, true});
			// add a label to the param which is the label used in param panel
			masterList.add(new Object[] {qsParamKey, "PARAM:LABEL", paramKey, false});
			masterList.add(new Object[] {qsParamKey, RDF.TYPE.stringValue(), resourceURI, true});

			// see if the param key has options (not a query) associated with it
			// usually it is of the form qsKey + _ + paramKey + _ + OPTION
			// if so, remove the list of options and set the type ot be a literal
			String optionKey = qsKey + "_" + type + "_" + Constants.OPTION;
			if (parameterProperties.get(optionKey) != null) 
			{
				String option = parameterProperties.get(optionKey);
				masterList.add(new Object[] {qsParamKey, "PARAM:OPTION", option, false});
				masterList.add(new Object[] {qsParamKey, "PARAM:TYPE", type, false});
				masterList.add(new Object[] {qsParamKey, "PARAM:HAS:DEPEND", "false", false});
				masterList.add(new Object[] {qsParamKey, "PARAM:DEPEND", "None", false});
			} else 
			{
				// see if the param key has a query associated with it
				// usually it is of the form qsKey + _ + paramKey + _ + Query
				// if there is no specific query defined, we will use the type query from DIHelper
				String query = null;
				if(engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME || engine.getEngineType() == IEngine.ENGINE_TYPE.JENA || engine.getEngineType() == IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE  )
				{
					query = DIHelper.getInstance().getProperty("TYPE" + "_" + Constants.QUERY);
				}
				else if(engine.getEngineType() == IEngine.ENGINE_TYPE.RDBMS)
				{
					query = "SELECT DISTINCT @entity@ FROM @entity@";
					// take the param and convert to query
					//Hashtable paramHash = Utility.getParams(sparql);
					//Enumeration keys = paramHash.
				}

				String customQueryKey = qsKey + "_" + paramKey + "_" + Constants.QUERY;
				if (parameterProperties.get(customQueryKey) != null)
				{
					query = parameterProperties.get(customQueryKey);
				}

				masterList.add(new Object[] {qsParamKey, "PARAM:QUERY", query, false});

				// see if there is dependency
				// dependency is of the form qsKey + _ + paramKey + _ + Depend
				String dependsKey = qsKey + "_" + paramKey + "_" + Constants.DEPEND;
				if (parameterProperties.get(dependsKey) != null) 
				{
					String depend = parameterProperties.get(dependsKey);
					StringTokenizer depTokens = new StringTokenizer(depend, ";");
					masterList.add(new Object[] {qsParamKey, "PARAM:HAS:DEPEND", "true", false});
					while (depTokens.hasMoreElements()) {
						String depToken = depTokens.nextToken();
						masterList.add(new Object[] {qsParamKey, "PARAM:DEPEND", depToken, false});
					}
				} else {//has no dependencies
					masterList.add(new Object[] {qsParamKey, "PARAM:HAS:DEPEND", "false", false});
					masterList.add(new Object[] {qsParamKey, "PARAM:DEPEND", "None", false});
				}

				// remove the type to be a uri
				masterList.add(new Object[] {qsParamKey, "PARAM:TYPE", type, true});
			}
		}

		return masterList;
	}
}
