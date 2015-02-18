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

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.mvel2.util.ThisLiteral;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.rdfxml.RDFXMLWriter;

import prerna.om.Insight;
import prerna.om.SEMOSSParam;
import prerna.rdf.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.google.gson.internal.StringMap;
import com.ibm.icu.util.StringTokenizer;

public class QuestionAdministrator {
	Logger logger = Logger.getLogger(QuestionAdministrator.class.getName());

	IEngine engine;
	RDFFileSesameEngine insightBaseXML;
	private String selectedEngine = null;

	String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

	// TODO need to clean up the below; shouldn't need those as class objects
	boolean reorder = true;
	String questionModType = "";
	String selectedPerspective = "";
	public ArrayList<String> questionList = new ArrayList<String>();
	String newPerspective;

	protected static final String semossURI = "http://semoss.org/ontologies/";
	protected static final String conceptBaseURI = semossURI
			+ Constants.DEFAULT_NODE_CLASS;
	protected static final String relationBaseURI = semossURI
			+ Constants.DEFAULT_RELATION_CLASS;
	protected static final String engineBaseURI = semossURI
			+ Constants.DEFAULT_NODE_CLASS + "/Engine";
	protected static final String perspectiveBaseURI = semossURI
			+ Constants.DEFAULT_NODE_CLASS + "/Perspective";
	protected static final String insightBaseURI = semossURI
			+ Constants.DEFAULT_NODE_CLASS + "/Insight";
	protected static final String paramBaseURI = semossURI
			+ Constants.DEFAULT_NODE_CLASS + "/Param";
	protected final static String resourceURI = "http://www.w3.org/2000/01/rdf-schema#Resource";

	protected static final String enginePerspectiveBaseURI = semossURI
			+ Constants.DEFAULT_RELATION_CLASS + "/Engine:Perspective";
	protected static final String perspectiveInsightBaseURI = semossURI
			+ Constants.DEFAULT_RELATION_CLASS + "/Perspective:Insight";
	protected static final String engineInsightBaseURI = semossURI
			+ Constants.DEFAULT_RELATION_CLASS + "/Engine:Insight";
	protected static final String containsBaseURI = semossURI
			+ Constants.DEFAULT_RELATION_CLASS + "/Contains";
	protected static final String labelBaseURI = containsBaseURI + "/Label";
	protected static final String orderBaseURI = containsBaseURI + "/Order";
	protected static final String idLabelBaseURI = containsBaseURI + "/IDLabel";
	protected static final String layoutBaseURI = containsBaseURI + "/Layout";
	protected static final String sparqlBaseURI = containsBaseURI + "/SPARQL";
	// protected static final String tagBaseURI = containsBaseURI + "/Tag";
	protected static final String descriptionBaseURI = containsBaseURI
			+ "/Description";

	String engineURI2;

	public QuestionAdministrator(IEngine engine) {
		this.engine = engine;
		this.selectedEngine = engine.getEngineName();
		insightBaseXML = ((AbstractEngine) engine).getInsightBaseXML();
		engineURI2 = engineBaseURI + "/" + selectedEngine;
	}

	public QuestionAdministrator(IEngine engine,
			ArrayList<String> questionList, String selectedPerspective,
			String questionModType) {
		this(engine);

		this.questionList = questionList;
		this.selectedPerspective = selectedPerspective;
		this.questionModType = questionModType;
	}

	public RDFFileSesameEngine getInsightBaseXML() {
		return insightBaseXML;
	}

	public void setEngineURI2(String engineURI2) {
		this.engineURI2 = engineURI2;
	}

	public void createQuestionXMLFile() {
		String insights = this.engine.getProperty(Constants.INSIGHTS);
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		createQuestionXMLFile(insights, baseFolder);
	}

	public void createQuestionXMLFile(String questionXMLFile, String baseFolder) {
		FileWriter fWrite = null;
		RDFXMLWriter questionXMLWriter = null;

		try {
			String xmlFileName = baseFolder + "/" + questionXMLFile;

			fWrite = new FileWriter(xmlFileName);
			questionXMLWriter = new RDFXMLWriter(fWrite);
			if (insightBaseXML instanceof RDFFileSesameEngine)
				((RDFFileSesameEngine) insightBaseXML).rc
						.export(questionXMLWriter);

			logger.info("Updated XML Question File at: " + xmlFileName);
		} catch (IOException | RDFHandlerException | RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if (fWrite != null) {
					fWrite.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void add2XML(RDFFileSesameEngine xml, ArrayList<Object[]> masterList){
		for(Object[] triple : masterList){
			String sub = (String) triple[0];
			String pred = (String) triple[1];
			String obj = (String) triple[2];
			Boolean concept = (Boolean) triple[3];
			xml.addStatement(sub, pred, obj, concept);
		}
	}
	
	private void removeFromXML(RDFFileSesameEngine xml, ArrayList<Object[]> masterList){
		for(Object[] triple : masterList){
			String sub = (String) triple[0];
			String pred = (String) triple[1];
			String obj = (String) triple[2];
			Boolean concept = (Boolean) triple[3];
			xml.removeStatement(sub, pred, obj, concept);
		}
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

	/**
	 * Adds a perspective.
	 * 
	 * @param perspectivePred
	 *            String for relationship between engine and perspective full
	 *            URI
	 * @param perspectiveURI
	 *            String for the perspective instance full URI
	 * @param perspective
	 *            String for the perspective instance
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

	/**
	 * Adds question to the engine and perspective
	 * 
	 * @param ePred
	 * @param qURI
	 * @param perspectiveURI
	 * @param qPred
	 */
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

	private ArrayList<Object[]> getQuestionPropTriples(String qURI, String qsOrder, String qsDescr, String qsKey, String sparql, String layoutName, String descriptionPred, String description) 
	{
		ArrayList<Object[]> masterList = new ArrayList<Object[]>();
		// TODO might need to add a relationship between a perspective and the
		// label associated with the insight that perspective is related too,
		// but i think this should just be through queries.
		masterList.add(new Object[] {qURI, labelBaseURI, qsDescr, false});
		masterList.add(new Object[] {qURI, idLabelBaseURI, qsKey, false});
		masterList.add(new Object[] {qURI, sparqlBaseURI, sparql, false});
		masterList.add(new Object[] {qURI, layoutBaseURI, layoutName, false});
		if (qsOrder != null) {
			masterList.add(new Object[] {qURI, orderBaseURI, qsOrder, false});
		}
		if(description!=null){
			masterList.add(new Object[] {qURI, descriptionPred, description, false});
		}
		return masterList;
	}

	private void addQuestionProperties(String qURI, String qsOrder, String qsDescr, String qsKey, String sparql, String layoutName, String descriptionPred, String description) {
		logger.info("Add question properties:: order " + qURI + "      " + qsOrder + "     Descr  " + qsDescr);
		ArrayList<Object[]> masterList = getQuestionPropTriples(qURI, qsOrder, qsDescr, qsKey, sparql, layoutName, descriptionPred, description);
		add2XML(insightBaseXML, masterList);
	}

	private void removeQuestionProperties(String qURI, String qsOrder, String qsDescr, String qsKey, String sparql, String layoutName, String descriptionPred, String description) {
		logger.info("Removing question properties order " + qURI + "      " + qsOrder + "     Descr  " + qsDescr);
		ArrayList<Object[]> masterList = getQuestionPropTriples(qURI, qsOrder, qsDescr, qsKey, sparql, layoutName, descriptionPred, description);
		removeFromXML(insightBaseXML, masterList);
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
				String query = DIHelper.getInstance().getProperty("TYPE" + "_" + Constants.QUERY);

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

	private void reorderQuestions(String order, String currentOrder) {
		Insight in = new Insight();

		int newOrderNumber = Integer.parseInt(order);
		int currentOrderNumber = Integer.parseInt(currentOrder);

		// get the indexed at question for current position to start
		// manipulating order numbers for all questions under the current
		// position
		// when moving from high # to a low #
		if (newOrderNumber < currentOrderNumber) {
			for (int i = newOrderNumber; i < currentOrderNumber; i++) {
				// get the value at [i] (store the old question) and replace
				// first [i] with +1 (store the new question)
				String oldQuestion = questionList.get(i - 1);
				// get the questionkey to create the appropriate triples to add
				// and remove
				in = ((AbstractEngine) engine).getInsight2(oldQuestion).get(0);// TODO
				String localID = in.getId();
				if(localID.equals("DN"))
					in = ((AbstractEngine) engine).getInsight2URI(oldQuestion).get(0);
				localID = in.getId();

				String newQuestionOrder = Integer.toString(i + 1);
				String oldQuestionOrder = Integer.toString(i);
				//remove the old order and add the new order
				reorderQuestion(localID, newQuestionOrder, oldQuestionOrder);
			}
		} else {
			for (int i = currentOrderNumber; i < newOrderNumber; i++) {
				// get the value at [i] (store the old question) and replace
				// first [i] with +1 (store the new question)
				String oldQuestion = questionList.get(i);

				// get the questionkey to create the appropriate triples to add
				// and remove
				in = ((AbstractEngine) engine).getInsight2(oldQuestion).get(0);
				String localID = in.getId();
				if(localID.equals("DN"))
					in = ((AbstractEngine) engine).getInsight2URI(oldQuestion).get(0);
				localID = in.getId();

				String oldQuestionOrder = Integer.toString(i + 1);
				String newQuestionOrder = Integer.toString(i);
				reorderQuestion(localID, newQuestionOrder, oldQuestionOrder);
			}
		}
	}

	public String createQuestionKey(String perspective) {
		String questionKey = null;
		boolean existingPerspective = false;
		boolean existingAutoGenQuestionKey = false;

		Vector<String> perspectives = engine.getPerspectives();
		Vector<String> questionsV = engine.getInsights(perspective);

		for (int i = 0; i < perspectives.size(); i++) {
			if (perspectives.get(i).equals(perspective)) {
				existingPerspective = true;
				if (existingPerspective) {
					for (int j = 0; j < questionsV.size(); j++) {
						String question = questionsV.get(j);
						Insight in = ((AbstractEngine) engine).getInsight2(
								question).get(0);

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
				break;
			} else {
				existingPerspective = false;
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
				for (int i = 0; i < questionList.size(); i++) {
					String question = questionList.get(i);

					Insight in = ((AbstractEngine) engine)
							.getInsight2(question).get(0);

					String questionID = in.getId();
					String[] questionIDArray = questionID.split(":");
					String currentQuestionKey = questionIDArray[2];
					int currentQuestionKeyValue = 0;
					if (questionIDArray[2].contains(perspective)) {
						currentQuestionKeyValue = Integer
								.parseInt(currentQuestionKey.replace(
										perspective + "_", ""));
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

	public void addQuestion(String perspective, String questionKey,
			String questionOrder, String question, String sparql,
			String layout, String questionDescription,
			Vector<String> parameterDependList,
			Vector<String> parameterQueryList,
			Vector<String> parameterOptionList) {

		logger.info("Adding question with the following information: perspective="
				+ perspective
				+ "; questionKey="
				+ questionKey
				+ "; questionOrder="
				+ questionOrder
				+ "; questionLabel="
				+ question
				+ "; sparql="
				+ sparql
				+ "; layout="
				+ layout
				+ "; questionDescription="
				+ questionDescription
				+ "; parameterDependList="
				+ parameterDependList
				+ "; parameterQueryList="
				+ parameterQueryList
				+ "; parameterOptionList=" + parameterOptionList);

		HashMap<String, String> parameterProperties = new HashMap<String, String>();

		// take in the parameter properties and store in a hashmap; will be used
		// when adding param properties (dependencies and param queries)
		populateParamProps(parameterProperties, parameterDependList,
				parameterQueryList, parameterOptionList, questionKey);

		// create the perspective uris
		String perspectiveURI = perspectiveBaseURI + "/" + selectedEngine + ":" + perspective;
		String perspectivePred = getPerspectivePred(perspective);

		String qsKey = questionKey;

		if (qsKey == null) {
			qsKey = createQuestionKey(perspective);
		}

		String qsOrder = questionOrder;
		String qsDescr = question;
		String layoutName = layout;

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
		addQuestionProperties(qURI, qsOrder, qsDescr, qsKey, sparql, layoutName, descriptionBaseURI, questionDescription);

		// TODO why? isnt this done in QuestionID
		// insightBaseXML.addStatement(engineURI2, perspectivePred,
		// perspectiveURI, true);

		addQuestionParam(paramKeys, perspective, qsKey, qURI,
				parameterProperties);

		// createQuestionXMLFile(xmlFile, baseFolder);
		if (questionModType.equals("Add Question")) {
			// String[] questionArray = question.split("\\. ", 2);
			// String questionOrderNum = questionArray[0].trim();
			if (!selectedPerspective.equals("*NEW Perspective")) {
				// if it's not the last question in the perspective; no need to
				// reorder if question is added as the last question
				if (!(Integer.parseInt(questionOrder) == questionList.size() + 1)
						&& reorder) {
					// the addQuestion method will call the reOrder method once;
					// prevents infinite loops from addQuestion to
					// reorderQuestion
					reorder = false;
					reorderQuestions(questionOrder, questionList.size() + 1
							+ "");
				}
			}
		}
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

		newPerspective = perspective;

		HashMap<String, String> currentParameterProperties = new HashMap<String, String>();

		currentParameterProperties.clear();

		String qsKey = questionKey;

		if (qsKey == null) {
			qsKey = createQuestionKey(perspective);
		}

		// check if currentperspective is equal to perspective; if not, change
		// questionkey, get the last order number in perspective and replace the
		// current order

		// also loop through perspective list and see if perspective is new; if
		// new, just add it no need for additional logic;
		// if not new, we have to get the order number and add it as the last
		// question in the existing perspective

		deleteQuestion(currentPerspective, currentQuestionKey,
				currentQuestionOrder, currentQuestion, currentSparql,
				currentLayout, currentQuestionDescription,
				currentParameterDependListVector,
				currentParameterQueryListVector,
				currentParameterOptionListVector);
		addQuestion(perspective, questionKey, questionOrder, question, sparql,
				layout, questionDescription, parameterDependList,
				parameterQueryList, parameterOptionList);

		// check if user has modified the question label
		if (!currentQuestion.equals(question)
				|| !currentQuestionOrder.equals(questionOrder)) {
			// check if the order number changed; if it did then add the new
			// query and loops through all subsequent questions to re-order them
			String currentOrder = currentQuestionOrder;

			String order = questionOrder;

			if (!perspective.equals(currentPerspective)) {
				// change the order here to the total # of questions in
				// currentPerspective
				order = currentNumberofQuestions;
			}

			if (!currentOrder.equals(order) && reorder) {
				logger.info("Question reordering from " + currentOrder + " to "
						+ order);
				reorder = false;
				reorderQuestions(order, currentOrder);
			}
		}
	}
	
	public Boolean deleteQuestion(String perspective, String questionTitle)
	{
		//first have to get the question
		Insight insight = ((AbstractEngine)engine).getInsight2(questionTitle).get(0);
		String questionID = insight.getId();
		String[] questionIDArray = questionID.split(":");
		String questionKey = questionIDArray[2];
		String questionOrder = insight.getOrder();
		String question = questionTitle;
		String sparql = insight.getSparql();
		String layout = insight.getOutput();
		String questionDescription = insight.getDescription();
		Hashtable<String, Vector> paramHash = getParameterVectors(insight.getURI());
		Vector parameterQueryVector = paramHash.get("parameterQueryVector");
		Vector dependVector = paramHash.get("dependVector");
		Vector optionVector = paramHash.get("optionVector");
		this.questionList = new ArrayList<String>();
		String perspectiveURI = getPerspectiveURI(perspective);
		this.questionList.addAll(((AbstractEngine) engine).getOrderedInsightsURI(perspectiveURI));
		return deleteQuestion(perspective, questionKey, questionOrder, question, sparql, layout, questionDescription, dependVector, parameterQueryVector, optionVector);
		
	}
	public Boolean deleteQuestion(String perspective, String questionKey,
			String questionOrder, String question, String sparql,
			String layout, String questionDescription,
			Vector<String> parameterDependList,
			Vector<String> parameterQueryList,
			Vector<String> parameterOptionList) {
		boolean lastQuestion = false;

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
		String perspectivePred = getPerspectivePred(perspective);

		// add the question
		Hashtable paramHash = Utility.getParams(sparql);

		// insight uri
		String qURI = getQURI(perspective, questionKey);
		// perspective to insight relationship
		String qPred = getQPred(perspective, questionKey);
		// engine to insight relationship
		String ePred = getEPred(perspective, questionKey);

		// checks if there is only one question left in the perspective
		if (questionList.size() == 1) {
			lastQuestion = true;
		}

		// if there is only one question left, and the user is deleting it; then
		// delete the perspective as well.
		if (lastQuestion) {
			removePerspective(perspectivePred, perspectiveURI, perspective);
		}

		removeQuestionID(ePred, qURI, perspectiveURI, perspective, qPred,
				questionKey);

		// add all question properties
		removeQuestionProperties(qURI, questionOrder, question, questionKey, sparql, layout, descriptionBaseURI, questionDescription);
		

		Enumeration<String> paramKeys = paramHash.keys();

		removeQuestionParam(paramKeys, perspective, questionKey, qURI,
				parameterProperties);

		if (questionModType.equals("Delete Question")
				|| !perspective.equals(newPerspective)) {
			if (questionOrder == null) {
				String[] questionArray = question.split("\\. ", 2);
				questionOrder = questionArray[0].trim();
			}
			// if it's not the last question in the perspective; no need to
			// reorder if last question is deleted
			if (!(Integer.parseInt(questionOrder) == questionList.size())
					&& reorder) {
				reorder = false;
				reorderQuestions(questionList.size() + "",
						Integer.parseInt(questionOrder) + "");
			}
		}
		return true;
	}

	public Boolean deleteAllFromPersp(String perspective) {
		String pURI = getPerspectiveURI(perspective);
		deleteAllFromPerspective(pURI);
		createQuestionXMLFile();
		return true;
	}

	private String getPerspectiveURI(String perspective) {
		String pURI = "";
		Vector<String> pURIs = ((AbstractEngine) engine).getPerspectivesURI();
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
		else {
			pURI = perspectiveBaseURI + "/" + selectedEngine + ":"
					+ perspective;
		}
		
		return pURI;
	}

	public void deleteAllFromPerspective(String perspectiveURI) {
		logger.info("Deleting all questions from perspective with this URI: "
				+ perspectiveURI);

		Vector questionsVector = ((AbstractEngine) engine)
				.getInsightsURI(perspectiveURI);
		ArrayList<String> questionList2 = new ArrayList<String>();
		String perspective = perspectiveURI.substring(perspectiveURI
				.lastIndexOf(":") + 1);
		String perspectivePred = getPerspectivePred(perspective);

		for (Object question : questionsVector) {
			questionList2.add((String) question);
		}

		for (String question2 : questionList2) {
			Insight in = ((AbstractEngine) engine).getInsight2URI(question2)
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
			Hashtable<String, Vector> paramHash = getParameterVectors(question2);
			Vector parameterQueryVector = paramHash.get("parameterQueryVector");
			Vector dependVector = paramHash.get("dependVector");
			Vector optionVector = paramHash.get("optionVector");
			
			reorder = false;
			deleteQuestion(perspective, questionKey, questionOrder, question,
					sparql, layoutValue, questionDescription, dependVector,
					parameterQueryVector, optionVector);
		}
		// delete perspective from the engine
		removePerspective(perspectivePred, perspectiveURI, perspective);
	}
	
	public Hashtable<String, Vector> getParameterVectors(String questionURI){
		Vector parameterQueryVector = new Vector();
		Vector dependVector = new Vector();
		Vector optionVector = new Vector();

		Vector<SEMOSSParam> paramInfoVector = ((AbstractEngine) engine).getParamsURI(questionURI);
		if (!paramInfoVector.isEmpty()) {
			for (int i = 0; i < paramInfoVector.size(); i++) {
				if (paramInfoVector.get(i).getQuery() != null
						&& !paramInfoVector
								.get(i)
								.getQuery()
								.equals(DIHelper.getInstance().getProperty(
										"TYPE" + "_" + Constants.QUERY))) {
					parameterQueryVector.add(paramInfoVector.get(i)
							.getName()
							+ "_QUERY_-_"
							+ paramInfoVector.get(i).getQuery());
				}

				if (!paramInfoVector.get(i).getDependVars().isEmpty()
						&& !paramInfoVector.get(i).getDependVars().get(0)
								.equals("None")) {
					for (int j = 0; j < paramInfoVector.get(i)
							.getDependVars().size(); j++) {
						dependVector.add(paramInfoVector.get(i).getName()
								+ "_DEPEND_-_"
								+ paramInfoVector.get(i).getDependVars()
										.get(j));
					}
				}

				if (paramInfoVector.get(i).getOptions() != null
						&& !paramInfoVector.get(i).getOptions().isEmpty()) {
					Vector options = paramInfoVector.get(i).getOptions();
					String optionsConcat = "";
					for (int j = 0; j < options.size(); j++) {
						optionsConcat += options.get(j);
						if (j != options.size() - 1) {
							optionsConcat += ";";
						}
					}
					optionVector.add(paramInfoVector.get(i).getType()
							+ "_OPTION_-_" + optionsConcat);
				}
			}
		}
		Hashtable<String, Vector> resHash = new Hashtable<String,Vector>();
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
		Vector<String> questionsVector = ((AbstractEngine) engine)
				.getOrderedInsightsURI(perspectiveURI);
		System.out.println("questions currenlty ordered as " + questionsVector);
		
		reorderPerspective2(questionsVector, orderedInsightIDs);
	}

	public void reorderPerspective2(Vector<String> currentOrderOfInsightsIDs, Vector<String> newOrderOfInsightsIDs) {

		// determine difference in order between orderedInsights and
		// questionsVector so that we only update the questions that need
		// updating
		Hashtable<String, Integer> needsReorder = new Hashtable<String, Integer>();
		for (int i = 0; i < newOrderOfInsightsIDs.size(); i++) {
			String qID = newOrderOfInsightsIDs.get(i);
			if (!qID.equals(Utility.getInstanceName(currentOrderOfInsightsIDs.get(i)))) {
				needsReorder.put(qID, i + 1);
			}
		}

		// now reorder!
		Iterator<String> it = needsReorder.keySet().iterator();
		boolean save = false;
		while (it.hasNext()) {
			save = true;
			String question = it.next();
			String qURI = "http://semoss.org/ontologies/Concept/Insight/"
					+ question;
			Insight in = ((AbstractEngine) engine).getInsight2URI(qURI).get(0);

			System.out.println("Removing order question " + question);
			String questionOrder = in.getOrder();
			String newOrder = needsReorder.get(question) + "";
			reorderQuestion(question, newOrder, questionOrder);

		}
		if (save)
			createQuestionXMLFile();
	}

	private void reorderQuestion(String localID, String newQuestionOrder, String oldQuestionOrder){
		String qURI = "http://semoss.org/ontologies/Concept/Insight/" + localID;

		if (oldQuestionOrder != null) {
			System.out.println("Removing order " + oldQuestionOrder
					+ " from question " + localID);
			insightBaseXML.removeStatement(qURI, orderBaseURI,
					oldQuestionOrder, false);
		}

		System.out.println("Adding order " + newQuestionOrder + " to question "
				+ localID);
		insightBaseXML.addStatement(qURI, orderBaseURI, newQuestionOrder, false);
		
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
}
