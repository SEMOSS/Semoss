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
package prerna.engine.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.google.gson.Gson;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectWrapper;
import prerna.om.Insight;
import prerna.om.SEMOSSParam;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.ISEMOSSAction;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.util.Utility;

public class QuestionAdministrator {

	private static final Logger LOGGER = Logger.getLogger(QuestionAdministrator.class.getName());
	private static final String GET_LAST_INSIGHT_ID = "SELECT DISTINCT ID FROM QUESTION_ID ORDER BY ID DESC";
	private static final String GET_IDS_FOR_PERSPECTIVES = "SELECT DISTINCT QUESTION_ID FROM QUESTION_ID WHERE QUESTION_PERSPECTIVE IN ";
	
	private IEngine engine;
	private IEngine insightEngine;
	
	public QuestionAdministrator(IEngine engine) {
		this.engine = engine;
		this.insightEngine = engine.getInsightDatabase();
	}

	//TODO: need to set up the update method in RDBMSEngine
	//TODO: need to change order to int
	public void addQuestion(
			String insightName,
			String perspective,
			List<DataMakerComponent> comps,
			String layout,
			String order,
			String dataMaker,
			boolean isDbQuery,
			Map<String, String> dataTableAlign,
//			boolean multiInsightQuery,
			List<SEMOSSParam> parameters
			) 
	{
		LOGGER.info("Adding new question with name :::: " + insightName);
		LOGGER.info("Adding new question with perspective :::: " + perspective);
		LOGGER.info("Adding new question with layout :::: " + layout);
		LOGGER.info("Adding new question with order :::: " + order);
		LOGGER.info("Adding new question with dataMaker :::: " + dataMaker);
		LOGGER.info("Adding new question with dataTableAlign :::: " + dataTableAlign);
		//TODO: need to find best way to create question IDs
		/* Current logic:
		 * Find the last insight id (all insight ids based on engine name concatenated with "_*UNIQUE_NUMBER*"
		 * Find the largest number and add one to it
		 * Use that as the new insight id
		 */
		
		//clean up values
		insightName = escapeForSQLStatement(insightName);
		perspective = escapeForSQLStatement(perspective);
//		insightDefinition = escapeForSQLStatement(insightDefinition);
		String insightDefinition = this.generateXMLInsightMakeup(comps);
		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(insightEngine, GET_LAST_INSIGHT_ID);
		String retName = wrapper.getVariables()[0];
		Object lastIdNum = 0;
		if(wrapper.hasNext()){ // need to call hasNext before you call next()
			lastIdNum = wrapper.next().getVar(retName);
		}
		String lastIDNum = ((int)lastIdNum+1) + "";
		String newInsightID = engine.getEngineName() + "_" + lastIDNum;
		
		// readjust the ordering of current insights
		if(order == null || order.isEmpty()) {
			order = calculateDefaultOrdering(perspective) + "";
		} else {
			cleanPerspectiveOrdering(perspective, Integer.parseInt(order));
		}
		
		// insert into table the new record
		StringBuilder insertQueryBuilder = new StringBuilder();
		insertQueryBuilder.append("INSERT INTO QUESTION_ID "
				+ "(ID, QUESTION_ID, QUESTION_NAME, QUESTION_PERSPECTIVE, "
				+ "QUESTION_LAYOUT, QUESTION_ORDER, QUESTION_DATA_MAKER, QUESTION_MAKEUP, "
				+ "QUESTION_IS_DB_QUERY, DATA_TABLE_ALIGN) VALUES (");
		insertQueryBuilder.append(lastIDNum).append(", ");
		insertQueryBuilder.append("'").append(newInsightID).append("', ");
		insertQueryBuilder.append("'").append(insightName).append("', ");
		insertQueryBuilder.append("'").append(perspective).append("', ");
		insertQueryBuilder.append("'").append(layout).append("', ");
		insertQueryBuilder.append(order).append(", ");
		insertQueryBuilder.append("'").append(dataMaker).append("', ");
		insertQueryBuilder.append("'").append(insightDefinition).append("', ");
		insertQueryBuilder.append("'").append(isDbQuery).append("', ");
		if(dataTableAlign != null && !dataTableAlign.isEmpty()) {
			Gson gson = new Gson();
			insertQueryBuilder.append("'").append(gson.toJson(dataTableAlign)).append("')");
		} else {
			insertQueryBuilder.append("'')");
		}
//		insertQueryBuilder.append("'").append(multiInsightQuery).append("')");
		insightEngine.insertData(insertQueryBuilder.toString());

		LOGGER.info("Done adding main part of question... now parameters");
		//now add in parameters
		addParameters(parameters, newInsightID);
		
		insightEngine.commit();

		LOGGER.info("Done adding question");
	}
	
	private int calculateDefaultOrdering(String perspective) {
		Vector<String> currInsightKeys = engine.getInsights(perspective);
		return currInsightKeys.size()+1;
	}
	
	/**
	 * 
	 * @param perspective - perspective that may need reordering
	 * @param order - this is either the order of the insight added or null in which case we will check every insight
	 */
	private void cleanPerspectiveOrdering(String perspective, Integer addedInsightOrder){
		Vector<String> currInsightKeys = engine.getInsights(perspective);
		if(!currInsightKeys.isEmpty()) { // not a current perspective
			if ( addedInsightOrder != null ){ // if we know the added insight order this is very simple. for each insight after the added one, increase its order by 1
				Vector<Insight> currInsights = engine.getInsight(currInsightKeys.toArray(new String[currInsightKeys.size()]));
					for(Insight in : currInsights) {
						int currInOrder = Integer.parseInt(in.getOrder());
						if(addedInsightOrder <= currInOrder) {
							int newOrder = currInOrder++;
							setInsightOrder(newOrder, in.getInsightID());
						}
					}
				}
			else{
				reorderPerspective(perspective, currInsightKeys);
			}
		}
	}
	
	//TODO: need to set up the update method in RDBMSEngine
	public void modifyQuestion(
			String insightID, 
			String insightName,
			String perspective,
			List<DataMakerComponent> comps,
			String layout,
			String order,
			String dataMaker,
			boolean isDbQuery,
			Map<String, String> dataTableAlign,
//			boolean multiInsightQuery,
			List<SEMOSSParam> parameters
			) 
	{
		Gson gson = new Gson();
		
		Insight currInsightInfo = engine.getInsight(insightID).get(0);
		String currInsightName = currInsightInfo.getInsightName();
		String currPerspective = currInsightInfo.getPerspective();
		List<DataMakerComponent> curComps = currInsightInfo.getDataMakerComponents();
		String currLayout = currInsightInfo.getOutput();
		String currOrder = currInsightInfo.getOrder();
		boolean currIsDbQuery = currInsightInfo.isDbQuery();
		Map<String, String> currDataTableAlign = currInsightInfo.getDataTableAlign();
		String currDataTableAlignStr = gson.toJson(currDataTableAlign);
//		boolean currMultiInsightQuery = currInsightInfo.isMultiInsightQuery();
		
		boolean orderChange = false;
		boolean perspectiveChange = false;
		
		String query = "UPDATE QUESTION_ID SET ";
		if(insightName != null && !insightName.equals(currInsightName)) {
			query += "QUESTION_NAME='"+ insightName + "', ";
		}
		if(perspective != null && !perspective.equals(currPerspective)) {
			query += "QUESTION_PERSPECTIVE='" + perspective + "', ";
			perspectiveChange = true;
		}
		if(!comps.equals(curComps)) {
			query += "QUESTION_MAKEUP='" + this.generateXMLInsightMakeup(comps) + "', ";
		}
		if(layout != null && !layout.equals(currLayout)) {
			query += "QUESTION_LAYOUT='" + layout + "', ";
		}
//		if(multiInsightQuery != currMultiInsightQuery) {
//			query += "MULTIPLE_QUERIES='" + multiInsightQuery + "', ";
//			insightEngine.insertData(query);
//		}
		if(isDbQuery != currIsDbQuery) {
			query += "QUESTION_IS_DB_QUERY='" + isDbQuery + "', ";
		}
		String dataTableAlignStr = gson.toJson(dataTableAlign);
		if(!dataTableAlignStr.equals(currDataTableAlignStr)) {
			query += "DATA_TABLE_ALIGN='" + dataTableAlignStr + "', ";
		}
		if(order != null && !order.equals(currOrder)) {
			orderChange = true;
			query += "QUESTION_ORDER=" + order + ", ";
		}
		query = query.substring(0, query.length() - 2);
		query = query + " WHERE QUESTION_ID='" + insightID + "'"; 
		
		// modify order for questions in perspective question is being set to
		if(orderChange) {
			cleanPerspectiveOrdering(perspective, null);
		}
		// modify order for previous perspective
		if(perspectiveChange) {
			cleanPerspectiveOrdering(currPerspective, null);
		}
		
		//finally update the actual question
		insightEngine.insertData(query);
		
		//TODO: need to figure out a better way than to just delete the parameters and readd
		deleteParameter(insightID);
		addParameters(parameters, insightID);
		
		insightEngine.commit();
	}
	
	//TODO: What about reorder???????????????????????????????????????
	public void removeQuestion(String... insightIDs) {		
		deleteInsight(insightIDs);
		deleteParameter(insightIDs);
		
		insightEngine.commit();
	}
	
	public void reorderPerspective(String perspective, List<String> orderedInsightIds){
		LOGGER.info("Reording perspective  "+ perspective + " to " + orderedInsightIds.toString());
		
		for(int idx = 0; idx < orderedInsightIds.size(); idx++){
			String id = orderedInsightIds.get(idx);
			setInsightOrder(idx, id);
		}
	}
	
	public void removePerspective(String... perspectives){
		String perspectivesString = createString(perspectives);
		List<String> questionIds = getQuestionIds(perspectivesString);
		removeQuestion(questionIds.toArray(new String[questionIds.size()]));
	}
	
	private List<String> getQuestionIds(String perspectivesString){
		String query = GET_IDS_FOR_PERSPECTIVES + perspectivesString;
		return Utility.getVectorOfReturn(query, insightEngine, false);
	}
	
	private void deleteInsight(String... insightIDs) {
		String idsString = createString(insightIDs);
		String questionQuery = "DELETE FROM QUESTION_ID WHERE QUESTION_ID IN " + idsString;
		LOGGER.info("running remove query :::: " + questionQuery);
		insightEngine.removeData(questionQuery);
	}
	
	private void deleteParameter(String... insightIDs) {
		String idsString = createString(insightIDs);
		String parameterQuery = "DELETE FROM PARAMETER_ID WHERE QUESTION_ID_FK IN " + idsString ;
		LOGGER.info("running remove query :::: " + parameterQuery);
		insightEngine.removeData(parameterQuery);
	}
	
	private String removeTrailingZeros(String s) {
		return s.replaceAll("\\.0*$", "");
	}
	
	private void addParameters(List<SEMOSSParam> parameters, String insightID) {
		if(parameters != null) {
			LOGGER.info("Beginning to add parameters for insight ");
			for(SEMOSSParam param : parameters) {
				LOGGER.info("Adding parameter with details " + param.toString());
				String paramID = insightID.concat("_").concat(param.getName());
				String paramLabel = param.getName();
				String paramType = param.getType();
				String paramCompFilterId = param.getComponentFilterId();
				boolean isDbQuery = param.isDbQuery();
				boolean isMultiSelect = param.isMultiSelect();
				if(paramType == null) {
					paramType = "";
				}
				
				Vector<String> paramDependencyArr = param.getDependVars();
				StringBuilder paramDependencyBuilder = new StringBuilder("");
				if(paramDependencyArr != null) {
					for(String dependency : paramDependencyArr) {
						String dependencyKey = insightID.concat("_").concat(dependency);
						paramDependencyBuilder.append(dependencyKey).append(";");
					}
				}
				String paramDependency = paramDependencyBuilder.toString();
				LOGGER.info("Parameter depends on :::: " + paramDependency);
				
				String paramQuery = param.getQuery();
				LOGGER.info("Parameter has query :::: " + paramQuery);
				if(paramQuery == null) {
					paramQuery = "";
				} else {
					paramQuery = escapeForSQLStatement(paramQuery);
				}
				
				Vector<String> paramOptionsArr = param.getOptions();
				StringBuilder paramOptionsBuilder = new StringBuilder("");
				if(paramOptionsArr != null) {
					for(String option : paramOptionsArr) {
						paramOptionsBuilder.append(option).append(";");
					}
				}
				String paramOptions = paramOptionsBuilder.toString();
				LOGGER.info("Parameter has options :::: " + paramOptions);
				if(!paramOptions.isEmpty()) {
					paramOptions = escapeForSQLStatement(paramOptions);
				}
				
				StringBuilder paramInsertQueryBuilder = new StringBuilder();
				paramInsertQueryBuilder.append("INSERT INTO PARAMETER_ID "
						+ "(PARAMETER_ID, PARAMETER_LABEL, PARAMETER_TYPE, PARAMETER_DEPENDENCY, "
						+ "PARAMETER_QUERY, PARAMETER_OPTIONS, PARAMETER_IS_DB_QUERY, PARAMETER_MULTI_SELECT, "
						+ "PARAMETER_COMPONENT_FILTER_ID, QUESTION_ID_FK) VALUES('");
				paramInsertQueryBuilder.append(paramID).append("', ");
				paramInsertQueryBuilder.append("'").append(paramLabel).append("', ");
				paramInsertQueryBuilder.append("'").append(paramType).append("', ");
				paramInsertQueryBuilder.append("'").append(paramDependency).append("', ");
				paramInsertQueryBuilder.append("'").append(paramQuery).append("', ");
				paramInsertQueryBuilder.append("'").append(paramOptions).append("', ");
				paramInsertQueryBuilder.append("'").append(isDbQuery).append("', ");
				paramInsertQueryBuilder.append("'").append(isMultiSelect).append("', ");
				paramInsertQueryBuilder.append("'").append(paramCompFilterId).append("', ");
				paramInsertQueryBuilder.append("'").append(insightID).append("')");
				insightEngine.insertData(paramInsertQueryBuilder.toString());
			}
		}
		LOGGER.info("Done adding parameters");
	}
	
	protected String generateXMLInsightMakeup(List<DataMakerComponent> dmcList) {
		LOGGER.info("Generating NTriples for insight makeup");
		StringBuilder builder = new StringBuilder();
		Set<String> engineSet = new HashSet<String>();
		Gson gson = new Gson();
		
		// need to keep track of total to ensure unique concepts
		int numPreTransformations = 0;
		int numPostTransformations = 0;
		int numAction = 0;
		
		// create engine concept
		builder.append("<http://semoss.org/ontologies/Concept/Engine> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> .\n");
		// create component concept
		builder.append("<http://semoss.org/ontologies/Concept/Component> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> .\n");
		// create pre-transformation concept
		builder.append("<http://semoss.org/ontologies/Concept/PreTransformation> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> .\n");
		// create post-transformation concept
		builder.append("<http://semoss.org/ontologies/Concept/PostTransformation> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> .\n");
		// type is a type of contains - for transformations
		builder.append("<http://semoss.org/ontologies/Relation/Contains/Type>  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> .\n");
		// propMap is a type of contains - for transformations
		builder.append("<http://semoss.org/ontologies/Relation/Contains/propMap>  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> .\n");
		
		for(int i = 0; i < dmcList.size(); i++) {
			LOGGER.info("Creating nTriples for compoenent:::: " + i);
			// create component based on number "i"
			builder.append("<http://semoss.org/ontologies/Concept/Component/" + i + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Component> .\n");
			// add order to component
			builder.append("<http://semoss.org/ontologies/Concept/Component/" + i + "> <http://semoss.org/ontologies/Relation/Contains/Order> \"" + i + "\"^^<http://www.w3.org/2001/XMLSchema#int> .\n");

//			Map<String, Object> compMap = insightMakeup.get(i);
			DataMakerComponent dmc = dmcList.get(i);
			String engineName = dmc.getEngine().getEngineName();
			String realEngineName = escapeForSQLStatement(engineName);
			String cleanEngineName = Utility.cleanString(engineName, true);
			LOGGER.info("Component " + i + " has engine name::: " + cleanEngineName);
			// create engine and add to component 
			if(!engineSet.contains(cleanEngineName)) {
				engineSet.add(cleanEngineName);
				builder.append("<http://semoss.org/ontologies/Concept/Engine/" + cleanEngineName + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine> .\n");
				builder.append("<http://semoss.org/ontologies/Concept/Engine/" + cleanEngineName + "> <http://semoss.org/ontologies/Relation/Contains/Name> \"" + realEngineName + "\" .\n");
			}
			builder.append("<http://semoss.org/ontologies/Concept/Component/" + i + "> <Comp:Eng> <http://semoss.org/ontologies/Concept/Engine/" + cleanEngineName + "> .\n");

			String query = dmc.getQuery();
			// add query property to component if query is not null
			
			if(query == null) {
				Map<String, Object> metamodel = dmc.getMetamodelData();
				String jsonMetamodel = gson.toJson(metamodel);
				LOGGER.info("Component " + i + " does NOT have query... instead saving metamodel::: " + jsonMetamodel);
				jsonMetamodel = escapeForNTripleAndSQLStatement(jsonMetamodel);
				builder.append("<http://semoss.org/ontologies/Concept/Component/" + i + "> <http://semoss.org/ontologies/Relation/Contains/Metamodel> \"" + jsonMetamodel + "\" .\n");
			} else {
				LOGGER.info("Component " + i + " has query::: " + query);
				query = escapeForNTripleAndSQLStatement(query);
				builder.append("<http://semoss.org/ontologies/Concept/Component/" + i + "> <http://semoss.org/ontologies/Relation/Contains/Query> \"" + query + "\" .\n");
			}

			List<ISEMOSSTransformation> preTransformationList = dmc.getPreTrans();
			if(preTransformationList != null && !preTransformationList.isEmpty()) {
				LOGGER.info("Component " + i + " has pre-transformations!!");
				int j = 0;
				for(; j < preTransformationList.size(); j++) {
					LOGGER.info("Component " + i + " .... building pre-transformation " + j);
					ISEMOSSTransformation preTrans = preTransformationList.get(j);
					// add transformation based on "j"
					builder.append("<http://semoss.org/ontologies/Concept/PreTransformation/" + (j+numPreTransformations) + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/PreTransformation> .\n");
					builder.append("<http://semoss.org/ontologies/Concept/PreTransformation/" + (j+numPreTransformations) + "> <http://semoss.org/ontologies/Relation/Contains/Order> \"" + j + "\"^^<http://www.w3.org/2001/XMLSchema#int> .\n");

					// connection transformation to component
					builder.append("<http://semoss.org/ontologies/Concept/Component/" + i + "> <Comp:PreTrans> <http://semoss.org/ontologies/Concept/PreTransformation/" + (j+numPreTransformations) + "> .\n");
					
					// add parameters for transformation
					Map<String, Object> paramMap =  (Map<String, Object>) preTrans.getProperties();
					String paramStringify = gson.toJson(paramMap);
					paramStringify = escapeForNTripleAndSQLStatement(paramStringify);
					builder.append("<http://semoss.org/ontologies/Concept/PreTransformation/" + (j+numPreTransformations) + "> <http://semoss.org/ontologies/Relation/Contains/propMap> \"" + paramStringify + "\" .\n");
				}
				numPreTransformations+=j;
			}
			List<ISEMOSSTransformation> postTransformationList = dmc.getPostTrans();
			if(postTransformationList != null && !postTransformationList.isEmpty()) {
				LOGGER.info("Component " + i + " has post-transformations!!");
				int j = 0;
				for(; j < postTransformationList.size(); j++) {
					LOGGER.info("Component " + i + " .... building post-transformation " + j);
					ISEMOSSTransformation postTrans = postTransformationList.get(j);
					// add transformation based on "j"
					builder.append("<http://semoss.org/ontologies/Concept/PostTransformation/" + (j+numPostTransformations) + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/PostTransformation> .\n");
					builder.append("<http://semoss.org/ontologies/Concept/PostTransformation/" + (j+numPostTransformations) + "> <http://semoss.org/ontologies/Relation/Contains/Order> \"" + j + "\"^^<http://www.w3.org/2001/XMLSchema#int> .\n");

					// connection transformation to component
					builder.append("<http://semoss.org/ontologies/Concept/Component/" + i + "> <Comp:PostTrans> <http://semoss.org/ontologies/Concept/PostTransformation/" + (j+numPostTransformations) + "> .\n");
					
					// add parameters for transformation
					Map<String, Object> paramMap =  (Map<String, Object>) postTrans.getProperties();
					String paramStringify = gson.toJson(paramMap);
					paramStringify = escapeForNTripleAndSQLStatement(paramStringify);
					builder.append("<http://semoss.org/ontologies/Concept/PostTransformation/" + (j+numPostTransformations) + "> <http://semoss.org/ontologies/Relation/Contains/propMap> \"" + paramStringify + "\" .\n");
				}
				numPostTransformations+=j;
			}
			List<ISEMOSSAction> actionList = dmc.getActions();
			if(actionList != null && !actionList.isEmpty()) {
				LOGGER.info("Component " + i + " has actions!!");
				int j = 0;
				for(; j < actionList.size(); j++) {
					LOGGER.info("Component " + i + " .... building action " + j);
					ISEMOSSAction action = actionList.get(j);
					// add transformation based on "j"
					builder.append("<http://semoss.org/ontologies/Concept/Action/" + (j+numAction) + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Action> .\n");
					builder.append("<http://semoss.org/ontologies/Concept/Action/" + (j+numAction) + "> <http://semoss.org/ontologies/Relation/Contains/Order> \"" + j + "\"^^<http://www.w3.org/2001/XMLSchema#int> .\n");

					// connection transformation to component
					builder.append("<http://semoss.org/ontologies/Concept/Component/" + i + "> <Comp:Action> <http://semoss.org/ontologies/Concept/Action/" + (j+numAction) + "> .\n");
					
					// add parameters for transformation
					Map<String, Object> paramMap =  (Map<String, Object>) action.getProperties();
					String paramStringify = gson.toJson(paramMap);
					paramStringify = escapeForNTripleAndSQLStatement(paramStringify);
					builder.append("<http://semoss.org/ontologies/Concept/Action/" + (j+numAction) + "> <http://semoss.org/ontologies/Relation/Contains/propMap> \"" + paramStringify + "\" .\n");
				}
				numAction+=j;
			}
		}
		String clob = builder.toString();
		LOGGER.info("Done building NTRIPLES");
		LOGGER.info("CLOB to save is :: " + clob);
		
		return clob;
	}
	
	private void setInsightOrder(int order, String insightId){
		StringBuilder updateQueryBuilder = new StringBuilder();
		updateQueryBuilder.append("UPDATE QUESTION_ID SET QUESTION_ORDER=");
		updateQueryBuilder.append(order);
		updateQueryBuilder.append(" WHERE QUESTION_ID='");
		updateQueryBuilder.append(insightId);
		updateQueryBuilder.append("'");
		insightEngine.insertData(updateQueryBuilder.toString());
	}
	
	private String createString(String... ids){
		String idsString = "(";
		for(String id : ids){
			idsString = idsString + "'" + id + "', ";
		}
		idsString = idsString.substring(0, idsString.length() - 2) + ")";
		LOGGER.info("IDs string :::: " + idsString);
		
		return idsString;
	}
	
	private String escapeForSQLStatement(String s) {
		return s.replaceAll("'", "''");
	}
	
	private String escapeForNTripleAndSQLStatement(String s) {
		s = escapeForSQLStatement(s);
		return s.replaceAll("\"", "\\\\\"");
	}
}
