///*******************************************************************************
// * Copyright 2015 Defense Health Agency (DHA)
// *
// * If your use of this software does not include any GPLv2 components:
// * 	Licensed under the Apache License, Version 2.0 (the "License");
// * 	you may not use this file except in compliance with the License.
// * 	You may obtain a copy of the License at
// *
// * 	  http://www.apache.org/licenses/LICENSE-2.0
// *
// * 	Unless required by applicable law or agreed to in writing, software
// * 	distributed under the License is distributed on an "AS IS" BASIS,
// * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * 	See the License for the specific language governing permissions and
// * 	limitations under the License.
// * ----------------------------------------------------------------------------
// * If your use of this software includes any GPLv2 components:
// * 	This program is free software; you can redistribute it and/or
// * 	modify it under the terms of the GNU General Public License
// * 	as published by the Free Software Foundation; either version 2
// * 	of the License, or (at your option) any later version.
// *
// * 	This program is distributed in the hope that it will be useful,
// * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
// * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * 	GNU General Public License for more details.
// *******************************************************************************/
//
//package prerna.engine.impl;
//
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Hashtable;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.Vector;
//
//import org.apache.log4j.Logger;
//
//import com.google.gson.Gson;
//
//import prerna.ds.QueryStruct;
//import prerna.engine.api.IEngine;
//import prerna.engine.api.ISelectWrapper;
//import prerna.om.Insight;
//import prerna.om.OldInsight;
//import prerna.om.SEMOSSParam;
//import prerna.rdf.engine.wrappers.WrapperManager;
//import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
//import prerna.ui.components.playsheets.datamakers.FilterTransformation;
//import prerna.ui.components.playsheets.datamakers.ISEMOSSAction;
//import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
//import prerna.ui.components.playsheets.datamakers.JoinTransformation;
//import prerna.util.Utility;
//
//public class QuestionAdministrator {
//
//	private static final Logger logger = Logger.getLogger(QuestionAdministrator.class);
//	private static final String GET_LAST_INSIGHT_ID = "SELECT DISTINCT ID FROM QUESTION_ID ORDER BY ID DESC";
//	private static final String GET_IDS_FOR_PERSPECTIVES = "SELECT DISTINCT ID FROM QUESTION_ID WHERE QUESTION_PERSPECTIVE IN ";
//	
//	private IEngine engine;
//	private IEngine insightEngine;
//	
//	public QuestionAdministrator(IEngine engine) {
//		this.engine = engine;
//		this.insightEngine = engine.getInsightDatabase();
//	}
//
//	//TODO: need to change order to int
//	public String addQuestion(
//			String insightName,
//			String perspective,
//			List<DataMakerComponent> comps,
//			String layout,
//			String order,
//			String dataMaker,
//			boolean isDbQuery,
//			Map<String, String> dataTableAlign,
//			List<SEMOSSParam> parameters,
//			String uiOptions
//			) 
//	{
//		logger.info("Adding new question with name :::: " + Utility.cleanLogString(insightName));
//		logger.info("Adding new question with perspective :::: " + Utility.cleanLogString(perspective));
//		logger.info("Adding new question with layout :::: " + Utility.cleanLogString(layout));
//		logger.info("Adding new question with order :::: " + Utility.cleanLogString(order));
//		logger.info("Adding new question with dataMaker :::: " + Utility.cleanLogString(dataMaker));
//		logger.info("Adding new question with dataTableAlign :::: " + dataTableAlign);
//		//TODO: need to find best way to create question IDs
//		/* Current logic:
//		 * Find the last insight id (all insight ids based on engine name concatenated with "_*UNIQUE_NUMBER*"
//		 * Find the largest number and add one to it
//		 * Use that as the new insight id
//		 */
//		
//		//clean up values
//		insightName = escapeForSQLStatement(insightName);
//		perspective = escapeForSQLStatement(perspective);
////		insightDefinition = escapeForSQLStatement(insightDefinition);
//		String insightDefinition = this.generateXMLInsightMakeup(comps, parameters);
//		
//		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(insightEngine, GET_LAST_INSIGHT_ID);
//		String retName = wrapper.getVariables()[0];
//		Object lastIdNum = 0;
//		if(wrapper.hasNext()){ // need to call hasNext before you call next()
//			lastIdNum = wrapper.next().getVar(retName);
//		}
//		String lastIDNum = ((int)lastIdNum+1) + "";
//		
//		// readjust the ordering of current insights
//		if(order == null || order.isEmpty()) {
//			order = calculateDefaultOrdering(perspective) + "";
//		} else {
//			// cast as double and then get integer value due to examples when string comes back as "5.0"
//			cleanPerspectiveOrdering(perspective, (int) Double.parseDouble(order));
//		}
//		
//		// insert into table the new record
//		StringBuilder insertQueryBuilder = new StringBuilder();
//		insertQueryBuilder.append("INSERT INTO QUESTION_ID "
//				+ "(ID, QUESTION_NAME, QUESTION_PERSPECTIVE, "
//				+ "QUESTION_LAYOUT, QUESTION_ORDER, QUESTION_DATA_MAKER, QUESTION_MAKEUP, "
//				+ "QUESTION_IS_DB_QUERY, DATA_TABLE_ALIGN) VALUES (");
//		insertQueryBuilder.append(lastIDNum).append(", ");
//		insertQueryBuilder.append("'").append(insightName).append("', ");
//		insertQueryBuilder.append("'").append(perspective).append("', ");
//		insertQueryBuilder.append("'").append(layout).append("', ");
//		insertQueryBuilder.append(order).append(", ");
//		insertQueryBuilder.append("'").append(dataMaker).append("', ");
//		insertQueryBuilder.append("'").append(insightDefinition).append("', ");
//		insertQueryBuilder.append("'").append(isDbQuery).append("', ");
//		if(dataTableAlign != null && !dataTableAlign.isEmpty()) {
//			Gson gson = new Gson();
//			insertQueryBuilder.append("'").append(gson.toJson(dataTableAlign)).append("')");
//		} else {
//			insertQueryBuilder.append("'')");
//		}
//		try {
//			insightEngine.insertData(insertQueryBuilder.toString());
//		} catch (Exception e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//
//		logger.info("Done adding main part of question... now parameters");
//		//now add in parameters
//		addParameters(parameters, lastIDNum);
//		//now add in ui options
//		addUiOptions(uiOptions, lastIDNum);
//		
//		insightEngine.commit();
//
//		logger.info("Done adding question");
//		
//		return lastIDNum;
//	}
//	
//	private int calculateDefaultOrdering(String perspective) {
//		Vector<String> currInsightKeys = engine.getInsights(perspective);
//		return currInsightKeys.size()+1;
//	}
//	
//	/**
//	 * 
//	 * @param perspective - perspective that may need reordering
//	 * @param order - this is either the order of the insight added or null in which case we will check every insight
//	 */
//	private void cleanPerspectiveOrdering(String perspective, Integer addedInsightOrder){
//		Vector<String> currInsightKeys = engine.getInsights(perspective);
//		if(!currInsightKeys.isEmpty()) { // not a current perspective
//			if ( addedInsightOrder != null ){ // if we know the added insight order this is very simple. for each insight after the added one, increase its order by 1
//				Vector<Insight> currInsights = engine.getInsight(currInsightKeys.toArray(new String[currInsightKeys.size()]));
//					for(Insight in : currInsights) {
//						int currInOrder = Integer.parseInt(in.getOrder());
//						if(addedInsightOrder <= currInOrder) {
//							int newOrder = currInOrder++;
//							setInsightOrder(newOrder, in.getInsightId());
//						}
//					}
//				}
//			else{
//				reorderPerspective(perspective, currInsightKeys);
//			}
//		}
//	}
//	
//	public void modifyQuestion(
//			String insightID, 
//			String insightName,
//			String perspective,
//			List<DataMakerComponent> comps,
//			String layout,
//			String order,
//			String dataMaker,
//			boolean isDbQuery,
//			Map<String, String> dataTableAlign,
//			List<SEMOSSParam> parameters,
//			String uiOptions
//			) 
//	{
//		Gson gson = new Gson();
//		
//		Insight currInsightInfo = engine.getInsight(insightID).get(0);
//		String currInsightName = currInsightInfo.getInsightName();
////		String currPerspective = currInsightInfo.getPerspective();
////		List<DataMakerComponent> currComps = currInsightInfo.getDataMakerComponents();
////		String currLayout = currInsightInfo.getOutput();
//		String currOrder = currInsightInfo.getOrder();
////		boolean currIsDbQuery = currInsightInfo.isDbQuery();
////		Map<String, String> currDataTableAlign = currInsightInfo.getDataTableAlign();
////		String currDataTableAlignStr = gson.toJson(currDataTableAlign);
//		
//		boolean orderChange = false;
//		boolean perspectiveChange = false;
//		
//		String query = "UPDATE QUESTION_ID SET ";
//		if(insightName != null && !insightName.equals(currInsightName)) {
//			query += "QUESTION_NAME='"+ insightName + "', ";
//		}
////		if(perspective != null && !perspective.equals(currPerspective)) {
////			query += "QUESTION_PERSPECTIVE='" + perspective + "', ";
////			perspectiveChange = true;
////		}
////		if(!comps.equals(currComps) || parameters.equals(currParams)) {
//			query += "QUESTION_MAKEUP='" + this.generateXMLInsightMakeup(comps, parameters) + "', ";
////		}
////		if(layout != null && !layout.equals(currLayout)) {
////			query += "QUESTION_LAYOUT='" + layout + "', ";
////		}
////		if(multiInsightQuery != currMultiInsightQuery) {
////			query += "MULTIPLE_QUERIES='" + multiInsightQuery + "', ";
////			insightEngine.insertData(query);
////		}
////		if(isDbQuery != currIsDbQuery) {
////			query += "QUESTION_IS_DB_QUERY='" + isDbQuery + "', ";
////		}
//		String dataTableAlignStr = gson.toJson(dataTableAlign);
////		if(!dataTableAlignStr.equals(currDataTableAlignStr)) {
////			query += "DATA_TABLE_ALIGN='" + dataTableAlignStr + "', ";
////		}
//		if(order != null && !order.equals(currOrder)) {
//			orderChange = true;
//			query += "QUESTION_ORDER=" + order + ", ";
//		}
//		query = query.substring(0, query.length() - 2);
//		query = query + " WHERE ID='" + insightID + "'"; 
//		
//		// modify order for questions in perspective question is being set to
//		if(orderChange) {
//			cleanPerspectiveOrdering(perspective, null);
//		}
//		// modify order for previous perspective
////		if(perspectiveChange) {
////			cleanPerspectiveOrdering(currPerspective, null);
////		}
//		
//		//finally update the actual question
//		try {
//			insightEngine.insertData(query);
//		} catch (Exception e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//		
//		//TODO: need to figure out a better way than to just delete the parameters and readd
//		deleteParameter(insightID);
//		addParameters(parameters, insightID);
//		
//		//TODO: need to figure out a better way than to just delete the parameters and readd
//		deleteUiOptions(insightID);
//		addUiOptions(uiOptions, insightID);
//
//		insightEngine.commit();
//	}
//	
//	//TODO: What about reorder???????????????????????????????????????
//	public void removeQuestion(String... insightIDs) {		
//		deleteInsight(insightIDs);
//		deleteParameter(insightIDs);
//		deleteUiOptions(insightIDs);
//		insightEngine.commit();
//	}
//	
//	public void reorderPerspective(String perspective, List<String> orderedInsightIds){
//		logger.info("Reording perspective  "+ perspective + " to " + orderedInsightIds.toString());
//		
//		for(int idx = 0; idx < orderedInsightIds.size(); idx++){
//			String id = orderedInsightIds.get(idx);
//			setInsightOrder(idx, id);
//		}
//	}
//	
//	public List<String> removePerspective(String... perspectives){
//		String perspectivesString = createString(perspectives);
//		List<String> questionIds = getQuestionIds(perspectivesString);
//		removeQuestion(questionIds.toArray(new String[questionIds.size()]));
//		
//		return questionIds;
//	}
//	
//	private List<String> getQuestionIds(String perspectivesString){
//		String query = GET_IDS_FOR_PERSPECTIVES + perspectivesString;
//		return Utility.getVectorOfReturn(query, insightEngine, false);
//	}
//	
//	private void deleteInsight(String... insightIDs) {
//		String idsString = createString(insightIDs);
//		String questionQuery = "DELETE FROM QUESTION_ID WHERE ID IN " + idsString;
//		logger.info("running remove query :::: " + questionQuery);
//		try {
//			insightEngine.removeData(questionQuery);
//		} catch (Exception e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//	}
//	
//	private void deleteParameter(String... insightIDs) {
//		String idsString = createString(insightIDs);
//		String parameterQuery = "DELETE FROM PARAMETER_ID WHERE QUESTION_ID_FK IN " + idsString ;
//		logger.info("running remove query :::: " + parameterQuery);
//		try {
//			insightEngine.removeData(parameterQuery);
//		} catch (Exception e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//	}
//	
//	private void deleteUiOptions(String... insightIDs) {
//		String idsString = createString(insightIDs);
//		String parameterQuery = "DELETE FROM UI WHERE QUESTION_ID_FK IN " + idsString ;
//		logger.info("running remove query :::: " + parameterQuery);
//		try {
//			insightEngine.removeData(parameterQuery);
//		} catch (Exception e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//	}
//	
////	private String removeTrailingZeros(String s) {
////		return s.replaceAll("\\.0*$", "");
////	}
//	
//	private void addUiOptions(String uiOptions, String questionID) {
//		if(uiOptions != null && !uiOptions.isEmpty()) {
//			StringBuilder insertQueryBuilder = new StringBuilder();
//			insertQueryBuilder.append("INSERT INTO UI (QUESTION_ID_FK, UI_DATA) VALUES(");
//			insertQueryBuilder.append(questionID).append(", '");
//			insertQueryBuilder.append(escapeForSQLStatement(uiOptions)).append("')");
//			try {
//				insightEngine.insertData(insertQueryBuilder.toString());
//			} catch (Exception e) {
//				classLogger.error(Constants.STACKTRACE, e);
//			}
//		}
//	}
//	
//	private void addParameters(List<SEMOSSParam> parameters, String insightID) {
//		if(parameters != null) {
//			logger.info("Beginning to add parameters for insight ");
//			for(SEMOSSParam param : parameters) {
//				logger.info("Adding parameter with details " + Utility.cleanLogString(param.toString()));
//				String paramID = "";
//				if(!param.getName().startsWith(insightID.concat("_")))
//					paramID = insightID.concat("_").concat(param.getName());
//				else
//					paramID = param.getName();
//				String paramLabel = param.getName();
//				String paramType = param.getType();
//				String paramCompFilterId = param.getComponentFilterId();
//				boolean isDbQuery = param.isDbQuery();
//				boolean isMultiSelect = param.isMultiSelect();
//				if(paramType == null) {
//					paramType = "";
//				}
//				
//				Vector<String> paramDependencyArr = param.getDependVars();
//				StringBuilder paramDependencyBuilder = new StringBuilder("");
//				if(paramDependencyArr != null) {
//					for(String dependency : paramDependencyArr) {
//						String dependencyKey = "";
//						if(!dependency.startsWith(insightID.concat("_")))
//						dependencyKey = insightID.concat("_").concat(dependency);
//						else
//							dependencyKey = dependency;
//						paramDependencyBuilder.append(dependencyKey).append(";");
//					}
//				}
//				String paramDependency = paramDependencyBuilder.toString();
//				logger.info("Parameter depends on :::: " + Utility.cleanLogString(paramDependency));
//				
//				String paramQuery = param.getQuery();
//				logger.info("Parameter has query :::: " + paramQuery);
//				if(paramQuery == null) {
//					paramQuery = "";
//				} else {
//					paramQuery = escapeForSQLStatement(paramQuery);
//				}
//				
//				Vector<String> paramOptionsArr = param.getOptions();
//				StringBuilder paramOptionsBuilder = new StringBuilder("");
//				if(paramOptionsArr != null) {
//					for(String option : paramOptionsArr) {
//						paramOptionsBuilder.append(option).append(";");
//					}
//				}
//				String paramOptions = paramOptionsBuilder.toString();
//				logger.info("Parameter has options :::: " + Utility.cleanLogString(paramOptions));
//				if(!paramOptions.isEmpty()) {
//					paramOptions = escapeForSQLStatement(paramOptions);
//				}
//				
//				StringBuilder paramInsertQueryBuilder = new StringBuilder();
//				paramInsertQueryBuilder.append("INSERT INTO PARAMETER_ID "
//						+ "(PARAMETER_ID, PARAMETER_LABEL, PARAMETER_TYPE, PARAMETER_DEPENDENCY, "
//						+ "PARAMETER_QUERY, PARAMETER_OPTIONS, PARAMETER_IS_DB_QUERY, PARAMETER_MULTI_SELECT, "
//						+ "PARAMETER_COMPONENT_FILTER_ID, QUESTION_ID_FK) VALUES('");
//				paramInsertQueryBuilder.append(paramID).append("', ");
//				paramInsertQueryBuilder.append("'").append(paramLabel).append("', ");
//				paramInsertQueryBuilder.append("'").append(paramType).append("', ");
//				paramInsertQueryBuilder.append("'").append(paramDependency).append("', ");
//				paramInsertQueryBuilder.append("'").append(paramQuery).append("', ");
//				paramInsertQueryBuilder.append("'").append(paramOptions).append("', ");
//				paramInsertQueryBuilder.append("'").append(isDbQuery).append("', ");
//				paramInsertQueryBuilder.append("'").append(isMultiSelect).append("', ");
//				paramInsertQueryBuilder.append("'").append(paramCompFilterId).append("', ");
//				paramInsertQueryBuilder.append("'").append(insightID).append("')");
//				try {
//					insightEngine.insertData(paramInsertQueryBuilder.toString());
//				} catch (Exception e) {
//					classLogger.error(Constants.STACKTRACE, e);
//				}
//			}
//		}
//		logger.info("Done adding parameters");
//	}
//	
//	protected String generateXMLInsightMakeup(List<DataMakerComponent> dmcList, List<SEMOSSParam> parameters) {
//		logger.info("Generating NTriples for insight makeup");
//		StringBuilder builder = new StringBuilder();
//		Set<String> engineSet = new HashSet<String>();
//		Gson gson = new Gson();
//		
//		// need to keep track of total to ensure unique concepts
//		int numPreTransformations = 0;
//		int numPostTransformations = 0;
//		int numAction = 0;
//		
//		// create engine concept
//		builder.append("<http://semoss.org/ontologies/Concept/Engine> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> .\n");
//		// create component concept
//		builder.append("<http://semoss.org/ontologies/Concept/Component> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> .\n");
//		// create pre-transformation concept
//		builder.append("<http://semoss.org/ontologies/Concept/PreTransformation> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> .\n");
//		// create post-transformation concept
//		builder.append("<http://semoss.org/ontologies/Concept/PostTransformation> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> .\n");
//		// type is a type of contains - for transformations
//		builder.append("<http://semoss.org/ontologies/Relation/Contains/Type>  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> .\n");
//		// propMap is a type of contains - for transformations
//		builder.append("<http://semoss.org/ontologies/Relation/Contains/propMap>  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> .\n");
//
//		List<SEMOSSParam> paramsAccountedFor = new Vector<SEMOSSParam>();
//		for(int i = 0; i < dmcList.size(); i++) {
//			
//			// Adding in parameter checking.......
//			// Here is the logic
//			//  If a component does not involve any of the parameters we are saving (if metamodel -- neither nodes nor properties. if query -- don't think we can check it)
//			//   Then pre trans are safe
//			//  Else
//			//   If a pre trans with the param already exists
//			//    Wipe the list because it needs to be empty when we fill the param
//			//   Else
//			//    Add a pre trans with empty list
//			//   set parameter id
//			//  Post transformations --- if its a filter and involves the parameter should be removed
//			//  Post transformations --- if its a join and involves the parameter should be type inner
//			
//			
//			logger.info("Creating nTriples for compoenent:::: " + i);
//			// create component based on number "i"
//			builder.append("<http://semoss.org/ontologies/Concept/Component/" + i + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Component> .\n");
//			// add order to component
//			builder.append("<http://semoss.org/ontologies/Concept/Component/" + i + "> <http://semoss.org/ontologies/Relation/Contains/Order> \"" + i + "\"^^<http://www.w3.org/2001/XMLSchema#int> .\n");
//
////			Map<String, Object> compMap = insightMakeup.get(i);
//			DataMakerComponent dmc = dmcList.get(i);
//			List<SEMOSSParam> involvedParams = new Vector<SEMOSSParam>();
//			String engineName = dmc.getEngine().getEngineId();
//			String realEngineName = escapeForSQLStatement(engineName);
//			String cleanEngineName = Utility.cleanString(engineName, true);
//			logger.info("Component " + i + " has engine name::: " + cleanEngineName);
//			// create engine and add to component 
//			if(!engineSet.contains(cleanEngineName)) {
//				engineSet.add(cleanEngineName);
//				builder.append("<http://semoss.org/ontologies/Concept/Engine/" + cleanEngineName + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine> .\n");
//				builder.append("<http://semoss.org/ontologies/Concept/Engine/" + cleanEngineName + "> <http://semoss.org/ontologies/Relation/Contains/Name> \"" + realEngineName + "\" .\n");
//			}
//			builder.append("<http://semoss.org/ontologies/Concept/Component/" + i + "> <Comp:Eng> <http://semoss.org/ontologies/Concept/Engine/" + cleanEngineName + "> .\n");
//
//			String query = dmc.getQuery();
//			// add query property to component if query is not null
//			
//			if(query == null) {
//				involvedParams = getInvolvedParams(dmc, parameters, paramsAccountedFor);
//				String jsonMetamodel = gson.toJson(dmc.getQueryStruct());
//				logger.info("Component " + i + " does NOT have query... instead saving metamodel::: " + jsonMetamodel);
//				jsonMetamodel = escapeForNTripleAndSQLStatement(jsonMetamodel);
//				builder.append("<http://semoss.org/ontologies/Concept/Component/" + i + "> <http://semoss.org/ontologies/Relation/Contains/Metamodel> \"" + jsonMetamodel + "\" .\n");
//			} else {
//				logger.info("Component " + i + " has query::: " + Utility.cleanLogString(query));
//				involvedParams = getInvolvedParamsFromQuery(query, parameters, paramsAccountedFor);//
//				query = escapeForNTripleAndSQLStatement(query);
//				builder.append("<http://semoss.org/ontologies/Concept/Component/" + i + "> <http://semoss.org/ontologies/Relation/Contains/Query> \"" + query + "\" .\n");
//			}
//
//			List<ISEMOSSTransformation> preTransformationList = dmc.getPreTrans();
//			int preIdx = 0;
//			if(preTransformationList != null && !preTransformationList.isEmpty()) {
//				logger.info("Component " + i + " has pre-transformations!!");
//				for(; preIdx < preTransformationList.size(); preIdx++) {
//					logger.info("Component " + i + " .... building pre-transformation " + preIdx);
//					ISEMOSSTransformation preTrans = preTransformationList.get(preIdx);
//					buildPreTransString(preTrans, preIdx, i, numPreTransformations, builder, involvedParams, gson);
//				}
//			}
//			while (!involvedParams.isEmpty()) { // these are the params that don't have a pretrans set up for them.
//				//    Add a pre trans with empty list
//				logger.info("Component " + i + " .... building pre-transformation " + preIdx + " JUST FOR PARAM");
//				SEMOSSParam param = involvedParams.remove(0);
//				ISEMOSSTransformation newPreTrans = buildEmptyFilterTrans(param.getName());
//				param.setComponentFilterId(OldInsight.COMP + i + ":" + OldInsight.PRE_TRANS + preIdx);
//				buildPreTransString(newPreTrans, preIdx, i, numPreTransformations, builder, null, gson);
//				preIdx++;
//			}
//			numPreTransformations+=preIdx;
//			List<ISEMOSSTransformation> postTransformationList = dmc.getPostTrans();
//			if(postTransformationList != null && !postTransformationList.isEmpty()) {
//				logger.info("Component " + i + " has post-transformations!!");
//				int j = 0;
//				int postTransListIdx = 0;
//				for(; postTransListIdx < postTransformationList.size(); postTransListIdx++) {
//					logger.info("Component " + i + " .... building post-transformation " + j);
//					ISEMOSSTransformation postTrans = postTransformationList.get(postTransListIdx);
//					if (postTrans instanceof FilterTransformation && filterInvolvesParams(parameters, (FilterTransformation)postTrans)){
//						logger.info("Component " + i + " .... skipping this trans cuz involves param... will try next");
//						continue;
//					}
//					// add transformation based on "j"
//					builder.append("<http://semoss.org/ontologies/Concept/PostTransformation/" + (j+numPostTransformations) + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/PostTransformation> .\n");
//					builder.append("<http://semoss.org/ontologies/Concept/PostTransformation/" + (j+numPostTransformations) + "> <http://semoss.org/ontologies/Relation/Contains/Order> \"" + j + "\"^^<http://www.w3.org/2001/XMLSchema#int> .\n");
//
//					// connection transformation to component
//					builder.append("<http://semoss.org/ontologies/Concept/Component/" + i + "> <Comp:PostTrans> <http://semoss.org/ontologies/Concept/PostTransformation/" + (j+numPostTransformations) + "> .\n");
//					
//					// add parameters for transformation
//					Map<String, Object> paramMap =  (Map<String, Object>) postTrans.getProperties();
//					if(parameters.size() != 0){
//						if (postTrans instanceof JoinTransformation){
//							makeInnerJoin((JoinTransformation) postTrans);
//						}
//					}
//					String paramStringify = gson.toJson(paramMap);
//					paramStringify = escapeForNTripleAndSQLStatement(paramStringify);
//					builder.append("<http://semoss.org/ontologies/Concept/PostTransformation/" + (j+numPostTransformations) + "> <http://semoss.org/ontologies/Relation/Contains/propMap> \"" + paramStringify + "\" .\n");
//					j++;
//				}
//				numPostTransformations+=j;
//			}
//			List<ISEMOSSAction> actionList = dmc.getActions();
//			if(actionList != null && !actionList.isEmpty()) {
//				logger.info("Component " + i + " has actions!!");
//				int j = 0;
//				for(; j < actionList.size(); j++) {
//					logger.info("Component " + i + " .... building action " + j);
//					ISEMOSSAction action = actionList.get(j);
//					// add transformation based on "j"
//					builder.append("<http://semoss.org/ontologies/Concept/Action/" + (j+numAction) + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Action> .\n");
//					builder.append("<http://semoss.org/ontologies/Concept/Action/" + (j+numAction) + "> <http://semoss.org/ontologies/Relation/Contains/Order> \"" + j + "\"^^<http://www.w3.org/2001/XMLSchema#int> .\n");
//
//					// connection transformation to component
//					builder.append("<http://semoss.org/ontologies/Concept/Component/" + i + "> <Comp:Action> <http://semoss.org/ontologies/Concept/Action/" + (j+numAction) + "> .\n");
//					
//					// add parameters for transformation
//					Map<String, Object> paramMap =  (Map<String, Object>) action.getProperties();
//					String paramStringify = gson.toJson(paramMap);
//					paramStringify = escapeForNTripleAndSQLStatement(paramStringify);
//					builder.append("<http://semoss.org/ontologies/Concept/Action/" + (j+numAction) + "> <http://semoss.org/ontologies/Relation/Contains/propMap> \"" + paramStringify + "\" .\n");
//				}
//				numAction+=j;
//			}
//		}
//		String clob = builder.toString();
//		logger.info("Done building NTRIPLES");
//		logger.debug("CLOB to save is :: " + clob);
//		
//		return clob;
//	}
//	
//	private boolean filterInvolvesParams(List<SEMOSSParam> involvedParams, FilterTransformation postTrans) {
//		String postTransCol = (String) postTrans.getProperties().get(FilterTransformation.COLUMN_HEADER_KEY);
//		for(SEMOSSParam p : involvedParams){
//			if(postTransCol.equals(p.getName())){
//				return true;
//			}
//		}
//		return false;
//	}
//
//	private void buildPreTransString(ISEMOSSTransformation preTrans, int j, int i, int numPreTransformations, StringBuilder builder, List<SEMOSSParam> involvedParamsPre, Gson gson){
//		// add transformation based on "j"
//		builder.append("<http://semoss.org/ontologies/Concept/PreTransformation/" + (j+numPreTransformations) + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/PreTransformation> .\n");
//		builder.append("<http://semoss.org/ontologies/Concept/PreTransformation/" + (j+numPreTransformations) + "> <http://semoss.org/ontologies/Relation/Contains/Order> \"" + j + "\"^^<http://www.w3.org/2001/XMLSchema#int> .\n");
//
//		// connection transformation to component
//		builder.append("<http://semoss.org/ontologies/Concept/Component/" + i + "> <Comp:PreTrans> <http://semoss.org/ontologies/Concept/PreTransformation/" + (j+numPreTransformations) + "> .\n");
//		
//		// add parameters for transformation
//		if(involvedParamsPre != null && !involvedParamsPre.isEmpty() && preTrans instanceof JoinTransformation) {
//			makeInnerJoin((JoinTransformation) preTrans);
//		}
//		Map<String, Object> paramMap =  new HashMap<String,Object>();
//		paramMap.putAll((Map<String, Object>) preTrans.getProperties());
//
//		if(involvedParamsPre != null && !involvedParamsPre.isEmpty() && preTrans instanceof FilterTransformation){
//			String preTransType = (String) preTrans.getProperties().get(FilterTransformation.COLUMN_HEADER_KEY);
//			//   If a pre trans with the param already exists
//			//    Wipe the list because it needs to be empty when we fill the param.
//			for(SEMOSSParam p: involvedParamsPre){//
////				if(Utility.getInstanceName(p.getType()).equals(preTransType)){ // cannot use this logic any more as rdbms p.getType is the physical uri for the column (..Concept/Column/Table) which wont match logical name (Table__Column)
//				if(p.getName().equalsIgnoreCase(preTransType)){
//					paramMap = new HashMap<String, Object> (paramMap);
//					paramMap.remove(FilterTransformation.VALUES_KEY);
//					involvedParamsPre.remove(p);
//					p.setComponentFilterId(OldInsight.COMP + i + ":" + OldInsight.PRE_TRANS + j);
//					break;
//				}
//			}
//		}
//		String paramStringify = gson.toJson(paramMap);
//		paramStringify = escapeForNTripleAndSQLStatement(paramStringify);
//		builder.append("<http://semoss.org/ontologies/Concept/PreTransformation/" + (j+numPreTransformations) + "> <http://semoss.org/ontologies/Relation/Contains/propMap> \"" + paramStringify + "\" .\n");
//	}
//	
////	private boolean compInvolvesParam(DataMakerComponent dmc, List<SEMOSSParam> parameters) {
////		QueryBuilderData builderData = dmc.getBuilderData();
////		if(builderData != null) {
////			List<List<String>> relTriples = builderData.getRelTriples();
////			List<Map<String, String>> nodeProps = builderData.getNodeProps();
////			if(relTriples != null) {
////				for(int j = 0; j < relTriples.size(); j++) {
////					List<String> triple = relTriples.get(j);
////					for(String uri : triple) {
////						for(SEMOSSParam param : parameters){
////							if(uri.equals(param.getType())) {
////								return true;
////							}
////						}
////					}
////				}
////			}
////			if(nodeProps != null) {
////				for(int j = 0; j < nodeProps.size(); j++) {
////					Object uri = nodeProps.get(j).get("uriKey");
////					for(SEMOSSParam param : parameters){
////						if(uri.equals(param.getType())) {
////							return true;
////						}
////					}
////				}
////			}
////		}
////		return false;
////	}
//
//	private void setInsightOrder(int order, String insightId){
//		StringBuilder updateQueryBuilder = new StringBuilder();
//		updateQueryBuilder.append("UPDATE QUESTION_ID SET QUESTION_ORDER=");
//		updateQueryBuilder.append(order);
//		updateQueryBuilder.append(" WHERE ID='");
//		updateQueryBuilder.append(insightId);
//		updateQueryBuilder.append("'");
//		try {
//			insightEngine.insertData(updateQueryBuilder.toString());
//		} catch (Exception e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//	}
//	
//	private String createString(String... ids){
//		String idsString = "(";
//		for(String id : ids){
//			idsString = idsString + "'" + id + "', ";
//		}
//		idsString = idsString.substring(0, idsString.length() - 2) + ")";
//		logger.info("IDs string :::: " + idsString);
//		
//		return idsString;
//	}
//	
//	private String escapeForSQLStatement(String s) {
//		return s.replaceAll("'", "''");
//	}
//	
//	private String escapeForNTripleAndSQLStatement(String s) {
//		s = s.replaceAll("(?<=\\\\)\\\"{1}", "\\\\\\\\\""); // positive look behind - if quote is already escaped, need to escape the \ again
//		s = s.replaceAll("(?<!\\\\)\\\"{1}", "\\\\\""); // negative look behind, escaped any double quote not already escaped needs to be escaped once
//		s = escapeForSQLStatement(s);
//		return s;
//	}
//
//	private FilterTransformation buildEmptyFilterTrans(String paramName) {
//		FilterTransformation pFilter = new FilterTransformation();
//		Map<String, Object> props = new Hashtable<String, Object>();
//		props.put(FilterTransformation.COLUMN_HEADER_KEY, paramName);
//		pFilter.setProperties(props);
//		return pFilter;
//	}
//
//	/**
//	 * Modifies the first post transformation of a DataMakerComponent to be an inner join
//	 * @param dmc				The DataMakerComponent to modify the Join Transformation
//	 */
//	private void makeInnerJoin(JoinTransformation trans) {
//		Map<String, Object> props = trans.getProperties();
//		props.put(JoinTransformation.JOIN_TYPE, "inner");
//		trans.setProperties(props);
//	}
//	
//	/**
//	 * This method appends the parameter options to the DataMakerComponent metamodel
//	 * @param paramMapList				The list of parameters to save.  Comes as a map with the URI and the parent if it is a property
//	 * @param dmcList					The list of the DataMakerComponents for the insight
//	 * @param params					A list of SEMOSSParams to store the parameters with the correct options
//	 */
//	private List<SEMOSSParam> getInvolvedParams(DataMakerComponent dmc, List<SEMOSSParam> params, List<SEMOSSParam> paramsAccountedFor) {
//		List<SEMOSSParam> involvedParams = new Vector<SEMOSSParam>();
//		PARAMS_FOR : for(SEMOSSParam param : params) {
//			if(!paramsAccountedFor.contains(param)){
//				String paramURI = param.getType();
//				String paramPhysName = Utility.getInstanceName(paramURI);
//				QueryStruct builderData = dmc.getQueryStruct();
//				if(builderData != null) {
//					Map<String, Map<String, List>> relTriples = builderData.getRelations();
//					boolean containsParam =  false;
//					if(relTriples != null) {
//						for(String subject: relTriples.keySet()) {
//							// check the subject
//							String name = subject;
//							if(subject.contains("__")){
//								name = subject.substring(subject.indexOf("__") + 2);
//							}
//							if(name.equals(paramPhysName)) {
//								involvedParams.add(param);
//								continue PARAMS_FOR;
//							}
//							Collection<List> objectsC = relTriples.get(subject).values();
//							for(List objs : objectsC ){
//								for(Object obj : objs){
//									//check the objects
//									String checkName = obj +"";
//									if(checkName.contains("__")){
//										checkName = checkName.substring(checkName.indexOf("__") + 2);
//									}
//									if(checkName.equals(paramPhysName)) {
//										involvedParams.add(param);
//										continue PARAMS_FOR;
//									}
//								}
//							}
//						}
//					}
//					Map<String, List<String>> nodeProps = builderData.getSelectors();
//					if(nodeProps != null && !containsParam) {
//						for(String subject: nodeProps.keySet()) {
//							List<String> vector = nodeProps.get(subject);
//							for(String prop : vector){
//								if(prop.equals(paramPhysName)) {
//									involvedParams.add(param);
//									continue PARAMS_FOR;
//								}
//							}
//						}
//					}
//				}
//			}
//		}
//		paramsAccountedFor.addAll(involvedParams);
//		return involvedParams;
//	}
//
//	private List<SEMOSSParam> getInvolvedParamsFromQuery(String query, List<SEMOSSParam> params, List<SEMOSSParam> paramsAccountedFor) {
//		List<SEMOSSParam> involvedParams = new Vector<SEMOSSParam>();
//		if(params != null) {
//			PARAMS_FOR : for(SEMOSSParam param : params) {
//				if(!paramsAccountedFor.contains(param)){
//					String paramURI = param.getType(); // this will either be the physical uri if coming from actions (e.g. ..Concept/Column/Table) or custom defined (e.g. Table:Column)
//					
//					if(paramURI==null){ // if paramURI is null this means it is a param with custom query or static options. in this case, it must be associated with the first component, so immediately add it
//						logger.info("this param type is null. adding to first component! :)");
//						involvedParams.add(param);
//						continue PARAMS_FOR;
//					}
//					
//					logger.info("is my param : " + paramURI + " involved in query " + query );
//					Map<String, String> paramsInQuery = Utility.getParams(query);
//					for(String paramInQuery : paramsInQuery.keySet()){
//						logger.info("checking param in query : " + Utility.cleanLogString(paramInQuery));
//						String[] split = paramInQuery.split("-"); // this will be label-type where type is custom defined (e.g. Table:Column)
//						if(paramURI.equals(split[1])){
//							logger.info("this param is involved");
//							involvedParams.add(param);
//							continue PARAMS_FOR;
//						}
//						String rebuiltParamUri = Utility.getInstanceName(paramURI) + ":"+ Utility.getClassName(paramURI);
//						if(rebuiltParamUri.equals(split[1])){
//							logger.info("this param is involved");
//							involvedParams.add(param);
//							continue PARAMS_FOR;
//						}
//					}
//				}
//			}
//		}
//		paramsAccountedFor.addAll(involvedParams);
//		return involvedParams;
//	}
//}
