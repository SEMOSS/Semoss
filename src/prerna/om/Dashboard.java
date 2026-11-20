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
package prerna.om;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;

@Deprecated
public class Dashboard implements IDataMaker {

	private static final Logger classLogger = LogManager.getLogger(Dashboard.class.getName());
	private String insightID;
	private String userID;
//	private DataFrameJoiner joiner;

	// insight id -> frame id
	private Map<String, String[]> insight2frameMap = new HashMap<>();

	// keeps track of config structure
	@Deprecated
	Object config = new HashMap<>();

	// need to store the output for each insight and return later to capture pkql
	// calls
	private Map<String, Map<String, Object>> insightOutputMap = new HashMap<>();

	@Deprecated
	public Dashboard() {
		// joiner = new DataFrameJoiner(this);
	}

	@Deprecated
	@Override
	public void processDataMakerComponent(DataMakerComponent component) {

		processPreTransformations(component, component.getPreTrans());
		processPostTransformations(component, component.getPostTrans());
	}

	@Deprecated
	@Override
	public void processPreTransformations(DataMakerComponent dmc, List<ISEMOSSTransformation> transforms) {

	}

	@Deprecated
	@Override
	public void processPostTransformations(DataMakerComponent dmc, List<ISEMOSSTransformation> transforms,
			IDataMaker... dataFrame) {
		classLogger.info("We are processing " + transforms.size() + " post transformations");
		// if other data frames present, create new array with this at position 0
		IDataMaker[] extendedArray = new IDataMaker[] { this };
		if (dataFrame.length > 0) {
			extendedArray = new IDataMaker[dataFrame.length + 1];
			extendedArray[0] = this;
			for (int i = 0; i < dataFrame.length; i++) {
				extendedArray[i + 1] = dataFrame[i];
			}
		}
		for (ISEMOSSTransformation transform : transforms) {
			// get the id from the pkql transformation and run in on that join
			transform.setDataMakers(extendedArray);
			transform.setDataMakerComponent(dmc);
			transform.runMethod();
		}
	}

	@Deprecated
	@Override
	/**
	 * Return the data maker output for each attached insight
	 * 
	 * TODO : Take into account selectors
	 */
	public Map<String, Object> getDataMakerOutput(String... selectors) {

		Map<String, Object> returnHash = new HashMap<>();
		List insightList = new ArrayList();

		List<Insight> insights = null;// this.joiner.getInsights();
		Map<String, List<String>> joinedInsightMap = null;// this.joiner.getJoinedInsightMap();

//		List<String> insightIDs = new ArrayList<>();
//		for(Insight insight : insights) {
//			insightIDs.add(insight.getInsightID());
//		}

		for (Insight insight : insights) {
			Map<String, Object> nextInsightMap = new HashMap<>();

			nextInsightMap.put("insightID", insight.getInsightId());
			nextInsightMap.put("engine", insight.getProjectId());
			nextInsightMap.put("questionID", insight.getRdbmsId());

			String[] ids = this.insight2frameMap.get(insight.getInsightId());
			if (ids != null) {
				nextInsightMap.put("widgetID", ids[0]);
				nextInsightMap.put("panelID", ids[1]);
			} else {
				nextInsightMap.put("widgetID", "");
				nextInsightMap.put("panelID", "");
			}

//			List<String> joinedInsights = new ArrayList<>();

//			joinedInsights.addAll(insightIDs);
//			joinedInsights.remove(insight.getInsightID());
			nextInsightMap.put("joinedInsights", joinedInsightMap.get(insight.getInsightId()));

			// instead of doing this...have data.open save the output to the
			// insight/dashboard
			// grab that data from the insight/dashboard...then delete
			// this doesn't have sufficient pkql data
			Map<String, Object> insightOutput;
			if (this.insightOutputMap.containsKey(insight.getInsightId())) {
				insightOutput = insightOutputMap.get(insight.getInsightId());
			} else {
				insightOutput = insight.getWebData();
			}
			nextInsightMap.putAll(insightOutput);
			insightList.add(nextInsightMap);
		}
		returnHash.put("Dashboard", insightList);
		this.insightOutputMap.clear();
		return returnHash;
	}

	@Deprecated
	@Override
	public Map<String, String> getScriptReactors() {
		Map<String, String> reactorNames = new HashMap<>();
//		reactorNames.put(PKQLEnum.DASHBOARD_JOIN, "prerna.sablecc.DashboardJoinReactor");
//		reactorNames.put(PKQLEnum.DASHBOARD_UNJOIN, "prerna.sablecc.DashboardUnjoinReactor");
//		reactorNames.put(PKQLEnum.OPEN_DATA, "prerna.sablecc.OpenDataReactor");
//		reactorNames.put(PKQLReactor.VAR.toString(), "prerna.sablecc.VarReactor");
//		reactorNames.put(PKQLReactor.INPUT.toString(), "prerna.sablecc.InputReactor");
//		reactorNames.put(PKQLEnum.COL_CSV, "prerna.sablecc.ColCsvReactor");
//		reactorNames.put(PKQLEnum.DASHBOARD_ADD, "prerna.sablecc.DashboardAddReactor");
//		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.VizReactor");
//		reactorNames.put(PKQLEnum.EXPR_TERM, "prerna.sablecc.ExprReactor");
//		reactorNames.put(PKQLEnum.EXPR_SCRIPT, "prerna.sablecc.ExprReactor");
//		reactorNames.put(PKQLReactor.MATH_FUN.toString(),"prerna.sablecc.MathReactor");
//		reactorNames.put(PKQLEnum.CLEAR_DATA, "prerna.sablecc.DashboardClearDataReactor");
		return reactorNames;
	}

	@Deprecated
	@Override
	public void updateDataId() {
	}

	@Deprecated
	@Override
	public int getDataId() {
		return 0;
	}

	@Deprecated
	@Override
	public void setUserId(String userId) {
		this.userID = userId;
	}

	@Deprecated
	@Override
	public String getUserId() {
		return this.userID;
	}

	@Deprecated
	@Override
	public String getDataMakerName() {
		return this.getClass().getSimpleName();
	}

	@Deprecated
	public void setInsightID(String insightID) {
		this.insightID = insightID;
	}

	@Deprecated
	public String getInsightID() {
		return this.insightID;
	}

	@Deprecated
	@Override
	public void resetDataId() {

	}

	@Deprecated
	public void setConfig(Object config) {
		this.config = config;
	}

	@Deprecated
	public Object getConfig() {
		return this.config;
	}

	@Deprecated
	public void setInsightOutput(String id, Map<String, Object> map) {
		this.insightOutputMap.put(id, map);
	}

	/*************************************
	 * JOINING LOGIC
	 **************************************/

	/**
	 * 
	 * @param insights
	 * @param joinColumns
	 * 
	 *                    first doing the case where we have all unjoined insights
	 */
	@Deprecated
	public void joinInsights(List<Insight> insights, List<List<String>> joinColumns) {
//		joiner.joinInsights(joinColumns, insights);
//		Insight parentInsight = InsightStore.getInstance().get(this.insightID);
//		for(Insight insight : insights) {
//			//add the insight to the view
//			insight.setParentInsight(parentInsight);
//		}
	}

	@Deprecated
	public void unjoinInsights(List<Insight> insights) {
		// joiner.unjoinInsights(insights);
	}

	/**
	 * 
	 * @param insights
	 * 
	 *                 Method to add insights
	 */
	@Deprecated
	public void addInsights(List<Insight> insights) {
		for (Insight insight : insights) {
			// joiner.addInsight(insight);
		}
	}

	@Deprecated
	public void setWidgetId(String insightId, String[] ids) {
		this.insight2frameMap.put(insightId, ids);
	}

	@Deprecated
	public void removeWidgetId(String insightId, String[] ids) {
		this.insight2frameMap.remove(insightId, ids);
	}

	@Deprecated
	public void dropDashboard() {
		// this.joiner = new DataFrameJoiner(this);
	}

	@Deprecated
	public void clearData() {
//		filterHash = new HashMap<>();
//		insightMap = new HashMap<>();
		insight2frameMap = new HashMap<>();
		config = new HashMap<>();
		insightOutputMap = new HashMap<>();
		// joiner = new DataFrameJoiner(this);
	}

	@Deprecated
	public List<Insight> getInsights() {
		// return this.joiner.getInsights();
		return null;
	}

	@Deprecated
	public boolean isJoined(Insight insight) {
		// return this.joiner.isJoined(insight);
		return false;
	}

	@Deprecated
	public void removeInsight(Insight insight) {
		// this.joiner.removeInsight(insight);

	}

	/*************************************
	 * END JOINING LOGIC
	 **************************************/
}
