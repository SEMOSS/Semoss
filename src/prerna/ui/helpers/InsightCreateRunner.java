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
package prerna.ui.helpers;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.swing.JDesktopPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.Dashboard;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.sablecc.PKQLRunner;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.AbstractPlaySheet;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.FilterTransformation;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.ui.components.playsheets.datamakers.PKQLTransformation;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class handles everything involved with creating an insight
 * The creating of an insight currently varies significantly between web and thick
 * Web: use data maker and components to create data, then send to front end
 * Thick: use data maker and components to create data, then set in playsheet and call necessary methods there
 */
public class InsightCreateRunner implements Runnable{

	private static final Logger LOGGER = LogManager.getLogger(InsightCreateRunner.class.getName());
	
	Insight insight= null;
	
	/**
	 * Constructor for PlaysheetCreateRunner.
	 * @param playSheet IPlaySheet
	 */
	public InsightCreateRunner(Insight insight)
	{
		this.insight = insight;
	}
	
	/**
	 * Method run.  Calls the create view method on the local play sheet.
	 */
	@Override
	public void run() {
		IDataMaker dm = createData();
		IPlaySheet playSheet = insight.getPlaySheet();
//		playSheet.setDataMaker(dm);
		Map<String, String> tableDataAlign = insight.getDataTableAlign();
		if(playSheet instanceof AbstractPlaySheet && !(tableDataAlign == null || tableDataAlign.isEmpty())) {
			((AbstractPlaySheet)playSheet).setTableDataAlign(tableDataAlign);
		}
		preparePlaySheet(playSheet, insight);

		if(!insight.getAppend()){
			playSheet.runAnalytics();
			playSheet.processQueryData();
			playSheet.createView();
		}
		else {
			playSheet.runAnalytics();
			playSheet.overlayView();
		}
		
	}
	
	/** 
	 * This creates the data needed for the insight
	 * Get the data maker from the insight
	 * Feed each component one by one into data maker
	 */
	private IDataMaker createData(){
		IDataMaker dm = insight.getDataMaker();
//		if(dm instanceof Dashboard) {
//			Dashboard dash = (Dashboard)dm;
//			dash.setInsightID(insight.getInsightID());
//		}
		
		// get the list of data maker components from the insight
		List<DataMakerComponent> dmComps = insight.getDataMakerComponents();
		// logic to append the parameter information that was selected on view into the data maker components
		// this either fills the query if the parameter was saved as a string using @INPUT_NAME@ taxonomy or
		// it fills in the values in a Filtering PreTransformation which appends the metamodel
		insight.appendParamsToDataMakerComponents();
		PKQLRunner runner = insight.getPKQLRunner();
		for(DataMakerComponent dmComp : dmComps){
//			// NOTE: this runs the data maker components directly onto the dm, not through the insight
			if(dmComp.isProcessed()) {
				continue;
			}
			DataMakerComponent copyDmc = dmComp.copy();
//			if(copyDmc.getBuilderData() != null) {
//				optimizeDataMakerComponents(copyDmc, dmComps);
//	    	}
			setRunnerInPKQLTrans(copyDmc, runner);
			dm.processDataMakerComponent(copyDmc);
		}
		insight.syncPkqlRunnerAndFrame(runner);
		
		if(needToRecalc(insight.getDataMakerComponents())) {
			insight.recalcDerivedColumns();
		}
		
		return dm;
	}
	
	/**
	 * this is just to get pkql runner data to fe through create
	 * need to have the same runner for full create process
	 * @param copyDmc
	 * @param runner
	 */
	private void setRunnerInPKQLTrans(DataMakerComponent copyDmc, PKQLRunner runner) {
		for(ISEMOSSTransformation trans : copyDmc.getPreTrans()){
			if(trans instanceof PKQLTransformation){
				((PKQLTransformation) trans).setRunner(runner);
			}
		}
		for(ISEMOSSTransformation trans : copyDmc.getPostTrans()){
			if(trans instanceof PKQLTransformation){
				((PKQLTransformation) trans).setRunner(runner);
			}
		}
	}

	//If the last transformation of the last component is a filter transformation, recalc the derived columns
	private boolean needToRecalc(List<DataMakerComponent> dmComps) {
		
		if(dmComps != null && dmComps.size() > 0) {
			
			DataMakerComponent lastComponent = dmComps.get(dmComps.size() - 1);
			if(lastComponent != null) {
				
				List<ISEMOSSTransformation> transformations = lastComponent.getPostTrans();
				if(transformations != null && transformations.size() > 0) {
					
					ISEMOSSTransformation lastTrans = transformations.get(transformations.size() - 1);
					if(lastTrans instanceof FilterTransformation) {
						return true;
					}
				}
			}
		}
		return false;
	}
	
	/**
	 * Runs the insight and returns the data table align for FE to view
	 */
	public Map<String, Object> runWeb()
	{
		boolean hasNoPkqlRecipeOrcontainsNonPkqlTransformation = (insight.getPkqlRecipe().length == 0) || insight.containsNonPkqlTransformations();
		if(hasNoPkqlRecipeOrcontainsNonPkqlTransformation) { 
			createData();
		}
		Map<String, String> tableDataAlign = insight.getDataTableAlign();
		
		// previous insights did not save the table data align
		// if it is not present, we get the table data align by setting the data maker in the playsheet and grabbing it
		if((tableDataAlign == null || tableDataAlign.isEmpty()) && !(insight.getDataMaker() instanceof Dashboard) && !insight.getOutput().toLowerCase().equals("ckeditor")) {
			IPlaySheet playSheet = insight.getPlaySheet();
			// as we move towards a more generic FE with many different viz's
			// we do not want to build out playsheets on the BE 
			// just keep the view separate
			if(playSheet != null) {
				tableDataAlign = (Map<String, String>) (((AbstractPlaySheet) playSheet).getDataTableAlign());
				insight.setDataTableAlign(tableDataAlign);
			}
		}
		
		IDataMaker dm = insight.getDataMaker();
		if(dm instanceof Dashboard) {
			Dashboard dash = (Dashboard)dm;
			dash.setInsightID(insight.getInsightID());
		}
		
		dm.resetDataId();
		Map<String, Object> retData = insight.getWebData();
		if(hasNoPkqlRecipeOrcontainsNonPkqlTransformation) {
			retData.put("recipe", new String[0]);
		}
		return retData;
	}
	
	public Map<String, Object> runSavedRecipe() {
		createData();
		Map<String, String> tableDataAlign = insight.getDataTableAlign();
		
		// previous insights did not save the table data align
		// if it is not present, we get the table data align by setting the data maker in the playsheet and grabbing it
		if((tableDataAlign == null || tableDataAlign.isEmpty()) && !(insight.getDataMaker() instanceof Dashboard) && !insight.getOutput().toLowerCase().equals("ckeditor")) {
			IPlaySheet playSheet = insight.getPlaySheet();
			tableDataAlign = (Map<String, String>) (((AbstractPlaySheet) playSheet).getDataTableAlign());
			insight.setDataTableAlign(tableDataAlign);
		}
		insight.getDataMaker().resetDataId();
		return insight.getOutputWebData();
//		return insight.getWebData();
	}
	
	private void preparePlaySheet(IPlaySheet playSheet, Insight insight){
		// SET THE DESKTOP PANE
		playSheet.setJDesktopPane((JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE));
		
		// CREATE INSIGHT ID AND STORE IT
		String insightID = InsightStore.getInstance().put(insight);
		playSheet.setQuestionID(insightID);
		
		// CREATE THE INSIGHT TITLE
		String playSheetTitle = "";
		Map<String, List<Object>> paramHash = insight.getParamHash();
		if(paramHash != null && !paramHash.isEmpty())
		{
			// loops through and appends the selected parameters in the play sheet title
			Iterator<String> enumKey = paramHash.keySet().iterator();
			while (enumKey.hasNext())
			{
				String key = (String) enumKey.next();
				List<Object> value = paramHash.get(key);
				for(int i = 0; i < value.size(); i++) {
					Object val = value.get(i);
					if(val instanceof String || val instanceof Double ) {
						playSheetTitle = playSheetTitle + Utility.getInstanceName(val+"") + " - ";
					}
				}
			}
		}
		String name = insight.getInsightName();
		if (name == null){
			name = "Custom";
		}
		System.out.println("Param Hash is " + paramHash);
		playSheetTitle = playSheetTitle+name.trim();
		playSheet.setTitle(playSheetTitle);
	}
}
