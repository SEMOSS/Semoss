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
package prerna.ui.components.playsheets;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.swing.JButton;
import javax.swing.JDesktopPane;
import javax.swing.JTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.rdbms.h2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.om.InsightStore;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.ui.components.GridFilterData;
import prerna.ui.components.GridRAWTableModel;
import prerna.ui.components.GridTableModel;
import prerna.ui.components.GridTableRowSorter;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * The BasicProcessingPlaySheet performs the essential query processing that is required to run a play sheet other than graph play sheet.
 */
public class TablePlaySheet extends AbstractPlaySheet implements IDataMaker{

	private static final Logger logger = LogManager.getLogger(TablePlaySheet.class.getName());
	protected GridFilterData gfd = new GridFilterData();
	public JTable table = null;
	protected ITableDataFrame dataFrame = null;
	private DataMakerComponent dmComponent; // this is only for legacy playsheets. should be removed once tap is cleaned
	
	/**
	 * This is the function that is used to create the first view 
	 * of any play sheet.  It often uses a lot of the variables previously set on the play sheet, such as {@link #setQuery(String)},
	 * {@link #setJDesktopPane(JDesktopPane)}, {@link #setRDFEngine(IEngine)}, and {@link #setTitle(String)} so that the play 
	 * sheet is displayed correctly when the view is first created.  It generally creates the model for visualization from 
	 * the specified engine, then creates the visualization, and finally displays it on the specified desktop pane
	 * 
	 * <p>This is the function called by the PlaysheetCreateRunner.  PlaysheetCreateRunner is the runner used whenever a play 
	 * sheet is to first be created, most notably in ProcessQueryListener.
	 */
	@Override
	public void createView() {
		List<Object[]> tabularData = getList();
		String[] names = getNames();
		addPanel();
		if(tabularData == null || tabularData.isEmpty()) {
			String questionID = getQuestionID();
			InsightStore.getInstance().remove(questionID); //TODO: QUESTION_ID MUST MATCH INSIGHT_ID
//			QuestionPlaySheetStore.getInstance().remove(questionID);
//			if(QuestionPlaySheetStore.getInstance().isEmpty())
			if(InsightStore.getInstance().isEmpty())
			{
				JButton btnShowPlaySheetsList = (JButton) DIHelper.getInstance().getLocalProp(Constants.SHOW_PLAYSHEETS_LIST);
				btnShowPlaySheetsList.setEnabled(false);
			}
			Utility.showError("Query returned no results.");
			return;		
		} else {
			updateProgressBar("80%...Creating Visualization", 80);
			gfd.setColumnNames(names);
			gfd.setDataList(tabularData);
			GridRAWTableModel model = setGridModel(gfd);
			table.setModel(model);
			table.setRowSorter(new GridTableRowSorter(model));
		}
		updateProgressBar("100%...Table Generation Complete", 100);
	}

	public GridRAWTableModel setGridModel(GridFilterData gfd) {
		GridRAWTableModel model = new GridRAWTableModel(gfd);
		return model;
	}

	/**
	 * This function is very similar to {@link #extendView()}.  Its adds additional data to the model already associated with 
	 * the play sheet.  The main difference between these two functions, though is {@link #overlayView()} is used to overlay 
	 * a query that could be unrelated to the data that currently exists in the play sheet's model whereas {@link #extendView()} 
	 * uses FilterNames to limit the results of the additional data so that it only add data relevant to the play sheet's current
	 *  model.
	 *  <p>This function is used by PlaysheetOverlayRunner which is called when the Overlay button is selected.
	 */
	@Override
	public void overlayView() {
		gfd.setDataList(dataFrame.getData());
		((GridTableModel)table.getModel()).fireTableDataChanged();
		updateProgressBar("100%...Table Generation Complete", 100);
	}

	/**
	 * Method addPanel.  Creates a panel and adds the table to the panel.
	 */
	public void addPanel()
	{
		//Fill
	}

	/**
	 * Recreates the visualizer and the repaints the play sheet without recreating the model or pulling anything 
	 * from the specified engine.
	 * This function is used when the model to be displayed has not been changed, but rather the visualization itself 
	 * must be redone.
	 * <p>This function takes into account the nodes that have been filtered either through FilterData or Hide Nodes so that these 
	 * do not get included in the recreation of the visualization.
	 */
	@Override
	public void refineView() {
		// TODO: Implement
		// this is easy
		// just use the filter to not show stuff I dont need to show
		// but this also means I need to create the vertex filter data etc. 
	}

//	@Override
//	public Hashtable getData() {
//		Hashtable dataHash = (Hashtable) super.getData();
//		if(getList()!=null) {
//			dataHash.put("data", getList());
//		}
//		if(getNames()!=null) {
//			dataHash.put("headers", getNames());
//		}
//		return dataHash;
//	}
	
	@Override
	public void runAnalytics() {
		// TODO Auto-generated method stub
	}
	
	public List<Object[]> getList() {
		if(dataFrame == null) {
			return null;
		}
		return dataFrame.getData();
	}
	
	public String[] getNames() {
		if(dataFrame == null) {
			return null;
		}
		return dataFrame.getColumnHeaders();
	}

	@Override
	public Map<String, String> getDataTableAlign() {
		if(this.tableDataAlign != null){
			return this.tableDataAlign;
		}
		// will be overridden by specific playsheets
		return null;
	}

	@Override
	public void setDataMaker(IDataMaker data) {
		if (data instanceof IPlaySheet){
			data = ((IPlaySheet) data).getDataMaker();
		}
		if(data instanceof ITableDataFrame){
			this.dataFrame = (ITableDataFrame) data;
		}
		else {
			logger.error("Trying to set illegal data type in TablePlaySheet");
			if(data == null) {
				logger.error("Failed setting null when only ITableDataFrames are allowed");
			} else {
				logger.error("Failed setting " + data.getClass() + " when only ITableDataFrames are allowed");
			}
		}
		
	}

	@Override
	public void processQueryData() {
		// TODO Auto-generated method stub
		
	}
	
	// this method should not be here
	// this is only being used for legacy tap playsheets
	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
		processPreTransformations(component, component.getPreTrans());
		setQuery(component.getQuery());
		setRDFEngine(component.getEngine());
		component.setQuery(this.query);
		this.dmComponent = component;
		createData();
		processQueryData();
		runAnalytics();
	}

	// this method should not be here
	// this is only being used for legacy tap playsheets
	@Override
	@Deprecated
	public void createData() {
		if(this.dmComponent == null){
			this.dmComponent = new DataMakerComponent(this.engine, this.query);
		}
		this.dataFrame = new H2Frame();
		this.dataFrame.processDataMakerComponent(this.dmComponent);
	}

	@Override
	public ITableDataFrame getDataMaker() {
		return this.dataFrame;
	}

	@Override
	public void processPreTransformations(DataMakerComponent dmc, List<ISEMOSSTransformation> transforms) {
		logger.info("We are processing " + transforms.size() + " pre transformations");
		for(ISEMOSSTransformation transform : transforms){
			transform.setDataMakers(this);
			transform.setDataMakerComponent(dmc);
			transform.runMethod();
		}
	}

	@Override
	public void processPostTransformations(DataMakerComponent dmc, List<ISEMOSSTransformation> transforms, IDataMaker... dataFrame) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map getDataMakerOutput(String... selectors) {
		if(this.dataFrame != null) {
			return this.dataFrame.getDataMakerOutput(selectors);
		} else {
//			Hashtable dataHash = (Hashtable) super.getData();
			Hashtable dataHash =  new Hashtable();
			if(getList()!=null) {
				dataHash.put("data", getList());
			}
			if(getNames()!=null) {
				dataHash.put("headers", getNames());
			}
			return dataHash;
		}
	}
	
	@Override
	public IDataMaker getDefaultDataMaker(){
		return this;
	}

	@Override
	public Map<String, String> getScriptReactors() {
		Map<String, String> reactorNames = new HashMap<String, String>();
		reactorNames.put(PKQLEnum.EXPR_TERM, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLEnum.EXPR_SCRIPT, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLReactor.MATH_FUN.toString(), "prerna.sablecc.MathReactor");
//		reactorNames.put(PKQLEnum.COL_CSV, "prerna.sablecc.TinkerColAddReactor");
		reactorNames.put(PKQLEnum.ROW_CSV, "prerna.sablecc.RowCsvReactor");
		reactorNames.put(PKQLEnum.API, "prerna.sablecc.ApiReactor");
//		reactorNames.put(PKQLEnum.PASTED_DATA, "prerna.sablecc.PastedDataReactor");
		reactorNames.put(PKQLEnum.WHERE, "prerna.sablecc.ColWhereReactor");
		reactorNames.put(PKQLEnum.REL_DEF, "prerna.sablecc.RelReactor");
//		reactorNames.put(PKQLEnum.COL_ADD, "prerna.sablecc.ColAddReactor");
//		reactorNames.put(PKQLEnum.IMPORT_DATA, "prerna.sablecc.GDMImportDataReactor");
//		reactorNames.put(PKQLEnum.REMOVE_DATA, "prerna.sablecc.RemoveDataReactor");
//		reactorNames.put(PKQLEnum.FILTER_DATA, "prerna.sablecc.ColFilterReactor");
		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.VizReactor");
//		reactorNames.put(PKQLEnum.UNFILTER_DATA, "prerna.sablecc.ColUnfilterReactor");
//		reactorNames.put(PKQLEnum.DATA_FRAME, "prerna.sablecc.DataFrameReactor");
//		switch(reactorType) {
//			case IMPORT_DATA : return new GDMImportDataReactor();
//			case COL_ADD : return new ColAddReactor();
//		}
		
		return reactorNames;
	}

	@Override
	public void setUserId(String userId) {
		// TODO Auto-generated method stub
		
	}
	
	
	@Override
	public String getUserId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDataMakerName() {
		if(this.dataFrame != null) {
			return this.dataFrame.getDataMakerName();
		} else {
			return this.getClass().getName();
		}
	}

	@Override
	public void updateDataId() {
		if(this.dataFrame != null) {
			this.dataFrame.updateDataId();
		}
	}

	@Override
	public int getDataId() {
		if(this.dataFrame != null) {
			return this.dataFrame.getDataId();
		}
		
		return 0;
	}
	
	@Override
	public void resetDataId() {
		if(this.dataFrame != null) {
			this.dataFrame.resetDataId();
		}
	}
}
