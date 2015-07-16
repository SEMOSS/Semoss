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
package prerna.ui.components.playsheets;

import java.util.Hashtable;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JDesktopPane;
import javax.swing.JTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.BTreeDataFrame;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.GridFilterData;
import prerna.ui.components.GridRAWTableModel;
import prerna.ui.components.GridTableModel;
import prerna.ui.components.GridTableRowSorter;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;
import prerna.util.Utility;

import com.hp.hpl.jena.query.ResultSet;

/**
 * The BasicProcessingPlaySheet performs the essential query processing that is required to run a play sheet other than graph play sheet.
 */
public class BasicProcessingPlaySheet extends AbstractRDFPlaySheet {

	private static final Logger logger = LogManager.getLogger(BasicProcessingPlaySheet.class.getName());
	protected GridFilterData gfd = new GridFilterData();
	public JTable table = null;
	public ISelectWrapper wrapper;
	protected ITableDataFrame dataFrame = null;
	protected ResultSet rs;
	
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
		List<Object[]> tabularData = getTabularData();
		String[] names = getColumnHeaders();
		addPanel();
		if(tabularData == null || tabularData.isEmpty()) {
			String questionID = getQuestionID();
			QuestionPlaySheetStore.getInstance().remove(questionID);
			if(QuestionPlaySheetStore.getInstance().isEmpty())
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

			
		//		if(list!=null && list.isEmpty()){
		//			String questionID = getQuestionID();
		//			// fill the nodetype list so that they can choose from
		//			// remove from store
		//			// this will also clear out active sheet
		//			QuestionPlaySheetStore.getInstance().remove(questionID);
		//			if(QuestionPlaySheetStore.getInstance().isEmpty())
		//			{
		//				JButton btnShowPlaySheetsList = (JButton) DIHelper.getInstance().getLocalProp(Constants.SHOW_PLAYSHEETS_LIST);
		//				btnShowPlaySheetsList.setEnabled(false);
		//			}
		//			Utility.showError("Query returned no results.");
		//			return;		
		//		}
		//
		//		//		if(rs==null) {
		//		//			Utility.showError("Query returned no results.");
		//		//			return;
		//		//		}
		//		super.createView();
		//		//createData();
		//		if(table==null)
		//			addPanel();
		//
		//		updateProgressBar("80%...Creating Visualization", 80);
		//		if(list!=null){
		//			gfd.setColumnNames(names);
		//			gfd.setDataList(list);
		//			GridRAWTableModel model = setGridModel(gfd);
		//			table.setModel(model);
		//			table.setRowSorter(new GridTableRowSorter(model));
		//		}
		//
		//		updateProgressBar("100%...Table Generation Complete", 100);
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

	@Override
	public void createData() {
		// TODO Auto-generated method stub
		// the create view needs to refactored to this

		wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		String[] names = wrapper.getVariables();
		// need to figure out overlay
		if(this.overlay) {
			// shouldn't need to do anything?
			// list = gfd.dataList;
		} else {
			dataFrame = new BTreeDataFrame(names);
		}
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext()) {
				ISelectStatement sjss = wrapper.next();
				dataFrame.addRow(sjss);
			}
		} catch (RuntimeException e) {
			logger.fatal(e);
		}
	}

	@Override
	public Hashtable getData() {
		Hashtable dataHash = (Hashtable) super.getData();
		if(dataFrame!=null) {
			dataHash.put("data", dataFrame.getRawData());
			dataHash.put("headers", dataFrame.getColumnHeaders());
		}
		return dataHash;
	}

	//TODO: THIS IS NEEDED FOR GraphPlaySheetExportListener but was never used....
	/**
	 * Method setRs. Sets the result set to the results generated from the query.
	 * @param rs ResultSet.
	 */
	public void setRs(ResultSet rs) {
		this.rs = rs;
	}
	
	@Override
	public void runAnalytics() {
		// TODO Auto-generated method stub
	}

	public ITableDataFrame getDataFrame(){
		return this.dataFrame;
	}

	public void setDataFrame(ITableDataFrame dataFrame) {
		this.dataFrame = dataFrame;
	}
	
	public String[] getNames(){
		return this.dataFrame.getColumnHeaders();
	}
	
	public List<Object[]> getTabularData() {
		if(dataFrame == null) {
			return null;
		}
		return dataFrame.getRawData();
	}
	
	public String[] getColumnHeaders() {
		if(dataFrame == null) {
			return null;
		}
		return dataFrame.getColumnHeaders();
	}

	@Override
	public Hashtable<String, String> getDataTableAlign() {
		// will be overridden by specific playsheets
		return null;
	}

	@Override
	public void processQueryData() {
		// will be overridden by specific playsheets
	}

}
