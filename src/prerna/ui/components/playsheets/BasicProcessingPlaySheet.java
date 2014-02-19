/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.components.playsheets;

import java.util.ArrayList;
import java.util.Hashtable;

import javax.swing.JButton;
import javax.swing.JDesktopPane;
import javax.swing.JTable;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.GridFilterData;
import prerna.ui.components.GridTableModel;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;
import prerna.util.Utility;

import com.hp.hpl.jena.query.ResultSet;

/**
 * The BasicProcessingPlaySheet performs the essential query processing that is required to run a play sheet other than graph play sheet.
 */
public class BasicProcessingPlaySheet extends AbstractRDFPlaySheet {

	protected ResultSet rs = null;
	protected GridFilterData gfd = new GridFilterData();
	String [] names = null;
	public JTable table = null;
	public ArrayList <Object []> list = null;
	public SesameJenaSelectWrapper wrapper;

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
		if(list.isEmpty()){
			String questionID = getQuestionID();
			// fill the nodetype list so that they can choose from
			// remove from store
			// this will also clear out active sheet
			QuestionPlaySheetStore.getInstance().remove(questionID);
			if(QuestionPlaySheetStore.getInstance().isEmpty())
			{
				JButton btnShowPlaySheetsList = (JButton) DIHelper.getInstance().getLocalProp(Constants.SHOW_PLAYSHEETS_LIST);
				btnShowPlaySheetsList.setEnabled(false);
			}
			Utility.showError("Query returned no results.");
			return;		
		}

		//		if(rs==null) {
		//			Utility.showError("Query returned no results.");
		//			return;
		//		}
		super.createView();
		//createData();
		if(table==null)
			addPanel();


		updateProgressBar("80%...Creating Visualization", 80);
		gfd.setColumnNames(names);
		gfd.setDataList(list);
		GridTableModel model = new GridTableModel(gfd);
		table.setModel(model);

		updateProgressBar("100%...Table Generation Complete", 100);
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

		gfd.setDataList(list);
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
	 * Method setRs. Sets the result set to the results generated from the query.
	 * @param rs ResultSet.
	 */
	public void setRs(ResultSet rs) {
		this.rs = rs;
	}

	/**
	 * Method getVariable. Gets the variable names from the query results.
	 * @param varName String - the variable name.
	 * @param sjss SesameJenaSelectStatement - the associated sesame jena select statement.

	 * @return Object - results.*/
	public Object getVariable(String varName, SesameJenaSelectStatement sjss){
		return sjss.getVar(varName);
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
		if(this.overlay)
			list = gfd.dataList;
		else list = new ArrayList();
		wrapper = new SesameJenaSelectWrapper();
		if(engine!= null && rs == null){

			wrapper.setQuery(query);
			wrapper.setEngine(engine);
			try{
				wrapper.executeQuery();	
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}		

		}
		else if (engine==null && rs!=null){
			wrapper.setResultSet(rs);
			wrapper.setEngineType(IEngine.ENGINE_TYPE.JENA);
		}

		// get the bindings from it
		names = wrapper.getVariables();
		int count = 0;
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();

				Object [] values = new Object[names.length];
				for(int colIndex = 0;colIndex < names.length;colIndex++)
				{
					values[colIndex] = getVariable(names[colIndex], sjss);
					logger.debug("Binding Name " + names[colIndex]);
					logger.debug("Binding Value " + values[colIndex]);
				}
				logger.debug("Creating new Value " + values);
				list.add(count, values);
				count++;
			}
		} catch (Exception e) {
			logger.fatal(e);
		}
	}

	@Override
	public Object getData() {
		Hashtable dataHash = new Hashtable();
		dataHash.put("data", list);
		dataHash.put("headers", names);
		return dataHash;
	}

	@Override
	public void runAnalytics() {
		// TODO Auto-generated method stub

	}

}
