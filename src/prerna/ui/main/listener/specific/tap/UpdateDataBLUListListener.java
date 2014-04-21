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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JToggleButton;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.main.listener.impl.AbstractListener;
import prerna.ui.swing.custom.SelectScrollList;
import prerna.util.Utility;

/**
 * Determines which functional areas the user wants to incorporate in RFP report
 * Used to determine if user wants to include HSD, HSS, or FHP functional areas in RFP report
 * Will populate sourceSelectPanel with all capabilities included in functional areas
 */
public class UpdateDataBLUListListener extends AbstractListener {
	IEngine engine;
	SelectScrollList capScrollList, dataScrollList, bluScrollList;
	JToggleButton updateDataBLUPanelButton;
	JButton updateDataBLUButton,updateComplementDataBLUButton;
	JLabel lblDataSelectHeader, lblBLUSelectHeader;


	/**
	 * Determines if the user has selected HSD, HSS, FHP check box's in MHS TAP to include functional areas to include in RFP report
	 * Will populate sourceSelectPanel to show all capabilities for the functional area's selected
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {

		if((e.getSource().equals(updateDataBLUPanelButton)&&!updateDataBLUPanelButton.isSelected()))
		{
			lblDataSelectHeader.setVisible(false);
			lblBLUSelectHeader.setVisible(false);
			updateDataBLUButton.setVisible(false);
			updateComplementDataBLUButton.setVisible(false);
			dataScrollList.setVisible(false);
			bluScrollList.setVisible(false);
		}
		else if(e.getSource().equals(updateDataBLUPanelButton)||e.getSource().equals(updateDataBLUButton))
		{
			lblDataSelectHeader.setVisible(true);
			lblBLUSelectHeader.setVisible(true);
			updateDataBLUButton.setVisible(true);
			updateComplementDataBLUButton.setVisible(true);
			dataScrollList.setVisible(true);
			bluScrollList.setVisible(true);
			//once you have the list of capabilities, bind them to a data query and a blu query to pull list of data and blus
			String dataQuery = "SELECT DISTINCT ?Data WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Task ?Needs ?Data.} }";
			String bluQuery = "SELECT DISTINCT ?BLU WHERE { {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?Task_Needs_BusinessLogicUnit <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?Capability ?Consists ?Task.}{?Task ?Task_Needs_BusinessLogicUnit ?BLU}}";
	
			ArrayList<String> capabilities = new ArrayList<String>();
			ArrayList<String> dataList =  new ArrayList<String>();
			ArrayList<String> bluList =  new ArrayList<String>();
			if(!capScrollList.getSelectedValues().isEmpty())
			{
				capabilities = capScrollList.getSelectedValues();
				dataQuery = addBindings("Capability",capabilities, dataQuery);
				bluQuery = addBindings("Capability",capabilities, bluQuery);
				dataList = runListQuery(engine,dataQuery);
				bluList = runListQuery(engine,bluQuery);	
			}
			
			//select the data and blu's on the scrollLists based on the list of data and blu

			dataScrollList.setSelectedValues(new Vector<String>(dataList));
			bluScrollList.setSelectedValues(new Vector<String>(bluList));

		}
		else
		{
			ArrayList<String> dataList =  dataScrollList.getUnselectedValues();
			ArrayList<String> bluList =  bluScrollList.getUnselectedValues();
			dataScrollList.setSelectedValues(new Vector<String>(dataList));
			bluScrollList.setSelectedValues(new Vector<String>(bluList));
		}
	}
	public String addBindings(String type, List bindingsList,String query)
	{
		if(bindingsList.size()==0)
			return query;
		query += "BINDINGS ?"+type+" {";
		for(int i=0;i<bindingsList.size();i++)
			query+="(<http://health.mil/ontologies/Concept/"+type+"/"+(String)bindingsList.get(i)+">)";
		query+="}";
		return query;
	}
	/**
	 * Runs a query on a specific engine to make a list of systems to report on
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 */
	public ArrayList<String> runListQuery(IEngine engine, String query) {
		ArrayList<String> list = new ArrayList<String>();
		try {

			SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
			wrapper.setQuery(query);
			wrapper.setEngine(engine);
			wrapper.executeQuery();
	
			String[] names = wrapper.getVariables();
			while (wrapper.hasNext()) {
				SesameJenaSelectStatement sjss = wrapper.next();
				list.add((String) sjss.getVar(names[0]));
				}
		} catch (Exception e) {
			Utility.showError("Cannot find engine: "+engine.getEngineName());
		}
		return list;
	}
	public void setEngine(IEngine engine)
	{
		this.engine = engine;
		
	}
	public void setScrollListsAndLabels(SelectScrollList capScrollList,SelectScrollList dataScrollList,SelectScrollList bluScrollList,JLabel lblDataSelectHeader,JLabel lblBLUSelectHeader)
	{
		this.capScrollList = capScrollList;
		this.dataScrollList = dataScrollList;
		this.bluScrollList = bluScrollList;
		this.lblDataSelectHeader = lblDataSelectHeader;
		this.lblBLUSelectHeader = lblBLUSelectHeader;
	}
	public void setUpdateButtons(JToggleButton updateDataBLUPanelButton,JButton updateDataBLUButton,JButton updateComplementDataBLUButton)
	{
		this.updateDataBLUPanelButton = updateDataBLUPanelButton;
		this.updateDataBLUButton = updateDataBLUButton;
		this.updateComplementDataBLUButton = updateComplementDataBLUButton;

	}
	/**
	 * Override method from AbstractListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {

	}
}
