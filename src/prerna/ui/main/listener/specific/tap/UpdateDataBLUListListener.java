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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JToggleButton;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.specific.tap.DHMSMCapabilitySelectPanel;
import prerna.ui.components.specific.tap.DHMSMDataBLUSelectPanel;
import prerna.ui.components.specific.tap.DHMSMHelper;
import prerna.ui.components.specific.tap.DHMSMSystemSelectPanel;
import prerna.ui.main.listener.impl.AbstractListener;
import prerna.util.Utility;

/**
 * Updates Data and BLU lists for TAP system optimization based upon selected systems or selected capabilities.
 * Shows lists and selects corresponding data/blu if toggle is selected.
 * Also selects the complement of selected data and blu.
 */
public class UpdateDataBLUListListener extends AbstractListener {
	IEngine engine;
	JToggleButton showSystemSelectBtn;
	JButton updateProvideDataBLUButton,updateConsumeDataBLUButton,updateComplementDataBLUButton;
	
	boolean includeCapabilityPanel = false;

	DHMSMSystemSelectPanel sysSelectPanel;
	DHMSMCapabilitySelectPanel capSelectPanel;
	DHMSMDataBLUSelectPanel dataBLUSelectPanel;
	DHMSMHelper dhelp;
	/**
	 * Determines if the user has selected the view data/blu toggle, updateDataBLUButton or updateComplementDataBLUButton
	 * Shows the data and blu lists if toggle is selected and then updates the selected data and blu based on system or capabilities selected by user
	 * Updates Data and BLU from system or capabilities if updateDataBLUButton is selected
	 * Selects the complement of the data and BLU selected if updateComplementDataBLUButton is selected.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		if(((JButton)e.getSource()).getName().equals(updateProvideDataBLUButton.getName()) || ((JButton)e.getSource()).getName().equals(updateConsumeDataBLUButton.getName()))
		{
			dataBLUSelectPanel.setVisible(true);
			ArrayList<String> dataList =  new ArrayList<String>();
			ArrayList<String> bluList =  new ArrayList<String>();
			
			//if capabilities are not included or the system only select panel is shown, populate data and blu from the selected systems
			if(!includeCapabilityPanel||showSystemSelectBtn.isSelected())
			{
				ArrayList<String> systems = new ArrayList<String>();
				if(!sysSelectPanel.getSelectedSystems().isEmpty())
				{
					systems = sysSelectPanel.getSelectedSystems();
					for(int sysInd = 0;sysInd < systems.size();sysInd++)
					{
						String sys = systems.get(sysInd);
						if(((JButton)e.getSource()).getName().equals(updateProvideDataBLUButton.getName()))
							dataList.addAll(dhelp.getAllDataFromSys(sys, "C"));
						else
						{
							dataList.addAll(dhelp.getAllDataFromSys(sys,"R"));
							dataList.removeAll(dhelp.getAllDataFromSys(sys,"C"));
						}
					}
					if(((JButton)e.getSource()).getName().equals(updateProvideDataBLUButton.getName()))
					{
						String bluQuery = "SELECT DISTINCT ?BLU WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?System <http://semoss.org/ontologies/Relation/Provide> ?BLU.}}";
						bluQuery = addBindings("System",systems, bluQuery);
						bluList = runListQuery(engine,bluQuery);
					}
				}
			}
			//otherwise if the system and capability select panel is shown, populate data and blu from the selected capabilities
			else
			{
				String dataProvideQuery = "SELECT DISTINCT ?Data WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Task ?Needs ?Data.} }";
				String dataConsumeQuery = "SELECT DISTINCT ?Data WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'R'}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Task ?Needs ?Data.} }";
				String bluQuery = "SELECT DISTINCT ?BLU WHERE { {?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?Task_Needs_BusinessLogicUnit <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?Capability ?Consists ?Task.}{?Task ?Task_Needs_BusinessLogicUnit ?BLU}}";
				if(((JButton)e.getSource()).getName().equals(updateProvideDataBLUButton.getName()))
				{}
				ArrayList<String> capabilities = new ArrayList<String>();
				if(!capSelectPanel.getSelectedCapabilities().isEmpty())
				{
					capabilities = capSelectPanel.getSelectedCapabilities();
					dataProvideQuery = addBindings("Capability",capabilities, dataProvideQuery);
					dataConsumeQuery = addBindings("Capability",capabilities, dataConsumeQuery);
					ArrayList<String> dataProvideList = runListQuery(engine,dataProvideQuery);
					ArrayList<String> dataConsumeList = runListQuery(engine,dataConsumeQuery);
					if(((JButton)e.getSource()).getName().equals(updateProvideDataBLUButton.getName()))
					{
						dataList = dataProvideList;
						bluQuery = addBindings("Capability",capabilities, bluQuery);
						bluList = runListQuery(engine,bluQuery);	
					}
					else
					{
						dataList = dataConsumeList;
						dataList.removeAll(dataProvideList);
					}
				}

			}
			dataBLUSelectPanel.dataSelectDropDown.setSelectedValues(new Vector<String>(dataList));
			dataBLUSelectPanel.bluSelectDropDown.setSelectedValues(new Vector<String>(bluList));
		}
		//if the complement button is clicked, then get all the unselected data and blu values and set them as selected
		else
		{
			ArrayList<String> dataList =  dataBLUSelectPanel.dataSelectDropDown.getUnselectedValues();
			ArrayList<String> bluList =  dataBLUSelectPanel.bluSelectDropDown.getUnselectedValues();
			dataBLUSelectPanel.dataSelectDropDown.setSelectedValues(new Vector<String>(dataList));
			dataBLUSelectPanel.bluSelectDropDown.setSelectedValues(new Vector<String>(bluList));
		}
	}
	/**
	 * Adds a list of bindings to a query
	 * @param type			String representing the type of node to include in the bindings, must be the same as the retvariable name in the query
	 * @param bindingsList	List of node instances to bind to the variable
	 * @param query			String representing the query to append the bindings to.
	 * @return String		query with added bindings. If bindingsList is empty, then returns query as is
	 */
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

			ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

			/*SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
			wrapper.setQuery(query);
			wrapper.setEngine(engine);
			wrapper.executeQuery();
			*/
			
			String[] names = wrapper.getVariables();
			while (wrapper.hasNext()) {
				ISelectStatement sjss = wrapper.next();
				list.add((String) sjss.getVar(names[0]));
				}
		} catch (RuntimeException e) {
			Utility.showError("Cannot find engine: "+engine.getEngineName());
		}
		return list;
	}
	/**
	 * Sets engine
	 * @param engine 	IEngine to be queried
	 */
	public void setEngine(IEngine engine)
	{
		this.engine = engine;
	}
	/**
	 * Sets components
	 * @param engine 	IEngine to be queried
	 * @param sysScrollList SelectScrollList with list of systems
	 * @param capScrollList SelectScrollList with list of capabilities
	 * @param dataScrollList SelectScrollList with list of data
	 * @param bluScrollList SelectScrollList with list of blu
	 * @param lblDataSelectHeader JLabel for data objects
	 * @param lblBLUSelectHeader JLabel for blu
	 * @param showSystemSelectBtn JToggleButton to show whether system select only is shown
	 */
	public void setComponents(DHMSMSystemSelectPanel sysSelectPanel,DHMSMCapabilitySelectPanel capSelectPanel,DHMSMDataBLUSelectPanel dataBLUSelectPanel, JToggleButton showSystemSelectBtn)
	{
		this.sysSelectPanel = sysSelectPanel;
		if(capSelectPanel == null)
			this.includeCapabilityPanel = false;
		else {
			this.capSelectPanel = capSelectPanel;
			this.includeCapabilityPanel = true;
		}
		this.dataBLUSelectPanel = dataBLUSelectPanel;
		this.showSystemSelectBtn = showSystemSelectBtn;
	}
	/**
	 * Sets buttons that could be clicked
	 * @param updateDataBLUPanelButton 		JToggleButton to show and update data and blu lists
	 * @param updateProvideDataBLUButton 			JButton to update data and blu lists
	 * @param updateComplementDataBLUButton JButton to update complement of data and blu lists
	 */
	public void setUpdateButtons(JButton updateProvideDataBLUButton,JButton updateConsumeDataBLUButton,JButton updateComplementDataBLUButton)
	{
		this.updateProvideDataBLUButton = updateProvideDataBLUButton;
		this.updateConsumeDataBLUButton = updateConsumeDataBLUButton;
		this.updateComplementDataBLUButton = updateComplementDataBLUButton;
	}
	/**
	 * Sets up DHMSMHelper to calculate system SOR for data objects
	 */
	public void setUpDHMSMHelper()
	{
		dhelp = new DHMSMHelper();
		dhelp.setUseDHMSMOnly(false);
		dhelp.runData(engine);
	}
	
	public void setIncludeCapabilityPanel(boolean includeCapabilityPanel)
	{
		this.includeCapabilityPanel = includeCapabilityPanel;
	}
	/**
	 * Override method from AbstractListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {

	}
}
