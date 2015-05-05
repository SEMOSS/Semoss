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
package prerna.ui.components.specific.tap;

import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.ListSelectionModel;

import prerna.engine.api.IEngine;
import prerna.ui.helpers.EntityFiller;
import prerna.ui.swing.custom.CustomButton;
import prerna.ui.swing.custom.SelectScrollList;
import prerna.util.DIHelper;
import aurelienribon.ui.css.Style;

@SuppressWarnings("serial")
public class DHMSMDataBLUSelectPanel extends JPanel {
	public IEngine engine;
	
	public JLabel lblDataSelectHeader,lblBLUSelectHeader;
	public SelectScrollList dataSelectDropDown,bluSelectDropDown;
	public JButton updateProvideDataBLUButton,updateConsumeDataBLUButton,updateComplementDataBLUButton;
	
	public DHMSMDataBLUSelectPanel()
	{
		//addElements();
	}
	
	public void addElements(DHMSMSystemSelectPanel systemSelectPanel)
	{
		
		this.removeAll();
		GridBagLayout gbl_dataBLUSelectPanel = new GridBagLayout();
		gbl_dataBLUSelectPanel.columnWidths = new int[]{0, 0, 0, 0,};
		gbl_dataBLUSelectPanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0};
		gbl_dataBLUSelectPanel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		gbl_dataBLUSelectPanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		this.setLayout(gbl_dataBLUSelectPanel);
		
		lblDataSelectHeader = new JLabel("Select Data Objects:");
		lblDataSelectHeader.setFont(new Font("Tahoma", Font.BOLD, 12));
		lblDataSelectHeader.setVisible(true);
		GridBagConstraints gbc_lblDataSelectHeader = new GridBagConstraints();
		gbc_lblDataSelectHeader.gridwidth = 3;
		gbc_lblDataSelectHeader.anchor = GridBagConstraints.WEST;
		gbc_lblDataSelectHeader.insets = new Insets(0, 5, 5, 5);
		gbc_lblDataSelectHeader.gridx = 0;
		gbc_lblDataSelectHeader.gridy = 2;
		this.add(lblDataSelectHeader, gbc_lblDataSelectHeader);
		
		lblBLUSelectHeader = new JLabel("Select BLUs:");
		lblBLUSelectHeader.setFont(new Font("Tahoma", Font.BOLD, 12));
		lblBLUSelectHeader.setVisible(true);
		GridBagConstraints gbc_lblBLUSelectHeader = new GridBagConstraints();
		gbc_lblBLUSelectHeader.gridwidth = 3;
		gbc_lblBLUSelectHeader.anchor = GridBagConstraints.WEST;
		gbc_lblBLUSelectHeader.insets = new Insets(0, 0, 5, 5);
		gbc_lblBLUSelectHeader.gridx = 4;
		gbc_lblBLUSelectHeader.gridy = 2;
		this.add(lblBLUSelectHeader, gbc_lblBLUSelectHeader);
		
		updateProvideDataBLUButton = new CustomButton("Select Provide");
		updateProvideDataBLUButton.setName("updateProvideDataBLUButton");
		updateProvideDataBLUButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(updateProvideDataBLUButton,  ".toggleButton");
		updateProvideDataBLUButton.setVisible(true);		
		
		GridBagConstraints gbc_updateDataBLUButton = new GridBagConstraints();
		gbc_updateDataBLUButton.anchor = GridBagConstraints.WEST;
		gbc_updateDataBLUButton.gridheight = 2;
		gbc_updateDataBLUButton.insets = new Insets(10, 0, 5, 5);
		gbc_updateDataBLUButton.gridx = 1;
		gbc_updateDataBLUButton.gridy = 0;
		this.add(updateProvideDataBLUButton, gbc_updateDataBLUButton);
		
		updateConsumeDataBLUButton = new CustomButton("Select Consume");
		updateConsumeDataBLUButton.setName("updateConsumeDataBLUButton");
		updateConsumeDataBLUButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(updateConsumeDataBLUButton,  ".toggleButton");
		updateConsumeDataBLUButton.setVisible(true);		
		
		GridBagConstraints gbc_updateConsumeDataBLUButton = new GridBagConstraints();
		gbc_updateConsumeDataBLUButton.anchor = GridBagConstraints.WEST;
		gbc_updateConsumeDataBLUButton.gridheight = 2;
		gbc_updateConsumeDataBLUButton.insets = new Insets(10, 0, 5, 5);
		gbc_updateConsumeDataBLUButton.gridx = 2;
		gbc_updateConsumeDataBLUButton.gridy = 0;
		this.add(updateConsumeDataBLUButton, gbc_updateConsumeDataBLUButton);		
		
		updateComplementDataBLUButton = new CustomButton("Select Complement");
		updateComplementDataBLUButton.setName("updateComplementDataBLUButton");
		updateComplementDataBLUButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(updateComplementDataBLUButton,  ".toggleButton");
		updateComplementDataBLUButton.setVisible(true);		
		
		GridBagConstraints gbc_updateComplementDataBLUButton = new GridBagConstraints();
		gbc_updateComplementDataBLUButton.anchor = GridBagConstraints.WEST;
		gbc_updateComplementDataBLUButton.gridwidth = 3;
		gbc_updateComplementDataBLUButton.gridheight = 2;
		gbc_updateComplementDataBLUButton.insets = new Insets(10, 0, 5, 5);
		gbc_updateComplementDataBLUButton.gridx = 3;
		gbc_updateComplementDataBLUButton.gridy = 0;
		this.add(updateComplementDataBLUButton, gbc_updateComplementDataBLUButton);

		dataSelectDropDown = new SelectScrollList("Select Individual Data");
		dataSelectDropDown.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
		GridBagConstraints gbc_dataSelectDropDown = new GridBagConstraints();
		gbc_dataSelectDropDown.gridwidth = 3;
		gbc_dataSelectDropDown.fill = GridBagConstraints.HORIZONTAL;
		gbc_dataSelectDropDown.insets = new Insets(0, 0, 0, 5);
		gbc_dataSelectDropDown.gridx = 0;
		gbc_dataSelectDropDown.gridy = 3;
		this.add(dataSelectDropDown.pane, gbc_dataSelectDropDown);

		//String[] dataArray = makeListFromQuery("DataObject","SELECT DISTINCT ?entity WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Task ?Needs ?entity.} }");
		String[] dataArray = makeListFromQuery("DataObject","SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}}");
		dataSelectDropDown.setupButton(dataArray,40,120); //need to give list of all systems
		dataSelectDropDown.setVisible(true);
		
		bluSelectDropDown = new SelectScrollList("Select Individual BLU");
		bluSelectDropDown.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
		GridBagConstraints gbc_bluSelectDropDown = new GridBagConstraints();
		gbc_bluSelectDropDown.gridwidth = 3;
		gbc_bluSelectDropDown.fill = GridBagConstraints.HORIZONTAL;
		gbc_bluSelectDropDown.insets = new Insets(0, 0, 0, 5);
		gbc_bluSelectDropDown.gridx = 4;
		gbc_bluSelectDropDown.gridy = 3;
		this.add(bluSelectDropDown.pane, gbc_bluSelectDropDown);
		
		String[] bluArray = makeListFromQuery("BusinessLogicUnit","SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}}");
		bluSelectDropDown.setupButton(bluArray,40,120); //need to give list of all systems
		bluSelectDropDown.setVisible(true);
	
	}
	
	public String[] makeListFromQuery(String type, String query)
	{
		engine = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
		EntityFiller filler = new EntityFiller();
		filler.engineName = engine.getEngineName();
		filler.type = "Capability";
		filler.setExternalQuery(query);
		filler.run();
		Vector<String> names = filler.nameVector;
		String[] listArray=new String[names.size()];
		for (int i = 0;i<names.size();i++)
		{
			listArray[i]=(String) names.get(i);
		}
		return listArray;
	}

	public void clearList() {
		dataSelectDropDown.clearSelection();
		bluSelectDropDown.clearSelection();
	}
	
	public void setFromSystem(boolean isPullFromSystem)
	{
			if(isPullFromSystem)
			{
  				updateProvideDataBLUButton.setText("Select Create");
  				updateConsumeDataBLUButton.setText("Select Read");
			}else{
 				updateProvideDataBLUButton.setText("Select Provide");
	  			updateConsumeDataBLUButton.setText("Select Consume");
			}
	}
	
	public boolean noneSelected() {
		if(getSelectedDataAndBLU().isEmpty())
			return true;
		return false;
	}
	
	public ArrayList<String> getSelectedData()
	{
		return dataSelectDropDown.getSelectedValues();
	}
	public ArrayList<String> getSelectedBLU()
	{
		return bluSelectDropDown.getSelectedValues();
	}
	public ArrayList<String> getSelectedDataAndBLU()
	{
		ArrayList<String> combinedList = new ArrayList<String>(dataSelectDropDown.getSelectedValues());
		combinedList.addAll(bluSelectDropDown.getSelectedValues());
		return combinedList;
	}
}
