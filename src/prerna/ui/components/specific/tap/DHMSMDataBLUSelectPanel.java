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

import aurelienribon.ui.css.Style;
import prerna.engine.api.IDatabaseEngine;
import prerna.ui.helpers.EntityFiller;
import prerna.ui.swing.custom.CustomButton;
import prerna.ui.swing.custom.SelectScrollList;
import prerna.util.DIHelper;

@SuppressWarnings("serial")
public class DHMSMDataBLUSelectPanel extends JPanel {
	public IDatabaseEngine engine;
	
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

		Vector<String> dataList = makeListFromQuery("DataObject","SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}}");
		dataSelectDropDown.setupButton(dataList,40,120);
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
		
		Vector<String> bluList = makeListFromQuery("BusinessLogicUnit","SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}}");
		bluSelectDropDown.setupButton(bluList,40,120);
		bluSelectDropDown.setVisible(true);
	
	}
	
	public Vector<String> makeListFromQuery(String type, String query)
	{
		engine = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("TAP_Core_Data");
		EntityFiller filler = new EntityFiller();
		filler.engineName = engine.getEngineId();
		filler.type = "Capability";
		filler.setExternalQuery(query);
		filler.run();
		return filler.nameVector;
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