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

import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.ListSelectionModel;

import prerna.ui.main.listener.specific.tap.CapCheckBoxSelectorListener;
import prerna.ui.swing.custom.SelectScrollList;

@SuppressWarnings("serial")
public class DHMSMCapabilitySelectPanel extends JPanel {
	

	private JCheckBox allCapButton, dhmsmCapButton, hsdCapButton, hssCapButton, fhpCapButton;
	private SelectScrollList capSelectDropDown; 
	
	public DHMSMCapabilitySelectPanel(SysOptCheckboxListUpdater checkboxListUpdater) {

		createView(checkboxListUpdater);
	}
	
	private void createView(SysOptCheckboxListUpdater checkboxListUpdater)
	{
		this.removeAll();
		
		GridBagLayout gbl_capabilitySelectPanel = new GridBagLayout();
		gbl_capabilitySelectPanel.columnWidths = new int[]{0, 0, 0, 0,};
		gbl_capabilitySelectPanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0};
		gbl_capabilitySelectPanel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		gbl_capabilitySelectPanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		this.setLayout(gbl_capabilitySelectPanel);
		
		JLabel lblCapSelectHeader = new JLabel("Select Capabilities:");
		lblCapSelectHeader.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblCapSelectHeader = new GridBagConstraints();
		gbc_lblCapSelectHeader.gridwidth = 3;
		gbc_lblCapSelectHeader.anchor = GridBagConstraints.WEST;
		gbc_lblCapSelectHeader.insets = new Insets(10, 0, 5, 5);
		gbc_lblCapSelectHeader.gridx = 0;
		gbc_lblCapSelectHeader.gridy = 0;
		this.add(lblCapSelectHeader, gbc_lblCapSelectHeader);
		

		capSelectDropDown = new SelectScrollList("Select Individual Capabilities");
		capSelectDropDown.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
		capSelectDropDown.setupButton(checkboxListUpdater.getAllCapabilityList(),40,120); //need to give list of all systems
		
		GridBagConstraints gbc_capSelectDropDown = new GridBagConstraints();
		gbc_capSelectDropDown.gridwidth = 3;
		gbc_capSelectDropDown.fill = GridBagConstraints.HORIZONTAL;
		gbc_capSelectDropDown.insets = new Insets(0, 0, 0, 5);
		gbc_capSelectDropDown.gridx = 0;
		gbc_capSelectDropDown.gridy = 3;
		this.add(capSelectDropDown.pane, gbc_capSelectDropDown);
		
		addCheckBoxes(checkboxListUpdater);

	}

	protected void addCheckBoxes(SysOptCheckboxListUpdater checkboxListUpdater) {
		
		allCapButton = new JCheckBox("All Cap");
		allCapButton.setName("allCapButton");
		GridBagConstraints gbc_allCapButton = new GridBagConstraints();
		gbc_allCapButton.anchor = GridBagConstraints.WEST;
		gbc_allCapButton.gridx = 0;
		gbc_allCapButton.gridy = 1;
		this.add(allCapButton, gbc_allCapButton);

		dhmsmCapButton = new JCheckBox("DHMSM");
		dhmsmCapButton.setName("dhmsmCapButton");
		GridBagConstraints gbc_dhmsmCapButton = new GridBagConstraints();
		gbc_dhmsmCapButton.anchor = GridBagConstraints.WEST;
		gbc_dhmsmCapButton.gridx = 1;
		gbc_dhmsmCapButton.gridy = 1;
		this.add(dhmsmCapButton, gbc_dhmsmCapButton);

		hsdCapButton = new JCheckBox("HSD");
		hsdCapButton.setName("hsdCapButton");
		GridBagConstraints gbc_hsdCapButton = new GridBagConstraints();
		gbc_hsdCapButton.anchor = GridBagConstraints.WEST;
		gbc_hsdCapButton.gridx = 0;
		gbc_hsdCapButton.gridy = 2;
		this.add(hsdCapButton, gbc_hsdCapButton);
		
		hssCapButton = new JCheckBox("HSS");
		hssCapButton.setName("hssCapButton");
		GridBagConstraints gbc_hssCapButton = new GridBagConstraints();
		gbc_hssCapButton.anchor = GridBagConstraints.WEST;
		gbc_hssCapButton.gridx = 1;
		gbc_hssCapButton.gridy = 2;
		this.add(hssCapButton, gbc_hssCapButton);
		
		fhpCapButton = new JCheckBox("FHP");
		fhpCapButton.setName("fhpCapButton");
		GridBagConstraints gbc_fhpCapButton = new GridBagConstraints();
		gbc_fhpCapButton.anchor = GridBagConstraints.WEST;
		gbc_fhpCapButton.gridx = 2;
		gbc_fhpCapButton.gridy = 2;
		this.add(fhpCapButton, gbc_fhpCapButton);
		
		CapCheckBoxSelectorListener capCheckBoxListener = new CapCheckBoxSelectorListener(checkboxListUpdater, capSelectDropDown,allCapButton,dhmsmCapButton, hsdCapButton,hssCapButton, fhpCapButton);
		allCapButton.addActionListener(capCheckBoxListener);
		dhmsmCapButton.addActionListener(capCheckBoxListener);
		hsdCapButton.addActionListener(capCheckBoxListener);
		hssCapButton.addActionListener(capCheckBoxListener);
		fhpCapButton.addActionListener(capCheckBoxListener);
	}
	
	public void clearList() {
		allCapButton.setSelected(false);
		dhmsmCapButton.setSelected(false);
		hsdCapButton.setSelected(false);
		hssCapButton.setSelected(false);
		fhpCapButton.setSelected(false);
		capSelectDropDown.clearSelection();
	}
	public boolean noneSelected() {
		if(getSelectedCapabilities().isEmpty())
			return true;
		return false;
	}
	public ArrayList<String> getSelectedCapabilities()
	{
		return capSelectDropDown.getSelectedValues();
	}

}
