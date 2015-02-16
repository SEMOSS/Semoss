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

import prerna.rdf.engine.api.IEngine;
import prerna.ui.main.listener.specific.tap.HighSystemCheckBoxSelectorListener;
import prerna.ui.swing.custom.SelectScrollList;

@SuppressWarnings("serial")
public class DHMSMHighSystemSelectPanel extends JPanel {
	public IEngine engine;
	String header = "Select Systems:";
	public JCheckBox allSysCheckBox, ehrCoreCheckBox;
	public SelectScrollList sysSelectDropDown;
	
	public DHMSMHighSystemSelectPanel(ArrayList<String> systemList)
	{
	}
	public void setHeader(String header) {
		this.header = header;
	}
	public void addElements()
	{
		this.removeAll();
		
		GridBagLayout gbl_systemSelectPanel = new GridBagLayout();
		gbl_systemSelectPanel.columnWidths = new int[]{0, 0, 0, 0,};
		gbl_systemSelectPanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0};
		gbl_systemSelectPanel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		gbl_systemSelectPanel.rowWeights = new double[]{0.0, 0.0, 1.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		this.setLayout(gbl_systemSelectPanel);

		JLabel lblSystemSelectHeader = new JLabel(header);
		lblSystemSelectHeader.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblSystemSelectHeader = new GridBagConstraints();
		gbc_lblSystemSelectHeader.gridwidth = 5;
		gbc_lblSystemSelectHeader.anchor = GridBagConstraints.WEST;
		gbc_lblSystemSelectHeader.insets = new Insets(10, 0, 5, 5);
		gbc_lblSystemSelectHeader.gridx = 0;
		gbc_lblSystemSelectHeader.gridy = 0;
		this.add(lblSystemSelectHeader, gbc_lblSystemSelectHeader);
		
		sysSelectDropDown = new SelectScrollList("Select Individual Systems");
		sysSelectDropDown.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
		GridBagConstraints gbc_sysSelectDropDown = new GridBagConstraints();
		gbc_sysSelectDropDown.gridwidth = 5;
		gbc_sysSelectDropDown.anchor = GridBagConstraints.WEST;
		gbc_sysSelectDropDown.insets = new Insets(0, 0, 0, 5);
		gbc_sysSelectDropDown.fill = GridBagConstraints.BOTH;
		gbc_sysSelectDropDown.gridx = 0;
		gbc_sysSelectDropDown.gridy = 2;
		this.add(sysSelectDropDown.pane, gbc_sysSelectDropDown);
		
		addCheckBoxes();

		String[] sysArray = new String[]{"Error: no systems"};
		sysSelectDropDown.setupButton(sysArray);
		
		addListener();

	}
	protected void addCheckBoxes() {	
		allSysCheckBox = new JCheckBox("All");
		allSysCheckBox.setName("allSysCheckBox");
		GridBagConstraints gbc_allSysButton = new GridBagConstraints();
		gbc_allSysButton.anchor = GridBagConstraints.WEST;
		gbc_allSysButton.gridx = 0;
		gbc_allSysButton.gridy = 1;
		this.add(allSysCheckBox, gbc_allSysButton);
		
		ehrCoreCheckBox = new JCheckBox("EHR Core");
		ehrCoreCheckBox.setName("recdSysCheckBox");
		GridBagConstraints gbc_recdSysButton = new GridBagConstraints();
		gbc_recdSysButton.anchor = GridBagConstraints.WEST;
		gbc_recdSysButton.gridx = 1;
		gbc_recdSysButton.gridy = 1;
		this.add(ehrCoreCheckBox, gbc_recdSysButton);
	}
	protected void addListener() {
		HighSystemCheckBoxSelectorListener sysCheckBoxListener = new HighSystemCheckBoxSelectorListener(engine, sysSelectDropDown,allSysCheckBox,ehrCoreCheckBox);
		allSysCheckBox.addActionListener(sysCheckBoxListener);
		ehrCoreCheckBox.addActionListener(sysCheckBoxListener);
	}
	
	public void clearList() {
		allSysCheckBox.setSelected(false);
		ehrCoreCheckBox.setSelected(false);
		sysSelectDropDown.clearList();
	}
	
	public ArrayList<String> getSelectedSystems()
	{
		return sysSelectDropDown.getSelectedValues();
	}
}
