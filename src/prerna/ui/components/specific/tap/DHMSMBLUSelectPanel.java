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
import prerna.ui.main.listener.specific.tap.BLUCheckBoxSelectorListener;
import prerna.ui.swing.custom.SelectScrollList;

@SuppressWarnings("serial")
public class DHMSMBLUSelectPanel extends JPanel {

	private SysOptPlaySheet playSheet;

	private SelectScrollList dataSelectDropDown;
	private JCheckBox hsdCheck, hssCheck, fhpCheck, dhmsmCheck, allDataCheck;

	public DHMSMBLUSelectPanel(IEngine engine, DHMSMSystemSelectPanel systemSelectPanel)
	{
		SysOptCheckboxListUpdater checkboxListUpdater = new SysOptCheckboxListUpdater(engine);
		createView(checkboxListUpdater, systemSelectPanel);
	}
	
	public DHMSMBLUSelectPanel(SysOptCheckboxListUpdater checkboxListUpdater, DHMSMSystemSelectPanel systemSelectPanel)
	{
		createView(checkboxListUpdater, systemSelectPanel);
	}

	private void createView(SysOptCheckboxListUpdater checkboxListUpdater, DHMSMSystemSelectPanel systemSelectPanel)
	{
		this.removeAll();
		
		GridBagLayout gbl_systemSelectPanel = new GridBagLayout();
		gbl_systemSelectPanel.columnWidths = new int[]{0, 0, 0, 0,};
		gbl_systemSelectPanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0};
		gbl_systemSelectPanel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		gbl_systemSelectPanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		this.setLayout(gbl_systemSelectPanel);

		JLabel lblSystemSelectHeader = new JLabel("Select Business Logic Units:");
		lblSystemSelectHeader.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblSystemSelectHeader = new GridBagConstraints();
		gbc_lblSystemSelectHeader.gridwidth = 4;
		gbc_lblSystemSelectHeader.anchor = GridBagConstraints.WEST;
		gbc_lblSystemSelectHeader.insets = new Insets(10, 0, 5, 5);
		gbc_lblSystemSelectHeader.gridx = 0;
		gbc_lblSystemSelectHeader.gridy = 0;
		this.add(lblSystemSelectHeader, gbc_lblSystemSelectHeader);

		dataSelectDropDown = new SelectScrollList("Select Individual Data");
		dataSelectDropDown.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
		dataSelectDropDown.setupButton(playSheet.getCheckboxListUpdater().getAllBLUList(),100,120);
		dataSelectDropDown.setVisible(true);


		GridBagConstraints gbc_dataSelectDropDown = new GridBagConstraints();
		gbc_dataSelectDropDown.gridwidth = 4;
		gbc_dataSelectDropDown.fill = GridBagConstraints.HORIZONTAL;
		gbc_dataSelectDropDown.insets = new Insets(0, 0, 0, 5);
		gbc_dataSelectDropDown.gridx = 0;
		gbc_dataSelectDropDown.gridy = 3;
		this.add(dataSelectDropDown.pane, gbc_dataSelectDropDown);

		addCheckBoxes(checkboxListUpdater);
	}
	
	private void addCheckBoxes(SysOptCheckboxListUpdater checkboxListUpdater) {

		allDataCheck = new JCheckBox("All");
		allDataCheck.setName("allDataCheck");
		GridBagConstraints gbc_allDataCheck = new GridBagConstraints();
		gbc_allDataCheck.anchor = GridBagConstraints.WEST;
		gbc_allDataCheck.gridx = 0;
		gbc_allDataCheck.gridy = 1;
		this.add(allDataCheck, gbc_allDataCheck);

		hsdCheck = new JCheckBox("HSD");
		GridBagConstraints gbc_hsdCheck = new GridBagConstraints();
		gbc_hsdCheck.anchor = GridBagConstraints.WEST;
		gbc_hsdCheck.gridx = 1;
		gbc_hsdCheck.gridy = 1;
		this.add(hsdCheck, gbc_hsdCheck);

		hssCheck = new JCheckBox("HSS");
		GridBagConstraints gbc_hssCheck = new GridBagConstraints();
		gbc_hssCheck.anchor = GridBagConstraints.WEST;
		gbc_hssCheck.gridx = 2;
		gbc_hssCheck.gridy = 1;
		this.add(hssCheck, gbc_hssCheck);

		fhpCheck = new JCheckBox("FHP");
		GridBagConstraints gbc_fhpCheck = new GridBagConstraints();
		gbc_fhpCheck.anchor = GridBagConstraints.WEST;
		gbc_fhpCheck.gridx = 0;
		gbc_fhpCheck.gridy = 2;
		this.add(fhpCheck, gbc_fhpCheck);

		dhmsmCheck = new JCheckBox("DHMSM");
		GridBagConstraints gbc_dhmsmCheck = new GridBagConstraints();
		gbc_dhmsmCheck.anchor = GridBagConstraints.WEST;
		gbc_dhmsmCheck.gridx = 1;
		gbc_dhmsmCheck.gridy = 2;
		this.add(dhmsmCheck, gbc_dhmsmCheck);

		BLUCheckBoxSelectorListener dataCheckBoxListener = new BLUCheckBoxSelectorListener(checkboxListUpdater,dataSelectDropDown,allDataCheck,hsdCheck,hssCheck, fhpCheck,dhmsmCheck);
		allDataCheck.addActionListener(dataCheckBoxListener);
		hsdCheck.addActionListener(dataCheckBoxListener);
		hssCheck.addActionListener(dataCheckBoxListener);
		fhpCheck.addActionListener(dataCheckBoxListener);
		dhmsmCheck.addActionListener(dataCheckBoxListener);
	}

	public void setVisible(boolean visible)
	{
		super.setVisible(visible);
		if(!visible)
		{
			dataSelectDropDown.clearSelection();
		}
	}


	public ArrayList<String> getSelectedData()
	{
		return dataSelectDropDown.getSelectedValues();
	}

}
