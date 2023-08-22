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

import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.ListSelectionModel;

import prerna.engine.api.IDatabaseEngine;
import prerna.ui.main.listener.specific.tap.SystemCheckBoxSelectorListener;
import prerna.ui.swing.custom.SelectScrollList;

@SuppressWarnings("serial")
public class DHMSMSystemSelectPanel extends JPanel {
	
	private Boolean includeMHSEHR = false;
	private String header = "Select Systems:";
	
	private JCheckBox lpiSysCheckBox, lpniSysCheckBox, highSysCheckBox, theaterSysCheckBox, garrisonSysCheckBox, faaCheckBox, notFAACheckBox, mhsSpecificCheckBox, ehrCoreCheckBox;
	private SelectScrollList sysSelectDropDown;
	private SystemCheckBoxSelectorListener sysCheckBoxListener;
	
	public DHMSMSystemSelectPanel() {
		createView(null);
	}
	
	public DHMSMSystemSelectPanel(IDatabaseEngine engine) {
		SysOptCheckboxListUpdater checkboxListUpdater = new SysOptCheckboxListUpdater(engine, true, false, false);
		createView(checkboxListUpdater);
	}
	
	public DHMSMSystemSelectPanel(SysOptCheckboxListUpdater checkboxListUpdater) {
		createView(checkboxListUpdater);
	}
	
	public DHMSMSystemSelectPanel(String header, Boolean includeMHSEHR, SysOptCheckboxListUpdater checkboxListUpdater)
	{
		this.header = header;
		this.includeMHSEHR = includeMHSEHR;
		createView(checkboxListUpdater);
	}
	
	public void changeEngine(IDatabaseEngine engine) {
		SysOptCheckboxListUpdater checkboxListUpdater = new SysOptCheckboxListUpdater(engine, true, false, false);
		removeListeners();
		sysSelectDropDown.resetList(checkboxListUpdater.getReceivedSysList());
		addListeners(checkboxListUpdater);
		
	}
	
	private void createView(SysOptCheckboxListUpdater checkboxListUpdater)
	{
		this.removeAll();
		
		GridBagLayout gbl_systemSelectPanel = new GridBagLayout();
		gbl_systemSelectPanel.columnWidths = new int[]{0, 0, 0, 0,};
		gbl_systemSelectPanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0};
		gbl_systemSelectPanel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		gbl_systemSelectPanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
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
		if(checkboxListUpdater == null)
			sysSelectDropDown.setupButton(new Vector<String>(),40,120); //need to give list of all systems
		else
			sysSelectDropDown.setupButton(checkboxListUpdater.getReceivedSysList(),40,120); //need to give list of all systems
//		sysSelectDropDown.setupButton(playSheet.getCheckboxListUpdater().getReceivedSysList(),40,120); //need to give list of all systems

		GridBagConstraints gbc_sysSelectDropDown = new GridBagConstraints();
		gbc_sysSelectDropDown.gridwidth = 5;
		gbc_sysSelectDropDown.insets = new Insets(0, 0, 0, 5);
		gbc_sysSelectDropDown.fill = GridBagConstraints.HORIZONTAL;
		gbc_sysSelectDropDown.gridx = 0;
		gbc_sysSelectDropDown.gridy = 3;
		this.add(sysSelectDropDown.pane, gbc_sysSelectDropDown);
		
		addCheckBoxes(checkboxListUpdater);
		if(checkboxListUpdater != null)
			addListeners(checkboxListUpdater);
		
	}
	private void addCheckBoxes(SysOptCheckboxListUpdater checkboxListUpdater) {

		lpiSysCheckBox = new JCheckBox("LPI");
		lpiSysCheckBox.setName("lpiSysCheckBox");
		GridBagConstraints gbc_lpiSysCheckBox = new GridBagConstraints();
		gbc_lpiSysCheckBox.anchor = GridBagConstraints.WEST;
		gbc_lpiSysCheckBox.gridx = 0;
		gbc_lpiSysCheckBox.gridy = 1;
		this.add(lpiSysCheckBox, gbc_lpiSysCheckBox);
		
		lpniSysCheckBox = new JCheckBox("LPNI");
		lpniSysCheckBox.setName("lpniSysCheckBox");
		GridBagConstraints gbc_lpniSysCheckBox = new GridBagConstraints();
		gbc_lpniSysCheckBox.anchor = GridBagConstraints.WEST;
		gbc_lpniSysCheckBox.gridx = 0;
		gbc_lpniSysCheckBox.gridy = 2;
		this.add(lpniSysCheckBox, gbc_lpniSysCheckBox);
		
		highSysCheckBox = new JCheckBox("High");
		highSysCheckBox.setName("highSysCheckBox");
		GridBagConstraints gbc_highSysCheckBox = new GridBagConstraints();
		gbc_highSysCheckBox.anchor = GridBagConstraints.WEST;
		gbc_highSysCheckBox.gridx = 1;
		gbc_highSysCheckBox.gridy = 1;
		this.add(highSysCheckBox, gbc_highSysCheckBox);

		theaterSysCheckBox = new JCheckBox("Theater");
		theaterSysCheckBox.setName("theaterSysCheckBox");
		GridBagConstraints gbc_theaterSysButton = new GridBagConstraints();
		gbc_theaterSysButton.anchor = GridBagConstraints.WEST;
		gbc_theaterSysButton.gridx = 2;
		gbc_theaterSysButton.gridy = 1;
		this.add(theaterSysCheckBox, gbc_theaterSysButton);
		
		garrisonSysCheckBox = new JCheckBox("Garrison");
		garrisonSysCheckBox.setName("garrisonSysCheckBox");
		GridBagConstraints gbc_garrisonSysButton = new GridBagConstraints();
		gbc_garrisonSysButton.anchor = GridBagConstraints.WEST;
		gbc_garrisonSysButton.gridx = 2;
		gbc_garrisonSysButton.gridy = 2;
		this.add(garrisonSysCheckBox, gbc_garrisonSysButton);
		
		faaCheckBox = new JCheckBox("FAA");
		faaCheckBox.setName("faaCheckBox");
		GridBagConstraints gbc_faaCheckBox = new GridBagConstraints();
		gbc_faaCheckBox.anchor = GridBagConstraints.WEST;
		gbc_faaCheckBox.gridx = 3;
		gbc_faaCheckBox.gridy = 1;
		this.add(faaCheckBox, gbc_faaCheckBox);
		
		notFAACheckBox = new JCheckBox("Not FAA");
		notFAACheckBox.setName("notFAACheckBox");
		GridBagConstraints gbc_notFAACheckBox = new GridBagConstraints();
		gbc_notFAACheckBox.anchor = GridBagConstraints.WEST;
		gbc_notFAACheckBox.gridx = 3;
		gbc_notFAACheckBox.gridy = 2;
		this.add(notFAACheckBox, gbc_notFAACheckBox);
		
		if(includeMHSEHR) {
			mhsSpecificCheckBox = new JCheckBox("MHS Specific");
			mhsSpecificCheckBox.setName("mhsSpecificCheckBox");
			GridBagConstraints gbc_mhsSpecificCheckBox = new GridBagConstraints();
			gbc_mhsSpecificCheckBox.anchor = GridBagConstraints.WEST;
			gbc_mhsSpecificCheckBox.gridx = 4;
			gbc_mhsSpecificCheckBox.gridy = 1;
			this.add(mhsSpecificCheckBox, gbc_mhsSpecificCheckBox);
			
			ehrCoreCheckBox = new JCheckBox("EHR Core");
			ehrCoreCheckBox.setName("ehrCoreCheckBox");
			GridBagConstraints gbc_ehrCoreCheckBox = new GridBagConstraints();
			gbc_ehrCoreCheckBox.anchor = GridBagConstraints.WEST;
			gbc_ehrCoreCheckBox.gridx = 4;
			gbc_ehrCoreCheckBox.gridy = 2;
			this.add(ehrCoreCheckBox, gbc_ehrCoreCheckBox);
		}

	}
	
	private void addListeners(SysOptCheckboxListUpdater checkboxListUpdater) {
		sysCheckBoxListener = new SystemCheckBoxSelectorListener(checkboxListUpdater, sysSelectDropDown, lpiSysCheckBox, lpniSysCheckBox, highSysCheckBox,theaterSysCheckBox,garrisonSysCheckBox, faaCheckBox, notFAACheckBox, mhsSpecificCheckBox,ehrCoreCheckBox);
		lpiSysCheckBox.addActionListener(sysCheckBoxListener);
		lpniSysCheckBox.addActionListener(sysCheckBoxListener);
		highSysCheckBox.addActionListener(sysCheckBoxListener);
		theaterSysCheckBox.addActionListener(sysCheckBoxListener);
		garrisonSysCheckBox.addActionListener(sysCheckBoxListener);
		faaCheckBox.addActionListener(sysCheckBoxListener);
		notFAACheckBox.addActionListener(sysCheckBoxListener);
		if(includeMHSEHR) {
			mhsSpecificCheckBox.addActionListener(sysCheckBoxListener);
			ehrCoreCheckBox.addActionListener(sysCheckBoxListener);
		}
	}
	
	private void removeListeners() {
		lpiSysCheckBox.removeActionListener(sysCheckBoxListener);
		lpniSysCheckBox.removeActionListener(sysCheckBoxListener);
		highSysCheckBox.removeActionListener(sysCheckBoxListener);
		theaterSysCheckBox.removeActionListener(sysCheckBoxListener);
		garrisonSysCheckBox.removeActionListener(sysCheckBoxListener);
		faaCheckBox.removeActionListener(sysCheckBoxListener);
		notFAACheckBox.removeActionListener(sysCheckBoxListener);
		if(includeMHSEHR) {
			mhsSpecificCheckBox.removeActionListener(sysCheckBoxListener);
			ehrCoreCheckBox.removeActionListener(sysCheckBoxListener);
		}
	}

	//TODO change the name of this
	private void clearList() {
		lpiSysCheckBox.setSelected(false);
		lpniSysCheckBox.setSelected(false);
		highSysCheckBox.setSelected(false);
		theaterSysCheckBox.setSelected(false);
		garrisonSysCheckBox.setSelected(false);
		faaCheckBox.setSelected(false);
		notFAACheckBox.setSelected(false);
		if(includeMHSEHR) {
			mhsSpecificCheckBox.setSelected(false);
			ehrCoreCheckBox.setSelected(false);
		}
		sysSelectDropDown.clearSelection();
	}
	
	public void selectAllSystems()
	{
		sysSelectDropDown.selectAll();
	}
	
	public ArrayList<String> getSelectedSystems()
	{
		return sysSelectDropDown.getSelectedValues();
	}

	public Boolean isTheaterCheckBoxSelected() {
		return theaterSysCheckBox.isSelected();
	}
	
	public Boolean isGarrisonCheckBoxSelected() {
		return garrisonSysCheckBox.isSelected();
	}

}
