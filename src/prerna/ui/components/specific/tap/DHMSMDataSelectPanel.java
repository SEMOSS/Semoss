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
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.ListSelectionModel;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.helpers.EntityFiller;
import prerna.ui.main.listener.specific.tap.DataCheckBoxSelectorListener;
import prerna.ui.swing.custom.CustomButton;
import prerna.ui.swing.custom.SelectScrollList;
import prerna.util.DIHelper;
import aurelienribon.ui.css.Style;

@SuppressWarnings("serial")
public class DHMSMDataSelectPanel extends JPanel {
	public IEngine engine;

	public JLabel lblDataSelectHeader;
	public SelectScrollList dataSelectDropDown;
	public JCheckBox hsdCheck, hssCheck, fhpCheck, dhmsmCheck, allDataCheck;
	public JButton updateProvideDataButton, updateConsumeDataButton;

	public DHMSMDataSelectPanel()
	{
		//addElements();
	}

	public void addElements(DHMSMSystemSelectPanel systemSelectPanel)
	{

		this.removeAll();
		GridBagLayout gbl_systemSelectPanel = new GridBagLayout();
		gbl_systemSelectPanel.columnWidths = new int[]{0, 0, 0, 0,};
		gbl_systemSelectPanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0};
		gbl_systemSelectPanel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		gbl_systemSelectPanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		this.setLayout(gbl_systemSelectPanel);

		JLabel lblSystemSelectHeader = new JLabel("Select Data Objects:");
		lblSystemSelectHeader.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblSystemSelectHeader = new GridBagConstraints();
		gbc_lblSystemSelectHeader.gridwidth = 4;
		gbc_lblSystemSelectHeader.anchor = GridBagConstraints.WEST;
		gbc_lblSystemSelectHeader.insets = new Insets(10, 0, 5, 5);
		gbc_lblSystemSelectHeader.gridx = 0;
		gbc_lblSystemSelectHeader.gridy = 0;
		this.add(lblSystemSelectHeader, gbc_lblSystemSelectHeader);

		allDataCheck = new JCheckBox("All");
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

		dataSelectDropDown = new SelectScrollList("Select Individual Data");
		dataSelectDropDown.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
		GridBagConstraints gbc_dataSelectDropDown = new GridBagConstraints();
		gbc_dataSelectDropDown.gridwidth = 3;
		gbc_dataSelectDropDown.fill = GridBagConstraints.HORIZONTAL;
		gbc_dataSelectDropDown.insets = new Insets(0, 0, 0, 5);
		gbc_dataSelectDropDown.gridx = 0;
		gbc_dataSelectDropDown.gridy = 3;
		this.add(dataSelectDropDown.pane, gbc_dataSelectDropDown);

		String[] dataArray = makeListFromQuery("DataObject","SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}}");
		dataSelectDropDown.setupButton(dataArray,100,120); 
		dataSelectDropDown.setVisible(true);

		DataCheckBoxSelectorListener dataCheckBoxListener = new DataCheckBoxSelectorListener(engine, dataSelectDropDown,allDataCheck,hsdCheck,hssCheck, fhpCheck,dhmsmCheck);
		allDataCheck.addActionListener(dataCheckBoxListener);
		hsdCheck.addActionListener(dataCheckBoxListener);
		hssCheck.addActionListener(dataCheckBoxListener);
		fhpCheck.addActionListener(dataCheckBoxListener);
		dhmsmCheck.addActionListener(dataCheckBoxListener);

		updateProvideDataButton = new CustomButton("Select Provide");
		updateProvideDataButton.setName("updateProvideDataButton");
		updateProvideDataButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(updateProvideDataButton,  ".toggleButton");
		updateProvideDataButton.setVisible(true);		

		GridBagConstraints gbc_updateProvideDataButton = new GridBagConstraints();
		gbc_updateProvideDataButton.anchor = GridBagConstraints.WEST;
		gbc_updateProvideDataButton.gridheight = 2;
		gbc_updateProvideDataButton.insets = new Insets(5, 0, 5, 5);
		gbc_updateProvideDataButton.gridx = 10;
		gbc_updateProvideDataButton.gridy = 0;
		this.add(updateProvideDataButton, gbc_updateProvideDataButton);

		updateConsumeDataButton = new CustomButton("Select Consume");
		updateConsumeDataButton.setName("updateConsumeDataButton");
		updateConsumeDataButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(updateConsumeDataButton,  ".toggleButton");
		updateConsumeDataButton.setVisible(true);		

		GridBagConstraints gbc_updateConsumeDataButton = new GridBagConstraints();
		gbc_updateConsumeDataButton.anchor = GridBagConstraints.WEST;
		gbc_updateConsumeDataButton.gridheight = 2;
		gbc_updateConsumeDataButton.insets = new Insets(5, 0, 5, 5);
		gbc_updateConsumeDataButton.gridx = 15;
		gbc_updateConsumeDataButton.gridy = 0;
		this.add(updateConsumeDataButton, gbc_updateConsumeDataButton);	
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

	public void setVisible(boolean visible)
	{
		super.setVisible(visible);
		if(!visible)
		{
			dataSelectDropDown.clearList();
		}
	}


	public ArrayList<String> getSelectedData()
	{
		return dataSelectDropDown.getSelectedValues();
	}

}
