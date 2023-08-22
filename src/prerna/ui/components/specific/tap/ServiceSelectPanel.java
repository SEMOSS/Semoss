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
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;

import prerna.engine.api.IDatabaseEngine;
import prerna.ui.main.listener.specific.tap.ServiceSelectAllListener;
import prerna.ui.main.listener.specific.tap.ServiceSelectListener;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class is used to select services using checkboxes.
 */
@SuppressWarnings("serial")
public class ServiceSelectPanel extends JPanel {
	public IDatabaseEngine engine;
	public Hashtable<String, JCheckBox> checkBoxHash = new Hashtable<String, JCheckBox>();
	public JCheckBox selectAllCheck = new JCheckBox();
	
	/**
	 * Constructor for ServiceSelectPanel.
	 */
	public ServiceSelectPanel(){

	}
	
	/**
	 * Gets the name of services via SPARQL query.
	 */
	public void getServices()
	{
		Vector <String> names = new Vector<String>();
		String sparqlQuery = DIHelper.getInstance().getProperty(
				"TYPE" + "_" + Constants.QUERY);
		
		Hashtable<String, List<Object>> paramTable = new Hashtable<String, List<Object>>();
		String entityNS = DIHelper.getInstance().getProperty("Service"+Constants.CLASS);
		List<Object> valueList = new ArrayList<Object>();
		valueList.add(entityNS);
		paramTable.put(Constants.ENTITY, valueList);
		sparqlQuery = Utility.fillParam(sparqlQuery, paramTable);	
		
		names = Utility.getVectorOfReturn(sparqlQuery,engine, true);
		Hashtable<String, String> paramHash = Utility.getInstanceNameViaQuery(names);
		Set<String> nameC = paramHash.keySet();
		names = new Vector<String>(nameC);
		Collections.sort(names);
		removeAll();
		createCheckBoxes(names);
	}
	
	/**
	 * Method createCheckBoxes.
	 * @param serviceV 	Vector containing a list of all the services.
	 */
	public void createCheckBoxes(Vector<String> serviceV)
	{
		GridBagLayout gridBagLayout = new GridBagLayout();
		this.setLayout(gridBagLayout);
		GridBagConstraints gbc_element = new GridBagConstraints();
		gbc_element = new GridBagConstraints();	
		gbc_element.anchor = GridBagConstraints.WEST;
		//gbc_element.fill = GridBagConstraints.BOTH;
		gbc_element.insets = new Insets(0, 5, 5, 5);
		gbc_element.gridx = 0;
		gbc_element.gridy = 0;
		JLabel label = new JLabel("Services");
		label.setFont(new Font("Tahoma", Font.PLAIN, 12));
		this.add(label, gbc_element);
		
		gbc_element.anchor = GridBagConstraints.WEST;
		//gbc_element.fill = GridBagConstraints.BOTH;
		gbc_element.insets = new Insets(0, 5, 5, 5);
		gbc_element.gridx = 0;
		gbc_element.gridy = 1;
		selectAllCheck= new JCheckBox("Select All");
		selectAllCheck.setSelected(true);
		selectAllCheck.setFont(new Font("Tahoma", Font.PLAIN, 12));
		Font f = selectAllCheck.getFont();
		Font newF = new Font(f.getName(), Font.BOLD, f.getSize());
		selectAllCheck.setFont(newF);
		this.add(selectAllCheck, gbc_element);
		
		ServiceSelectListener ssListener = new ServiceSelectListener();
		ssListener.setSelectAllCheck(selectAllCheck);
		
		for (int i=0; i < serviceV.size(); i++)
		{
			JCheckBox checkBox= new JCheckBox((String)serviceV.get(i));
			checkBox.setFont(new Font("Tahoma", Font.PLAIN, 12));
			checkBox.setSelected(true);
			checkBox.addActionListener(ssListener);
			checkBoxHash.put((String)serviceV.get(i), checkBox);
			gbc_element = new GridBagConstraints();	
			gbc_element.anchor = GridBagConstraints.WEST;
			//gbc_element.fill = GridBagConstraints.BOTH;
			gbc_element.insets = new Insets(0, 5, 5, 5);
			gbc_element.gridx = 0;
			gbc_element.gridy = i+2;
			this.add(checkBox, gbc_element);
		}
		
		ServiceSelectAllListener ssaListener = new ServiceSelectAllListener();
		ssaListener.setCheckHash(checkBoxHash);
		selectAllCheck.addActionListener(ssaListener);
	}

	
}
