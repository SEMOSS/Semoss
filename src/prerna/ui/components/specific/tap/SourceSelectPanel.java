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
import java.util.Collections;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import javax.swing.JCheckBox;
import javax.swing.JPanel;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.main.listener.specific.tap.SourceSelectAllListener;
import prerna.ui.main.listener.specific.tap.SourceSelectListener;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Used in the starter class to create checkboxes that are used to select a source.
 */
@SuppressWarnings("serial")
public class SourceSelectPanel extends JPanel {
	
	public IEngine engine;
	public Hashtable<String, JCheckBox> checkBoxHash = new Hashtable<String, JCheckBox>();
	public JCheckBox selectAllCheck = new JCheckBox();
//	public boolean shouldPopulateData = false;
	
	/**
	 * Constructor for SourceSelectPanel.
	 */
	public SourceSelectPanel(){

	}
	
	/**
	 * Boolean to see if this capability list should populate a data object list.
	 * If true, used for DHMSM page, otherwise used on the Report Generator Source Selector.
	 */
//	public void setShouldPopulateData(boolean shouldPopulateData)
//	{
//		this.shouldPopulateData = shouldPopulateData;
//	}
	
	/**
	 * Gets the list of services via SPARQL query.
	 * Creates checkboxes for each service.
	 */
	public void getCapabilities()
	{
		removeAll();
		checkBoxHash = new Hashtable<String, JCheckBox>();
		Vector <String> names = new Vector<String>();
		try{
		String sparqlQuery = DIHelper.getInstance().getProperty(ConstantsTAP.SOURCE_SELECT_REPORT_QUERY);
		if(sparqlQuery==null)
			return;
		Hashtable<String, String> paramTable = new Hashtable<String, String>();
		String entityNS = DIHelper.getInstance().getProperty("Capability"+Constants.CLASS);
		paramTable.put(Constants.ENTITY, entityNS );
		sparqlQuery = Utility.fillParam(sparqlQuery, paramTable);	
		
		names = engine.getEntityOfType(sparqlQuery);
		Hashtable<String, String> paramHash = Utility.getInstanceNameViaQuery(names);
		Set<String> nameC = paramHash.keySet();
		names = new Vector<String>(nameC);
		Collections.sort(names);
		removeAll();
		repaint();
		if(names.size()>0)
			createCheckBoxes(names);
		}catch(RuntimeException e){}
	}
	
	/**
	 * Creates check boxes for all of the capabilities.
	 * @param capabilityV 	Vector containing all of the capabilities.
	 */
	public void createCheckBoxes(Vector<String> capabilityV)
	{
		GridBagLayout gridBagLayout = new GridBagLayout();
		this.setLayout(gridBagLayout);
		GridBagConstraints gbc_element = new GridBagConstraints();
		gbc_element = new GridBagConstraints();	
		gbc_element.anchor = GridBagConstraints.WEST;
		gbc_element.insets = new Insets(0, 5, 5, 5);
		gbc_element.gridx = 0;
		gbc_element.gridy = 0;
		
		gbc_element.anchor = GridBagConstraints.WEST;
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
		
		SourceSelectListener ssListener = new SourceSelectListener();
		ssListener.setSelectAllCheck(selectAllCheck);
		
		for (int i=0; i < capabilityV.size(); i++)
		{
			JCheckBox checkBox= new JCheckBox((String)capabilityV.get(i));
			checkBox.setFont(new Font("Tahoma", Font.PLAIN, 12));
			checkBox.setSelected(true);
			checkBox.addActionListener(ssListener);
			checkBoxHash.put((String)capabilityV.get(i), checkBox);
			gbc_element = new GridBagConstraints();	
			gbc_element.anchor = GridBagConstraints.WEST;
			//gbc_element.fill = GridBagConstraints.BOTH;
			gbc_element.insets = new Insets(0, 5, 5, 5);
			gbc_element.gridx = 0;
			gbc_element.gridy = i+2;
			this.add(checkBox, gbc_element);
		}
		
		SourceSelectAllListener ssaListener = new SourceSelectAllListener();
		ssaListener.setCheckHash(checkBoxHash);
		selectAllCheck.addActionListener(ssaListener);

	}

	
}
