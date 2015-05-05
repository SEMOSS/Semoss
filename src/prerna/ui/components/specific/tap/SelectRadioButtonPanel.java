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
import java.util.Collections;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import javax.swing.ButtonGroup;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;

/**
 * Used in the starter class to create checkboxes that are used to select a source.
 */
@SuppressWarnings("serial")
public class SelectRadioButtonPanel extends JPanel {
	
	public IEngine engine;
	public Hashtable<String, JRadioButton> radioIntegratedBoxHash = new Hashtable<String, JRadioButton>();
	public Hashtable<String, JRadioButton> radioHybridBoxHash = new Hashtable<String, JRadioButton>();
	public Hashtable<String, JRadioButton> radioManualBoxHash = new Hashtable<String, JRadioButton>();
	
	public Hashtable<String, JRadioButton> radioRealBoxHash = new Hashtable<String, JRadioButton>();
	public Hashtable<String, JRadioButton> radioNearBoxHash = new Hashtable<String, JRadioButton>();
	public Hashtable<String, JRadioButton> radioArchiveBoxHash = new Hashtable<String, JRadioButton>();
	public Hashtable<String, JRadioButton> radioIgnoreBoxHash = new Hashtable<String, JRadioButton>();
	
	public Hashtable<String, String> dataToDataAccessHash;
	public Hashtable<String, String> dataToDataLatencyHash;
	/**
	 * Constructor for SourceSelectPanel.
	 */
	public SelectRadioButtonPanel(){

	}
	
	public void clear()
	{
		removeAll();
	}
	/**
	 * Gets the list of services via SPARQL query.
	 * Creates checkboxes for each service.
	 */
	public void getDataObjectsFromCapabilities(ArrayList<String> capabilities)
	{
		removeAll();

		radioIntegratedBoxHash = new Hashtable<String, JRadioButton>();
		radioHybridBoxHash = new Hashtable<String, JRadioButton>();
		radioManualBoxHash = new Hashtable<String, JRadioButton>();
		
		radioRealBoxHash = new Hashtable<String, JRadioButton>();
		radioNearBoxHash = new Hashtable<String, JRadioButton>();
		radioArchiveBoxHash = new Hashtable<String, JRadioButton>();
		radioIgnoreBoxHash = new Hashtable<String, JRadioButton>();
		
		dataToDataAccessHash = new Hashtable<String,String>();
		dataToDataLatencyHash = new Hashtable<String,String>();

		runCapabilityQuery(capabilities);
		Vector<String> names = makeVectorOfDataObjects(dataToDataAccessHash,dataToDataLatencyHash);

		Collections.sort(names);
		removeAll();
		repaint();
		if(names.size()>0)
			createCheckBoxes(names);
	}
	public Vector<String> makeVectorOfDataObjects(Hashtable<String,String> dataToDataAccessHash,Hashtable<String,String> dataToDataLatencyHash)
	{
		Vector<String> retVect = new Vector<String>(dataToDataAccessHash.keySet());
		for(String key : dataToDataLatencyHash.keySet())
		{
			if(!retVect.contains(key))
				retVect.add(key);
		}
		return retVect;
	}
	public void getDataObjectsFromHashes(Hashtable<String, String> dataAccessTypeHash,Hashtable<String, String>dataLatencyTypeHash)
	{
		removeAll();

		radioIntegratedBoxHash = new Hashtable<String, JRadioButton>();
		radioHybridBoxHash = new Hashtable<String, JRadioButton>();
		radioManualBoxHash = new Hashtable<String, JRadioButton>();
		
		radioRealBoxHash = new Hashtable<String, JRadioButton>();
		radioNearBoxHash = new Hashtable<String, JRadioButton>();
		radioArchiveBoxHash = new Hashtable<String, JRadioButton>();
		radioIgnoreBoxHash = new Hashtable<String, JRadioButton>();
		
		dataToDataAccessHash=dataAccessTypeHash;
		dataToDataLatencyHash=dataLatencyTypeHash;
		Set<String> nameC = dataToDataAccessHash.keySet();
		Vector<String> names = new Vector<String>(nameC);
		Collections.sort(names);
		removeAll();
		repaint();
		if(names.size()>0)
			createCheckBoxes(names);
		
	}
	
	public void runCapabilityQuery(ArrayList<String> capabilities)
	{
		String capabilityBindings = "";
		for(String capability : capabilities)
		{
			capabilityBindings += "(<http://health.mil/ontologies/Concept/Capability/"+capability+">)";
		}
		String query = "SELECT DISTINCT ?Data ?Crm WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Task ?Needs ?Data.} } BINDINGS ?Capability {@CAPABILITY-BINDINGS@}";
		query = query.replace("@CAPABILITY-BINDINGS@", capabilityBindings);
		
		ISelectWrapper sjsw = WrapperManager.getInstance().getSWrapper(engine, query);

		/*SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.executeQuery();
		sjsw.getVariables();
		*/
		
		String[] vars = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			String data = sjss.getVar(vars[0]).toString();
			String crm = sjss.getVar(vars[1]).toString();
			//cap reads then access is integrated and realtime
			//cap creates then access is hybrid and realtime
			if(crm.contains("C") || crm.contains("M"))
			{
				dataToDataAccessHash.put(data,"Hybrid");
				dataToDataLatencyHash.put(data,"Real");
			}
			else if(crm.contains("R")&&!dataToDataAccessHash.containsKey(data))
			{
				dataToDataAccessHash.put(data,"Integrated");
				dataToDataLatencyHash.put(data,"Real");
			}
		}
	}
	
	/**
	 * Creates radio buttons for all of the data objects.
	 * @param dataV 	Vector containing all of the data objects.
	 */
	public void createCheckBoxes(Vector<String> dataV)
	{
		GridBagLayout gridBagLayout = new GridBagLayout();
		this.setLayout(gridBagLayout);
		GridBagConstraints gbc_element = new GridBagConstraints();
		gbc_element = new GridBagConstraints();	
		gbc_element.anchor = GridBagConstraints.WEST;
		gbc_element.insets = new Insets(0, 5, 5, 5);
		gbc_element.gridx = 0;
		gbc_element.gridy = 0;
		
		JLabel labelData= new JLabel("Data Object");
		labelData.setFont(new Font("Tahoma", Font.BOLD, 12));

		JLabel labelIntegrated= new JLabel("Integrated");
		labelIntegrated.setFont(new Font("Tahoma", Font.BOLD, 12));
		JLabel labelHybrid= new JLabel("Hybrid");
		labelHybrid.setFont(new Font("Tahoma", Font.BOLD, 12));
		JLabel labelManual= new JLabel("Manual");
		labelManual.setFont(new Font("Tahoma", Font.BOLD, 12));
		
		JLabel labelReal= new JLabel("RealTime");
		labelReal.setFont(new Font("Tahoma", Font.BOLD, 12));
		JLabel labelNear= new JLabel("Near RealTime");
		labelNear.setFont(new Font("Tahoma", Font.BOLD, 12));
		JLabel labelArchive= new JLabel("Archive");
		labelArchive.setFont(new Font("Tahoma", Font.BOLD, 12));
		JLabel labelIgnore= new JLabel("Ignore");
		labelIgnore.setFont(new Font("Tahoma", Font.BOLD, 12));
		
		GridBagConstraints gbc_elementLabelData = new GridBagConstraints();	
		gbc_elementLabelData.anchor = GridBagConstraints.WEST;
		gbc_elementLabelData.insets = new Insets(0, 5, 5, 5);
		gbc_elementLabelData.gridx = 0;
		gbc_elementLabelData.gridy = 1;
		this.add(labelData, gbc_elementLabelData);
		
		GridBagConstraints gbc_elementLabelIntegrated = new GridBagConstraints();	
		gbc_elementLabelIntegrated.anchor = GridBagConstraints.WEST;
		gbc_elementLabelIntegrated.insets = new Insets(0, 5, 5, 5);
		gbc_elementLabelIntegrated.gridx = 1;
		gbc_elementLabelIntegrated.gridy = 1;
		this.add(labelIntegrated, gbc_elementLabelIntegrated);
		
		GridBagConstraints gbc_elementLabelHybrid = new GridBagConstraints();	
		gbc_elementLabelHybrid.anchor = GridBagConstraints.WEST;
		gbc_elementLabelHybrid.insets = new Insets(0, 5, 5, 5);
		gbc_elementLabelHybrid.gridx = 2;
		gbc_elementLabelHybrid.gridy = 1;
		this.add(labelHybrid, gbc_elementLabelHybrid);
		
		GridBagConstraints gbc_elementLabelManual = new GridBagConstraints();	
		gbc_elementLabelManual.anchor = GridBagConstraints.WEST;
		gbc_elementLabelManual.insets = new Insets(0, 5, 5, 5);
		gbc_elementLabelManual.gridx = 3;
		gbc_elementLabelManual.gridy = 1;
		this.add(labelManual, gbc_elementLabelManual);

		GridBagConstraints gbc_elementLabelReal = new GridBagConstraints();	
		gbc_elementLabelReal.anchor = GridBagConstraints.WEST;
		gbc_elementLabelReal.insets = new Insets(0, 5, 5, 5);
		gbc_elementLabelReal.gridx = 4;
		gbc_elementLabelReal.gridy = 1;
		this.add(labelReal, gbc_elementLabelReal);
		
		GridBagConstraints gbc_elementLabelNear = new GridBagConstraints();	
		gbc_elementLabelNear.anchor = GridBagConstraints.WEST;
		gbc_elementLabelNear.insets = new Insets(0, 5, 5, 5);
		gbc_elementLabelNear.gridx = 5;
		gbc_elementLabelNear.gridy = 1;
		this.add(labelNear, gbc_elementLabelNear);
		
		GridBagConstraints gbc_elementLabelArchive = new GridBagConstraints();	
		gbc_elementLabelArchive.anchor = GridBagConstraints.WEST;
		gbc_elementLabelArchive.insets = new Insets(0, 5, 5, 5);
		gbc_elementLabelArchive.gridx = 6;
		gbc_elementLabelArchive.gridy = 1;
		this.add(labelArchive, gbc_elementLabelArchive);
		
		GridBagConstraints gbc_elementLabelIgnore = new GridBagConstraints();	
		gbc_elementLabelIgnore.anchor = GridBagConstraints.WEST;
		gbc_elementLabelIgnore.insets = new Insets(0, 5, 5, 5);
		gbc_elementLabelIgnore.gridx = 7;
		gbc_elementLabelIgnore.gridy = 1;
		this.add(labelIgnore, gbc_elementLabelIgnore);
		
		
		for (int i=0; i < dataV.size(); i++)
		{
			String data = (String)dataV.get(i);
			JLabel label= new JLabel(data);
			ButtonGroup dataAccessButtonGroup = new ButtonGroup();		
			JRadioButton radioIntegratedButton = new JRadioButton();//"integrated
			radioIntegratedButton.setFont(new Font("Tahoma", Font.PLAIN, 12));
			JRadioButton radioHybridButton = new JRadioButton();//"hybrid
			radioHybridButton.setFont(new Font("Tahoma", Font.PLAIN, 12));
			JRadioButton radioManualButton = new JRadioButton();//manual
			radioManualButton.setFont(new Font("Tahoma", Font.PLAIN, 12));
			String access = (String)dataToDataAccessHash.get(data);
			if(access==null)
				access = "Integrated";
			if(access.equals("Integrated"))
				radioIntegratedButton.setSelected(true);
			else if(access.equals("Hybrid"))
				radioHybridButton.setSelected(true);
			else
				radioManualButton.setSelected(true);

			ButtonGroup dataLatencyButtonGroup = new ButtonGroup();
			JRadioButton radioRealButton = new JRadioButton();//"Real Time");
			radioRealButton.setFont(new Font("Tahoma", Font.PLAIN, 12));
			JRadioButton radioNearButton = new JRadioButton();//"Near Real Time");
			radioNearButton.setFont(new Font("Tahoma", Font.PLAIN, 12));
			JRadioButton radioArchiveButton = new JRadioButton();//"Archived");
			radioArchiveButton.setFont(new Font("Tahoma", Font.PLAIN, 12));
			JRadioButton radioIgnoreButton = new JRadioButton();//"Ignore");
			radioIgnoreButton.setFont(new Font("Tahoma", Font.PLAIN, 12));
			
			String latency = (String)dataToDataLatencyHash.get(data);
			if(latency==null)
				latency = "Integrated";
			if(latency.equals("Real"))
				radioRealButton.setSelected(true);
			else if(latency.equals("NearReal"))
				radioNearButton.setSelected(true);
			else if(latency.equals("Archive"))
				radioArchiveButton.setSelected(true);
			else
				radioIgnoreButton.setSelected(true);
			
			dataAccessButtonGroup.add(radioIntegratedButton);
			dataAccessButtonGroup.add(radioHybridButton);
			dataAccessButtonGroup.add(radioManualButton);
			dataLatencyButtonGroup.add(radioRealButton);
			dataLatencyButtonGroup.add(radioNearButton);
			dataLatencyButtonGroup.add(radioArchiveButton);
			dataLatencyButtonGroup.add(radioIgnoreButton);
			
			radioIntegratedBoxHash.put((String)dataV.get(i),radioIntegratedButton);
			radioHybridBoxHash.put((String)dataV.get(i),radioHybridButton);
			radioManualBoxHash.put((String)dataV.get(i),radioManualButton);
			radioRealBoxHash.put((String)dataV.get(i),radioRealButton);
			radioNearBoxHash.put((String)dataV.get(i),radioNearButton);
			radioArchiveBoxHash.put((String)dataV.get(i),radioArchiveButton);
			radioIgnoreBoxHash.put((String)dataV.get(i),radioIgnoreButton);
			
			GridBagConstraints gbc_elementLabel = new GridBagConstraints();	
			gbc_elementLabel.anchor = GridBagConstraints.WEST;
			gbc_elementLabel.insets = new Insets(0, 5, 5, 5);
			gbc_elementLabel.gridx = 0;
			gbc_elementLabel.gridy = i+2;
			this.add(label, gbc_elementLabel);
			
			GridBagConstraints gbc_elementRadioIntegrated = new GridBagConstraints();	
			gbc_elementRadioIntegrated.anchor = GridBagConstraints.CENTER;
			gbc_elementRadioIntegrated.insets = new Insets(0, 5, 5, 5);
			gbc_elementRadioIntegrated.gridx = 1;
			gbc_elementRadioIntegrated.gridy = i+2;
			this.add(radioIntegratedButton, gbc_elementRadioIntegrated);
			
			GridBagConstraints gbc_elementRadioHybrid = new GridBagConstraints();	
			gbc_elementRadioHybrid.anchor = GridBagConstraints.CENTER;
			gbc_elementRadioHybrid.insets = new Insets(0, 5, 5, 5);
			gbc_elementRadioHybrid.gridx = 2;
			gbc_elementRadioHybrid.gridy = i+2;
			this.add(radioHybridButton, gbc_elementRadioHybrid);
			
			GridBagConstraints gbc_elementRadioManual = new GridBagConstraints();	
			gbc_elementRadioManual.anchor = GridBagConstraints.CENTER;
			gbc_elementRadioManual.insets = new Insets(0, 5, 5, 5);
			gbc_elementRadioManual.gridx = 3;
			gbc_elementRadioManual.gridy = i+2;
			this.add(radioManualButton, gbc_elementRadioManual);
			
			GridBagConstraints gbc_elementRadioReal = new GridBagConstraints();	
			gbc_elementRadioReal.anchor = GridBagConstraints.CENTER;
			gbc_elementRadioReal.insets = new Insets(0, 5, 5, 5);
			gbc_elementRadioReal.gridx = 4;
			gbc_elementRadioReal.gridy = i+2;
			this.add(radioRealButton, gbc_elementRadioReal);
			
			GridBagConstraints gbc_elementRadioNear = new GridBagConstraints();	
			gbc_elementRadioNear.anchor = GridBagConstraints.CENTER;
			gbc_elementRadioNear.insets = new Insets(0, 5, 5, 5);
			gbc_elementRadioNear.gridx = 5;
			gbc_elementRadioNear.gridy = i+2;
			this.add(radioNearButton, gbc_elementRadioNear);
			
			GridBagConstraints gbc_elementRadioArchive = new GridBagConstraints();	
			gbc_elementRadioArchive.anchor = GridBagConstraints.CENTER;
			gbc_elementRadioArchive.insets = new Insets(0, 5, 5, 5);
			gbc_elementRadioArchive.gridx = 6;
			gbc_elementRadioArchive.gridy = i+2;
			this.add(radioArchiveButton, gbc_elementRadioArchive);
			
			GridBagConstraints gbc_elementRadioIgnore = new GridBagConstraints();	
			gbc_elementRadioIgnore.anchor = GridBagConstraints.CENTER;
			gbc_elementRadioIgnore.insets = new Insets(0, 5, 5, 5);
			gbc_elementRadioIgnore.gridx = 7;
			gbc_elementRadioIgnore.gridy = i+2;
			this.add(radioIgnoreButton, gbc_elementRadioIgnore);
		}
		
	}

	
}
