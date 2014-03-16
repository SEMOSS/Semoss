/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
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
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;

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
public class SelectRadioButtonPanel extends JPanel {
	
	public IEngine engine;
	public Hashtable radioRealBoxHash = new Hashtable();
	public Hashtable radioNearBoxHash = new Hashtable();
	public Hashtable radioArchiveBoxHash = new Hashtable();
	
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
	public void getDataObjects(ArrayList<String> capabilities)
	{
		removeAll();
		String capabilityBindings = "";
		for(String capability : capabilities)
		{
			capabilityBindings += "(<http://health.mil/ontologies/Concept/Capability/"+capability+">)";
		}
		Vector <String> names = new Vector<String>();
		try{
		String sparqlQuery = "SELECT DISTINCT ?entity WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> ?Crm;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Task ?Needs ?entity.} } BINDINGS ?Capability {@CAPABILITY-BINDINGS@}";
		sparqlQuery = sparqlQuery.replace("@CAPABILITY-BINDINGS@", capabilityBindings);
		if(sparqlQuery==null)
			return;
		Hashtable paramTable = new Hashtable();
		String entityNS = DIHelper.getInstance().getProperty("DataObject"+Constants.CLASS);
		paramTable.put(Constants.ENTITY, entityNS);
		sparqlQuery = Utility.fillParam(sparqlQuery, paramTable);	
		
		names = engine.getEntityOfType(sparqlQuery);
		Hashtable paramHash = Utility.getInstanceNameViaQuery(names);
		Set nameC = paramHash.keySet();
		names = new Vector(nameC);
		Collections.sort(names);
		removeAll();
		repaint();
		if(names.size()>0)
			createCheckBoxes(names);
		}catch(Exception e){}
	}
	
	/**
	 * Creates radio buttons for all of the data objects.
	 * @param dataV 	Vector containing all of the data objects.
	 */
	public void createCheckBoxes(Vector dataV)
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
		JLabel labelReal= new JLabel("RealTime");
		labelReal.setFont(new Font("Tahoma", Font.BOLD, 12));
		JLabel labelNear= new JLabel("Near RealTime");
		labelNear.setFont(new Font("Tahoma", Font.BOLD, 12));
		JLabel labelArchive= new JLabel("Archived");
		labelArchive.setFont(new Font("Tahoma", Font.BOLD, 12));
		
		GridBagConstraints gbc_elementLabelData = new GridBagConstraints();	
		gbc_elementLabelData.anchor = GridBagConstraints.WEST;
		gbc_elementLabelData.insets = new Insets(0, 5, 5, 5);
		gbc_elementLabelData.gridx = 0;
		gbc_elementLabelData.gridy = 1;
		this.add(labelData, gbc_elementLabelData);
		
		GridBagConstraints gbc_elementLabelReal = new GridBagConstraints();	
		gbc_elementLabelReal.anchor = GridBagConstraints.WEST;
		gbc_elementLabelReal.insets = new Insets(0, 5, 5, 5);
		gbc_elementLabelReal.gridx = 1;
		gbc_elementLabelReal.gridy = 1;
		this.add(labelReal, gbc_elementLabelReal);
		
		GridBagConstraints gbc_elementLabelNear = new GridBagConstraints();	
		gbc_elementLabelNear.anchor = GridBagConstraints.WEST;
		gbc_elementLabelNear.insets = new Insets(0, 5, 5, 5);
		gbc_elementLabelNear.gridx = 2;
		gbc_elementLabelNear.gridy = 1;
		this.add(labelNear, gbc_elementLabelNear);
		
		GridBagConstraints gbc_elementLabelArchive = new GridBagConstraints();	
		gbc_elementLabelArchive.anchor = GridBagConstraints.WEST;
		gbc_elementLabelArchive.insets = new Insets(0, 5, 5, 5);
		gbc_elementLabelArchive.gridx = 3;
		gbc_elementLabelArchive.gridy = 1;
		this.add(labelArchive, gbc_elementLabelArchive);
		
		
		Font f = new Font("Tahoma", Font.PLAIN, 12);
		Font newF = new Font(f.getName(), Font.BOLD, f.getSize());
		
		for (int i=0; i < dataV.size(); i++)
		{
			JLabel label= new JLabel((String)dataV.get(i));
			ButtonGroup buttonGroup = new ButtonGroup();
			JRadioButton radioRealButton = new JRadioButton();//"Real Time");
			JRadioButton radioNearButton = new JRadioButton();//"Near Real Time");
			JRadioButton radioArchiveButton = new JRadioButton();//"Archived");
			radioRealButton.setFont(new Font("Tahoma", Font.PLAIN, 12));
			radioRealButton.setSelected(false);
			radioNearButton.setFont(new Font("Tahoma", Font.PLAIN, 12));
			radioNearButton.setSelected(false);
			radioArchiveButton.setFont(new Font("Tahoma", Font.PLAIN, 12));
			radioArchiveButton.setSelected(false);
			
			buttonGroup.add(radioRealButton);
			buttonGroup.add(radioNearButton);
			buttonGroup.add(radioArchiveButton);
			
			radioRealBoxHash.put((String)dataV.get(i),radioRealButton);
			radioNearBoxHash.put((String)dataV.get(i),radioNearButton);
			radioArchiveBoxHash.put((String)dataV.get(i),radioArchiveButton);
			
			GridBagConstraints gbc_elementLabel = new GridBagConstraints();	
			gbc_elementLabel.anchor = GridBagConstraints.WEST;
			gbc_elementLabel.insets = new Insets(0, 5, 5, 5);
			gbc_elementLabel.gridx = 0;
			gbc_elementLabel.gridy = i+2;
			this.add(label, gbc_elementLabel);
			
			GridBagConstraints gbc_elementRadioReal = new GridBagConstraints();	
			gbc_elementRadioReal.anchor = GridBagConstraints.CENTER;
			gbc_elementRadioReal.insets = new Insets(0, 5, 5, 5);
			gbc_elementRadioReal.gridx = 1;
			gbc_elementRadioReal.gridy = i+2;
			this.add(radioRealButton, gbc_elementRadioReal);
			
			GridBagConstraints gbc_elementRadioNear = new GridBagConstraints();	
			gbc_elementRadioNear.anchor = GridBagConstraints.CENTER;
			gbc_elementRadioNear.insets = new Insets(0, 5, 5, 5);
			gbc_elementRadioNear.gridx = 2;
			gbc_elementRadioNear.gridy = i+2;
			this.add(radioNearButton, gbc_elementRadioNear);
			
			GridBagConstraints gbc_elementRadioArchive = new GridBagConstraints();	
			gbc_elementRadioArchive.anchor = GridBagConstraints.CENTER;
			gbc_elementRadioArchive.insets = new Insets(0, 5, 5, 5);
			gbc_elementRadioArchive.gridx = 3;
			gbc_elementRadioArchive.gridy = i+2;
			this.add(radioArchiveButton, gbc_elementRadioArchive);
		}
		
	}

	
}
