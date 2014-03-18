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
	public Hashtable radioIntegratedBoxHash = new Hashtable();
	public Hashtable radioHybridBoxHash = new Hashtable();
	public Hashtable radioManualBoxHash = new Hashtable();
	
	public Hashtable radioRealBoxHash = new Hashtable();
	public Hashtable radioNearBoxHash = new Hashtable();
	public Hashtable radioArchiveBoxHash = new Hashtable();
	public Hashtable radioIgnoreBoxHash = new Hashtable();
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
		
		Font f = new Font("Tahoma", Font.PLAIN, 12);
		Font newF = new Font(f.getName(), Font.BOLD, f.getSize());
		
		for (int i=0; i < dataV.size(); i++)
		{
			JLabel label= new JLabel((String)dataV.get(i));
			
			ButtonGroup dataAccessButtonGroup = new ButtonGroup();		
			JRadioButton radioIntegratedButton = new JRadioButton();//"integrated
			radioIntegratedButton.setFont(new Font("Tahoma", Font.PLAIN, 12));
			radioIntegratedButton.setSelected(true);
			JRadioButton radioHybridButton = new JRadioButton();//"hybrid
			radioHybridButton.setFont(new Font("Tahoma", Font.PLAIN, 12));
			radioHybridButton.setSelected(false);
			JRadioButton radioManualButton = new JRadioButton();//manual
			radioManualButton.setFont(new Font("Tahoma", Font.PLAIN, 12));
			radioManualButton.setSelected(false);

			ButtonGroup dataLatencyButtonGroup = new ButtonGroup();
			JRadioButton radioRealButton = new JRadioButton();//"Real Time");
			radioRealButton.setFont(new Font("Tahoma", Font.PLAIN, 12));
			radioRealButton.setSelected(true);
			JRadioButton radioNearButton = new JRadioButton();//"Near Real Time");
			radioNearButton.setFont(new Font("Tahoma", Font.PLAIN, 12));
			radioNearButton.setSelected(false);
			JRadioButton radioArchiveButton = new JRadioButton();//"Archived");
			radioArchiveButton.setFont(new Font("Tahoma", Font.PLAIN, 12));
			radioArchiveButton.setSelected(false);
			JRadioButton radioIgnoreButton = new JRadioButton();//"Ignore");
			radioIgnoreButton.setFont(new Font("Tahoma", Font.PLAIN, 12));
			radioIgnoreButton.setSelected(false);
			
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
