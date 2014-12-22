/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

import prerna.rdf.engine.api.IEngine;
import prerna.ui.helpers.EntityFiller;
import prerna.ui.main.listener.specific.tap.CapCheckBoxSelectorListener;
import prerna.ui.swing.custom.SelectScrollList;
import prerna.util.DIHelper;

@SuppressWarnings("serial")
public class DHMSMCapabilitySelectPanel extends JPanel {
	public IEngine engine;
	
	public JCheckBox allCapButton, dhmsmCapButton;
	public JCheckBox hsdCapButton, hssCapButton, fhpCapButton;
	public SelectScrollList capSelectDropDown; 
	
	public DHMSMCapabilitySelectPanel()
	{
		//addElements();
	}
	
	public void addElements()
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
		
		capSelectDropDown = new SelectScrollList("Select Individual Capabilities");
		capSelectDropDown.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
		GridBagConstraints gbc_capSelectDropDown = new GridBagConstraints();
		gbc_capSelectDropDown.gridwidth = 3;
		gbc_capSelectDropDown.fill = GridBagConstraints.HORIZONTAL;
		gbc_capSelectDropDown.insets = new Insets(0, 0, 0, 5);
		gbc_capSelectDropDown.gridx = 0;
		gbc_capSelectDropDown.gridy = 3;
		this.add(capSelectDropDown.pane, gbc_capSelectDropDown);
		
		String[] capArray = makeListFromQuery("Capability","SELECT DISTINCT ?entity WHERE {{?CapabilityTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityTag>;}{?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/Capability> ;}{?CapabilityTag ?TaggedBy ?entity}}");
		capSelectDropDown.setupButton(capArray,40,120); //need to give list of all systems
		
		CapCheckBoxSelectorListener capCheckBoxListener = new CapCheckBoxSelectorListener();
		capCheckBoxListener.setEngine(engine);
		capCheckBoxListener.setScrollList(capSelectDropDown);
		capCheckBoxListener.setCheckBox(allCapButton,dhmsmCapButton, hsdCapButton,hssCapButton, fhpCapButton);
		allCapButton.addActionListener(capCheckBoxListener);
		dhmsmCapButton.addActionListener(capCheckBoxListener);
		hsdCapButton.addActionListener(capCheckBoxListener);
		hssCapButton.addActionListener(capCheckBoxListener);
		fhpCapButton.addActionListener(capCheckBoxListener);
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

	public void clearList() {
		allCapButton.setSelected(false);
		dhmsmCapButton.setSelected(false);
		hsdCapButton.setSelected(false);
		hssCapButton.setSelected(false);
		fhpCapButton.setSelected(false);
		capSelectDropDown.clearList();
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
