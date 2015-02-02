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
import prerna.ui.main.listener.specific.tap.SystemCheckBoxSelectorListener;
import prerna.ui.swing.custom.SelectScrollList;
import prerna.util.DIHelper;

@SuppressWarnings("serial")
public class DHMSMSystemSelectPanel extends JPanel {
	public IEngine engine;
	String header = "Select Systems:";
	public JCheckBox allSysCheckBox, recdSysCheckBox, intDHMSMSysCheckBox, notIntDHMSMSysCheckBox;
	public JCheckBox lowProbCheckBox, highProbCheckBox, theaterSysCheckBox, garrisonSysCheckBox;
	public SelectScrollList sysSelectDropDown;
	
	public DHMSMSystemSelectPanel()
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
		GridBagConstraints gbc_sysSelectDropDown = new GridBagConstraints();
		gbc_sysSelectDropDown.gridwidth = 5;
		gbc_sysSelectDropDown.insets = new Insets(0, 0, 0, 5);
		gbc_sysSelectDropDown.fill = GridBagConstraints.HORIZONTAL;
		gbc_sysSelectDropDown.gridx = 0;
		gbc_sysSelectDropDown.gridy = 3;
		this.add(sysSelectDropDown.pane, gbc_sysSelectDropDown);
		
		addCheckBoxes();

		String[] sysArray = makeListFromQuery("System","SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}} ");
		sysSelectDropDown.setupButton(sysArray,40,120); //need to give list of all systems
		
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
		
		recdSysCheckBox = new JCheckBox("Rec'd");
		recdSysCheckBox.setName("recdSysCheckBox");
		GridBagConstraints gbc_recdSysButton = new GridBagConstraints();
		gbc_recdSysButton.anchor = GridBagConstraints.WEST;
		gbc_recdSysButton.gridx = 1;
		gbc_recdSysButton.gridy = 1;
		this.add(recdSysCheckBox, gbc_recdSysButton);

		intDHMSMSysCheckBox = new JCheckBox("Interface");
		intDHMSMSysCheckBox.setName("intDHMSMSysCheckBox");
		GridBagConstraints gbc_intDHMSMSysButton = new GridBagConstraints();
		gbc_intDHMSMSysButton.anchor = GridBagConstraints.WEST;
		gbc_intDHMSMSysButton.gridx = 2;
		gbc_intDHMSMSysButton.gridy = 1;
		this.add(intDHMSMSysCheckBox, gbc_intDHMSMSysButton);
		
		notIntDHMSMSysCheckBox = new JCheckBox("No Interface");
		notIntDHMSMSysCheckBox.setName("notIntDHMSMSysCheckBox");
		GridBagConstraints gbc_notIntDHMSMSysButton = new GridBagConstraints();
		gbc_notIntDHMSMSysButton.anchor = GridBagConstraints.WEST;
		gbc_notIntDHMSMSysButton.gridx = 3;
		gbc_notIntDHMSMSysButton.gridy = 1;
		this.add(notIntDHMSMSysCheckBox, gbc_notIntDHMSMSysButton);
		
		lowProbCheckBox = new JCheckBox("Low");
		lowProbCheckBox.setName("lowProbCheckBox");
		GridBagConstraints gbc_lowProbButton = new GridBagConstraints();
		gbc_lowProbButton.anchor = GridBagConstraints.WEST;
		gbc_lowProbButton.gridx = 0;
		gbc_lowProbButton.gridy = 2;
		this.add(lowProbCheckBox, gbc_lowProbButton);
		
		highProbCheckBox = new JCheckBox("High");
		highProbCheckBox.setName("highProbCheckBox");
		GridBagConstraints gbc_highProbButton = new GridBagConstraints();
		gbc_highProbButton.anchor = GridBagConstraints.WEST;
		gbc_highProbButton.gridx = 1;
		gbc_highProbButton.gridy = 2;
		this.add(highProbCheckBox, gbc_highProbButton);		

		theaterSysCheckBox = new JCheckBox("Theater");
		theaterSysCheckBox.setName("theaterSysCheckBox");
		GridBagConstraints gbc_theaterSysButton = new GridBagConstraints();
		gbc_theaterSysButton.anchor = GridBagConstraints.WEST;
		gbc_theaterSysButton.gridx = 2;
		gbc_theaterSysButton.gridy = 2;
		this.add(theaterSysCheckBox, gbc_theaterSysButton);
		
		garrisonSysCheckBox = new JCheckBox("Garrison");
		garrisonSysCheckBox.setName("garrisonSysCheckBox");
		GridBagConstraints gbc_garrisonSysButton = new GridBagConstraints();
		gbc_garrisonSysButton.anchor = GridBagConstraints.WEST;
		gbc_garrisonSysButton.gridx = 3;
		gbc_garrisonSysButton.gridy = 2;
		this.add(garrisonSysCheckBox, gbc_garrisonSysButton);
	}
	protected void addListener() {
		SystemCheckBoxSelectorListener sysCheckBoxListener = new SystemCheckBoxSelectorListener(engine, sysSelectDropDown,allSysCheckBox,recdSysCheckBox, intDHMSMSysCheckBox,notIntDHMSMSysCheckBox,theaterSysCheckBox,garrisonSysCheckBox,lowProbCheckBox, highProbCheckBox);
		allSysCheckBox.addActionListener(sysCheckBoxListener);
		recdSysCheckBox.addActionListener(sysCheckBoxListener);
		intDHMSMSysCheckBox.addActionListener(sysCheckBoxListener);
		notIntDHMSMSysCheckBox.addActionListener(sysCheckBoxListener);
		theaterSysCheckBox.addActionListener(sysCheckBoxListener);
		garrisonSysCheckBox.addActionListener(sysCheckBoxListener);
		lowProbCheckBox.addActionListener(sysCheckBoxListener);
		highProbCheckBox.addActionListener(sysCheckBoxListener);
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
		recdSysCheckBox.setSelected(false);
		intDHMSMSysCheckBox.setSelected(false);
		notIntDHMSMSysCheckBox.setSelected(false);
		allSysCheckBox.setSelected(false);
		recdSysCheckBox.setSelected(false);
		intDHMSMSysCheckBox.setSelected(false);
		lowProbCheckBox.setSelected(false);
		highProbCheckBox.setSelected(false);
		theaterSysCheckBox.setSelected(false);
		garrisonSysCheckBox.setSelected(false);
		sysSelectDropDown.clearList();
	}
	
	public ArrayList<String> getSelectedSystems()
	{
		return sysSelectDropDown.getSelectedValues();
	}
}
