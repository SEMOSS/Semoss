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
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Vector;

import javax.swing.ButtonGroup;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.JTextPane;
import javax.swing.ListSelectionModel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ui.components.BrowserGraphPanel;
import prerna.ui.helpers.EntityFiller;
import prerna.ui.main.listener.specific.tap.SerOptBtnListener;
import prerna.ui.main.listener.specific.tap.SysSpecComboBoxListener;
import prerna.ui.swing.custom.ButtonMenuDropDown;
import prerna.util.Constants;
import prerna.util.DIHelper;


/**
 * This is the playsheet used exclusively for TAP service optimization.
 */
@SuppressWarnings("serial")
public class SerOptPlaySheet extends OptPlaySheet{
	
	private static final Logger classLogger = LogManager.getLogger(SerOptPlaySheet.class);
	
	//param panel components
	public JTextField yearField, icdSusField, mtnPctgField;
	public JTextField minBudgetField, maxBudgetField, hourlyRateField;
	
	//advanced param panel components
	public JTextField iniLearningCnstField, scdLearningTimeField, scdLearningCnstField, startingPtsField;
	public JTextField attRateField, hireRateField, infRateField, disRateField;
	//advanced param panel components to select enterprise wide view or system specific view
	public ButtonMenuDropDown sysSelect;
	public JComboBox<String> sysSpecComboBox;

	// param panel components to select the type of optimization to run
	public JRadioButton rdbtnBreakeven, rdbtnProfit, rdbtnROI;
	
	//display components - overview tab showing high level metrics after algorithm is run
	public JLabel costLbl, recoupLbl, savingLbl, roiLbl, bkevenLbl;
	//display components - overview tab showing graphs after algorithm is run
	public BrowserGraphPanel tab1, tab2, tab3, tab4, tab5, tab6;
	
	//display components - timeline tab shows timeline of service SDLC
	public JPanel timelinePanel;
	public BrowserGraphPanel timeline;
	
	//display components - tabs for service view and system view
	public JPanel specificAlysPanel, specificSysAlysPanel;
	//display components - tab for graph of services developed
	public JPanel playSheetPanel = new JPanel();
	//display components - tab for help text surrounding the algorithm
	public JTextPane helpTextArea;
	
	
	@Override
	protected void createBasicParamComponents() {
		
		super.createBasicParamComponents();
		
		yearField = addNewButtonToCtrlPanel("10", "Maximum Number of Years", 4, 1, 1);
		mtnPctgField = addNewButtonToCtrlPanel("10", "Annual Service Sustainment Percentage (%)", 4, 1,2);
		icdSusField = addNewButtonToCtrlPanel(".1", "Annual Interface Sustainment Cost ($M)", 4, 1, 3);
		minBudgetField = addNewButtonToCtrlPanel("0", "Minimum Annual Budget ($M)", 1, 6, 1);
		maxBudgetField = addNewButtonToCtrlPanel("1000", "Maximum Annual Budget ($M)", 1, 6, 2);
		hourlyRateField = addNewButtonToCtrlPanel("150", "Hourly Build Cost Rate ($)", 1, 6, 3);

	}
	
	
	@Override
	protected void createOptimizationComponents()
	{
		super.createOptimizationComponents();
		
		rdbtnProfit = addOptimizationTypeButton("Savings", 1, 4);
		rdbtnROI = addOptimizationTypeButton("ROI", 3, 4); 
		rdbtnBreakeven = addOptimizationTypeButton("Recoup Period", 5, 4);
		
		ButtonGroup btnGrp = new ButtonGroup();
		btnGrp.add(rdbtnProfit);
		btnGrp.add(rdbtnROI);
		btnGrp.add(rdbtnBreakeven);
		btnGrp.setSelected(rdbtnProfit.getModel(), true);		
	}
	
	@Override
	protected void addOptimizationBtnListener(JButton btnRunOptimization) {
		SerOptBtnListener obl = new SerOptBtnListener();
		obl.setOptPlaySheet(this);
		btnRunOptimization.addActionListener(obl);
	}
	
	@Override
	protected void createAdvParamPanels() {
		super.createAdvParamPanels();
		
		attRateField = addNewButtonToAdvParamPanel("3", "Attrition Rate (%)", 1, 0, 1);
		hireRateField = addNewButtonToAdvParamPanel("3", "Hiring Rate (%)", 1, 0, 2);
		infRateField = addNewButtonToAdvParamPanel("1.5", "Inflation Rate (%)", 1, 0, 3);
		disRateField = addNewButtonToAdvParamPanel("2.5", "Discount Rate (%)", 1, 0, 4);
		iniLearningCnstField = addNewButtonToAdvParamPanel("0", "Experience Level (%) at year 0", 3, 2, 1);
		scdLearningCnstField = addNewButtonToAdvParamPanel("0.9", "Experience Level (%) at year", 2, 2, 2);
		startingPtsField = addNewButtonToAdvParamPanel("5", "Number of Starting Points", 3, 2, 3);

		scdLearningTimeField = new JTextField();
		scdLearningTimeField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_scdLearningTimeField = new GridBagConstraints();
		gbc_scdLearningTimeField.anchor = GridBagConstraints.WEST;
		gbc_scdLearningTimeField.insets = new Insets(0, 0, 5, 0);
		gbc_scdLearningTimeField.gridx = 5;
		gbc_scdLearningTimeField.gridy = 2;
		advParamPanel.add(scdLearningTimeField, gbc_scdLearningTimeField);
		scdLearningTimeField.setText("5");
		scdLearningTimeField.setColumns(3);


		sysSpecComboBox = new JComboBox<String>();
		DefaultComboBoxModel<String> sysSpecComboBoxModel = new DefaultComboBoxModel<String>(new String[] {"Choose Optimization Option:", "Enterprise(Default)", "System Specific"});
		sysSpecComboBox.setModel(sysSpecComboBoxModel);
		GridBagConstraints gbc_sysSpecComboBox = new GridBagConstraints();
		gbc_sysSpecComboBox.gridwidth = 4;
		gbc_sysSpecComboBox.insets = new Insets(0, 0, 0, 5);
		gbc_sysSpecComboBox.fill = GridBagConstraints.HORIZONTAL;
		gbc_sysSpecComboBox.gridx = 0;
		gbc_sysSpecComboBox.gridy = 5;
		advParamPanel.add(sysSpecComboBox, gbc_sysSpecComboBox);

//		String [] fetching = {"Fetching"};
		EntityFiller filler = new EntityFiller();
		filler.engineName = "TAP_Core_Data";
		filler.type = "System";
		filler.setExternalQuery("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/System> ;}{?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>;} BIND(<http://health.mil/ontologies/Concept/SystemOwner/Central> AS ?central){?entity ?OwnedBy ?central}}");
		filler.run();
		Vector<String> names = filler.nameVector;
		String[] listArray=new String[names.size()];
		for (int i = 0;i<names.size();i++)
		{
			listArray[i]=(String) names.get(i);
		}
		sysSelect = new ButtonMenuDropDown("Select Systems");
		sysSelect.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
		SysSpecComboBoxListener sscb = new SysSpecComboBoxListener();
		sscb.setShowItem(sysSelect);
		sysSpecComboBox.addActionListener(sscb);
		sysSelect.setVisible(false);
		GridBagConstraints gbc_sysSelect = new GridBagConstraints();
		gbc_sysSelect.anchor = GridBagConstraints.WEST;
		gbc_sysSelect.gridwidth = 2;
		gbc_sysSelect.gridx = 4;
		gbc_sysSelect.gridy = 5;
		advParamPanel.add(sysSelect, gbc_sysSelect);
		sysSelect.setupButton(listArray);	
	}
	
	@Override
	protected void createAdvParamPanelsToggleListeners() {
		
		showParamBtn.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e) {

				if(showParamBtn.isSelected())
					advParamPanel.setVisible(true);
				else
					advParamPanel.setVisible(false);
			}
		});
	}
	
	@Override
	protected void createDisplayPanel()
	{
		super.createDisplayPanel();

		savingLbl = addNewLabelToOverviewPanel("Total transition savings over time horizon:", 0, 1);
		costLbl = addNewLabelToOverviewPanel("Total SOA build cost over time horizon:\r\n", 0, 2);
		roiLbl = addNewLabelToOverviewPanel("Total ROI over time horizon:", 2, 1);
		bkevenLbl = addNewLabelToOverviewPanel("Breakeven point during time horizon:", 2, 2);
		recoupLbl = addNewLabelToOverviewPanel("Investment Recoup Time:", 4, 1);
		
		//first tab: overall systems with charts
		//top panel that has labels
		tab1 = addNewChartToOverviewPanel(0, 0);
		tab2 = addNewChartToOverviewPanel(1, 0);
		tab3 = addNewChartToOverviewPanel(0, 1);
		tab4 = addNewChartToOverviewPanel(1, 1);
		tab5 = addNewChartToOverviewPanel(0, 2);
		tab6 = addNewChartToOverviewPanel(1, 2);
		
		timelinePanel = addNewDisplayPanel("Timeline");		
		timeline = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/timeline.html");
		GridBagConstraints gbc_panel_2 = new GridBagConstraints();
		gbc_panel_2.fill = GridBagConstraints.BOTH;
		gbc_panel_2.gridx = 0;
		gbc_panel_2.gridy = 0;
		timelinePanel.add(timeline, gbc_panel_2);
		timeline.setVisible(false);
		
		specificAlysPanel = addNewDisplayPanel("Service Analysis");
		specificSysAlysPanel= addNewDisplayPanel("System Analysis");

		playSheetPanel = new JPanel();
		displayTabbedPane.addTab("Graph Representation", null, playSheetPanel,null);

		String helpNotesData = "";
		BufferedReader releaseNotesTextReader = null;
		try{
			//Here we read the help text file
			String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String  releaseNotesTextFile= workingDir + "/help/optimizationHelp.txt";
			FileReader fr = new FileReader(releaseNotesTextFile);
			releaseNotesTextReader = new BufferedReader(fr);

			helpNotesData = "<html>";
			String line = null;
			while ((line = releaseNotesTextReader.readLine()) != null)
			{
				helpNotesData = helpNotesData + line +"<br>";
			}
			helpNotesData = helpNotesData + "</body></html>";
		} catch(IOException e){
			helpNotesData = "File Load Error";
		}finally{
			try{
				if(releaseNotesTextReader!=null)
					releaseNotesTextReader.close();
			}catch(IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}

		JPanel optimizationHelpPanel = addNewDisplayPanel("Help");

		JScrollPane optimizationHelpScrollPane = new JScrollPane();
		GridBagConstraints gbc_optimizationHelpScrollPane1 = new GridBagConstraints();
		gbc_optimizationHelpScrollPane1.fill = GridBagConstraints.BOTH;
		gbc_optimizationHelpScrollPane1.gridx = 0;
		gbc_optimizationHelpScrollPane1.gridy = 0;
		optimizationHelpPanel.add(optimizationHelpScrollPane, gbc_optimizationHelpScrollPane1);
		
		helpTextArea = new JTextPane();
		helpTextArea.setEditable(false);
		optimizationHelpScrollPane.setViewportView(helpTextArea);
		helpTextArea.setContentType("text/html");
		helpTextArea.setText(helpNotesData);

	}
	
	@Override
	public void setGraphsVisible(boolean visible) {
		super.setGraphsVisible(visible);
		tab1.setVisible(visible);
		tab2.setVisible(visible);
		tab3.setVisible(visible);
		tab4.setVisible(visible);
		tab5.setVisible(visible);
		tab6.setVisible(visible);
		timeline.setVisible(visible);
	}
	
	/**
	 * Clears panels within the playsheet
	 */
	@Override
	public void clearPanels() {
		super.clearPanels();
		specificSysAlysPanel.removeAll();
		specificAlysPanel.removeAll();
		playSheetPanel.removeAll();
	}
	
	/**
	 * Sets N/A or $0 for values in optimizations. Allows for different TAP algorithms to be run as empty functions.
	 */
	@Override
	public void clearLabels() {
		super.clearLabels();
		bkevenLbl.setText("N/A");
        savingLbl.setText("$0");
		roiLbl.setText("N/A");
		recoupLbl.setText("N/A");
		costLbl.setText("$0");
	}
}
