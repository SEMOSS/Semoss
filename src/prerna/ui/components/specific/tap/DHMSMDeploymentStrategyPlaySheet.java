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

import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.JToggleButton;
import javax.swing.border.BevelBorder;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.BrowserGraphPanel;
import prerna.ui.main.listener.specific.tap.DHMSMDeploymentStrategyRestoreDefaultsListener;
import prerna.ui.main.listener.specific.tap.DHMSMDeploymentStrategyRunBtnListener;
import prerna.ui.main.listener.specific.tap.DHMSMDeploymentStrategySetRegionListener;
import prerna.ui.main.listener.specific.tap.DHMSMDeploymentStrategySysBarChartListener;
import prerna.ui.swing.custom.CustomButton;
import prerna.ui.swing.custom.ToggleButton;
import prerna.util.DIHelper;
import prerna.util.Utility;
import aurelienribon.ui.css.Style;


/**
 * This is the playsheet used exclusively for TAP service optimization.
 */
@SuppressWarnings("serial")
public class DHMSMDeploymentStrategyPlaySheet extends InputPanelPlaySheet{
	private static final Logger LOGGER = LogManager.getLogger(DHMSMDeploymentStrategyPlaySheet.class.getName());

	//components for single begin/end date of deployment
	private JPanel timePanel;
	//begin/end quarter/year fields with user entries for deployment
	private JTextField qBeginField, yBeginField, qEndField, yEndField;
	//default begin/end quarter/year for deployment.
	private int qBeginDefault, yBeginDefault, qEndDefault, yEndDefault;

	//toggle to select specific region begin/end dates
	private JToggleButton selectRegionTimesButton;

	//list of regions
	private ArrayList<String> regionOrder;
	//waves in each region and their order
	private Hashtable<String, List<String>> regionWaveHash;
	private ArrayList<String> waveOrder;
	//start and end of each wave
	private HashMap<String, String[]> waveStartEndDate;
	
	//components for specific region begin/end dates
	private JPanel regionTimePanel;
	//hashtables holding the begin/end quarters/years fields with user entries for each region.
	private Hashtable<String,JTextField> qBeginFieldHash, yBeginFieldHash, qEndFieldHash, yEndFieldHash;
	//hashtables with default begin/end quarters/years for each region.
	private Hashtable<String,Integer> qBeginDefaultHash, yBeginDefaultHash, qEndDefaultHash, yEndDefaultHash;
	private Hashtable<String,Hashtable<String,Integer>> regionStartEndDate;

	//button to restore defaults
	private JButton restoreDefaultsButton;
	
	//button to run algorithm
	private JButton runButton;
	
	//display tabs
	public JPanel siteAnalysisPanel = new JPanel();
	
	//system geospatial panel
	public JPanel sysMapPanel = new JPanel();
	public BrowserGraphPanel sysMap;
	
	//system savings bar chart panel
	public JPanel sysBarChartPanel = new JPanel();
	public DHMSMHighSystemSelectPanel systemSelectBarChartPanel;
	public JButton runSysBarChartBtn;
	public BrowserGraphPanel sysSavingsChart;

	//system savings by year
	private String[] sysSavingsHeaders;
	private ArrayList<Object[]> systemYearlySavings;
	
	private IEngine coreEngine;
	
	public DHMSMDeploymentStrategyPlaySheet(){
		super();
		overallAnalysisTitle = "System Analysis";
		titleText = "Set Deployment Time Frame";

		coreEngine = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Core_Data");

	}

	/**
	 * Sets up the Param panel at the top of the split pane
	 */
	public void createGenericParamPanel()
	{
		queryRegions();

		if(regionOrder.isEmpty()) {
			Utility.showError("Cannot find regions in TAP Site");
		}

		super.createGenericParamPanel();

		timePanel = new JPanel();
		GridBagConstraints gbc_timePanel = new GridBagConstraints();
		gbc_timePanel.gridx = 1;
		gbc_timePanel.gridy = 1;
		ctlPanel.add(timePanel, gbc_timePanel);
		GridBagLayout gbl_timePanel = new GridBagLayout();
		gbl_timePanel.columnWidths = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_timePanel.rowHeights = new int[] { 0, 0, 0, 0};
		gbl_timePanel.columnWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		gbl_timePanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 1.0, Double.MIN_VALUE };
		timePanel.setLayout(gbl_timePanel);

		regionTimePanel = new JPanel();
		regionTimePanel.setBorder(new BevelBorder(BevelBorder.LOWERED, null, null, null, null));
		regionTimePanel.setVisible(false);
		GridBagConstraints gbc_regionTimePanel = new GridBagConstraints();
		gbc_regionTimePanel.gridx = 2;
		gbc_regionTimePanel.gridy = 0;
		gbc_regionTimePanel.gridheight = 2;
		ctlPanel.add(regionTimePanel, gbc_regionTimePanel);
		GridBagLayout gbl_regionTimePanel = new GridBagLayout();
		gbl_regionTimePanel.columnWidths = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_regionTimePanel.rowHeights = new int[] { 0, 0, 0, 0, 0, 0, 0 };
		gbl_regionTimePanel.columnWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		gbl_regionTimePanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		regionTimePanel.setLayout(gbl_regionTimePanel);

		// begin deployment
		JLabel lblDeployment1 = new JLabel("Deployment of West");
		lblDeployment1.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblDeployment1 = new GridBagConstraints();
		gbc_lblDeployment1.anchor = GridBagConstraints.WEST;
		gbc_lblDeployment1.insets = new Insets(0, 0, 5, 0);
		gbc_lblDeployment1.gridx = 0;
		gbc_lblDeployment1.gridy = 1;
		timePanel.add(lblDeployment1, gbc_lblDeployment1);

		JLabel lblBeginDeployment = new JLabel("begins in ");
		lblBeginDeployment.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblBeginDeployment = new GridBagConstraints();
		gbc_lblBeginDeployment.anchor = GridBagConstraints.WEST;
		gbc_lblBeginDeployment.insets = new Insets(0, 0, 5, 10);
		gbc_lblBeginDeployment.gridx = 1;
		gbc_lblBeginDeployment.gridy = 1;
		timePanel.add(lblBeginDeployment, gbc_lblBeginDeployment);

		JLabel lblbeginQuarter = new JLabel("Q");
		lblbeginQuarter.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblbeginQuarter = new GridBagConstraints();
		gbc_lblbeginQuarter.anchor = GridBagConstraints.WEST;
		gbc_lblbeginQuarter.insets = new Insets(0, 0, 5, 0);
		gbc_lblbeginQuarter.gridx = 2;
		gbc_lblbeginQuarter.gridy = 1;
		timePanel.add(lblbeginQuarter, gbc_lblbeginQuarter);

		qBeginField = new JTextField();
		qBeginField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		qBeginField.setText("1");
		qBeginField.setColumns(1);
		qBeginField.setName("Deployment begins quarter");

		GridBagConstraints gbc_beginQuarterField = new GridBagConstraints();
		gbc_beginQuarterField.anchor = GridBagConstraints.WEST;
		gbc_beginQuarterField.insets = new Insets(0, 0, 5, 10);
		gbc_beginQuarterField.gridx = 3;
		gbc_beginQuarterField.gridy = 1;
		timePanel.add(qBeginField, gbc_beginQuarterField);

		JLabel lblBeginYear = new JLabel("FY 20");
		lblBeginYear.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblBeginYear = new GridBagConstraints();
		gbc_lblBeginYear.anchor = GridBagConstraints.WEST;
		gbc_lblBeginYear.insets = new Insets(0, 0, 5, 0);
		gbc_lblBeginYear.gridx = 4;
		gbc_lblBeginYear.gridy = 1;
		timePanel.add(lblBeginYear, gbc_lblBeginYear);

		yBeginField = new JTextField();
		yBeginField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		yBeginField.setText("15");
		yBeginField.setColumns(2);
		yBeginField.setName("Deployment begins year");

		GridBagConstraints gbc_beginYearField = new GridBagConstraints();
		gbc_beginYearField.anchor = GridBagConstraints.WEST;
		gbc_beginYearField.insets = new Insets(0, 0, 5, 10);
		gbc_beginYearField.gridx = 5;
		gbc_beginYearField.gridy = 1;
		timePanel.add(yBeginField, gbc_beginYearField);

		//end deployment
		JLabel lblDeployment2 = new JLabel("Deployment of Pacific ");
		lblDeployment2.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblDeployment2 = new GridBagConstraints();
		gbc_lblDeployment2.anchor = GridBagConstraints.WEST;
		gbc_lblDeployment2.insets = new Insets(0, 0, 5, 0);
		gbc_lblDeployment2.gridx = 0;
		gbc_lblDeployment2.gridy = 2;
		timePanel.add(lblDeployment2, gbc_lblDeployment2);

		JLabel lblEndDeployment = new JLabel("ends in ");
		lblEndDeployment.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblEndDeployment = new GridBagConstraints();
		gbc_lblEndDeployment.anchor = GridBagConstraints.WEST;
		gbc_lblEndDeployment.insets = new Insets(0, 0, 5, 10);
		gbc_lblEndDeployment.gridx = 1;
		gbc_lblEndDeployment.gridy = 2;
		timePanel.add(lblEndDeployment, gbc_lblEndDeployment);

		JLabel lblEndQuarter = new JLabel("Q");
		lblEndQuarter.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblEndQuarter = new GridBagConstraints();
		gbc_lblEndQuarter.anchor = GridBagConstraints.WEST;
		gbc_lblEndQuarter.insets = new Insets(0, 0, 5, 0);
		gbc_lblEndQuarter.gridx = 2;
		gbc_lblEndQuarter.gridy = 2;
		timePanel.add(lblEndQuarter, gbc_lblEndQuarter);

		qEndField = new JTextField();
		qEndField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		qEndField.setText("1");
		qEndField.setColumns(1);
		qEndField.setName("Deployment ends quarter");

		GridBagConstraints gbc_endQuarterField = new GridBagConstraints();
		gbc_endQuarterField.anchor = GridBagConstraints.WEST;
		gbc_endQuarterField.insets = new Insets(0, 0, 5, 10);
		gbc_endQuarterField.gridx = 3;
		gbc_endQuarterField.gridy = 2;
		timePanel.add(qEndField, gbc_endQuarterField);		

		JLabel lblEndYear = new JLabel("FY 20");
		lblEndYear.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblEndYear = new GridBagConstraints();
		gbc_lblEndYear.anchor = GridBagConstraints.WEST;
		gbc_lblEndYear.insets = new Insets(0, 0, 5, 0);
		gbc_lblEndYear.gridx = 4;
		gbc_lblEndYear.gridy = 2;
		timePanel.add(lblEndYear, gbc_lblEndYear);

		yEndField = new JTextField();
		yEndField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		yEndField.setText("21");
		yEndField.setColumns(2);
		yEndField.setName("Deployment ends year");

		GridBagConstraints gbc_endYearField = new GridBagConstraints();
		gbc_endYearField.anchor = GridBagConstraints.WEST;
		gbc_endYearField.insets = new Insets(0, 0, 5, 10);
		gbc_endYearField.gridx = 5;
		gbc_endYearField.gridy = 2;
		timePanel.add(yEndField, gbc_endYearField);

		selectRegionTimesButton = new ToggleButton("Set deployment times by region");
		selectRegionTimesButton.setName("selectRegionTimesButton");
		selectRegionTimesButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(selectRegionTimesButton,  ".toggleButton");

		GridBagConstraints gbc_selectRegionTimesButton = new GridBagConstraints();
		gbc_selectRegionTimesButton.anchor = GridBagConstraints.NORTH;
		gbc_selectRegionTimesButton.gridwidth = 6;
		gbc_selectRegionTimesButton.fill = GridBagConstraints.HORIZONTAL;
		gbc_selectRegionTimesButton.insets = new Insets(0, 0, 5, 5);
		gbc_selectRegionTimesButton.gridx = 0;
		gbc_selectRegionTimesButton.gridy = 3;
		timePanel.add(selectRegionTimesButton, gbc_selectRegionTimesButton);

		//listener to show region panel
		DHMSMDeploymentStrategySetRegionListener setRegLis = new DHMSMDeploymentStrategySetRegionListener();
		setRegLis.setPlaySheet(this);
		selectRegionTimesButton.addActionListener(setRegLis);
		
		//select by region panel
		qBeginFieldHash = new Hashtable<String,JTextField>();
		yBeginFieldHash = new Hashtable<String,JTextField>();
		qEndFieldHash = new Hashtable<String,JTextField>();
		yEndFieldHash = new Hashtable<String,JTextField>();

		//add in the regions labels and fields to the region panel
		for(String region : regionOrder) {
			addRegion(region);
		}
		
		setDefaults();
		
		//restore defaults
		restoreDefaultsButton = new CustomButton("Restore defaults");
		restoreDefaultsButton.setName("restoreDefaultsButton");
		restoreDefaultsButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(restoreDefaultsButton,  ".toggleButton");

		GridBagConstraints gbc_restoreDefaultsButton = new GridBagConstraints();
		gbc_restoreDefaultsButton.anchor = GridBagConstraints.NORTH;
		gbc_restoreDefaultsButton.gridwidth = 6;
		gbc_restoreDefaultsButton.fill = GridBagConstraints.HORIZONTAL;
		gbc_restoreDefaultsButton.insets = new Insets(0, 0, 5, 5);
		gbc_restoreDefaultsButton.gridx = 0;
		gbc_restoreDefaultsButton.gridy = 4;
		timePanel.add(restoreDefaultsButton, gbc_restoreDefaultsButton);

		//listener to show region panel
		DHMSMDeploymentStrategyRestoreDefaultsListener restoreDefaultsLis = new DHMSMDeploymentStrategyRestoreDefaultsListener();
		restoreDefaultsLis.setPlaySheet(this);
		restoreDefaultsButton.addActionListener(restoreDefaultsLis);
		
		runButton = new CustomButton("Create deployment strategy");
		runButton.setName("runButton");
		runButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(runButton,  ".createBtn");

		GridBagConstraints gbc_runButton = new GridBagConstraints();
		gbc_runButton.anchor = GridBagConstraints.NORTH;
		gbc_runButton.gridwidth = 6;
		gbc_runButton.fill = GridBagConstraints.HORIZONTAL;
		gbc_runButton.insets = new Insets(20, 0, 5, 5);
		gbc_runButton.gridx = 0;
		gbc_runButton.gridy = 5;
		timePanel.add(runButton, gbc_runButton);

		DHMSMDeploymentStrategyRunBtnListener runList = new DHMSMDeploymentStrategyRunBtnListener();
		runList.setRegionWaveHash(regionWaveHash);
		runList.setWaveOrder(waveOrder);
		runList.setWaveStartEndDate(waveStartEndDate);
		runList.setPlaySheet(this);
		runButton.addActionListener(runList);
	}
	
	@Override
	public void createGenericDisplayPanel() {
		super.createGenericDisplayPanel();
		
		siteAnalysisPanel = new JPanel();
		tabbedPane.insertTab("Site Analysis", null, siteAnalysisPanel, null,1);
		GridBagLayout gbl_siteAnalysisPanel = new GridBagLayout();
		gbl_siteAnalysisPanel.columnWidths = new int[]{0, 0};
		gbl_siteAnalysisPanel.rowHeights = new int[]{0, 0};
		gbl_siteAnalysisPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_siteAnalysisPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		siteAnalysisPanel.setLayout(gbl_siteAnalysisPanel);
		
		//map deployment panel
		sysMapPanel = new JPanel();
		tabbedPane.insertTab("Deployment Map", null, sysMapPanel, null,2);
		GridBagLayout gbl_sysMapPanel = new GridBagLayout();
		gbl_sysMapPanel.columnWidths = new int[] { 0, 0 };
		gbl_sysMapPanel.rowHeights = new int[] { 0, 0 };
		gbl_sysMapPanel.columnWeights = new double[] { 1.0, Double.MIN_VALUE };
		gbl_sysMapPanel.rowWeights = new double[] { 1.0, Double.MIN_VALUE };
		sysMapPanel.setLayout(gbl_sysMapPanel);
		
		sysMap = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/world-map-timeline.html");
		
		GridBagConstraints gbc_sysMap = new GridBagConstraints();
		gbc_sysMap.insets = new Insets(0, 0, 0, 5);
		gbc_sysMap.fill = GridBagConstraints.BOTH;
		gbc_sysMap.gridx = 0;
		gbc_sysMap.gridy = 0;
		sysMapPanel.add(sysMap, gbc_sysMap);
		
		//panel with bar chart of savings for selected systems
		sysBarChartPanel = new JPanel();
		tabbedPane.insertTab("System Bar Chart", null, sysBarChartPanel, null,3);
		GridBagLayout gbl_sysBarChartPanel = new GridBagLayout();
		gbl_sysBarChartPanel.columnWidths = new int[]{0, 0};
		gbl_sysBarChartPanel.rowHeights = new int[]{0, 0};
		gbl_sysBarChartPanel.columnWeights = new double[]{0.25,1.0, Double.MIN_VALUE};
		gbl_sysBarChartPanel.rowWeights = new double[]{0.5, 1.0, Double.MIN_VALUE};
		sysBarChartPanel.setLayout(gbl_sysBarChartPanel);
		
		//selecct the systems
		systemSelectBarChartPanel = new DHMSMHighSystemSelectPanel(null);
		systemSelectBarChartPanel.engine = coreEngine;
		systemSelectBarChartPanel.setVisible(false);
		systemSelectBarChartPanel.setHeader("Select systems to include:");
		systemSelectBarChartPanel.addElements();
		
		GridBagConstraints gbc_systemSelectBarChartPanel = new GridBagConstraints();
		gbc_systemSelectBarChartPanel.insets = new Insets(10, 10, 0, 10);
		gbc_systemSelectBarChartPanel.anchor = GridBagConstraints.WEST;
		gbc_systemSelectBarChartPanel.fill = GridBagConstraints.BOTH;
		gbc_systemSelectBarChartPanel.gridx = 0;
		gbc_systemSelectBarChartPanel.gridy = 0;
		sysBarChartPanel.add(systemSelectBarChartPanel, gbc_systemSelectBarChartPanel);

		runSysBarChartBtn = new CustomButton("Show savings for selected");
		runSysBarChartBtn.setName("runSysBarChartBtn");
		runSysBarChartBtn.setFont(new Font("Tahoma", Font.BOLD, 11));
		runSysBarChartBtn.setVisible(false);
		Style.registerTargetClassName(runSysBarChartBtn,  ".createBtn");

		GridBagConstraints gbc_runSysBarChartBtn = new GridBagConstraints();
		gbc_runSysBarChartBtn.insets = new Insets(10, 10, 10, 10);
		gbc_runSysBarChartBtn.anchor = GridBagConstraints.NORTHWEST;
		gbc_runSysBarChartBtn.gridx = 0;
		gbc_runSysBarChartBtn.gridy = 1;
		sysBarChartPanel.add(runSysBarChartBtn, gbc_runSysBarChartBtn);
		
		DHMSMDeploymentStrategySysBarChartListener sysBarChartList = new DHMSMDeploymentStrategySysBarChartListener();
		sysBarChartList.setPlaySheet(this);
		runSysBarChartBtn.addActionListener(sysBarChartList);
		
		//charts for first tab
		sysSavingsChart = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/singlechart.html");
		sysSavingsChart.setPreferredSize(new Dimension(1000, 800));
		sysSavingsChart.setMinimumSize(new Dimension(1000, 800));
		sysSavingsChart.setVisible(false);

		GridBagConstraints gbc_sysSavingsChart = new GridBagConstraints();
		gbc_sysSavingsChart.anchor = GridBagConstraints.EAST;
		gbc_sysSavingsChart.fill = GridBagConstraints.BOTH;
		gbc_sysSavingsChart.insets = new Insets(0, 0, 5, 5);
		gbc_sysSavingsChart.gridheight = 2;
		gbc_sysSavingsChart.gridx = 1;
		gbc_sysSavingsChart.gridy = 0;
		sysBarChartPanel.add(sysSavingsChart, gbc_sysSavingsChart);
		
	}

	public void showSelectRegionTimesPanel(Boolean show) {
		if(show) {
			regionTimePanel.setVisible(true);
			qBeginField.setEnabled(false);
			yBeginField.setEnabled(false);
			qEndField.setEnabled(false);
			yEndField.setEnabled(false);

		}else {
			regionTimePanel.setVisible(false);
			qBeginField.setEnabled(true);
			yBeginField.setEnabled(true);
			qEndField.setEnabled(true);
			yEndField.setEnabled(true);
		}
	}

	/**
	 * makes a list of all the regions that will need to be included.
	 * returns them in order of deployment
	 * filters out IOC.
	 */
	private void queryRegions() {
		regionOrder = DHMSMDeploymentHelper.getRegionOrder(engine, true);
		regionWaveHash = DHMSMDeploymentHelper.getWavesInRegion(engine);
		waveOrder = DHMSMDeploymentHelper.getWaveOrder(engine);
		waveStartEndDate = DHMSMDeploymentHelper.getWaveStartAndEndDate(engine);
		
		regionStartEndDate = DHMSMDeploymentHelper.getRegionStartAndEndDate(engine,regionWaveHash,waveStartEndDate);
		qBeginDefaultHash = regionStartEndDate.get(DHMSMDeploymentHelper.REGION_START_Q_KEY);
		yBeginDefaultHash = regionStartEndDate.get(DHMSMDeploymentHelper.REGION_START_Y_KEY);
		qEndDefaultHash = regionStartEndDate.get(DHMSMDeploymentHelper.REGION_END_Q_KEY);
		yEndDefaultHash = regionStartEndDate.get(DHMSMDeploymentHelper.REGION_END_Y_KEY);

		//leaving out IOC in our deployment scheduling so getting the second region
		qBeginDefault = qBeginDefaultHash.get(regionOrder.get(1));
		yBeginDefault = yBeginDefaultHash.get(regionOrder.get(1));
		qEndDefault = qEndDefaultHash.get(regionOrder.get(regionOrder.size()-1));
		yEndDefault = yEndDefaultHash.get(regionOrder.get(regionOrder.size()-1));
	
	}
	
	private void addRegion(String region) {
		int y = qBeginFieldHash.size();

		JLabel lblRegion = new JLabel("Region "+region);
		lblRegion.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblRegion = new GridBagConstraints();
		gbc_lblRegion.anchor = GridBagConstraints.WEST;
		gbc_lblRegion.insets = new Insets(0, 0, 5, 10);
		gbc_lblRegion.gridx = 0;
		gbc_lblRegion.gridy = y+1;
		regionTimePanel.add(lblRegion, gbc_lblRegion);

		JLabel lblBeginRegion = new JLabel("Begins in ");
		lblBeginRegion.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblBeginRegion = new GridBagConstraints();
		gbc_lblBeginRegion.anchor = GridBagConstraints.WEST;
		gbc_lblBeginRegion.insets = new Insets(0, 0, 5, 10);
		gbc_lblBeginRegion.gridx = 1;
		gbc_lblBeginRegion.gridy = y+1;
		regionTimePanel.add(lblBeginRegion, gbc_lblBeginRegion);

		JLabel lblbeginQuarter = new JLabel("Q");
		lblbeginQuarter.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblbeginQuarter = new GridBagConstraints();
		gbc_lblbeginQuarter.anchor = GridBagConstraints.WEST;
		gbc_lblbeginQuarter.insets = new Insets(0, 0, 5, 0);
		gbc_lblbeginQuarter.gridx = 2;
		gbc_lblbeginQuarter.gridy = y+1;
		regionTimePanel.add(lblbeginQuarter, gbc_lblbeginQuarter);

		JTextField beginQuarterField = new JTextField();
		beginQuarterField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		beginQuarterField.setColumns(1);
		beginQuarterField.setName(region+" begins quarter");

		GridBagConstraints gbc_beginQuarterField = new GridBagConstraints();
		gbc_beginQuarterField.anchor = GridBagConstraints.WEST;
		gbc_beginQuarterField.insets = new Insets(0, 0, 5, 10);
		gbc_beginQuarterField.gridx = 3;
		gbc_beginQuarterField.gridy = y+1;
		regionTimePanel.add(beginQuarterField, gbc_beginQuarterField);

		JLabel lblBeginYear = new JLabel("FY 20");
		lblBeginYear.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblBeginYear = new GridBagConstraints();
		gbc_lblBeginYear.anchor = GridBagConstraints.WEST;
		gbc_lblBeginYear.insets = new Insets(0, 0, 5, 0);
		gbc_lblBeginYear.gridx = 4;
		gbc_lblBeginYear.gridy = y+1;
		regionTimePanel.add(lblBeginYear, gbc_lblBeginYear);

		JTextField beginYearField = new JTextField();
		beginYearField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		beginYearField.setColumns(2);
		beginYearField.setName(region+" begins year");

		GridBagConstraints gbc_beginYearField = new GridBagConstraints();
		gbc_beginYearField.anchor = GridBagConstraints.WEST;
		gbc_beginYearField.insets = new Insets(0, 0, 5, 20);
		gbc_beginYearField.gridx = 5;
		gbc_beginYearField.gridy = y+1;
		regionTimePanel.add(beginYearField, gbc_beginYearField);

		//end deployment
		JLabel lblEndDeployment = new JLabel("Ends in");
		lblEndDeployment.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblEndDeployment = new GridBagConstraints();
		gbc_lblEndDeployment.anchor = GridBagConstraints.WEST;
		gbc_lblEndDeployment.insets = new Insets(0, 0, 5, 10);
		gbc_lblEndDeployment.gridx = 6;
		gbc_lblEndDeployment.gridy = y+1;
		regionTimePanel.add(lblEndDeployment, gbc_lblEndDeployment);

		JLabel lblEndQuarter = new JLabel("Q");
		lblEndQuarter.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblEndQuarter = new GridBagConstraints();
		gbc_lblEndQuarter.anchor = GridBagConstraints.WEST;
		gbc_lblEndQuarter.insets = new Insets(0, 0, 5, 0);
		gbc_lblEndQuarter.gridx = 7;
		gbc_lblEndQuarter.gridy = y+1;
		regionTimePanel.add(lblEndQuarter, gbc_lblEndQuarter);

		JTextField endQuarterField = new JTextField();
		endQuarterField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		endQuarterField.setColumns(1);
		endQuarterField.setName(region+" ends quarter");

		GridBagConstraints gbc_endQuarterField = new GridBagConstraints();
		gbc_endQuarterField.anchor = GridBagConstraints.WEST;
		gbc_endQuarterField.insets = new Insets(0, 0, 5, 10);
		gbc_endQuarterField.gridx = 8;
		gbc_endQuarterField.gridy = y+1;
		regionTimePanel.add(endQuarterField, gbc_endQuarterField);		

		JLabel lblEndYear = new JLabel("FY 20");
		lblEndYear.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblEndYear = new GridBagConstraints();
		gbc_lblEndYear.anchor = GridBagConstraints.WEST;
		gbc_lblEndYear.insets = new Insets(0, 0, 5, 0);
		gbc_lblEndYear.gridx = 9;
		gbc_lblEndYear.gridy = y+1;
		regionTimePanel.add(lblEndYear, gbc_lblEndYear);

		JTextField endYearField = new JTextField();
		endYearField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		endYearField.setColumns(2);
		endYearField.setName(region+" ends year");

		GridBagConstraints gbc_endYearField = new GridBagConstraints();
		gbc_endYearField.anchor = GridBagConstraints.WEST;
		gbc_endYearField.insets = new Insets(0, 0, 5, 10);
		gbc_endYearField.gridx = 10;
		gbc_endYearField.gridy = y+1;
		regionTimePanel.add(endYearField, gbc_endYearField);

		qBeginFieldHash.put(region,beginQuarterField);
		yBeginFieldHash.put(region,beginYearField);
		qEndFieldHash.put(region,endQuarterField);
		yEndFieldHash.put(region,endYearField);

	}

	public void setDefaults() {
		qBeginField.setText("" + qBeginDefault);
		yBeginField.setText("" + yBeginDefault);
		qEndField.setText("" + qEndDefault);
		yEndField.setText("" + yEndDefault);
		
		for(String region : regionOrder) {
			String beginQ = "" + qBeginDefaultHash.get(region);
			String beginY = "" + yBeginDefaultHash.get(region);
			String endQ = "" + qEndDefaultHash.get(region);
			String endY = "" + yEndDefaultHash.get(region);
			qBeginFieldHash.get(region).setText(beginQ);
			qEndFieldHash.get(region).setText(endQ);
			yBeginFieldHash.get(region).setText(beginY);
			yEndFieldHash.get(region).setText(endY);
	
		}
	}

	public JToggleButton getSelectRegionTimesButton() {
		return selectRegionTimesButton;
	}

	public JTextField getQBeginField() {
		return qBeginField;
	}

	public JTextField getYBeginField() {
		return yBeginField;
	}

	public JTextField getQEndField() {
		return qEndField;
	}

	public JTextField getYEndField() {
		return yEndField;
	}

	public ArrayList<String> getRegionsList() {
		return regionOrder;
	}

	public Hashtable<String,JTextField> getQBeginFieldHash() {
		return qBeginFieldHash;
	}

	public Hashtable<String,JTextField> getYBeginFieldHash() {
		return yBeginFieldHash;
	}

	public Hashtable<String,JTextField> getQEndFieldHash() {
		return qEndFieldHash;
	}

	public Hashtable<String,JTextField> getYEndFieldHash() {
		return yEndFieldHash;
	}

	public Hashtable<String,Integer> getQBeginDefaultHash() {
		return qBeginDefaultHash;
	}
	public Hashtable<String,Integer> getYBeginDefaultHash() {
		return yBeginDefaultHash;
	}
	public Hashtable<String,Integer> getQEndDefaultHash() {
		return qEndDefaultHash;
	}
	public Hashtable<String,Integer> getYEndDefaultHash() {
		return yEndDefaultHash;
	}

	public int getqBeginDefault() {
		return qBeginDefault;
	}

	public void setqBeginDefault(int qBeginDefault) {
		this.qBeginDefault = qBeginDefault;
	}

	public int getyBeginDefault() {
		return yBeginDefault;
	}

	public void setyBeginDefault(int yBeginDefault) {
		this.yBeginDefault = yBeginDefault;
	}

	public int getqEndDefault() {
		return qEndDefault;
	}

	public void setqEndDefault(int qEndDefault) {
		this.qEndDefault = qEndDefault;
	}

	public int getyEndDefault() {
		return yEndDefault;
	}

	public void setyEndDefault(int yEndDefault) {
		this.yEndDefault = yEndDefault;
	}
	
	public Hashtable<String, Integer> getqBeginDefaultHash() {
		return qBeginDefaultHash;
	}

	public void setqBeginDefaultHash(Hashtable<String, Integer> qBeginDefaultHash) {
		this.qBeginDefaultHash = qBeginDefaultHash;
	}

	public Hashtable<String, Integer> getyBeginDefaultHash() {
		return yBeginDefaultHash;
	}

	public void setyBeginDefaultHash(Hashtable<String, Integer> yBeginDefaultHash) {
		this.yBeginDefaultHash = yBeginDefaultHash;
	}

	public Hashtable<String, Integer> getqEndDefaultHash() {
		return qEndDefaultHash;
	}

	public void setqEndDefaultHash(Hashtable<String, Integer> qEndDefaultHash) {
		this.qEndDefaultHash = qEndDefaultHash;
	}

	public Hashtable<String, Integer> getyEndDefaultHash() {
		return yEndDefaultHash;
	}

	public void setyEndDefaultHash(Hashtable<String, Integer> yEndDefaultHash) {
		this.yEndDefaultHash = yEndDefaultHash;
	}

	
	public void setSystemYearlySavings(ArrayList<Object[]> systemYearlySavings) {
		this.systemYearlySavings = systemYearlySavings;
	}
	public ArrayList<Object[]> getSystemYearlySavings() {
		return systemYearlySavings;
	}
	public void setSysSavingsHeaders(String[] sysSavingsHeaders) {
		this.sysSavingsHeaders = sysSavingsHeaders;
	}
	public String[] getSysSavingsHeaders() {
		return sysSavingsHeaders;
	}
	
	@Override
	public Object getData() {
		Hashtable returnHash = (Hashtable) super.getData();
		Hashtable dataHash = new Hashtable();
		//query and set the default playsheet values
		queryRegions();
		if(regionOrder.isEmpty()) {
			Utility.showError("Cannot find regions in TAP Site");
		}
		//select by region panel
		regionTimePanel = new JPanel();
		qBeginField = new JTextField();
		yBeginField = new JTextField();
		qEndField = new JTextField();
		yEndField = new JTextField();
		qBeginFieldHash = new Hashtable<String,JTextField>();
		yBeginFieldHash = new Hashtable<String,JTextField>();
		qEndFieldHash = new Hashtable<String,JTextField>();
		yEndFieldHash = new Hashtable<String,JTextField>();
		for(String region : regionOrder) {
			addRegion(region);
		}		
		setDefaults();
		
		//send to the listener...
		DHMSMDeploymentStrategyRunBtnListener runDeploymentStrategyListener = new DHMSMDeploymentStrategyRunBtnListener();
		runDeploymentStrategyListener.setRegionWaveHash(regionWaveHash);
		runDeploymentStrategyListener.setWaveOrder(waveOrder);
		runDeploymentStrategyListener.setWaveStartEndDate(waveStartEndDate);
		runDeploymentStrategyListener.setPlaySheet(this);
		dataHash = runDeploymentStrategyListener.generateJSONData(true);
		
		if (dataHash != null)
			returnHash.put("data", dataHash);
		return returnHash;
	}
	
	@Override
	public Hashtable registerControlPanelClick(Hashtable webDataHash) {
		Hashtable returnHash = (Hashtable) super.getData();
		Hashtable dataHash = new Hashtable();
		//query and set the default playsheet values
		queryRegions();
		if(regionOrder.isEmpty()) {
			Utility.showError("Cannot find regions in TAP Site");
		}
		//select by region panel
		regionTimePanel = new JPanel();
		qBeginField = new JTextField();
		yBeginField = new JTextField();
		qEndField = new JTextField();
		yEndField = new JTextField();
		qBeginFieldHash = new Hashtable<String,JTextField>();
		yBeginFieldHash = new Hashtable<String,JTextField>();
		qEndFieldHash = new Hashtable<String,JTextField>();
		yEndFieldHash = new Hashtable<String,JTextField>();
		for(String region : regionOrder) {
			addRegion(region);
		}
		setDefaults();
		
		DHMSMDeploymentStrategyRunBtnListener runDeploymentStrategyListener = new DHMSMDeploymentStrategyRunBtnListener();
		runDeploymentStrategyListener.setWebValuesHash(webDataHash);
		runDeploymentStrategyListener.setRegionWaveHash(regionWaveHash);
		runDeploymentStrategyListener.setWaveOrder(waveOrder);
		runDeploymentStrategyListener.setWaveStartEndDate(waveStartEndDate);
		runDeploymentStrategyListener.setPlaySheet(this);
		dataHash = runDeploymentStrategyListener.generateJSONData(false);
		
		if (dataHash != null)
			returnHash.put("data", dataHash);
		return returnHash;		
	};
}
