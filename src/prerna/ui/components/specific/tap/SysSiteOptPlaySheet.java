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
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import javax.swing.JToggleButton;
import javax.swing.border.BevelBorder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import aurelienribon.ui.css.Style;
import prerna.algorithm.impl.specific.tap.SysSiteOptimizer;
import prerna.engine.api.IDatabaseEngine;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.ui.components.BrowserGraphPanel;
import prerna.ui.main.listener.specific.tap.SysSiteOptBtnListener;
import prerna.ui.swing.custom.ToggleButton;
import prerna.util.Utility;

/**
 * This is the playsheet used exclusively for TAP system optimization at the site level.
 * Optimizes systems to find minimum future sustainment budget while still maintaining 
 * same functionality (data and business logic) at each site and in same environments
 * (theater/garrison).
 */
@SuppressWarnings("serial")
public class SysSiteOptPlaySheet extends OptPlaySheet{
	
	//param panel components
	public JTextField yearField, maxBudgetField;
	
	//advanced param panel components
	public JTextField hourlyRateField, infRateField, disRateField, centralPercOfBudgetField, trainingPercField;
	public JCheckBox useDHMSMFuncCheckbox;

	//advanced param panels
	private JPanel systemSelectPanel, systemModDecomSelectPanel;	
	public DHMSMSystemSelectPanel sysSelectPanel, systemModernizePanel, systemDecomissionPanel;
	
	// toggles to show the System Select Panel and System force Mod/Decom Panel
	private JToggleButton showSystemSelectBtn, showSystemModDecomBtn;
	
	// param panel components to select the type of optimization to run
	private JRadioButton rdbtnProfit, rdbtnROI, rdbtnIRR;
	
	//display components - overview tab showing high level metrics after algorithm is run
	public JLabel irrLbl, costLbl, timeTransitionLbl, savingLbl, roiLbl, bkevenLbl;
	//display components - overview tab showing graphs after algorithm is run
	public BrowserGraphPanel tab3, tab4, tab5;
	
	private SysOptCheckboxListUpdater sysUpdater;
	private SysSiteOptimizer opt;
	
	private String capOrBPURI;
	private String siteEngineName;
	
	private static final Logger LOGGER = LogManager.getLogger(SysSiteOptPlaySheet.class.getName());
	
	/**
	 * Creates the data needed for the system, capability, data, and blu selection/scroll lists.
	 */
	@Override
	public void createData() {
		sysUpdater = new SysOptCheckboxListUpdater(engine, true, true, false, true);
	}
	
	/**
	 *  Sets up the Basic param input on the left of the param panel
	 * 	Includes number of years for transition and savings gathering and the max annual budget
	 */
	@Override
	protected void createBasicParamComponents() {
		super.createBasicParamComponents();
		
		yearField = addNewButtonToCtrlPanel("10", "Maximum Number of Years", 4, 1, 1);
		maxBudgetField = addNewButtonToCtrlPanel("100", "Maximum Annual Budget ($M)", 4, 1, 2);
	}
	
	/**
	 * Sets up the optimization components on the bottom left of the param panel
	 * Adds a button for optimization and the listener.
	 * Adds button so user can select what to optimize: Savings, ROI, IRR, etc.
	 */
	@Override
	protected void createOptimizationComponents() {
		
		super.createOptimizationComponents();
		
		rdbtnProfit = addOptimizationTypeButton("Savings", 1, 4);
		rdbtnROI = addOptimizationTypeButton("ROI", 3, 4); 
		rdbtnIRR = addOptimizationTypeButton("IRR", 5, 4); 
			
		ButtonGroup btnGrp = new ButtonGroup();
		btnGrp.add(rdbtnProfit);
		btnGrp.add(rdbtnROI);
		btnGrp.add(rdbtnIRR);
		btnGrp.setSelected(rdbtnProfit.getModel(), true);		
		
	}
	
	/**
	 * Adds the SysSiteOptBtnListener to the run optimization button.
	 * @param btnRunOptimization
	 */
	@Override
	protected void addOptimizationBtnListener(JButton btnRunOptimization) {
		SysSiteOptBtnListener obl = new SysSiteOptBtnListener();
		obl.setOptPlaySheet(this);
		btnRunOptimization.addActionListener(obl);
	}
	
	/**
	 * Sets up the advanced panels on the right of the param panel
	 * 1) Advanced param panel: inflation rate, discount rate, hourly cost,
	 * portion of budget that is central cost, portion of budget that is training,
	 * and whether to use DHMSM functionality.
	 * 2) System select panel: select systems using checkboxes or manually.
	 * User can also limit data/blu through this.
	 * 3) Force mod/decom panel: force the algorithm to modernize or decommission specified systems.
	 */
	protected void createAdvParamPanels()
	{
		super.createAdvParamPanels();
		
		infRateField = addNewButtonToAdvParamPanel("1.5", "Inflation Rate (%)", 1, 0, 1);
		disRateField = addNewButtonToAdvParamPanel("2.5", "Discount Rate (%)", 1, 0, 2);
		hourlyRateField = addNewButtonToAdvParamPanel("150", "Hourly Build Cost Rate ($)", 1, 0, 3);
		centralPercOfBudgetField = addNewButtonToAdvParamPanel("80", "Central Percent of Budget (%)", 1,  2, 1);
		trainingPercField = addNewButtonToAdvParamPanel("15", "Training Factor (%)", 1, 2, 2);
		
		useDHMSMFuncCheckbox = new JCheckBox("Use MHS GENESIS Functionality");
		GridBagConstraints gbc_useDHMSMFuncCheckbox = new GridBagConstraints();
		gbc_useDHMSMFuncCheckbox.gridwidth = 2;
		gbc_useDHMSMFuncCheckbox.insets = new Insets(0, 0, 5, 5);
		gbc_useDHMSMFuncCheckbox.anchor = GridBagConstraints.WEST;
		gbc_useDHMSMFuncCheckbox.gridx = 0;
		gbc_useDHMSMFuncCheckbox.gridy = 6;
		advParamPanel.add(useDHMSMFuncCheckbox, gbc_useDHMSMFuncCheckbox);
			
		systemSelectPanel = new JPanel();
		systemSelectPanel.setBorder(new BevelBorder(BevelBorder.LOWERED, null, null, null, null));
		
		GridBagConstraints gbc_systemSelectPanel = new GridBagConstraints();
		gbc_systemSelectPanel.gridheight = 6;
		gbc_systemSelectPanel.fill = GridBagConstraints.BOTH;
		gbc_systemSelectPanel.gridx = 8;
		gbc_systemSelectPanel.gridy = 0;
		ctlPanel.add(systemSelectPanel, gbc_systemSelectPanel);
		
		GridBagLayout gbl_systemSelectPanel = new GridBagLayout();
		gbl_systemSelectPanel.columnWidths = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_systemSelectPanel.rowHeights = new int[] { 0, 0, 0, 0, 0, 0, 0 };
		gbl_systemSelectPanel.columnWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		gbl_systemSelectPanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		systemSelectPanel.setLayout(gbl_systemSelectPanel);
		
		sysSelectPanel = new DHMSMSystemSelectPanel(sysUpdater);
		GridBagConstraints gbc_sysSelectPanel = new GridBagConstraints();
		gbc_sysSelectPanel.gridheight = 6;
		gbc_sysSelectPanel.fill = GridBagConstraints.BOTH;
		gbc_sysSelectPanel.gridx = 0;
		gbc_sysSelectPanel.gridy = 0;
		systemSelectPanel.add(sysSelectPanel, gbc_sysSelectPanel);
		
		// instantiate systemModDecomSelectPanel, systemModernizePanel, systemDecomissionPanel
		systemModDecomSelectPanel = new JPanel();
		systemModDecomSelectPanel.setBorder(new BevelBorder(BevelBorder.LOWERED, null, null, null, null));
		systemModDecomSelectPanel.setVisible(false);
		
		GridBagConstraints gbc_systemModDecomSelectPanel = new GridBagConstraints();
		gbc_systemModDecomSelectPanel.gridheight = 6;
		gbc_systemModDecomSelectPanel.fill = GridBagConstraints.BOTH;
		gbc_systemModDecomSelectPanel.gridx = 8;
		gbc_systemModDecomSelectPanel.gridy = 0;
		ctlPanel.add(systemModDecomSelectPanel, gbc_systemModDecomSelectPanel);
		
		GridBagLayout gbl_systemModDecomSelectPanel = new GridBagLayout();
		gbl_systemModDecomSelectPanel.columnWidths = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_systemModDecomSelectPanel.rowHeights = new int[] { 0, 0, 0, 0, 0, 0, 0 };
		gbl_systemModDecomSelectPanel.columnWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		gbl_systemModDecomSelectPanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		systemModDecomSelectPanel.setLayout(gbl_systemModDecomSelectPanel);
		
		systemModernizePanel = new DHMSMSystemSelectPanel("Select Systems that MUST be modernized:",true,sysUpdater);
		GridBagConstraints gbc_systemModernizePanel = new GridBagConstraints();
		gbc_systemModernizePanel.gridheight = 6;
		gbc_systemModernizePanel.fill = GridBagConstraints.BOTH;
		gbc_systemModernizePanel.gridx = 0;
		gbc_systemModernizePanel.gridy = 0;
		systemModDecomSelectPanel.add(systemModernizePanel, gbc_systemModernizePanel);
		
		systemDecomissionPanel = new DHMSMSystemSelectPanel("Select Systems that MUST be decommissioned:",true,sysUpdater);
		GridBagConstraints gbc_systemDecomissionPanel = new GridBagConstraints();
		gbc_systemDecomissionPanel.gridheight = 6;
		gbc_systemDecomissionPanel.fill = GridBagConstraints.BOTH;
		gbc_systemDecomissionPanel.gridx = 1;
		gbc_systemDecomissionPanel.gridy = 0;
		systemModDecomSelectPanel.add(systemDecomissionPanel, gbc_systemDecomissionPanel);		
	}
	
	/**
	 * Sets up the toggles to switch between advanced parameter panels:
	 * Advanced param panel, System select panel, and Force mod/decom panel.
	 */
	@Override
	protected void createAdvParamPanelsToggles() {
		super.createAdvParamPanelsToggles();
		
		showSystemSelectBtn = new ToggleButton("Select System Functionality");
		showSystemSelectBtn.setName("showSystemSelectBtn");
		showSystemSelectBtn.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(showSystemSelectBtn, ".toggleButton");
		showSystemSelectBtn.setSelected(true);
		
		GridBagConstraints gbc_showSystemSelectBtn = new GridBagConstraints();
		gbc_showSystemSelectBtn.anchor = GridBagConstraints.NORTHWEST;
		gbc_showSystemSelectBtn.gridwidth = 2;
		gbc_showSystemSelectBtn.insets = new Insets(0, 0, 5, 5);
		gbc_showSystemSelectBtn.gridx = 6;
		gbc_showSystemSelectBtn.gridy = 1;
		ctlPanel.add(showSystemSelectBtn, gbc_showSystemSelectBtn);
				
		showSystemModDecomBtn = new ToggleButton("Manually Set Systems");
		showSystemModDecomBtn.setName("showSystemModDecomBtn");
		showSystemModDecomBtn.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(showSystemModDecomBtn, ".toggleButton");
		
		GridBagConstraints gbc_showSystemModDecomBtn = new GridBagConstraints();
		gbc_showSystemModDecomBtn.anchor = GridBagConstraints.NORTHWEST;
		gbc_showSystemModDecomBtn.gridwidth = 2;
		gbc_showSystemModDecomBtn.insets = new Insets(0, 0, 5, 5);
		gbc_showSystemModDecomBtn.gridx = 6;
		gbc_showSystemModDecomBtn.gridy = 2;
		ctlPanel.add(showSystemModDecomBtn, gbc_showSystemModDecomBtn);

		GridBagConstraints gbc_showParamBtn = new GridBagConstraints();
		gbc_showParamBtn.anchor = GridBagConstraints.NORTHWEST;
		gbc_showParamBtn.gridwidth = 2;
		gbc_showParamBtn.insets = new Insets(0, 0, 5, 5);
		gbc_showParamBtn.gridx = 6;
		gbc_showParamBtn.gridy = 3;
		ctlPanel.add(showParamBtn, gbc_showParamBtn);
	}
	
	/**
	 * Sets up the listeners to switch between advanced param panels
	 * Extensions include the logic for switching the panels.
	 */
	@Override
	protected void createAdvParamPanelsToggleListeners() {
		
		showParamBtn.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				if(showParamBtn.isSelected()) {
					showSystemSelectBtn.setSelected(false);
					showSystemModDecomBtn.setSelected(false);
					systemSelectPanel.setVisible(false);
					systemModDecomSelectPanel.setVisible(false);
					advParamPanel.setVisible(true);
				} else
					advParamPanel.setVisible(false);
			}
		});
		
		showSystemSelectBtn.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				if(showSystemSelectBtn.isSelected()) {
					showParamBtn.setSelected(false);
					showSystemModDecomBtn.setSelected(false);
					advParamPanel.setVisible(false);
					systemModDecomSelectPanel.setVisible(false);
					systemSelectPanel.setVisible(true);
				} else {
					systemSelectPanel.setVisible(false);
				}
			}
		});
		
		showSystemModDecomBtn.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e) {
				if(showSystemModDecomBtn.isSelected()) {
					showParamBtn.setSelected(false);
					showSystemSelectBtn.setSelected(false);
					advParamPanel.setVisible(false);
					systemSelectPanel.setVisible(false);
					systemModDecomSelectPanel.setVisible(true);
				} else {
					systemModDecomSelectPanel.setVisible(false);
				}
			}
		});

	}
	
	/**
	 * Sets up the display panel at the bottom of the split pane.
	 * 1) overallAlysPanel shows high level metric labels and the graphs. This includes:
	 * 		a) total savings, number of years for transition, total roi, breakeven point,
	 * 		internal rate of return, and total cost to transition
	 * 		b) graphs of investment/sustainment cost, savings, breakeven 
	 * TODO other graphs should be here too?
	 */
	@Override
	protected void createDisplayPanel() {

		super.createDisplayPanel();
		
		tab3 = addNewChartToOverviewPanel(0, 1);
		tab4 = addNewChartToOverviewPanel(1, 1);
		tab5 = addNewChartToOverviewPanel(0, 2);
		
		savingLbl = addNewLabelToOverviewPanel("Total transition savings over time horizon:", 0, 1);
		timeTransitionLbl = addNewLabelToOverviewPanel("Number of Years for Transition:", 0, 2);
		roiLbl = addNewLabelToOverviewPanel("Total ROI over time horizon:", 2, 1);
		bkevenLbl = addNewLabelToOverviewPanel("Breakeven point during time horizon:", 2, 2);
		irrLbl = addNewLabelToOverviewPanel("Internal Rate of Return:", 4, 1);
		costLbl = addNewLabelToOverviewPanel("Cost to Transition:", 4, 2);
		
	}
	
	/**
	 * Pulls list of systems based on selections made in web version.
	 * Used to create the overall list of systems to use in the algorithm
	 * Also used to create list of force modernize or force sustain.
	 * @param webDataHash	Hashtable that includes names of checkboxes and whether they are selected
	 * @return	Hashtable that includes a list of systems that meet the input conditions
	 */
	public Map getSystems(Map<String, Object> webDataHash) {
		Map returnHash = (Map) super.getDataMakerOutput();
		Map<String, List<String>> dataHash = new Hashtable<String, List<String>>();
		
		Gson gson = new Gson();
		Boolean lpi = gson.fromJson(gson.toJson(webDataHash.get("lpi")), Boolean.class);
		Boolean lpni = gson.fromJson(gson.toJson(webDataHash.get("lpni")), Boolean.class);
		Boolean high = gson.fromJson(gson.toJson(webDataHash.get("high")), Boolean.class);
		Boolean theater;
		Boolean garrison;
		Boolean mhsSpecific;
		Boolean ehrCore;
		
		if(webDataHash.get("theater") == null)
			theater = false;
		else
			theater = gson.fromJson(gson.toJson(webDataHash.get("theater")), Boolean.class);
		if(webDataHash.get("garrison") == null)
			garrison = false;
		else
			garrison = gson.fromJson(gson.toJson(webDataHash.get("garrison")), Boolean.class);
		if(webDataHash.get("mhsspecific") == null)
			mhsSpecific = false;
		else
			mhsSpecific = gson.fromJson(gson.toJson(webDataHash.get("mhsspecific")), Boolean.class);
		if(webDataHash.get("ehrcore") == null)
			ehrCore = false;
		else
			ehrCore = gson.fromJson(gson.toJson(webDataHash.get("ehrcore")), Boolean.class);

		ArrayList<String> sysList;
		if(capOrBPURI != null && !capOrBPURI.isEmpty()) {
			if(capOrBPURI.contains("Capability")) {
				sysList = new ArrayList<String>(sysUpdater.getSelectedSystemListForCapability(capOrBPURI, lpi, lpni, high, theater, garrison, mhsSpecific, ehrCore));
			}else {
				sysList = new ArrayList<String>(sysUpdater.getSelectedSystemListForBP(capOrBPURI, lpi, lpni, high, theater, garrison, mhsSpecific, ehrCore));						
			}
		} else {
			sysList = new ArrayList<String>(sysUpdater.getSelectedSystemList(lpi, lpni, high, theater, garrison, false, false, mhsSpecific, ehrCore));
		}
		
		dataHash.put("systems",sysList);
		
		if (dataHash != null)
			returnHash.put("data", dataHash);
		return returnHash;
	}
	
	/**
	 * Runs default optimization for Web version.
	 * Selected when user chooses the portfolio rationalization question
	 * and selects "Default" as the option in the parameter drop down.
	 * Default selection includes all theater and garrison systems and
	 * forced modernization of LPI systems and forced decommission of EHR core.
	 * @param webDataHash
	 * @return ArrayList with all the systems and algorithm recommendation on whether to modernize or decommission
	 */
	public List<Map<String,String>> runDefaultOpt(Map<String, Object> webDataHash) {

		//check to make sure site engine is loaded
		IDatabaseEngine siteEngine = (IDatabaseEngine) Utility.getDatabase(MasterDatabaseUtility.testDatabaseIdIfAlias(siteEngineName));
		if(siteEngine == null) {
			LOGGER.error("Missing databases. Please make sure you have: TAP_Core_Data_Data and TAP_Site_Data");
			return new ArrayList<Map<String,String>>();
		}
		
		ArrayList<String> sysList;
		if(capOrBPURI != null && !capOrBPURI.isEmpty()) {
			if(capOrBPURI.contains("Capability")) {
				sysList= new ArrayList<String>(sysUpdater.getSelectedSystemListForCapability(capOrBPURI,false, false, false, true, true, false, false));
			} else {
				sysList = new ArrayList<String>(sysUpdater.getSelectedSystemListForBP(capOrBPURI,false, false, false, true, true, false, false));						
			}
		}else {
			sysList= new ArrayList<String>(sysUpdater.getSelectedSystemList(false, false, false, true, true, false, false, false, false));
		}
		if(sysList.isEmpty()) {
			LOGGER.error("There are no systems that fit requirements.");
			return new ArrayList<Map<String,String>>();
		}
		
		ArrayList<String> modList= new ArrayList<String>(sysUpdater.getSelectedSystemList(true, false, false, false, false, false, false, true, false));
		ArrayList<String> decomList= new ArrayList<String>(sysUpdater.getSelectedSystemList(false, false, false, false, false, false, false, false, true));
		
		if(!setUpOpt(siteEngine, 1000000000, 10, 1.5/100, 2.5/100, 80.0/100.0, 15.0/100.0, 150, "Savings", false, capOrBPURI)) {
			LOGGER.error("Optimization type is not valid.");
			return new ArrayList<Map<String,String>>();
		}
		opt.setSysList(sysList,modList,decomList);
		opt.executeWeb();
		return opt.getSysResultList();
	}
	
	/**
	 * Runs customized optimization for Web version.
	 * Selected when user chooses the portfolio rationalization question
	 * and selects "Customized" as the option in the parameter drop down.
	 * Customized selection includes: choice of systems, force modernizations or decommissions,
	 * annual budget, number of years, filtered data/blu list to DHMSM functionality, type of optimization,
	 * inflation rate, discount rate, hourly cost, pct of budget for training, and number of points.
	 * @param webDataHash
	 * @return ArrayList with all the systems and algorithm recommendation on whether to modernize or decommission
	 */
	public List<Map<String,String>> runOpt(Map<String, Object> webDataHash) {
		//TODO edit what is being sent in from web to remove optimization and number of points

		//check to make sure site engine is loaded
		IDatabaseEngine siteEngine = (IDatabaseEngine) Utility.getDatabase(MasterDatabaseUtility.testDatabaseIdIfAlias(siteEngineName));
		if(siteEngine == null) {
			LOGGER.error("Missing databases. Please make sure you have: TAP_Core_Data_Data and TAP_Site_Data");
			return new ArrayList<Map<String,String>>();
		}
		
		Gson gson = new Gson();
		//general params
		List<Map<String, String>> sysHashList = gson.fromJson(gson.toJson(webDataHash.get("systemList")), new TypeToken<List<Map<String, String>>>() {}.getType());
		double yearBudget = gson.fromJson(gson.toJson(webDataHash.get("maxAnnualBudget")), Double.class);
		int years = gson.fromJson(gson.toJson(webDataHash.get("maxYearValue")), Integer.class);
		Boolean useDHMSMCap = gson.fromJson(gson.toJson(webDataHash.get("dhmsmCap")), Boolean.class);
		String optType = gson.fromJson(gson.toJson(webDataHash.get("optTypes")), String.class);

		//advanced params
		double infl = gson.fromJson(gson.toJson(webDataHash.get("infl")), Double.class);
		double disc = gson.fromJson(gson.toJson(webDataHash.get("disc")), Double.class);
		double trainingRate = gson.fromJson(gson.toJson(webDataHash.get("training")), Double.class);
		int hourlyCost = gson.fromJson(gson.toJson(webDataHash.get("hbc")), Integer.class);

		if(!setUpOpt(siteEngine, yearBudget, years, infl/100.0, disc/100.0, 80.0/100.0, trainingRate/100.0, hourlyCost, optType, useDHMSMCap, capOrBPURI)) {
			LOGGER.error("Optimization type is not valid.");
			return new ArrayList<Map<String,String>>();
		}
		
		opt.setSysHashList(sysHashList);
		opt.executeWeb();
		return opt.getSysResultList();
	}

	/**
	 * Sets values for optimizer including: engines, budget, number of years, rates,
	 * method for filtering functionality list, type of optimization, and whether to
	 * limit to a capability or business process.
	 * @param siteEngine 		Engine to pull site data from
	 * @param yearBudget		Annual budget of years to run analysis over
	 * @param years				Number of years to run analysis over
	 * @param infl				Inflation rate as a decimal 
	 * @param disc				Discount rate as a decimal 
	 * @param centralPctOfBudget	Decimal between 0 and 1 representing portion of budget that is a central cost
	 * @param trainingPctOfBudget	Decimal between 0 and 1 representing portion of budget that is a training cost
	 * @param hourlyCost		Dollar cost for 1 hour of work
	 * @param optType			String representing type of optimization to run
	 * @param useDHMSMCap		Boolean representing whether to limit the data/blu to the DHMSM capabilities data/blu
	 * @param capOrBPURI		String representing the capability or business process URI to limit to.
	 * @return					Boolean representing whether the optimizer could be set up
	 */
	private boolean setUpOpt(IDatabaseEngine siteEngine, double yearBudget, int years, double infl, double disc, double centralPctOfBudget, double trainingPctOfBudget, int hourlyCost, String optType, boolean useDHMSMCap, String capOrBPURI) {
		opt = new SysSiteOptimizer();
		if(!opt.setOptimizationType(optType)) //savings, roi, or irr
			return false;
		opt.setEngines(engine, siteEngine); //likely TAP Core Data and tap site
		opt.setVariables(yearBudget, years, infl/100, disc/100, centralPctOfBudget, trainingPctOfBudget, hourlyCost);//budget per year, the number of years, inflation rate, discount rate, percent of budget that is central cost, percent of budget that is training, and hourly cost
		opt.setUseDHMSMFunctionality(useDHMSMCap); //whether to filter the data/blu to what is provided by the list of systems or the dhmsm provided capabilities
		opt.setCapOrBPURI(capOrBPURI); //if filtering to a specific capability or business process
		return true;
	}
	
	/**
	 * TODO
	 * @return
	 */
	public String getOptType() {
		if(rdbtnROI.isSelected())
			return rdbtnROI.getName();
		else if(rdbtnIRR.isSelected())
			return rdbtnIRR.getName();
		else
			return rdbtnProfit.getName();
	}
	
	
	/**
	 * Sets graphs to be visible after algorithm is run.
	 * @param visible
	 */
	@Override
	public void setGraphsVisible(boolean visible) {
		tab3.setVisible(visible);
		tab4.setVisible(visible);
		tab5.setVisible(visible);
	}

	/**
	 * Clears panels within the playsheet
	 * Called whenever the algorithm is rerun so no previous results left over.
	 */
	@Override
	public void clearPanels() {
		while(displayTabbedPane.getTabCount() > 2) {
			displayTabbedPane.removeTabAt(2);
		}
	}
	
	/**
	 * Sets N/A or $0 for values in optimizations.
	 * Called whenever the algorithm is rerun so no previous results left over.
	 */
	@Override
	public void clearLabels() {
		super.clearLabels();
		bkevenLbl.setText("N/A");
        savingLbl.setText("$0");
		roiLbl.setText("N/A");
		irrLbl.setText("N/A");
		timeTransitionLbl.setText("N/A");
		costLbl.setText("$0");
	}
	
	/**
	 * TODO comment
	 */
	@Override
	public void setQuery(String query) {
		LOGGER.info("New Query " + query);
		query = query.trim();
		this.query = query;
		String[] querySplit = query.split("\\+\\+\\+");
		if(querySplit[0].toLowerCase().equals("null")) {
			this.capOrBPURI = "";
		}else {
			this.capOrBPURI = querySplit[0];
		}
		if(querySplit.length < 3) {
			this.siteEngineName = "TAP_Site_Data";
		}else {
			this.siteEngineName = querySplit[2];
		}
	}
	
	/**
	 * Returns display information for SEMOSS Web.
	 * Overview page data shown by default after algorithm is run
	 * 1) Info - Total savings and number of systems decommissioned
	 * 2) Map - Systems and savings at each site
	 * 3) Health Grid - all sustained and consolidated systems
	 * 4) Coverage - Data/BLU provided by all systems
	 */
	public Map getOverviewPageData(Map webDataHash) {
		Map retHash = new Hashtable();
		String type = (String) webDataHash.get("type");
        if (type.equals("info")) {
        	retHash = opt.getOverviewInfoData();
        }
        if (type.equals("map")) {
        	retHash.put("Savings", opt.getOverviewSiteSavingsMapData());
        	if(capOrBPURI!=null && !capOrBPURI.isEmpty()) {
        		retHash.put("CapCoverage", opt.getOverviewCapBPCoverageMapData());
        	}
        }
        if (type.equals("healthGrid")) {
        	retHash = opt.getHealthGrid();
        }
        if (type.equals("coverage")) {
        	retHash = opt.getSystemCoverageData("");
        }
		return retHash;
	}
	
	/**
	 * Returns display information for SEMOSS Web, specifically System page view
	 * 1) Info - System info including sustainment budget, description, ato, data, blu, interfaces, etc
	 * 2) Map - Sites system was or will be deployed to
	 * 3) Health Grid - all sustained and consolidated systems
	 * 4) Coverage - Data/BLU coverage filtered to data/blu provided by this system
	 */
	public Map getSystemPageData(Map webDataHash) {
		Map retHash = new Hashtable();
        String type = (String) webDataHash.get("type");
        String system = (String) webDataHash.get("system");
        String ind = (String) webDataHash.get("ind");
        Boolean isModernized = false;
        if (ind.equals("Recommended Sustain")) 
        	isModernized = true;        
        if (type.equals("info"))
        	retHash = opt.getSystemInfoData(system, isModernized);
        if (type.equals("map"))
        	retHash = opt.getSystemSiteMapData(system);
        if (type.equals("healthGrid"))
        	retHash = opt.getHealthGrid();
        if (type.equals("coverage"))
        	retHash = opt.getSystemCoverageData(system);
		return retHash;
	}
}

