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
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.Hashtable;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import javax.swing.JToggleButton;
import javax.swing.border.BevelBorder;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.specific.tap.SysSiteOptimizer;
import prerna.rdf.engine.api.IEngine;
import prerna.ui.main.listener.specific.tap.OptFunctionRadioBtnListener;
import prerna.ui.main.listener.specific.tap.SysSiteOptBtnListener;
import prerna.ui.swing.custom.CustomButton;
import prerna.ui.swing.custom.ToggleButton;
import prerna.util.DIHelper;
import aurelienribon.ui.css.Style;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * The GridPlaySheet class creates the panel and table for a grid view of data from a SPARQL query.
 */
@SuppressWarnings("serial")
public class SysSiteOptPlaySheet extends OptPlaySheet{
	
	private JPanel systemSelectPanel, systemModDecomSelectPanel;
	public DHMSMSystemSelectPanel sysSelectPanel, systemModernizePanel, systemDecomissionPanel;
	private JToggleButton showSystemSelectBtn, showSystemModDecomBtn;
	
	public JTextField trainingPercField;
	public JCheckBox optimizeBudgetCheckbox, useDHMSMFuncCheckbox;
	
	private JRadioButton rdbtnProfit, rdbtnROI, rdbtnIRR;
	
	private SysOptCheckboxListUpdater sysUpdater;
	private SysSiteOptimizer opt;
	
	/**
	 * Method addPanel.  Creates a panel and adds the table to the panel.
	 */
	private static final Logger LOGGER = LogManager.getLogger(SysSiteOptPlaySheet.class.getName());
	
	public Hashtable getSystems(Hashtable<String, Object> webDataHash) {
		Hashtable returnHash = (Hashtable) super.getData();
		Hashtable dataHash = new Hashtable();
		
		Gson gson = new Gson();
		Boolean low = gson.fromJson(gson.toJson(webDataHash.get("low")), Boolean.class);
		Boolean high = gson.fromJson(gson.toJson(webDataHash.get("high")), Boolean.class);
		Boolean intDHMSM = gson.fromJson(gson.toJson(webDataHash.get("interface")), Boolean.class);
		Boolean notIntDHMSM = gson.fromJson(gson.toJson(webDataHash.get("nointerface")), Boolean.class);
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

		dataHash.put("systems",sysUpdater.getSelectedSystemList(intDHMSM, notIntDHMSM, theater, garrison, low, high, mhsSpecific, ehrCore));
		
		if (dataHash != null)
			returnHash.put("data", dataHash);
		return returnHash;
	}
	
	public Hashtable<String,Object> runOpt(Hashtable<String, Object> webDataHash) {

		//check to make sure site engine is loaded
		IEngine siteEngine = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Site_Data");
		IEngine costEngine = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Cost_Data");
		if(siteEngine == null || costEngine == null) {
			//TODO error here
			return new Hashtable<String,Object>();
		}

		Gson gson = new Gson();
		//general params
		ArrayList<Hashtable<String, String>> sysHashList = gson.fromJson(gson.toJson(webDataHash.get("systemList")), new TypeToken<ArrayList<Hashtable<String, String>>>() {}.getType());
		int yearBudget = gson.fromJson(gson.toJson(webDataHash.get("maxAnnualBudget")), Integer.class);
		int years = gson.fromJson(gson.toJson(webDataHash.get("maxYearValue")), Integer.class);
		Boolean useDHMSMCap = gson.fromJson(gson.toJson(webDataHash.get("dhmsmCap")), Boolean.class);
		String optType = gson.fromJson(gson.toJson(webDataHash.get("optTypes")), String.class);
		
		//advanced params
		double infl = gson.fromJson(gson.toJson(webDataHash.get("infl")), Double.class);
		double disc = gson.fromJson(gson.toJson(webDataHash.get("disc")), Double.class);
		int numPts = gson.fromJson(gson.toJson(webDataHash.get("numPts")), Integer.class);
		
		opt = new SysSiteOptimizer();
		opt.setEngines(engine, costEngine, siteEngine); //likely hr core and tap site
		opt.setVariables(yearBudget,years, infl, disc, 0.15, numPts); //budget per year and the number of years
		opt.setUseDHMSMFunctionality(useDHMSMCap); //whether the data objects will come from the list of systems or the dhmsm provided capabilities
		opt.setOptimizationType(optType); //eventually will be savings, roi, or irr
		opt.setIsOptimizeBudget(false); //true means that we are looking for optimal budget. false means that we are running LPSolve just for the single budget input
		opt.setSysHashList(sysHashList);
		opt.executeWeb();
		return opt.getSysCapHash();
	}
	
	@Override
	public void createData() {
		sysUpdater = new SysOptCheckboxListUpdater(engine, true, false, false, true);
		
	}
	@Override
	public void runAnalytics() {
		
	}
	
	@Override
	protected void createBasicParamComponents() {

		yearField = new JTextField();
		yearField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		yearField.setText("10");
		yearField.setColumns(4);

		GridBagConstraints gbc_yearField = new GridBagConstraints();
		gbc_yearField.anchor = GridBagConstraints.NORTHWEST;
		gbc_yearField.insets = new Insets(0, 0, 5, 5);
		gbc_yearField.gridx = 1;
		gbc_yearField.gridy = 1;
		ctlPanel.add(yearField, gbc_yearField);

		JLabel label = new JLabel("Maximum Number of Years");
		label.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_label = new GridBagConstraints();
		gbc_label.gridwidth = 4;
		gbc_label.anchor = GridBagConstraints.WEST;
		gbc_label.insets = new Insets(0, 0, 5, 5);
		gbc_label.gridx = 2;
		gbc_label.gridy = 1;
		ctlPanel.add(label, gbc_label);
		
		maxBudgetField = new JTextField();
		maxBudgetField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		maxBudgetField.setText("100");
		maxBudgetField.setColumns(4);
		GridBagConstraints gbc_maxBudgetField = new GridBagConstraints();
		gbc_maxBudgetField.anchor = GridBagConstraints.NORTHWEST;
		gbc_maxBudgetField.insets = new Insets(0, 0, 5, 5);
		gbc_maxBudgetField.gridx = 1;
		gbc_maxBudgetField.gridy = 2;
		ctlPanel.add(maxBudgetField, gbc_maxBudgetField);

		JLabel lblMaximumYearlyBudget = new JLabel("Maximum Annual Budget ($M)");
		lblMaximumYearlyBudget.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblMaximumYearlyBudget = new GridBagConstraints();
		gbc_lblMaximumYearlyBudget.gridwidth = 4;
		gbc_lblMaximumYearlyBudget.anchor = GridBagConstraints.WEST;
		gbc_lblMaximumYearlyBudget.insets = new Insets(0, 0, 5, 5);
		gbc_lblMaximumYearlyBudget.gridx = 2;
		gbc_lblMaximumYearlyBudget.gridy = 2;
		ctlPanel.add(lblMaximumYearlyBudget, gbc_lblMaximumYearlyBudget);

		Object hidePopupKey = new JComboBox<String>().getClientProperty("doNotCancelPopup");  
		JButton btnRunOptimization = new CustomButton("Run Optimization");
		btnRunOptimization.putClientProperty("doNotCancelPopup", hidePopupKey);

		btnRunOptimization.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnRunOptimization = new GridBagConstraints();
		gbc_btnRunOptimization.gridwidth = 4;
		gbc_btnRunOptimization.insets = new Insets(0, 0, 0, 5);
		gbc_btnRunOptimization.anchor = GridBagConstraints.WEST;
		gbc_btnRunOptimization.gridx = 1;
		gbc_btnRunOptimization.gridy = 5;
		ctlPanel.add(btnRunOptimization, gbc_btnRunOptimization);
		addOptimizationBtnListener(btnRunOptimization);
		Style.registerTargetClassName(btnRunOptimization,  ".createBtn");
	}

	
	/**
	 * Creates the user interface of the playsheet. Calls functions to create param panel and tabbed display panel Stitches
	 * the param and display panels together.
	 */
	@Override
	protected void createOptimizationTypeComponents() {
		
		rdbtnProfit = new JRadioButton("Savings");
		rdbtnProfit.setName("Savings");
		rdbtnProfit.setFont(new Font("Tahoma", Font.PLAIN, 12));
		rdbtnProfit.setSelected(true);
		GridBagConstraints gbc_rdbtnProfit = new GridBagConstraints();
		gbc_rdbtnProfit.gridwidth = 2;
		gbc_rdbtnProfit.anchor = GridBagConstraints.SOUTHWEST;
		gbc_rdbtnProfit.insets = new Insets(0, 0, 5, 5);
		gbc_rdbtnProfit.gridx = 1;
		gbc_rdbtnProfit.gridy = 4;
		ctlPanel.add(rdbtnProfit, gbc_rdbtnProfit);
		
		rdbtnROI = new JRadioButton("ROI");
		rdbtnROI.setName("ROI");
		rdbtnROI.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_rdbtnRoi = new GridBagConstraints();
		gbc_rdbtnRoi.anchor = GridBagConstraints.SOUTHWEST;
		gbc_rdbtnRoi.insets = new Insets(0, 0, 5, 5);
		gbc_rdbtnRoi.gridx = 3;
		gbc_rdbtnRoi.gridy = 4;
		ctlPanel.add(rdbtnROI, gbc_rdbtnRoi);
		
		rdbtnIRR = new JRadioButton("IRR");
		rdbtnIRR.setName("IRR");
		rdbtnIRR.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_rdbtnIRR = new GridBagConstraints();
		gbc_rdbtnIRR.anchor = GridBagConstraints.SOUTHWEST;
		gbc_rdbtnIRR.insets = new Insets(0, 0, 5, 5);
		gbc_rdbtnIRR.gridx = 5;
		gbc_rdbtnIRR.gridy = 4;
		ctlPanel.add(rdbtnIRR, gbc_rdbtnIRR);
		
		OptFunctionRadioBtnListener opl = new OptFunctionRadioBtnListener();
		rdbtnROI.addActionListener(opl);
		rdbtnProfit.addActionListener(opl);
		rdbtnIRR.addActionListener(opl);
		opl.setSerOptRadioBtn(rdbtnProfit, rdbtnROI, rdbtnIRR);
	}
	
	
	protected void createAdvParamPanels()
	{
		advParamPanel = new JPanel();
		advParamPanel.setBorder(new BevelBorder(BevelBorder.LOWERED, null, null, null, null));
		advParamPanel.setVisible(false);
		
		GridBagConstraints gbc_advParamPanel = new GridBagConstraints();
		gbc_advParamPanel.gridheight = 6;
		gbc_advParamPanel.fill = GridBagConstraints.BOTH;
		gbc_advParamPanel.gridx = 8;
		gbc_advParamPanel.gridy = 0;
		ctlPanel.add(advParamPanel, gbc_advParamPanel);
		
		GridBagLayout gbl_advParamPanel = new GridBagLayout();
		gbl_advParamPanel.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_advParamPanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0};
		gbl_advParamPanel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		gbl_advParamPanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		advParamPanel.setLayout(gbl_advParamPanel);

		JLabel lblAdvancedParameters = new JLabel("Advanced Input Parameters:");
		lblAdvancedParameters.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblAdvancedParameters = new GridBagConstraints();
		gbc_lblAdvancedParameters.gridwidth = 5;
		gbc_lblAdvancedParameters.anchor = GridBagConstraints.WEST;
		gbc_lblAdvancedParameters.insets = new Insets(10, 0, 5, 5);
		gbc_lblAdvancedParameters.gridx = 0;
		gbc_lblAdvancedParameters.gridy = 0;
		advParamPanel.add(lblAdvancedParameters, gbc_lblAdvancedParameters);

		infRateField = new JTextField();
		infRateField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		infRateField.setText("1.5");
		infRateField.setColumns(4);
		GridBagConstraints gbc_infRateField = new GridBagConstraints();
		gbc_infRateField.insets = new Insets(0, 0, 5, 5);
		gbc_infRateField.anchor = GridBagConstraints.WEST;
//		gbc_infRateField.fill = GridBagConstraints.HORIZONTAL;
		gbc_infRateField.gridx = 0;
		gbc_infRateField.gridy = 1;
		advParamPanel.add(infRateField, gbc_infRateField);

		JLabel lblYearlyInflationRate = new JLabel("Inflation Rate (%)");
		lblYearlyInflationRate.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblYearlyInflationRate = new GridBagConstraints();
		gbc_lblYearlyInflationRate.insets = new Insets(0, 0, 5, 5);
		gbc_lblYearlyInflationRate.anchor = GridBagConstraints.WEST;
		gbc_lblYearlyInflationRate.gridx = 1;
		gbc_lblYearlyInflationRate.gridy = 1;
		advParamPanel.add(lblYearlyInflationRate, gbc_lblYearlyInflationRate);

		startingPtsField = new JTextField();
		startingPtsField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_startingPtsField = new GridBagConstraints();
		gbc_startingPtsField.anchor = GridBagConstraints.WEST;
		gbc_startingPtsField.insets = new Insets(0, 0, 5, 5);
		gbc_startingPtsField.gridx = 2;
		gbc_startingPtsField.gridy = 1;
		advParamPanel.add(startingPtsField, gbc_startingPtsField);
		startingPtsField.setText("5");
		startingPtsField.setColumns(4);

		JLabel lblInitialYearlyBudget = new JLabel("Number of Starting Points");
		lblInitialYearlyBudget.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblInitialYearlyBudget = new GridBagConstraints();
//		gbc_lblInitialYearlyBudget.gridwidth = 3;
		gbc_lblInitialYearlyBudget.insets = new Insets(0, 0, 5, 5);
		gbc_lblInitialYearlyBudget.anchor = GridBagConstraints.WEST;
		gbc_lblInitialYearlyBudget.gridx = 3;
		gbc_lblInitialYearlyBudget.gridy = 1;
		advParamPanel.add(lblInitialYearlyBudget, gbc_lblInitialYearlyBudget);

		disRateField = new JTextField();
		disRateField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		disRateField.setText("2.5");
		disRateField.setColumns(4);
		GridBagConstraints gbc_disRateField = new GridBagConstraints();
		gbc_disRateField.insets = new Insets(0, 0, 5, 5);
		gbc_disRateField.anchor = GridBagConstraints.WEST;
		gbc_disRateField.gridx = 0;
		gbc_disRateField.gridy = 2;
		advParamPanel.add(disRateField, gbc_disRateField);

		JLabel lblYearlyDiscountRate = new JLabel("Discount Rate (%)");
		lblYearlyDiscountRate.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblYearlyDiscountRate = new GridBagConstraints();
		gbc_lblYearlyDiscountRate.insets = new Insets(0, 0, 5, 5);
		gbc_lblYearlyDiscountRate.anchor = GridBagConstraints.WEST;
		gbc_lblYearlyDiscountRate.gridx = 1;
		gbc_lblYearlyDiscountRate.gridy = 2;
		advParamPanel.add(lblYearlyDiscountRate, gbc_lblYearlyDiscountRate);

		trainingPercField = new JTextField();
		trainingPercField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_trainingPercField = new GridBagConstraints();
		gbc_trainingPercField.anchor = GridBagConstraints.WEST;
		gbc_trainingPercField.insets = new Insets(0, 0, 5, 5);
		gbc_trainingPercField.gridx = 2;
		gbc_trainingPercField.gridy = 2;
		advParamPanel.add(trainingPercField, gbc_trainingPercField);
		trainingPercField.setText(".15");
		trainingPercField.setColumns(4);

		JLabel lblTrainingPercField = new JLabel("Training Perc");
		lblTrainingPercField.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblTrainingPercField = new GridBagConstraints();
		gbc_lblTrainingPercField.insets = new Insets(0, 0, 5, 5);
		gbc_lblTrainingPercField.anchor = GridBagConstraints.WEST;
		gbc_lblTrainingPercField.gridx = 3;
		gbc_lblTrainingPercField.gridy = 2;
		advParamPanel.add(lblTrainingPercField, gbc_lblTrainingPercField);
		
		useDHMSMFuncCheckbox = new JCheckBox("Use DHMSM Functionality");
		GridBagConstraints gbc_useDHMSMFuncCheckbox = new GridBagConstraints();
		gbc_useDHMSMFuncCheckbox.gridwidth = 2;
		gbc_useDHMSMFuncCheckbox.insets = new Insets(0, 0, 5, 5);
		gbc_useDHMSMFuncCheckbox.anchor = GridBagConstraints.WEST;
		gbc_useDHMSMFuncCheckbox.gridx = 0;
		gbc_useDHMSMFuncCheckbox.gridy = 3;
		advParamPanel.add(useDHMSMFuncCheckbox, gbc_useDHMSMFuncCheckbox);
		
		optimizeBudgetCheckbox = new JCheckBox("Find Optimal Budget");
		optimizeBudgetCheckbox.setSelected(true);
		GridBagConstraints gbc_optimizeBudgetCheckbox = new GridBagConstraints();
		gbc_optimizeBudgetCheckbox.gridwidth = 2;
		gbc_optimizeBudgetCheckbox.anchor = GridBagConstraints.WEST;
		gbc_optimizeBudgetCheckbox.insets = new Insets(0, 0, 5, 5);
		gbc_optimizeBudgetCheckbox.gridx = 2;
		gbc_optimizeBudgetCheckbox.gridy = 3;
		advParamPanel.add(optimizeBudgetCheckbox, gbc_optimizeBudgetCheckbox);
//				
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
		
		final JComponent contentPane = (JComponent) this.getContentPane();
		contentPane.addMouseListener(new MouseAdapter() {
			
			@Override
			public void mouseClicked(MouseEvent e) {
				maybeShowPopup(e);
			}
			
			@Override
			public void mousePressed(MouseEvent e) {
				maybeShowPopup(e);
			}
			
			@Override
			public void mouseReleased(MouseEvent e) {
				maybeShowPopup(e);
			}
			
			private void maybeShowPopup(MouseEvent e) {
				if (e.isPopupTrigger()) {
				}
			}
		});
		
	}
	
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

	@Override
	protected void addOptimizationBtnListener(JButton btnRunOptimization) {
		SysSiteOptBtnListener obl = new SysSiteOptBtnListener();
		obl.setOptPlaySheet(this);
		btnRunOptimization.addActionListener(obl);
	}
	
	public String getOptType() {
		if(rdbtnROI.isSelected())
			return rdbtnROI.getName();
		else if(rdbtnIRR.isSelected())
			return rdbtnIRR.getName();
		else
			return rdbtnProfit.getName();
	}
	
	public Hashtable<String,Object> getOverviewInfoData() {
		
		return opt.getOverviewInfoData();
	}
	
	
	public Hashtable<String,Object> getOverviewCostData() {
		
		return opt.getOverviewCostData();
	}
	
	public Hashtable<String,Object> getHealthGrid(String capability) {
		return opt.getHealthGrid(capability);
	}
	
	public Hashtable<String,Object> getOverviewSiteMapData() {
		return opt.getOverviewSiteMapData();
	}
	
	public Hashtable<String,Object> getSystemInfoData(String system, Boolean isModernizedPage) {
		return opt.getSystemInfoData(system, isModernizedPage);
	}
	
	public Hashtable<String,Object> getCapabilityInfoData(String capability) {
		return opt.getCapabilityInfoData(capability);
	}
}
