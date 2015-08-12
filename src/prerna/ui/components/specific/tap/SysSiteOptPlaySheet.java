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
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.StringTokenizer;

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
import prerna.engine.api.IEngine;
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

	private Boolean defaultSettings;
	private String capabilityURI;
	
	private JPanel systemSelectPanel, systemModDecomSelectPanel;	
	public DHMSMSystemSelectPanel sysSelectPanel, systemModernizePanel, systemDecomissionPanel;
	private JToggleButton showSystemSelectBtn, showSystemModDecomBtn;
	
	public JTextField centralPercOfBudgetField, trainingPercField, relConvergenceField, absConvergenceField;
	public JCheckBox optimizeBudgetCheckbox, useDHMSMFuncCheckbox;
	
	private JRadioButton rdbtnProfit, rdbtnROI, rdbtnIRR;
	
	//display panel components
	public JLabel irrLbl, costLbl, timeTransitionLbl;
	
	private SysOptCheckboxListUpdater sysUpdater;
	private SysSiteOptimizer opt;
	private String costEngineName;
	private String siteEngineName;
	
	/**
	 * Method addPanel.  Creates a panel and adds the table to the panel.
	 */
	private static final Logger LOGGER = LogManager.getLogger(SysSiteOptPlaySheet.class.getName());
	
	
	public Boolean defaultSettings() {
		return defaultSettings;
	}
	
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

		ArrayList<String> sysList;
		if(capabilityURI == null || capabilityURI.isEmpty()) {sysList = new ArrayList<String>(sysUpdater.getSelectedSystemList(intDHMSM, notIntDHMSM, theater, garrison, low, high, mhsSpecific, ehrCore));
		}else {
			sysList = new ArrayList<String>(sysUpdater.getSelectedSystemListForCapability(capabilityURI,intDHMSM, notIntDHMSM, theater, garrison, low, high, mhsSpecific, ehrCore));
		}
		
		dataHash.put("systems",sysList);
		
		if (dataHash != null)
			returnHash.put("data", dataHash);
		return returnHash;
	}
	
	public void setUpOpt(IEngine costEngine, IEngine siteEngine, double yearBudget, int years, double infl, double disc, double trainingRate, int hourlyRate, int numPts, boolean useDHMSMCap, String optType, String capabilityURI) {
		opt = new SysSiteOptimizer();
		opt.setEngines(engine, costEngine, siteEngine); //likely TAP Core Data and tap site
		opt.setVariables(yearBudget,years, infl/100, disc/100, 0.80, trainingRate, hourlyRate, numPts, 0.05, 0.20); //budget per year and the number of years
		opt.setUseDHMSMFunctionality(useDHMSMCap); //whether the data objects will come from the list of systems or the dhmsm provided capabilities
		opt.setOptimizationType(optType); //eventually will be savings, roi, or irr
		opt.setIsOptimizeBudget(false); //true means that we are looking for optimal budget. false means that we are running LPSolve just for the single budget input
		opt.setCapabilityURI(capabilityURI);
	}
	
	public ArrayList<Hashtable<String,String>> runDefaultOpt(Hashtable<String, Object> webDataHash) {

		//check to make sure site engine is loaded
		IEngine siteEngine = (IEngine) DIHelper.getInstance().getLocalProp(siteEngineName);
		IEngine costEngine = (IEngine) DIHelper.getInstance().getLocalProp(costEngineName);
		if(siteEngine == null || costEngine == null) {
			//TODO error here
			return new ArrayList<Hashtable<String,String>>();
		}

		ArrayList<String> sysList;
		if(capabilityURI == null || capabilityURI.isEmpty()) {
			sysList= new ArrayList<String>(sysUpdater.getSelectedSystemList(false, false, true, true, false, false, false, false));
		}else {
			sysList= new ArrayList<String>(sysUpdater.getSelectedSystemListForCapability(capabilityURI,false, false, true, true, false, false, false, false));
		}
		if(sysList.isEmpty()) {
			System.out.println("There are no systems that fit requirements.");
			return null;
		}
		
		ArrayList<String> modList= new ArrayList<String>(sysUpdater.getSelectedSystemList(true, false, false, false, true, false, false, false));
		ArrayList<String> decomList= new ArrayList<String>(sysUpdater.getSelectedSystemList(false, false, false, false, false, false, false, true));
		
		setUpOpt(costEngine, siteEngine, 1000000000, 10, 1.5/100, 2.5/100, 15/100.0, 150, 1, false, "Savings", capabilityURI);
		opt.setSysList(sysList,modList,decomList);
		opt.executeWeb();
		return opt.getSysResultList();
	}
	
	public ArrayList<Hashtable<String,String>> runOpt(Hashtable<String, Object> webDataHash) {


		//check to make sure site engine is loaded
		IEngine siteEngine = (IEngine) DIHelper.getInstance().getLocalProp(siteEngineName);
		IEngine costEngine = (IEngine) DIHelper.getInstance().getLocalProp(costEngineName);	
		if(siteEngine == null || costEngine == null) {
			//TODO error here
			return new ArrayList<Hashtable<String,String>>();
		}
		
		Gson gson = new Gson();
		//general params
		ArrayList<Hashtable<String, String>> sysHashList = gson.fromJson(gson.toJson(webDataHash.get("systemList")), new TypeToken<ArrayList<Hashtable<String, String>>>() {}.getType());
		double yearBudget = gson.fromJson(gson.toJson(webDataHash.get("maxAnnualBudget")), Double.class);
		int years = gson.fromJson(gson.toJson(webDataHash.get("maxYearValue")), Integer.class);
		Boolean useDHMSMCap = gson.fromJson(gson.toJson(webDataHash.get("dhmsmCap")), Boolean.class);
		String optType = gson.fromJson(gson.toJson(webDataHash.get("optTypes")), String.class);
		
		//advanced params
		double infl = gson.fromJson(gson.toJson(webDataHash.get("infl")), Double.class);
		double disc = gson.fromJson(gson.toJson(webDataHash.get("disc")), Double.class);
		int numPts = gson.fromJson(gson.toJson(webDataHash.get("numPts")), Integer.class);
		int hourlyRate = gson.fromJson(gson.toJson(webDataHash.get("hbc")), Integer.class);
		double trainingRate = gson.fromJson(gson.toJson(webDataHash.get("training")), Double.class);
		
		setUpOpt(costEngine, siteEngine, yearBudget, years, infl/100.0, disc/100.0, trainingRate/100.0, hourlyRate, numPts, useDHMSMCap, optType, capabilityURI);
		opt.setSysHashList(sysHashList);
		opt.executeWeb();
		return opt.getSysResultList();
	}
	
	
	@Override
	public void createData() {
		sysUpdater = new SysOptCheckboxListUpdater(engine, true, true, false, true);

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
		maxBudgetField.setText("10");
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

		disRateField = new JTextField();
		disRateField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		disRateField.setText("2.5");
		disRateField.setColumns(4);
		GridBagConstraints gbc_disRateField = new GridBagConstraints();
		gbc_disRateField.insets = new Insets(0, 0, 5, 5);
		gbc_disRateField.anchor = GridBagConstraints.WEST;
		gbc_disRateField.gridx = 2;
		gbc_disRateField.gridy = 1;
		advParamPanel.add(disRateField, gbc_disRateField);

		JLabel lblYearlyDiscountRate = new JLabel("Discount Rate (%)");
		lblYearlyDiscountRate.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblYearlyDiscountRate = new GridBagConstraints();
		gbc_lblYearlyDiscountRate.insets = new Insets(0, 0, 5, 5);
		gbc_lblYearlyDiscountRate.anchor = GridBagConstraints.WEST;
		gbc_lblYearlyDiscountRate.gridx = 3;
		gbc_lblYearlyDiscountRate.gridy = 1;
		advParamPanel.add(lblYearlyDiscountRate, gbc_lblYearlyDiscountRate);
		
		centralPercOfBudgetField = new JTextField();
		centralPercOfBudgetField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		centralPercOfBudgetField.setText("80");
		centralPercOfBudgetField.setColumns(4);
		GridBagConstraints gbc_centralPercOfBudgetField = new GridBagConstraints();
		gbc_centralPercOfBudgetField.insets = new Insets(0, 0, 5, 5);
		gbc_centralPercOfBudgetField.anchor = GridBagConstraints.WEST;
		gbc_centralPercOfBudgetField.gridx = 0;
		gbc_centralPercOfBudgetField.gridy = 2;
		advParamPanel.add(centralPercOfBudgetField, gbc_centralPercOfBudgetField);

		JLabel lblCentralPerc = new JLabel("Central Percent of Budget (%)");
		lblCentralPerc.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblCentralPerc = new GridBagConstraints();
		gbc_lblCentralPerc.insets = new Insets(0, 0, 5, 5);
		gbc_lblCentralPerc.anchor = GridBagConstraints.WEST;
		gbc_lblCentralPerc.gridx = 1;
		gbc_lblCentralPerc.gridy = 2;
		advParamPanel.add(lblCentralPerc, gbc_lblCentralPerc);

		trainingPercField = new JTextField();
		trainingPercField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_trainingPercField = new GridBagConstraints();
		gbc_trainingPercField.anchor = GridBagConstraints.WEST;
		gbc_trainingPercField.insets = new Insets(0, 0, 5, 5);
		gbc_trainingPercField.gridx = 2;
		gbc_trainingPercField.gridy = 2;
		advParamPanel.add(trainingPercField, gbc_trainingPercField);
		trainingPercField.setText("15");
		trainingPercField.setColumns(4);

		JLabel lblTrainingPercField = new JLabel("Training Factor (%)");
		lblTrainingPercField.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblTrainingPercField = new GridBagConstraints();
		gbc_lblTrainingPercField.insets = new Insets(0, 0, 5, 5);
		gbc_lblTrainingPercField.anchor = GridBagConstraints.WEST;
		gbc_lblTrainingPercField.gridx = 3;
		gbc_lblTrainingPercField.gridy = 2;
		advParamPanel.add(lblTrainingPercField, gbc_lblTrainingPercField);
		
		hourlyRateField = new JTextField();
		hourlyRateField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		hourlyRateField.setText("150");
		hourlyRateField.setColumns(4);
		GridBagConstraints gbc_hourlyRateField = new GridBagConstraints();
		gbc_hourlyRateField.insets = new Insets(0, 0, 5, 5);
		gbc_hourlyRateField.anchor = GridBagConstraints.WEST;
		gbc_hourlyRateField.gridx = 0;
		gbc_hourlyRateField.gridy = 3;
		advParamPanel.add(hourlyRateField, gbc_hourlyRateField);

		JLabel lblHourlyRate = new JLabel("Hourly Build Cost Rate ($)");
		lblHourlyRate.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblHourlyRate = new GridBagConstraints();
		gbc_lblHourlyRate.insets = new Insets(0, 0, 5, 5);
		gbc_lblHourlyRate.anchor = GridBagConstraints.WEST;
		gbc_lblHourlyRate.gridx = 1;
		gbc_lblHourlyRate.gridy = 3;
		advParamPanel.add(lblHourlyRate, gbc_lblHourlyRate);

		relConvergenceField = new JTextField();
		relConvergenceField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		relConvergenceField.setText("5");
		relConvergenceField.setColumns(4);
		GridBagConstraints gbc_relConvergenceField = new GridBagConstraints();
		gbc_relConvergenceField.insets = new Insets(0, 0, 5, 5);
		gbc_relConvergenceField.anchor = GridBagConstraints.WEST;
		gbc_relConvergenceField.gridx = 0;
		gbc_relConvergenceField.gridy = 4;
		advParamPanel.add(relConvergenceField, gbc_relConvergenceField);

		JLabel lblRelConvergence = new JLabel("Relative Convergence (%)");
		lblRelConvergence.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblRelConvergence = new GridBagConstraints();
		gbc_lblRelConvergence.insets = new Insets(0, 0, 5, 5);
		gbc_lblRelConvergence.anchor = GridBagConstraints.WEST;
		gbc_lblRelConvergence.gridx = 1;
		gbc_lblRelConvergence.gridy = 4;
		advParamPanel.add(lblRelConvergence, gbc_lblRelConvergence);

		absConvergenceField = new JTextField();
		absConvergenceField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		absConvergenceField.setText("20");
		absConvergenceField.setColumns(4);
		GridBagConstraints gbc_absConvergenceField = new GridBagConstraints();
		gbc_absConvergenceField.anchor = GridBagConstraints.WEST;
		gbc_absConvergenceField.insets = new Insets(0, 0, 5, 5);
		gbc_absConvergenceField.gridx = 2;
		gbc_absConvergenceField.gridy = 4;
		advParamPanel.add(absConvergenceField, gbc_absConvergenceField);

		JLabel lblAbsConvergence = new JLabel("Absolute Convergence (% of total budget)");
		lblAbsConvergence.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblAbsConvergence = new GridBagConstraints();
		gbc_lblAbsConvergence.insets = new Insets(0, 0, 5, 5);
		gbc_lblAbsConvergence.anchor = GridBagConstraints.WEST;
		gbc_lblAbsConvergence.gridx = 3;
		gbc_lblAbsConvergence.gridy = 4;
		advParamPanel.add(lblAbsConvergence, gbc_lblAbsConvergence);

		startingPtsField = new JTextField();
		startingPtsField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		startingPtsField.setText("1");
		startingPtsField.setColumns(4);
		GridBagConstraints gbc_startingPtsField = new GridBagConstraints();
		gbc_startingPtsField.insets = new Insets(0, 0, 5, 5);
		gbc_startingPtsField.anchor = GridBagConstraints.WEST;
		gbc_startingPtsField.gridx = 0;
		gbc_startingPtsField.gridy = 5;
		advParamPanel.add(startingPtsField, gbc_startingPtsField);

		JLabel lblInitialYearlyBudget = new JLabel("Number of Starting Points");
		lblInitialYearlyBudget.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblInitialYearlyBudget = new GridBagConstraints();
		gbc_lblInitialYearlyBudget.insets = new Insets(0, 0, 5, 5);
		gbc_lblInitialYearlyBudget.anchor = GridBagConstraints.WEST;
		gbc_lblInitialYearlyBudget.gridx = 1;
		gbc_lblInitialYearlyBudget.gridy = 5;
		advParamPanel.add(lblInitialYearlyBudget, gbc_lblInitialYearlyBudget);
		
		useDHMSMFuncCheckbox = new JCheckBox("Use DHMSM Functionality");
		GridBagConstraints gbc_useDHMSMFuncCheckbox = new GridBagConstraints();
		gbc_useDHMSMFuncCheckbox.gridwidth = 2;
		gbc_useDHMSMFuncCheckbox.insets = new Insets(0, 0, 5, 5);
		gbc_useDHMSMFuncCheckbox.anchor = GridBagConstraints.WEST;
		gbc_useDHMSMFuncCheckbox.gridx = 0;
		gbc_useDHMSMFuncCheckbox.gridy = 6;
		advParamPanel.add(useDHMSMFuncCheckbox, gbc_useDHMSMFuncCheckbox);
		
		optimizeBudgetCheckbox = new JCheckBox("Find Optimal Budget");
		optimizeBudgetCheckbox.setSelected(false);
		GridBagConstraints gbc_optimizeBudgetCheckbox = new GridBagConstraints();
		gbc_optimizeBudgetCheckbox.gridwidth = 2;
		gbc_optimizeBudgetCheckbox.anchor = GridBagConstraints.WEST;
		gbc_optimizeBudgetCheckbox.insets = new Insets(0, 0, 5, 5);
		gbc_optimizeBudgetCheckbox.gridx = 2;
		gbc_optimizeBudgetCheckbox.gridy = 6;
		advParamPanel.add(optimizeBudgetCheckbox, gbc_optimizeBudgetCheckbox);
			
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
	
	@Override
	protected void createDisplayPanel() {
		super.createDisplayPanel();

		JLabel lblIRR = new JLabel("Internal Rate of Return:");
		GridBagConstraints gbc_lblIRR = new GridBagConstraints();
		gbc_lblIRR.anchor = GridBagConstraints.WEST;
		gbc_lblIRR.insets = new Insets(0, 30, 5, 5);
		gbc_lblIRR.gridx = 4;
		gbc_lblIRR.gridy = 1;
		panel_1.add(lblIRR, gbc_lblIRR);
		
		irrLbl = new JLabel("");
		GridBagConstraints gbc_IRRLbl = new GridBagConstraints();
		gbc_IRRLbl.anchor = GridBagConstraints.WEST;
		gbc_IRRLbl.insets = new Insets(0, 0, 5, 5);
		gbc_IRRLbl.gridx = 5;
		gbc_IRRLbl.gridy = 1;
		panel_1.add(irrLbl, gbc_IRRLbl);
		
		JLabel lblTimeSpentTransitioning = new JLabel("Number of Years for Transition:");
		GridBagConstraints gbc_lblTimeSpentTransitioning = new GridBagConstraints();
		gbc_lblTimeSpentTransitioning.anchor = GridBagConstraints.WEST;
		gbc_lblTimeSpentTransitioning.insets = new Insets(0, 0, 0, 5);
		gbc_lblTimeSpentTransitioning.gridx = 0;
		gbc_lblTimeSpentTransitioning.gridy = 2;
		panel_1.add(lblTimeSpentTransitioning, gbc_lblTimeSpentTransitioning);
		
		timeTransitionLbl = new JLabel("");
		GridBagConstraints gbc_timeTransitionLbl = new GridBagConstraints();
		gbc_timeTransitionLbl.anchor = GridBagConstraints.WEST;
		gbc_timeTransitionLbl.insets = new Insets(0, 0, 0, 5);
		gbc_timeTransitionLbl.gridx = 1;
		gbc_timeTransitionLbl.gridy = 2;
		panel_1.add(timeTransitionLbl, gbc_timeTransitionLbl);
		
		JLabel lblCost = new JLabel("Cost to Transition:");
		GridBagConstraints gbc_lblCost = new GridBagConstraints();
		gbc_lblCost.anchor = GridBagConstraints.WEST;
		gbc_lblCost.insets = new Insets(0, 30, 5, 5);
		gbc_lblCost.gridx = 4;
		gbc_lblCost.gridy = 2;
		panel_1.add(lblCost, gbc_lblCost);
		
		costLbl = new JLabel("");
		GridBagConstraints gbc_costLbl = new GridBagConstraints();
		gbc_costLbl.anchor = GridBagConstraints.WEST;
		gbc_costLbl.insets = new Insets(0, 0, 5, 5);
		gbc_costLbl.gridx = 5;
		gbc_costLbl.gridy = 2;
		panel_1.add(costLbl, gbc_costLbl);
		
	}
	
	public String getOptType() {
		if(rdbtnROI.isSelected())
			return rdbtnROI.getName();
		else if(rdbtnIRR.isSelected())
			return rdbtnIRR.getName();
		else
			return rdbtnProfit.getName();
	}

	/**
	 * Clears panels within the playsheet
	 */
	@Override
	public void clearPanels() {
		while(tabbedPane.getTabCount() > 2) {
			tabbedPane.removeTabAt(2);
		}
	}
	
	/**
	 * Sets N/A or $0 for values in optimizations. Allows for different TAP algorithms to be run as empty functions.
	 */
	@Override
	public void clearLabels() {
		super.clearLabels();
		irrLbl.setText("N/A");
		timeTransitionLbl.setText("N/A");
		costLbl.setText("$0");
	}
	
	@Override
	public void setQuery(String query) {
		LOGGER.info("New Query " + query);
		query = query.trim();
		this.query = query;
		String[] querySplit = query.split("\\+\\+\\+");
		if(querySplit[0].toLowerCase().equals("null")) {
			this.capabilityURI = "";
		}else {
			this.capabilityURI = querySplit[0];
		}
		if(querySplit[1].toLowerCase().contains("default")) {
			this.defaultSettings = true;
		}else {
			this.defaultSettings = false;
		}
		if(querySplit.length < 4) {
			this.costEngineName = "TAP_Cost_Data";
			this.siteEngineName = "TAP_Site_Data";
		}else {
			this.costEngineName = querySplit[2];
			this.siteEngineName = querySplit[3];
		}
	}
	
	/**
	 * Functions for web dashboard calls.
	 */
	public Hashtable getOverviewPageData(Hashtable webDataHash) {
		Hashtable retHash = new Hashtable();
		String type = (String) webDataHash.get("type");
        if (type.equals("info")) {
        	retHash = opt.getOverviewInfoData();
        }
        if (type.equals("map")) {
        	retHash.put("Savings", opt.getOverviewSiteSavingsMapData());
        	if(capabilityURI!=null && !capabilityURI.isEmpty()) {
        		retHash.put("CapCoverage", opt.getOverviewCapCoverageMapData());
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
	
	public Hashtable getSystemPageData(Hashtable webDataHash) {
		Hashtable retHash = new Hashtable();
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

